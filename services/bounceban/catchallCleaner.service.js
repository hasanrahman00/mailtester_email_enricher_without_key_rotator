/**
 * Catch-All Cleaner Service
 *
 * Flow per row:
 *   1. Read the job's output CSV
 *   2. Filter rows where Status === 'catch_all'
 *   3. For each catch_all row, run the combo chain via BounceBan:
 *
 *      a) Verify original email
 *         - deliverable → set status = 'valid', done
 *         - risky       → set status = 'risky', keep email, done
 *         - undeliverable → proceed to combo chain
 *
 *      b) Combo 1: firstname.lastname@domain
 *         - deliverable → overwrite email, set status = 'valid', done
 *         - risky       → set status = 'risky', keep combo email, done
 *         - undeliverable → try combo 2
 *
 *      c) Combo 2: firstinitial+lastname@domain  (e.g. jsmith@domain)
 *         - deliverable → overwrite email, set status = 'valid', done
 *         - risky       → set status = 'risky', keep combo email, done
 *         - undeliverable → leave row unchanged (still catch_all)
 *
 * Guards:
 *   - Last name must be >= 2 chars to attempt combos (skips initials like "c", "g")
 *   - Domain blacklist: if ALL combos failed at a domain, later rows skip combos
 *     for that domain (saves BounceBan credits)
 *   - Combo that equals the original email is skipped silently
 *
 * Concurrency: ALL catch_all rows start concurrently. The serial queue in
 * bounceban.client throttles actual HTTP requests to <=100 req/s automatically.
 * Slow verifications never block other rows — they resolve in the background.
 *
 * CSV is flushed to disk every CSV_FLUSH_INTERVAL_MS to preserve partial progress.
 */

import fs from 'fs/promises';
import path from 'path';
import { verifyEmail } from './bounceban.client.js';
import { getTempRootDir, readMetadata, writeMetadata } from '../../utils/storage.js';
import { appendJobLog } from '../jobState.service.js';

// ── In-memory state per job ─────────────────────────────────────────────────

const activeCleaner = new Map();  // jobId → state

export function getCleanerState(jobId) {
  return activeCleaner.get(jobId) || null;
}

export function getAllCleanerStates() {
  const out = {};
  for (const [id, state] of activeCleaner) {
    out[id] = {
      status: state.status,
      counts: { ...state.counts },
      logs: state.logs.slice(-200),
    };
  }
  return out;
}

export function requestCleanerStop(jobId) {
  const state = activeCleaner.get(jobId);
  if (state) state.stop = true;
}

export function getCleanerLogs(jobId) {
  const state = activeCleaner.get(jobId);
  return state?.logs || [];
}

export function isCleanerRunning(jobId) {
  const state = activeCleaner.get(jobId);
  return state?.status === 'running';
}

// ── CSV helpers ─────────────────────────────────────────────────────────────

function parseCsvLine(line) {
  const values = [];
  let cur = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"' && line[i + 1] === '"') { cur += '"'; i++; }
      else if (ch === '"') { inQuotes = false; }
      else { cur += ch; }
    } else {
      if (ch === '"') { inQuotes = true; }
      else if (ch === ',') { values.push(cur); cur = ''; }
      else { cur += ch; }
    }
  }
  values.push(cur);
  return values;
}

function escapeCsv(value) {
  if (value === null || value === undefined) return '';
  const s = String(value);
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

async function loadCsv(filePath) {
  const content = await fs.readFile(filePath, 'utf-8');
  const lines = content.split('\n').filter((l) => l.trim());
  if (lines.length < 2) return { headers: [], rows: [] };
  const headers = parseCsvLine(lines[0]);
  const rows = lines.slice(1).map((line) => {
    const values = parseCsvLine(line);
    const row = {};
    headers.forEach((h, i) => { row[h] = values[i] ?? ''; });
    return row;
  });
  return { headers, rows };
}

function serializeCsv(headers, rows) {
  const headerLine = headers.map(escapeCsv).join(',');
  const bodyLines = rows.map((row) =>
    headers.map((h) => escapeCsv(row[h] ?? '')).join(','),
  );
  return [headerLine, ...bodyLines].join('\n') + '\n';
}

// ── Combo builder ────────────────────────────────────────────────────────────

const MIN_LAST_NAME_LENGTH = 2;  // skip initials like "c", "g", "p"

/**
 * Returns an ordered list of combo emails to try, excluding any that match
 * the original email. Returns empty array if name data is insufficient.
 */
function buildCombos(firstName, lastName, domain, originalEmail) {
  if (!firstName || !lastName || !domain) return [];
  if (lastName.length < MIN_LAST_NAME_LENGTH) return [];

  const orig = originalEmail.toLowerCase();
  const candidates = [
    `${firstName}.${lastName}@${domain}`,        // john.smith@domain
    `${firstName[0]}${lastName}@${domain}`,       // jsmith@domain
  ];

  // Deduplicate and remove if same as original
  const seen = new Set([orig]);
  return candidates.filter((c) => {
    const lc = c.toLowerCase();
    if (seen.has(lc)) return false;
    seen.add(lc);
    return true;
  });
}

// ── CSV flush interval ───────────────────────────────────────────────────────

const CSV_FLUSH_INTERVAL_MS = 8_000;  // write CSV to disk every 8 s

// ── Main cleaner ────────────────────────────────────────────────────────────

export async function startCatchAllCleaner(jobId) {
  if (isCleanerRunning(jobId)) {
    throw new Error('Catch-all cleaner is already running for this job');
  }

  const state = {
    status: 'running',
    stop: false,
    counts: {
      total: 0,
      deliverable: 0,   // original email confirmed deliverable
      risky: 0,         // BounceBan returned risky (original or combo)
      undeliverable: 0, // original undeliverable, no combo attempted (missing name/domain)
      comboValid: 0,    // a combo email was confirmed deliverable
      comboInvalid: 0,  // all combos were undeliverable
      comboSkipped: 0,  // domain blacklisted or last name too short
      error: 0,
      skipped: 0,       // empty email or unknown/error result
    },
    logs: [],
    startedAt: new Date().toISOString(),
  };
  activeCleaner.set(jobId, state);

  // Per-domain blacklist: added only when ALL combos failed for a row at that domain
  const failedComboDomains = new Set();

  const log = (msg) => {
    const ts = new Date().toISOString().slice(11, 23);
    const line = `[${ts}] ${msg}`;
    state.logs.push(line);
    if (state.logs.length > 500) state.logs.splice(0, state.logs.length - 500);
    appendJobLog(jobId, `[CatchAll] ${msg}`);
  };

  try {
    const root = await getTempRootDir();
    const jobDir = path.join(root, jobId);
    const metadata = await readMetadata(jobDir);
    if (!metadata) throw new Error('Job metadata not found');
    if (!metadata.outputFilename) throw new Error('Job has no output file');

    const csvPath = path.join(jobDir, metadata.outputFilename);
    const { headers, rows } = await loadCsv(csvPath);

    // Resolve column names (case-insensitive)
    const find = (candidates) => headers.find((h) => candidates.includes(h.toLowerCase().trim()));
    const statusCol    = find(['status'])                          || 'Status';
    const emailCol     = find(['email', 'e-mail'])                 || 'Email';
    const firstNameCol = find(['first name', 'firstname', 'first']) || 'First Name';
    const lastNameCol  = find(['last name', 'lastname', 'last'])   || 'Last Name';
    const notesCol     = find(['notes'])                           || 'Notes';

    // Filter catch_all rows
    const catchAllIndices = [];
    rows.forEach((row, idx) => {
      const s = (row[statusCol] || '').trim().toLowerCase().replace(/[\s-]+/g, '_');
      if (s === 'catch_all' || s === 'catchall') {
        catchAllIndices.push(idx);
      }
    });

    state.counts.total = catchAllIndices.length;
    log(`Found ${catchAllIndices.length} catch-all rows out of ${rows.length} total`);

    if (catchAllIndices.length === 0) {
      state.status = 'done';
      log('No catch-all rows to process. Done.');
      return state.counts;
    }

    // Save initial cleaner metadata
    await writeMetadata(jobDir, {
      ...metadata,
      catchAllCleaner: { status: 'running', startedAt: state.startedAt, counts: state.counts },
    });

    const ctx = { emailCol, firstNameCol, lastNameCol, statusCol, notesCol, log, state, failedComboDomains };

    // ── Launch ALL rows concurrently ─────────────────────────────────────────
    // The serial queue in bounceban.client throttles HTTP requests to <=100/s.
    // Slow verifications keep polling in the background without blocking others.
    const promises = catchAllIndices.map((rowIdx) => processSingleRow(rows, rowIdx, ctx));

    // ── Periodic CSV flush ───────────────────────────────────────────────────
    // Writes current state of rows to disk every N ms so partial progress is
    // preserved if the process crashes before all rows complete.
    const flushInterval = setInterval(async () => {
      try {
        await fs.writeFile(csvPath, serializeCsv(headers, rows), 'utf-8');
        const done = Object.values(state.counts).reduce((a, b) => a + b, 0) - state.counts.total;
        log(`Progress: ${Math.max(0, done)}/${state.counts.total} rows done`);

        const meta = await readMetadata(jobDir);
        if (meta) {
          await writeMetadata(jobDir, {
            ...meta,
            catchAllCleaner: { status: 'running', startedAt: state.startedAt, counts: { ...state.counts } },
          });
        }
      } catch (_) { /* non-fatal */ }
    }, CSV_FLUSH_INTERVAL_MS);

    await Promise.allSettled(promises);
    clearInterval(flushInterval);

    // Final CSV write
    await fs.writeFile(csvPath, serializeCsv(headers, rows), 'utf-8');

    state.status = state.stop ? 'stopped' : 'done';
    log(
      `Cleaner ${state.status}. ` +
      `Deliverable: ${state.counts.deliverable}, ` +
      `Risky: ${state.counts.risky}, ` +
      `Combo valid: ${state.counts.comboValid}, ` +
      `Combo invalid: ${state.counts.comboInvalid}, ` +
      `Combo skipped: ${state.counts.comboSkipped}, ` +
      `Undeliverable: ${state.counts.undeliverable}, ` +
      `Errors: ${state.counts.error}`
    );

    // Adjust main job status counts in metadata
    const totalUpgraded = state.counts.deliverable + state.counts.comboValid;
    const finalMeta = await readMetadata(jobDir);
    if (finalMeta) {
      if (finalMeta.progress?.statusCounts) {
        if (totalUpgraded > 0) {
          finalMeta.progress.statusCounts.catch_all = Math.max(
            0, (finalMeta.progress.statusCounts.catch_all || 0) - totalUpgraded - state.counts.risky,
          );
          finalMeta.progress.statusCounts.valid = (finalMeta.progress.statusCounts.valid || 0) + totalUpgraded;
          finalMeta.progress.statusCounts.risky = (finalMeta.progress.statusCounts.risky || 0) + state.counts.risky;
        }
      }
      await writeMetadata(jobDir, {
        ...finalMeta,
        catchAllCleaner: {
          status: state.status,
          startedAt: state.startedAt,
          completedAt: new Date().toISOString(),
          counts: { ...state.counts },
        },
      });
    }

    return state.counts;

  } catch (err) {
    state.status = 'error';
    log(`Fatal error: ${err.message}`);
    throw err;
  } finally {
    if (state.status === 'running') state.status = 'error';
  }
}

// ── Single row processor ─────────────────────────────────────────────────────

async function processSingleRow(rows, rowIdx, ctx) {
  const { emailCol, firstNameCol, lastNameCol, statusCol, notesCol, log, state, failedComboDomains } = ctx;
  const row = rows[rowIdx];
  const email = (row[emailCol] || '').trim();

  if (!email) {
    state.counts.skipped++;
    return;
  }

  if (state.stop) return;

  const domain     = (email.split('@')[1] || '').toLowerCase();
  const firstName  = (row[firstNameCol] || '').trim().toLowerCase().replace(/[^a-z]/g, '');
  const lastName   = (row[lastNameCol]  || '').trim().toLowerCase().replace(/[^a-z]/g, '');

  try {
    // ── Step 1: verify original catch_all email ──────────────────────────────
    const result = await verifyEmail(email);
    log(`[${rowIdx}] ${email} → ${result.result}`);

    if (result.result === 'deliverable') {
      row[statusCol] = 'valid';
      row[notesCol]  = 'CatchAll Cleaner: original confirmed deliverable';
      state.counts.deliverable++;
      return;
    }

    if (result.result === 'risky') {
      row[statusCol] = 'risky';
      row[notesCol]  = `CatchAll Cleaner: original returned risky (email: ${email})`;
      state.counts.risky++;
      return;
    }

    if (result.result !== 'undeliverable') {
      // unknown / error — leave as-is
      state.counts.skipped++;
      log(`[${rowIdx}] ${email} result=${result.result}, leaving unchanged`);
      return;
    }

    // ── Step 2: build combo chain ────────────────────────────────────────────
    if (!firstName || !lastName || !domain) {
      log(`[${rowIdx}] Cannot build combo — missing name or domain`);
      state.counts.undeliverable++;
      return;
    }

    if (lastName.length < MIN_LAST_NAME_LENGTH) {
      log(`[${rowIdx}] Last name "${lastName}" too short (< ${MIN_LAST_NAME_LENGTH} chars), skipping combos`);
      state.counts.comboSkipped++;
      return;
    }

    if (failedComboDomains.has(domain)) {
      log(`[${rowIdx}] Domain ${domain} already failed all combos — skipping`);
      state.counts.comboSkipped++;
      return;
    }

    const combos = buildCombos(firstName, lastName, domain, email);

    if (combos.length === 0) {
      state.counts.undeliverable++;
      return;
    }

    // Try combos one by one
    let anyComboAttempted = false;
    for (const comboEmail of combos) {
      if (state.stop) return;

      anyComboAttempted = true;
      const comboResult = await verifyEmail(comboEmail);
      log(`[${rowIdx}] combo ${comboEmail} → ${comboResult.result}`);

      if (comboResult.result === 'deliverable') {
        row[emailCol]  = comboEmail;
        row[statusCol] = 'valid';
        row[notesCol]  = `CatchAll Cleaner: combo deliverable (original: ${email})`;
        state.counts.comboValid++;
        return;
      }

      if (comboResult.result === 'risky') {
        row[emailCol]  = comboEmail;
        row[statusCol] = 'risky';
        row[notesCol]  = `CatchAll Cleaner: combo returned risky (combo: ${comboEmail}, original: ${email})`;
        state.counts.risky++;
        return;
      }

      // undeliverable / unknown / error → try next combo
    }

    // All combos exhausted — blacklist domain to skip future rows at same domain
    if (anyComboAttempted) {
      failedComboDomains.add(domain);
    }
    state.counts.comboInvalid++;

  } catch (err) {
    state.counts.error++;
    log(`[${rowIdx}] Error: ${err.message}`);
  }
}