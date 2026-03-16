/**
 * Catch-All Cleaner Service
 *
 * Flow per job:
 *   1. Read the job's output CSV
 *   2. Filter rows where Status === 'catch_all'
 *   3. For each catch_all row, send the existing email to BounceBan:
 *      a) deliverable → set status = 'valid', keep email
 *      b) undeliverable → build firstname.lastname@samedomain, verify that:
 *         - deliverable → overwrite email, set status = 'valid'
 *         - not deliverable → keep original (no change)
 *   4. Writes back to CSV immediately after each batch result
 *
 * Guards:
 *   - Last name must be ≥ 2 chars to attempt a combo (skips initials like "c", "g")
 *   - Domain blacklist: if original + combo both failed at a domain, later rows
 *     at the same domain skip the combo attempt (saves credits)
 *
 * Concurrency: processes up to CONCURRENCY emails at once (default 100)
 * to max out the 100 req/s BounceBan ceiling.
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

// ── Main cleaner ────────────────────────────────────────────────────────────

const CONCURRENCY = 100;
const MIN_LAST_NAME_LENGTH = 2;  // skip initials like "c", "g", "p"

export async function startCatchAllCleaner(jobId) {
  if (isCleanerRunning(jobId)) {
    throw new Error('Catch-all cleaner is already running for this job');
  }

  const state = {
    status: 'running',
    stop: false,
    counts: { total: 0, deliverable: 0, undeliverable: 0, comboValid: 0, comboInvalid: 0, comboSkipped: 0, error: 0, skipped: 0 },
    logs: [],
    startedAt: new Date().toISOString(),
  };
  activeCleaner.set(jobId, state);

  // Domains where both original AND combo were undeliverable — skip future combos
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
    const statusCol = find(['status']) || 'Status';
    const emailCol = find(['email', 'e-mail']) || 'Email';
    const firstNameCol = find(['first name', 'firstname', 'first']) || 'First Name';
    const lastNameCol = find(['last name', 'lastname', 'last']) || 'Last Name';
    const notesCol = find(['notes']) || 'Notes';

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

    // Save cleaner metadata
    await writeMetadata(jobDir, {
      ...metadata,
      catchAllCleaner: { status: 'running', startedAt: state.startedAt, counts: state.counts },
    });

    // Process in batches of CONCURRENCY
    for (let i = 0; i < catchAllIndices.length; i += CONCURRENCY) {
      if (state.stop) {
        log('Stop requested. Halting.');
        break;
      }

      const batch = catchAllIndices.slice(i, i + CONCURRENCY);
      const promises = batch.map((rowIdx) => processSingleRow(rows, rowIdx, {
        emailCol, firstNameCol, lastNameCol, statusCol, notesCol, log, state, failedComboDomains,
      }));

      await Promise.allSettled(promises);

      // Write CSV after each batch
      const csvContent = serializeCsv(headers, rows);
      await fs.writeFile(csvPath, csvContent, 'utf-8');

      // Update metadata
      const updatedMeta = await readMetadata(jobDir);
      if (updatedMeta) {
        await writeMetadata(jobDir, {
          ...updatedMeta,
          catchAllCleaner: { status: 'running', startedAt: state.startedAt, counts: { ...state.counts } },
        });
      }

      const processed = Math.min(i + CONCURRENCY, catchAllIndices.length);
      log(`Batch done: ${processed}/${catchAllIndices.length}`);
    }

    state.status = state.stop ? 'stopped' : 'done';
    log(`Cleaner ${state.status}. Deliverable: ${state.counts.deliverable}, Combo valid: ${state.counts.comboValid}, Combo invalid: ${state.counts.comboInvalid}, Combo skipped: ${state.counts.comboSkipped}, Errors: ${state.counts.error}`);

    // Final metadata update — adjust main progress counts
    const totalUpgraded = state.counts.deliverable + state.counts.comboValid;
    const finalMeta = await readMetadata(jobDir);
    if (finalMeta) {
      if (totalUpgraded > 0 && finalMeta.progress?.statusCounts) {
        finalMeta.progress.statusCounts.catch_all = Math.max(0, (finalMeta.progress.statusCounts.catch_all || 0) - totalUpgraded);
        finalMeta.progress.statusCounts.valid = (finalMeta.progress.statusCounts.valid || 0) + totalUpgraded;
      }
      await writeMetadata(jobDir, {
        ...finalMeta,
        catchAllCleaner: { status: state.status, startedAt: state.startedAt, completedAt: new Date().toISOString(), counts: { ...state.counts } },
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

async function processSingleRow(rows, rowIdx, ctx) {
  const { emailCol, firstNameCol, lastNameCol, statusCol, notesCol, log, state, failedComboDomains } = ctx;
  const row = rows[rowIdx];
  const email = (row[emailCol] || '').trim();

  if (!email) {
    state.counts.skipped++;
    return;
  }

  if (state.stop) return;

  const domain = (email.split('@')[1] || '').toLowerCase();

  try {
    // Step 1: verify original catch_all email via BounceBan
    const result = await verifyEmail(email);
    log(`[${rowIdx}] ${email} → ${result.result}`);

    if (result.result === 'deliverable') {
      // BounceBan confirmed this catch-all email is actually deliverable
      // Upgrade status to valid so it counts in the valid column
      row[statusCol] = 'valid';
      row[notesCol] = 'CatchAll Cleaner: original confirmed deliverable';
      state.counts.deliverable++;
      return;
    }

    if (result.result === 'undeliverable') {
      // Step 2: build firstname.lastname@samedomain
      const firstName = (row[firstNameCol] || '').trim().toLowerCase().replace(/[^a-z]/g, '');
      const lastName  = (row[lastNameCol]  || '').trim().toLowerCase().replace(/[^a-z]/g, '');

      if (!firstName || !lastName || !domain) {
        log(`[${rowIdx}] Cannot build combo — missing name or domain`);
        state.counts.undeliverable++;
        return;
      }

      // Guard: skip single-char last names (initials like "c", "g", "p")
      if (lastName.length < MIN_LAST_NAME_LENGTH) {
        log(`[${rowIdx}] Last name "${lastName}" too short (< ${MIN_LAST_NAME_LENGTH} chars), skipping combo`);
        state.counts.comboSkipped++;
        return;
      }

      // Guard: skip domains where a previous combo already failed
      if (failedComboDomains.has(domain)) {
        log(`[${rowIdx}] Domain ${domain} already failed combo — skipping`);
        state.counts.comboSkipped++;
        return;
      }

      const comboEmail = `${firstName}.${lastName}@${domain}`;

      if (comboEmail.toLowerCase() === email.toLowerCase()) {
        log(`[${rowIdx}] Combo same as original: ${comboEmail}`);
        state.counts.undeliverable++;
        return;
      }

      if (state.stop) return;

      // Step 3: verify the combo
      const comboResult = await verifyEmail(comboEmail);
      log(`[${rowIdx}] combo ${comboEmail} → ${comboResult.result}`);

      if (comboResult.result === 'deliverable') {
        row[emailCol] = comboEmail;
        row[statusCol] = 'valid';
        row[notesCol] = `CatchAll Cleaner: combo deliverable (original: ${email})`;
        state.counts.comboValid++;
      } else {
        // Both original and combo failed at this domain — blacklist it
        failedComboDomains.add(domain);
        state.counts.comboInvalid++;
      }
    } else {
      // risky / unknown / error — skip
      state.counts.skipped++;
      log(`[${rowIdx}] ${email} result=${result.result}, skipped`);
    }
  } catch (err) {
    state.counts.error++;
    log(`[${rowIdx}] Error: ${err.message}`);
  }
}