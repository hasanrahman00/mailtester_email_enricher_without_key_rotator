/**
 * Catch-All Cleaner Service
 *
 * Processes TWO categories of rows:
 *
 * ═══ Category A: catch_all rows (have an existing email) ═══
 *
 *   1. Verify original email via BounceBan:
 *      - deliverable   → status = 'valid', done
 *      - risky         → status = 'risky', keep email, done
 *      - undeliverable → proceed to combo chain (2 combos)
 *
 *   2. Combo chain (catch_all):
 *      a) firstname.lastname@domain
 *      b) firstinitial+lastname@domain  (e.g. jsmith@domain)
 *
 *      First deliverable/risky wins. If all fail → leave as catch_all.
 *
 * ═══ Category B: rate_limited / error rows (email is EMPTY) ═══
 *
 *   No original email to verify. Domain resolved from 'Domain Used' column,
 *   falling back to 'Website' → 'Website_one' → 'Website_two'.
 *
 *   Combo chain (3 combos):
 *      a) first@domain             (e.g. john@domain)
 *      b) firstname.lastname@domain
 *      c) firstinitial+lastname@domain
 *
 *      First deliverable → email = combo, status = 'valid', done.
 *      First risky/catch_all → email = combo, status = that result, done.
 *      All fail → status = 'not_found', email stays empty.
 *
 * Guards:
 *   - Last name must be >= 2 chars to attempt combos (skips initials like "c", "g")
 *   - Domain blacklist: if ALL combos failed at a domain, later rows skip combos
 *     for that domain (saves BounceBan credits)
 *   - Combo that equals the original email is skipped silently
 *
 * Concurrency: ALL rows start concurrently. The semaphore in
 * bounceban.client throttles actual HTTP requests to <=100 parallel.
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
  // Strip trailing \r left over from \r\n splitting
  if (line.endsWith('\r')) line = line.slice(0, -1);
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
  // Replace embedded newlines with space — prevents multi-line fields
  const s = String(value).replace(/[\r\n]+/g, ' ');
  if (/[",]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

/**
 * Splits raw CSV text into logical records (rows), respecting quoted fields
 * that may contain embedded newlines.
 */
function splitCsvRecords(text) {
  const records = [];
  let current = '';
  let inQuotes = false;

  for (let i = 0; i < text.length; i++) {
    const ch = text[i];

    if (inQuotes) {
      if (ch === '"') {
        if (text[i + 1] === '"') {
          current += '""';
          i++;
        } else {
          inQuotes = false;
          current += ch;
        }
      } else {
        current += ch;
      }
    } else {
      if (ch === '"') {
        inQuotes = true;
        current += ch;
      } else if (ch === '\n') {
        if (current.endsWith('\r')) current = current.slice(0, -1);
        if (current.trim()) records.push(current);
        current = '';
      } else {
        current += ch;
      }
    }
  }
  if (current.endsWith('\r')) current = current.slice(0, -1);
  if (current.trim()) records.push(current);

  return records;
}

async function loadCsv(filePath) {
  let content = await fs.readFile(filePath, 'utf-8');
  console.log(`[catchallCleaner] loadCsv — file size: ${content.length}, BOM present: ${content.charCodeAt(0) === 0xFEFF}`);
  // Strip UTF-8 BOM if present
  if (content.charCodeAt(0) === 0xFEFF) content = content.slice(1);
  const records = splitCsvRecords(content);
  console.log(`[catchallCleaner] loadCsv — records from splitCsvRecords: ${records.length}`);
  if (records.length < 2) return { headers: [], rows: [] };
  const headers = parseCsvLine(records[0]);
  console.log(`[catchallCleaner] loadCsv — headers: ${headers.length}, first 3: ${headers.slice(0,3).join(' | ')}`);
  const rows = records.slice(1).map((record) => {
    const values = parseCsvLine(record);
    const row = {};
    headers.forEach((h, i) => { row[h] = values[i] ?? ''; });
    return row;
  });
  console.log(`[catchallCleaner] loadCsv — parsed ${rows.length} data rows`);
  return { headers, rows };
}

function serializeCsv(headers, rows) {
  const BOM = '\uFEFF';
  console.log(`[catchallCleaner] serializeCsv — headers: ${headers.length}, rows: ${rows.length}`);
  const headerLine = headers.map(escapeCsv).join(',');
  console.log(`[catchallCleaner] serializeCsv — headerLine starts with quote: ${headerLine[0] === '"'}, first 80: ${headerLine.slice(0,80)}`);
  const bodyLines = rows.map((row) =>
    headers.map((h) => escapeCsv(row[h] ?? '')).join(','),
  );
  const result = BOM + [headerLine, ...bodyLines].join('\r\n') + '\r\n';
  console.log(`[catchallCleaner] serializeCsv — output BOM: ${result.charCodeAt(0) === 0xFEFF}, total length: ${result.length}`);
  return result;
}

// ── Combo builders ──────────────────────────────────────────────────────────

const MIN_LAST_NAME_LENGTH = 2;  // skip initials like "c", "g", "p"

/**
 * Combos for catch_all rows (have an original email).
 * Returns 2 combos: firstname.lastname@domain, firstinitial+lastname@domain
 * Excludes any that match the original email.
 */
function buildCatchAllCombos(firstName, lastName, domain, originalEmail) {
  if (!firstName || !lastName || !domain) return [];
  if (lastName.length < MIN_LAST_NAME_LENGTH) return [];

  const orig = originalEmail.toLowerCase();
  const candidates = [
    `${firstName}.${lastName}@${domain}`,        // john.smith@domain
    `${firstName[0]}${lastName}@${domain}`,       // jsmith@domain
  ];

  const seen = new Set([orig]);
  return candidates.filter((c) => {
    const lc = c.toLowerCase();
    if (seen.has(lc)) return false;
    seen.add(lc);
    return true;
  });
}

/**
 * Combos for rate_limited / error rows (email is EMPTY).
 * Returns 3 combos: first@domain, firstname.lastname@domain, firstinitial+lastname@domain
 */
function buildErrorCombos(firstName, lastName, domain) {
  if (!firstName || !domain) return [];

  const candidates = [
    `${firstName}@${domain}`,                     // john@domain
  ];

  // Only add lastname-based combos if last name is long enough
  if (lastName && lastName.length >= MIN_LAST_NAME_LENGTH) {
    candidates.push(
      `${firstName}.${lastName}@${domain}`,        // john.smith@domain
      `${firstName[0]}${lastName}@${domain}`,      // jsmith@domain
    );
  }

  // Deduplicate
  const seen = new Set();
  return candidates.filter((c) => {
    const lc = c.toLowerCase();
    if (seen.has(lc)) return false;
    seen.add(lc);
    return true;
  });
}

// ── CSV flush interval ───────────────────────────────────────────────────────

const CSV_FLUSH_INTERVAL_MS = 8_000;  // write CSV to disk every 8 s

// ── Status normalization helper ─────────────────────────────────────────────

function normalizeStatus(raw) {
  return (raw || '').trim().toLowerCase().replace(/[\s-]+/g, '_');
}

// ── Main cleaner ────────────────────────────────────────────────────────────

export async function startCatchAllCleaner(jobId) {
  if (isCleanerRunning(jobId)) {
    throw new Error('Catch-all cleaner is already running for this job');
  }

  const state = {
    status: 'running',
    stop: false,
    counts: {
      // ── catch_all counts ──
      catchAllTotal: 0,
      deliverable: 0,   // original email confirmed deliverable
      risky: 0,         // BounceBan returned risky (original or combo)
      undeliverable: 0, // original undeliverable, no combo attempted (missing name/domain)
      comboValid: 0,    // a combo email was confirmed deliverable
      comboInvalid: 0,  // all combos were undeliverable
      comboSkipped: 0,  // domain blacklisted or last name too short
      // ── rate_limited / error counts ──
      errorTotal: 0,    // total rate_limited + error rows found
      errorFixed: 0,    // combo found deliverable for error/rate_limited row
      errorRisky: 0,    // combo returned risky/catch_all for error/rate_limited row
      errorNotFound: 0, // all combos failed → status set to not_found
      errorSkipped: 0,  // missing name/domain, domain blacklisted
      // ── shared ──
      error: 0,
      skipped: 0,       // empty email on catch_all, or unknown/error bounce result
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
    const statusCol      = find(['status'])                                          || 'Status';
    const emailCol       = find(['email', 'e-mail'])                                 || 'Email';
    const firstNameCol   = find(['first name', 'firstname', 'first'])                || 'First Name';
    const lastNameCol    = find(['last name', 'lastname', 'last'])                   || 'Last Name';
    const notesCol       = find(['notes'])                                           || 'Notes';
    const domainUsedCol  = find(['domain used', 'domainused', 'domain_used'])        || 'Domain Used';
    const websiteCol     = find(['website', 'domain', 'company website'])            || 'Website';
    const websiteOneCol  = find(['website_one', 'website one', 'websiteone'])        || 'Website_one';
    const websiteTwoCol  = find(['website_two', 'website two', 'websitetwo'])        || 'Website_two';
    const sourceCol      = find(['source'])                                           || 'Source';

    // ── Collect rows by category ─────────────────────────────────────────────
    const catchAllIndices = [];
    const errorIndices = [];    // rate_limited + error rows

    rows.forEach((row, idx) => {
      const s = normalizeStatus(row[statusCol]);
      if (s === 'catch_all' || s === 'catchall') {
        catchAllIndices.push(idx);
      } else if (s === 'rate_limited' || s === 'ratelimited' || s === 'error') {
        errorIndices.push(idx);
      }
    });

    state.counts.catchAllTotal = catchAllIndices.length;
    state.counts.errorTotal = errorIndices.length;
    const totalRows = catchAllIndices.length + errorIndices.length;

    log(`Found ${catchAllIndices.length} catch-all + ${errorIndices.length} rate_limited/error rows out of ${rows.length} total`);

    if (totalRows === 0) {
      state.status = 'done';
      log('No rows to process. Done.');
      return state.counts;
    }

    // Save initial cleaner metadata
    await writeMetadata(jobDir, {
      ...metadata,
      catchAllCleaner: { status: 'running', startedAt: state.startedAt, counts: state.counts },
    });

    const ctx = {
      emailCol, firstNameCol, lastNameCol, statusCol, notesCol, sourceCol,
      domainUsedCol, websiteCol, websiteOneCol, websiteTwoCol,
      log, state, failedComboDomains,
    };

    // ── Launch ALL rows concurrently ─────────────────────────────────────────
    const promises = [
      ...catchAllIndices.map((rowIdx) => processCatchAllRow(rows, rowIdx, ctx)),
      ...errorIndices.map((rowIdx) => processErrorRow(rows, rowIdx, ctx)),
    ];

    // ── Periodic CSV flush ───────────────────────────────────────────────────
    const flushInterval = setInterval(async () => {
      try {
        await fs.writeFile(csvPath, serializeCsv(headers, rows), 'utf-8');
        const c = state.counts;
        const catchAllDone = c.deliverable + c.risky + c.undeliverable + c.comboValid + c.comboInvalid + c.comboSkipped + c.skipped;
        const errorDone = c.errorFixed + c.errorRisky + c.errorNotFound + c.errorSkipped;
        log(`Progress: catch_all ${catchAllDone}/${c.catchAllTotal}, error/rate_limited ${errorDone}/${c.errorTotal}`);

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

    // ── Final status remap: enricher labels → customer-facing labels ─────────
    // Three final statuses: verified, risky, valid email not found
    const FINAL_STATUS_MAP = {
      valid:        'verified',
      catch_all:    'risky',
      catchall:     'risky',
      not_found:    'valid email not found',
      mx_not_found: 'valid email not found',
    };

    for (const row of rows) {
      const raw = normalizeStatus(row[statusCol]);
      if (FINAL_STATUS_MAP[raw]) {
        row[statusCol] = FINAL_STATUS_MAP[raw];
      }
    }

    // Final CSV write
    await fs.writeFile(csvPath, serializeCsv(headers, rows), 'utf-8');

    state.status = state.stop ? 'stopped' : 'done';
    log(
      `Cleaner ${state.status}. ` +
      `[catch_all] Deliverable: ${state.counts.deliverable}, Risky: ${state.counts.risky}, ` +
      `Combo valid: ${state.counts.comboValid}, Combo invalid: ${state.counts.comboInvalid}, ` +
      `Combo skipped: ${state.counts.comboSkipped}, Undeliverable: ${state.counts.undeliverable}. ` +
      `[error/rate_limited] Fixed: ${state.counts.errorFixed}, Risky: ${state.counts.errorRisky}, ` +
      `Not found: ${state.counts.errorNotFound}, Skipped: ${state.counts.errorSkipped}. ` +
      `Errors: ${state.counts.error}`
    );

    // Adjust main job status counts in metadata
    const catchAllUpgraded = state.counts.deliverable + state.counts.comboValid;
    const catchAllRisky = state.counts.risky;
    const errorUpgraded = state.counts.errorFixed;
    const errorRisky = state.counts.errorRisky;
    const errorToNotFound = state.counts.errorNotFound;

    const finalMeta = await readMetadata(jobDir);
    if (finalMeta) {
      if (finalMeta.progress?.statusCounts) {
        const sc = finalMeta.progress.statusCounts;

        // ── catch_all adjustments ──
        if (catchAllUpgraded > 0 || catchAllRisky > 0) {
          sc.catch_all = Math.max(0, (sc.catch_all || 0) - catchAllUpgraded - catchAllRisky);
          sc.valid = (sc.valid || 0) + catchAllUpgraded;
          sc.risky = (sc.risky || 0) + catchAllRisky;
        }

        // ── rate_limited / error adjustments ──
        // Rows that got a valid combo → move from error/rate_limited to valid
        if (errorUpgraded > 0) {
          sc.error = Math.max(0, (sc.error || 0) - errorUpgraded);
          sc.rate_limited = Math.max(0, (sc.rate_limited || 0) - errorUpgraded);
          sc.valid = (sc.valid || 0) + errorUpgraded;
        }
        if (errorRisky > 0) {
          sc.error = Math.max(0, (sc.error || 0) - errorRisky);
          sc.rate_limited = Math.max(0, (sc.rate_limited || 0) - errorRisky);
          sc.risky = (sc.risky || 0) + errorRisky;
        }
        // Rows that became not_found → move from error/rate_limited to not_found
        if (errorToNotFound > 0) {
          sc.error = Math.max(0, (sc.error || 0) - errorToNotFound);
          sc.rate_limited = Math.max(0, (sc.rate_limited || 0) - errorToNotFound);
          sc.not_found = (sc.not_found || 0) + errorToNotFound;
        }

        // ── Remap status keys to customer-facing names ──
        // valid → verified, catch_all → risky, not_found + mx_not_found → valid email not found
        sc['verified'] = (sc.valid || 0);
        sc['risky'] = (sc.risky || 0) + (sc.catch_all || 0);
        sc['valid email not found'] = (sc.not_found || 0) + (sc.mx_not_found || 0);
        delete sc.valid;
        delete sc.catch_all;
        delete sc.not_found;
        delete sc.mx_not_found;
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

// ── Resolve domain for error/rate_limited rows ──────────────────────────────

function resolveDomain(row, domainUsedCol, websiteCol, websiteOneCol, websiteTwoCol) {
  // Priority: Domain Used → Website → Website_one → Website_two
  // Returns { domain, source } — source is 'main' or 'waterfall'
  const candidates = [
    { raw: row[domainUsedCol], source: '' },        // Domain Used column has no fixed source — skip it for source
    { raw: row[websiteCol],    source: 'main' },
    { raw: row[websiteOneCol], source: 'waterfall' },
    { raw: row[websiteTwoCol], source: 'waterfall' },
  ];
  for (const { raw, source } of candidates) {
    const d = (raw || '').trim().toLowerCase().replace(/^https?:\/\//, '').replace(/^www\./, '').replace(/\/.*$/, '');
    if (d) return { domain: d, source: source || 'main' };
  }
  return { domain: '', source: '' };
}

// ── Single row processor: CATCH_ALL (existing logic) ────────────────────────

async function processCatchAllRow(rows, rowIdx, ctx) {
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

    const combos = buildCatchAllCombos(firstName, lastName, domain, email);

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

// ── Single row processor: RATE_LIMITED / ERROR (new logic) ──────────────────

async function processErrorRow(rows, rowIdx, ctx) {
  const {
    emailCol, firstNameCol, lastNameCol, statusCol, notesCol, sourceCol,
    domainUsedCol, websiteCol, websiteOneCol, websiteTwoCol,
    log, state, failedComboDomains,
  } = ctx;
  const row = rows[rowIdx];
  const originalStatus = (row[statusCol] || '').trim();

  if (state.stop) return;

  const firstName = (row[firstNameCol] || '').trim().toLowerCase().replace(/[^a-z]/g, '');
  const lastName  = (row[lastNameCol]  || '').trim().toLowerCase().replace(/[^a-z]/g, '');
  const { domain, source } = resolveDomain(row, domainUsedCol, websiteCol, websiteOneCol, websiteTwoCol);

  if (!firstName || !domain) {
    log(`[${rowIdx}] [${originalStatus}] Cannot build combo — missing firstName or domain`);
    row[statusCol] = 'not_found';
    row[notesCol]  = `CatchAll Cleaner: ${originalStatus} → not_found (missing name or domain)`;
    state.counts.errorSkipped++;
    return;
  }

  if (failedComboDomains.has(domain)) {
    log(`[${rowIdx}] [${originalStatus}] Domain ${domain} already failed all combos — not_found`);
    row[statusCol] = 'not_found';
    row[notesCol]  = `CatchAll Cleaner: ${originalStatus} → not_found (domain blacklisted)`;
    state.counts.errorSkipped++;
    return;
  }

  const combos = buildErrorCombos(firstName, lastName, domain);

  if (combos.length === 0) {
    log(`[${rowIdx}] [${originalStatus}] No combos could be generated`);
    row[statusCol] = 'not_found';
    row[notesCol]  = `CatchAll Cleaner: ${originalStatus} → not_found (no combos)`;
    state.counts.errorSkipped++;
    return;
  }

  try {
    let anyComboAttempted = false;

    for (const comboEmail of combos) {
      if (state.stop) return;

      anyComboAttempted = true;
      const result = await verifyEmail(comboEmail);
      log(`[${rowIdx}] [${originalStatus}] combo ${comboEmail} → ${result.result}`);

      if (result.result === 'deliverable') {
        row[emailCol]  = comboEmail;
        row[statusCol] = 'valid';
        row[sourceCol] = source;
        row[notesCol]  = `CatchAll Cleaner: ${originalStatus} → valid (combo: ${comboEmail})`;
        state.counts.errorFixed++;
        return;
      }

      if (result.result === 'risky' || result.result === 'catch_all' || result.result === 'catchall') {
        row[emailCol]  = comboEmail;
        row[statusCol] = result.result === 'risky' ? 'risky' : 'catch_all';
        row[sourceCol] = source;
        row[notesCol]  = `CatchAll Cleaner: ${originalStatus} → ${row[statusCol]} (combo: ${comboEmail})`;
        state.counts.errorRisky++;
        return;
      }

      // undeliverable / unknown / error → try next combo
    }

    // All combos exhausted → set not_found and blacklist domain
    if (anyComboAttempted) {
      failedComboDomains.add(domain);
    }
    row[statusCol] = 'not_found';
    row[notesCol]  = `CatchAll Cleaner: ${originalStatus} → not_found (all combos failed)`;
    state.counts.errorNotFound++;

  } catch (err) {
    state.counts.error++;
    log(`[${rowIdx}] [${originalStatus}] Error: ${err.message}`);
  }
}