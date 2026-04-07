/**
 * services/job/job-rerunner.js — Reruns a stopped/paused job.
 *
 * Loads the existing CSV output and re-processes EVERY contact
 * that isn't already "valid". This ensures:
 *   - Contacts mid-wave when paused → reprocessed from wave 0
 *   - Contacts that finished Pass 1 as not_found → get Pass 2/3
 *   - Contacts that finished Pass 2 as not_found → get Pass 3
 *   - Zero rows skipped, all waterfall domains processed
 */

const path = require('path');
const { buildJobFilePath, readMetadata, writeMetadata } = require('../../utils/storage');
const { DELIVERY_STATUS } = require('../../utils/status-codes');
const { markJobActive, markJobComplete, appendJobLog } = require('./job-state');
const { parseWorkbook } = require('../upload/workbook-parser');
const { resolveColumns } = require('../upload/column-resolver');
const { normalizeRows } = require('../upload/row-normalizer');
const { buildCsvColumnOrder, createCsvSnapshotWriter, composeCsvRowData, loadExistingCsvRows } = require('../upload/csv-writer');
const { createProgressSnapshot } = require('../upload/progress-tracker');
const { buildResultSets } = require('../upload/result-builder');
const { writeReportToRows } = require('./report-generator');
const { runEnrichment } = require('./enrichment-runner');
const { finalizeJob } = require('./job-finalizer');

async function rerunJob({ jobId, jobDir }) {
  const log = (msg) => appendJobLog(jobId, msg);
  const origMeta = await readMetadata(jobDir);
  if (!origMeta) throw new Error('Job metadata not found');
  const writeMeta = (m) => writeMetadata(jobDir, m);

  const storedFile = path.join(jobDir, origMeta.storedFilename);
  const outPath = buildJobFilePath(jobDir, origMeta.outputFilename);

  markJobActive(jobId);
  let meta = { ...origMeta, status: 'run', rerunAt: new Date().toISOString(), unprocessedRowIds: undefined };
  await writeMeta(meta);

  try {
    // Re-parse the original uploaded file
    const parsed = await parseWorkbook(storedFile);
    const colMap = resolveColumns(parsed.headers);
    const normRows = normalizeRows(parsed.rows, colMap, parsed.headerRowIndex, parsed.headers);
    const csvCols = buildCsvColumnOrder(parsed.headers, colMap);

    // Load existing CSV results from prior run
    const existing = await loadExistingCsvRows(outPath);

    // Build initial rows (fallback if CSV is missing)
    const fallback = normRows.map((r) => {
      const o = {};
      if (r.existingEmail) { o.Email = r.existingEmail; o.Status = r.existingStatus || DELIVERY_STATUS.VALID; }
      else if (!r.contact) o.Status = DELIVERY_STATUS.NOT_FOUND;
      return composeCsvRowData(r.sanitizedRow, o);
    });
    const csvRows = existing || fallback;

    // Separate valid contacts from non-valid ones
    let alreadyValid = 0;
    const reprocessable = [];

    normRows.forEach((r) => {
      if (!r.contact) return; // skipped row (existing email, missing data)
      const csvStatus = (csvRows[r.rowId]?.Status || '').trim().toLowerCase();
      if (csvStatus === 'valid') {
        alreadyValid++;
      } else {
        reprocessable.push(r);
      }
    });

    // ── Clean slate: wipe Email/Status/Source for all non-valid rows ──
    // This makes the UI show empty cells immediately before enrichment starts
    for (const r of reprocessable) {
      csvRows[r.rowId] = composeCsvRowData(r.sanitizedRow, { Email: '', Status: '', Source: '' });
    }

    // Write cleaned CSV to disk so UI reflects reset state right away
    const csv = createCsvSnapshotWriter(outPath, csvCols, csvRows);
    await csv.writeSnapshot();

    const skipped = normRows.filter((r) => !r.contact);
    const skippedWithEmail = skipped.filter((r) => r.existingEmail).length;
    const skippedNoEmail = skipped.length - skippedWithEmail;

    log(`Rerun: ${reprocessable.length} contacts to reprocess (${alreadyValid} already valid, ${skipped.length} skipped)`);

    // Build progress — pre-count only valid + skipped, everything else starts at 0
    const progress = createProgressSnapshot(normRows.length, 0);
    progress.processedContacts = skipped.length + alreadyValid;
    progress.statusCounts[DELIVERY_STATUS.VALID] = skippedWithEmail + alreadyValid;
    progress.statusCounts[DELIVERY_STATUS.NOT_FOUND] = skippedNoEmail;

    meta = { ...meta, totals: origMeta.totals, progress, outputFilename: origMeta.outputFilename, downloadUrl: origMeta.downloadUrl, resultCount: progress.processedContacts };
    await writeMeta(meta);

    // Run enrichment on non-valid contacts (full 3-pass waterfall)
    const contacts = reprocessable.map((r) => ({ ...r.contact, rowId: r.rowId }));
    const rowLookup = new Map(normRows.map((r) => [r.rowId, r]));
    const batch = await runEnrichment({ contacts, jobId, progress, meta, rowLookup, csv, writeMeta });

    const { apiResults } = buildResultSets(normRows, batch.results || []);
    await writeReportToRows(csv);
    await finalizeJob(meta, batch, writeMeta, apiResults.length);
    log(`Rerun completed: ${reprocessable.length} reprocessed, ${alreadyValid} kept valid`);
  } catch (err) {
    log(`Rerun failed: ${err.message}`);
    await writeMeta({ ...meta, status: 'failed', error: err.message });
    throw err;
  } finally { markJobComplete(jobId); }
}

module.exports = { rerunJob };
