/**
 * services/job/job-processor.js — Processes an uploaded file end-to-end.
 *
 * Validates -> parses -> normalizes -> enriches -> writes CSV output.
 */

const path = require('path');
const { buildJobFilePath, writeMetadata } = require('../../utils/storage');
const { DELIVERY_STATUS } = require('../../utils/status-codes');
const { markJobActive, markJobComplete, appendJobLog } = require('./job-state');
const { validateExtension, enforceRowLimit } = require('../upload/file-validator');
const { parseWorkbook } = require('../upload/workbook-parser');
const { resolveColumns } = require('../upload/column-resolver');
const { normalizeRows } = require('../upload/row-normalizer');
const { buildCsvColumnOrder, createCsvSnapshotWriter, composeCsvRowData } = require('../upload/csv-writer');
const { createProgressSnapshot } = require('../upload/progress-tracker');
const { buildResultSets } = require('../upload/result-builder');
const { writeReportToRows } = require('./report-generator');
const { runEnrichment } = require('./enrichment-runner');
const { finalizeJob } = require('./job-finalizer');

async function processUploadedFile({ jobId, jobDir, file, userId }) {
  markJobActive(jobId);
  const log = (msg) => appendJobLog(jobId, msg);
  log(`Job started: ${file.originalname}`);
  const writeMeta = (m) => writeMetadata(jobDir, m);

  let meta = { jobId, userId, originalFilename: file.originalname, storedFilename: path.basename(file.path), createdAt: new Date().toISOString(), status: 'run' };
  await writeMeta(meta);

  try {
    validateExtension(file.originalname);
    const parsed = await parseWorkbook(file.path);
    enforceRowLimit(parsed.rows.length);

    const colMap = resolveColumns(parsed.headers);
    const normRows = normalizeRows(parsed.rows, colMap, parsed.headerRowIndex, parsed.headers);
    const runnable = normRows.filter((r) => r.contact);
    const skipped = normRows.filter((r) => !r.contact);
    const skippedWithEmail = skipped.filter((r) => r.existingEmail).length;
    const skippedNoEmail = skipped.length - skippedWithEmail;

    // Include ALL rows in progress so UI stats add up to totalRows
    const progress = createProgressSnapshot(normRows.length, 0);
    progress.processedContacts = skipped.length;
    progress.statusCounts[DELIVERY_STATUS.VALID] = skippedWithEmail;
    progress.statusCounts[DELIVERY_STATUS.NOT_FOUND] = skippedNoEmail;
    const csvCols = buildCsvColumnOrder(parsed.headers, colMap);
    const outFile = `output-${jobId}-${Date.now()}.csv`;
    const outPath = buildJobFilePath(jobDir, outFile);
    const downloadUrl = `/v1/scraper/enricher/download/${jobId}`;

    const initRows = normRows.map((r) => {
      const o = {};
      if (r.existingEmail) { o.Email = r.existingEmail; o.Status = r.existingStatus || DELIVERY_STATUS.VALID; }
      else if (!r.contact) o.Status = DELIVERY_STATUS.NOT_FOUND;
      return composeCsvRowData(r.sanitizedRow, o);
    });
    const csv = createCsvSnapshotWriter(outPath, csvCols, initRows);
    await csv.writeSnapshot();

    meta = { ...meta, totals: { totalRows: normRows.length, runnableContacts: runnable.length, skippedRows: normRows.length - runnable.length }, progress, outputFilename: outFile, downloadUrl, resultCount: 0 };
    await writeMeta(meta);

    const contacts = runnable.map((r) => ({ ...r.contact, rowId: r.rowId }));
    const rowLookup = new Map(normRows.map((r) => [r.rowId, r]));
    const batch = await runEnrichment({ contacts, jobId, progress, meta, rowLookup, csv, writeMeta });
    const { apiResults } = buildResultSets(normRows, batch.results || []);
    await writeReportToRows(csv);
    await finalizeJob(meta, batch, writeMeta, apiResults.length);
    log(`Job finished: ${apiResults.length} results`);
  } catch (err) {
    log(`Job failed: ${err.message}`);
    await writeMeta({ ...meta, status: 'failed', failedAt: new Date().toISOString(), error: err.message });
    throw err;
  } finally { markJobComplete(jobId); }
}

module.exports = { processUploadedFile };
