import path from 'path';
import { buildJobFilePath, writeMetadata, readMetadata } from '../utils/storage.js';
import { markJobActive, markJobComplete, appendJobLog } from './jobState.service.js';
import { enrichContacts } from './enricher.service.js';
import { validateExtension, enforceRowLimit } from './upload/fileValidation.service.js';
import { parseWorkbook } from './upload/workbookParser.service.js';
import { resolveColumns, normalizeRows } from './upload/rowNormalizer.service.js';
import { buildCsvColumnOrder, createCsvSnapshotWriter, composeCsvRowData, loadExistingCsvRows } from './upload/csvSnapshot.service.js';
import { createProgressSnapshot, normalizeStatusBucket } from './upload/progress.service.js';
import { buildResultSets } from './upload/resultBuilder.service.js';
import { normalizeDeliveryStatus, DELIVERY_STATUS } from './upload/status.utils.js';
import { REPORT_COLUMNS } from './upload/upload.constants.js';

/**
 * Maps the domainUsed field from enricher results to the Source column value.
 *   Website   → "main"       (primary domain)
 *   Website_one / Website_two → "waterfall"  (fallback domains)
 */
function resolveSource(domainUsed) {
  if (!domainUsed) return '';
  return domainUsed === 'Website' ? 'main' : 'waterfall';
}

/**
 * Computes report metrics from the final CSV rows.
 * Returns an array of { name, ratio } entries — one per row:
 *
 *   Row 0: Total Profiles     →  1000
 *   Row 1: Emails Found       →  450/1000 (45.0%)
 *   Row 2: Valid              →  300/450 (66.7%)
 *   Row 3: Catch-All          →  150/450 (33.3%)
 *   Row 4: Main               →  350/450 (77.8%)
 *   Row 5: Waterfall          →  100/450 (22.2%)
 */
function generateReport(rows) {
  const totalProfiles = rows.length;
  let emailsFound = 0;
  let validCount = 0;
  let catchAllCount = 0;
  let mainCount = 0;
  let waterfallCount = 0;

  for (const row of rows) {
    const email = (row?.Email || '').trim();
    if (!email) continue;
    emailsFound++;

    const status = (row?.Status || '').trim().toLowerCase().replace(/[\s-]+/g, '_');
    if (status === 'valid') validCount++;
    if (status === 'catch_all' || status === 'catchall') catchAllCount++;

    const source = (row?.Source || '').trim().toLowerCase();
    if (source === 'main') mainCount++;
    if (source === 'waterfall') waterfallCount++;
  }

  const pct = (n, d) => d > 0 ? ((n / d) * 100).toFixed(1) + '%' : '0.0%';

  return [
    { name: 'Total Profiles',  ratio: `${totalProfiles}` },
    { name: 'Emails Found',    ratio: `${emailsFound}/${totalProfiles} (${pct(emailsFound, totalProfiles)})` },
    { name: 'Valid',           ratio: `${validCount}/${emailsFound} (${pct(validCount, emailsFound)})` },
    { name: 'Catch-All',      ratio: `${catchAllCount}/${emailsFound} (${pct(catchAllCount, emailsFound)})` },
    { name: 'Main',           ratio: `${mainCount}/${emailsFound} (${pct(mainCount, emailsFound)})` },
    { name: 'Waterfall',      ratio: `${waterfallCount}/${emailsFound} (${pct(waterfallCount, emailsFound)})` },
  ];
}

/**
 * Writes report entries into the last two columns of rows 0–5.
 */
async function writeReportToRows(csvWriter) {
  const allRows = csvWriter.getRows();
  const reportEntries = generateReport(allRows);
  const [nameCol, ratioCol] = REPORT_COLUMNS;

  for (let i = 0; i < reportEntries.length && i < allRows.length; i++) {
    allRows[i][nameCol] = reportEntries[i].name;
    allRows[i][ratioCol] = reportEntries[i].ratio;
    await csvWriter.setRow(i, allRows[i]);
  }
}

export async function processUploadedFile({ jobId, jobDir, file, userId, onReady }) {
  markJobActive(jobId);
  const log = (msg) => appendJobLog(jobId, msg);
  log(`Job started: ${file.originalname}`);

  const baseMetadata = { jobId, userId, originalFilename: file.originalname, storedFilename: path.basename(file.path), createdAt: new Date().toISOString() };
  let metadataSnapshot = { ...baseMetadata, status: 'run' };
  await writeMetadata(jobDir, metadataSnapshot);

  let readyCallbackTriggered = false;
  const notifyReady = async () => {
    if (readyCallbackTriggered || typeof onReady !== 'function') return;
    readyCallbackTriggered = true;
    await onReady({ jobId, metadata: metadataSnapshot });
  };

  try {
    validateExtension(file.originalname);
    const parsed = await parseWorkbook(file.path);
    enforceRowLimit(parsed.rows.length);
    log(`Parsed ${parsed.rows.length} rows`);

    const initialColumnMap = resolveColumns(parsed.headers);
    const normalizedRows = normalizeRows(parsed.rows, initialColumnMap, parsed.headerRowIndex, parsed.headers);
    const runnableRows = normalizedRows.filter((row) => row.contact);
    const progress = createProgressSnapshot(runnableRows.length, normalizedRows.length - runnableRows.length);
    const csvColumns = buildCsvColumnOrder(parsed.headers, initialColumnMap);
    const outputFilename = `output-${jobId}-${Date.now()}.csv`;
    const outputPath = buildJobFilePath(jobDir, outputFilename);
    const downloadUrl = `/v1/scraper/enricher/download/${jobId}`;

    const initialCsvRows = normalizedRows.map((row) => {
      const overrides = {};
      if (row.existingEmail) {
        overrides.Email = row.existingEmail;
        overrides.Status = DELIVERY_STATUS.VALID;
      } else if (!row.contact) {
        overrides.Status = DELIVERY_STATUS.NOT_FOUND;
      }
      return composeCsvRowData(row.sanitizedRow, overrides);
    });
    const csvWriter = createCsvSnapshotWriter(outputPath, csvColumns, initialCsvRows);
    await csvWriter.writeSnapshot();

    log(`${runnableRows.length} contacts to verify, ${normalizedRows.length - runnableRows.length} skipped`);

    metadataSnapshot = {
      ...metadataSnapshot,
      totals: { totalRows: normalizedRows.length, runnableContacts: runnableRows.length, skippedRows: normalizedRows.length - runnableRows.length },
      progress, outputFilename, downloadUrl, resultCount: 0, lastUpdate: new Date().toISOString(),
    };
    await writeMetadata(jobDir, metadataSnapshot);
    await notifyReady();

    const rowLookup = new Map(normalizedRows.map((row) => [row.rowId, row]));

    const updateProgress = async (status, replaces) => {
      if (replaces) {
        const oldBucket = normalizeStatusBucket(replaces);
        progress.statusCounts[oldBucket] = Math.max(0, (progress.statusCounts[oldBucket] || 0) - 1);
      } else {
        progress.processedContacts += 1;
      }
      const bucket = normalizeStatusBucket(status);
      progress.statusCounts[bucket] = (progress.statusCounts[bucket] || 0) + 1;
      metadataSnapshot = { ...metadataSnapshot, progress: { ...progress }, resultCount: progress.processedContacts, lastUpdate: new Date().toISOString() };
      await writeMetadata(jobDir, metadataSnapshot);
    };

    const contacts = runnableRows.map((row) => ({ ...row.contact, rowId: row.rowId }));

    const updateCsvRowWithResult = async (resultPayload) => {
      const rowId = resultPayload?.contact?.rowId;
      if (typeof rowId !== 'number') return;
      const rowInfo = rowLookup.get(rowId);
      if (!rowInfo) return;
      const csvRow = composeCsvRowData(rowInfo.sanitizedRow, {
        Email: resultPayload.bestEmail || '',
        Status: normalizeDeliveryStatus(resultPayload.status),
        Source: resultPayload.bestEmail ? resolveSource(resultPayload.domainUsed) : '',
      });
      await csvWriter.setRow(rowId, csvRow);
    };

    const enrichmentBatch = contacts.length
      ? await enrichContacts(contacts, {
          jobId,
          onResult: async (result) => {
            await updateCsvRowWithResult(result);
            await updateProgress(result.status, result._replaces);
          },
        })
      : { results: [], haltType: null, unprocessedRowIds: [] };

    const enrichmentResults = enrichmentBatch.results || [];
    const haltType = enrichmentBatch.haltType;
    const unprocessedRowIds = enrichmentBatch.unprocessedRowIds || [];

    const { apiResults } = buildResultSets(normalizedRows, enrichmentResults);

    // ── Generate Report columns (rows 0–5) ──
    await writeReportToRows(csvWriter);

    if (haltType === 'stop') {
      const completionMetadata = {
        ...metadataSnapshot,
        status: 'stop',
        stoppedAt: new Date().toISOString(),
        unprocessedRowIds,
        resultCount: apiResults.length,
      };
      await writeMetadata(jobDir, completionMetadata);
      log(`Job stopped: ${apiResults.length} results, ${unprocessedRowIds.length} unprocessed`);
    } else if (haltType === 'pause') {
      const completionMetadata = {
        ...metadataSnapshot,
        status: 'pause',
        pausedAt: new Date().toISOString(),
        unprocessedRowIds,
        resultCount: apiResults.length,
      };
      await writeMetadata(jobDir, completionMetadata);
      log(`Job paused: ${apiResults.length} results, ${unprocessedRowIds.length} unprocessed`);
    } else if (haltType === 'spam' || haltType === 'limit') {
      const completionMetadata = {
        ...metadataSnapshot,
        status: 'pause',
        haltType,
        haltReason: enrichmentBatch.haltReason || haltType,
        pausedAt: new Date().toISOString(),
        unprocessedRowIds,
        resultCount: apiResults.length,
      };
      await writeMetadata(jobDir, completionMetadata);
      log(`Job halted (${haltType}): ${apiResults.length} results, ${unprocessedRowIds.length} unprocessed — rerun available`);
    } else {
      const completionMetadata = { ...metadataSnapshot, status: 'done', completedAt: new Date().toISOString(), resultCount: apiResults.length };
      await writeMetadata(jobDir, completionMetadata);
      log(`Job completed: ${apiResults.length} results`);
    }

    return { jobId, userId, outputFile: outputFilename, outputPath, downloadUrl, results: apiResults };
  } catch (error) {
    log(`Job failed: ${error.message}`);
    await writeMetadata(jobDir, { ...metadataSnapshot, status: 'failed', failedAt: new Date().toISOString(), error: error.message });
    throw error;
  } finally {
    markJobComplete(jobId);
    await notifyReady();
  }
}

export async function rerunJob({ jobId, jobDir }) {
  const log = (msg) => appendJobLog(jobId, msg);
  const metadata = await readMetadata(jobDir);
  if (!metadata) throw new Error('Job metadata not found');

  const storedFile = path.join(jobDir, metadata.storedFilename);
  const outputFilename = metadata.outputFilename;
  const outputPath = buildJobFilePath(jobDir, outputFilename);
  const unprocessedRowIds = new Set(metadata.unprocessedRowIds || []);

  markJobActive(jobId);
  log(`Rerun started: ${unprocessedRowIds.size} contacts to reprocess`);

  let metadataSnapshot = { ...metadata, status: 'run', rerunAt: new Date().toISOString(), unprocessedRowIds: undefined };
  await writeMetadata(jobDir, metadataSnapshot);

  try {
    const parsed = await parseWorkbook(storedFile);
    const initialColumnMap = resolveColumns(parsed.headers);
    const normalizedRows = normalizeRows(parsed.rows, initialColumnMap, parsed.headerRowIndex, parsed.headers);
    const csvColumns = buildCsvColumnOrder(parsed.headers, initialColumnMap);

    // Load existing CSV rows to preserve already-processed results
    const existingRows = await loadExistingCsvRows(outputPath, csvColumns);
    const fallbackRows = normalizedRows.map((row) => {
      const overrides = {};
      if (row.existingEmail) {
        overrides.Email = row.existingEmail;
        overrides.Status = DELIVERY_STATUS.VALID;
      } else if (!row.contact) {
        overrides.Status = DELIVERY_STATUS.NOT_FOUND;
      }
      return composeCsvRowData(row.sanitizedRow, overrides);
    });
    // Use existing CSV as base so already-processed rows are preserved
    const baseRows = existingRows || fallbackRows;
    const csvWriter = createCsvSnapshotWriter(outputPath, csvColumns, baseRows);
    // Only write snapshot if we had to fall back (no existing CSV found)
    if (!existingRows) await csvWriter.writeSnapshot();

    // Filter to only unprocessed contacts
    const runnableRows = normalizedRows.filter((row) => row.contact && (unprocessedRowIds.size === 0 || unprocessedRowIds.has(row.rowId)));
    log(`${runnableRows.length} contacts queued for rerun`);

    const progress = metadata.progress || createProgressSnapshot(runnableRows.length, 0);
    const rowLookup = new Map(normalizedRows.map((row) => [row.rowId, row]));

    const updateProgress = async (status, replaces) => {
      if (replaces) {
        const oldBucket = normalizeStatusBucket(replaces);
        progress.statusCounts[oldBucket] = Math.max(0, (progress.statusCounts[oldBucket] || 0) - 1);
      } else {
        progress.processedContacts += 1;
      }
      const bucket = normalizeStatusBucket(status);
      progress.statusCounts[bucket] = (progress.statusCounts[bucket] || 0) + 1;
      metadataSnapshot = { ...metadataSnapshot, progress: { ...progress }, resultCount: progress.processedContacts, lastUpdate: new Date().toISOString() };
      await writeMetadata(jobDir, metadataSnapshot);
    };

    const updateCsvRowWithResult = async (resultPayload) => {
      const rowId = resultPayload?.contact?.rowId;
      if (typeof rowId !== 'number') return;
      const rowInfo = rowLookup.get(rowId);
      if (!rowInfo) return;
      const csvRow = composeCsvRowData(rowInfo.sanitizedRow, {
        Email: resultPayload.bestEmail || '',
        Status: normalizeDeliveryStatus(resultPayload.status),
        Source: resultPayload.bestEmail ? resolveSource(resultPayload.domainUsed) : '',
      });
      await csvWriter.setRow(rowId, csvRow);
    };

    const contacts = runnableRows.map((row) => ({ ...row.contact, rowId: row.rowId }));

    const enrichmentBatch = contacts.length
      ? await enrichContacts(contacts, {
          jobId,
          onResult: async (result) => {
            await updateCsvRowWithResult(result);
            await updateProgress(result.status, result._replaces);
          },
        })
      : { results: [], haltType: null, unprocessedRowIds: [] };

    const enrichmentResults = enrichmentBatch.results || [];
    const haltType = enrichmentBatch.haltType;
    const newUnprocessedRowIds = enrichmentBatch.unprocessedRowIds || [];

    const { apiResults } = buildResultSets(normalizedRows, enrichmentResults);

    // ── Generate Report columns (rows 0–5) ──
    await writeReportToRows(csvWriter);

    if (haltType === 'stop') {
      await writeMetadata(jobDir, { ...metadataSnapshot, status: 'stop', stoppedAt: new Date().toISOString(), unprocessedRowIds: newUnprocessedRowIds, resultCount: apiResults.length });
      log(`Rerun stopped: ${newUnprocessedRowIds.length} still unprocessed`);
    } else if (haltType === 'pause') {
      await writeMetadata(jobDir, { ...metadataSnapshot, status: 'pause', pausedAt: new Date().toISOString(), unprocessedRowIds: newUnprocessedRowIds, resultCount: apiResults.length });
      log(`Rerun paused: ${newUnprocessedRowIds.length} still unprocessed`);
    } else if (haltType === 'spam' || haltType === 'limit') {
      await writeMetadata(jobDir, { ...metadataSnapshot, status: 'pause', haltType, haltReason: enrichmentBatch.haltReason || haltType, pausedAt: new Date().toISOString(), unprocessedRowIds: newUnprocessedRowIds, resultCount: apiResults.length });
      log(`Rerun halted (${haltType}): ${newUnprocessedRowIds.length} still unprocessed — rerun available`);
    } else {
      await writeMetadata(jobDir, { ...metadataSnapshot, status: 'done', completedAt: new Date().toISOString(), resultCount: apiResults.length });
      log(`Rerun completed: ${apiResults.length} results`);
    }

    return { jobId, outputFile: outputFilename, outputPath, downloadUrl: metadata.downloadUrl, results: apiResults };
  } catch (error) {
    log(`Rerun failed: ${error.message}`);
    await writeMetadata(jobDir, { ...metadataSnapshot, status: 'failed', failedAt: new Date().toISOString(), error: error.message });
    throw error;
  } finally {
    markJobComplete(jobId);
  }
}