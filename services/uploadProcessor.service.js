import path from 'path';
import { buildJobFilePath, writeMetadata } from '../utils/storage.js';
import { markJobActive, markJobComplete, appendJobLog } from './jobState.service.js';
import { enrichContacts } from './enricher.service.js';
import { validateExtension, enforceRowLimit } from './upload/fileValidation.service.js';
import { parseWorkbook } from './upload/workbookParser.service.js';
import { resolveColumns, normalizeRows } from './upload/rowNormalizer.service.js';
import { buildCsvColumnOrder, createCsvSnapshotWriter, composeCsvRowData } from './upload/csvSnapshot.service.js';
import { createProgressSnapshot, normalizeStatusBucket } from './upload/progress.service.js';
import { buildResultSets } from './upload/resultBuilder.service.js';
import { normalizeDeliveryStatus, DELIVERY_STATUS } from './upload/status.utils.js';

export async function processUploadedFile({ jobId, jobDir, file, userId, onReady }) {
  markJobActive(jobId);
  const log = (msg) => appendJobLog(jobId, msg);
  log(`Job started: ${file.originalname}`);

  const baseMetadata = { jobId, userId, originalFilename: file.originalname, storedFilename: path.basename(file.path), createdAt: new Date().toISOString() };
  let metadataSnapshot = { ...baseMetadata, status: 'processing' };
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
        overrides['Domain Used'] = '';
        overrides['Notes'] = 'Pre-existing email';
      } else if (!row.contact) {
        overrides.Status = DELIVERY_STATUS.NOT_FOUND;
        overrides['Domain Used'] = '';
        overrides['Notes'] = row.skipReason || '';
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

    const updateProgress = async (status) => {
      progress.processedContacts += 1;
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
        'Domain Used': resultPayload.domainUsed || '',
        'Notes': resultPayload.notes || '',
      });
      await csvWriter.setRow(rowId, csvRow);
    };

    const enrichmentResults = contacts.length
      ? await enrichContacts(contacts, {
          jobId,
          onResult: async (result) => {
            await updateCsvRowWithResult(result);
            await updateProgress(result.status);
          },
        })
      : [];

    const { apiResults } = buildResultSets(normalizedRows, enrichmentResults);

    const completionMetadata = { ...metadataSnapshot, status: 'completed', completedAt: new Date().toISOString(), resultCount: apiResults.length };
    await writeMetadata(jobDir, completionMetadata);
    log(`Job completed: ${apiResults.length} results`);

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
