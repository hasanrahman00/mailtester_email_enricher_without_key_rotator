/**
 * services/upload/csv-writer.js — Creates and updates the output CSV file.
 *
 * Provides a snapshot writer that can update rows one-at-a-time.
 * Row updates are batched in memory and flushed to disk periodically
 * (every FLUSH_INTERVAL_MS) to avoid blocking API workers.
 *
 * A 47K-row CSV is ~50MB — rewriting it on every single row update
 * would block the pipeline for minutes. Batched flushing solves this.
 */

const fs = require('fs/promises');
const { serializeCsv } = require('./csv-serializer');
const { CSV_APPEND_COLUMNS, REPORT_COLUMNS } = require('./constants');
const { buildCsvColumnOrder } = require('./csv-columns');
const { parseCsvFile } = require('./csv-parser');

const FLUSH_INTERVAL_MS = 2000; // flush to disk at most every 2s

// Create a writer that can update individual rows and save to disk
function createCsvSnapshotWriter(filePath, columns, initialRows) {
  const rows = initialRows.slice();
  let writeQueue = Promise.resolve();
  let dirty = false;
  let flushTimer = null;

  // Serialize and write the full CSV to disk
  const flush = () => {
    if (!dirty) return writeQueue;
    dirty = false;
    const data = serializeCsv(columns, rows);
    writeQueue = writeQueue.then(() => fs.writeFile(filePath, data, 'utf-8')).catch(() => {});
    return writeQueue;
  };

  // Schedule a deferred flush (batches many row updates into one disk write)
  const scheduleFlush = () => {
    dirty = true;
    if (!flushTimer) {
      flushTimer = setTimeout(() => { flushTimer = null; flush(); }, FLUSH_INTERVAL_MS);
    }
  };

  return {
    // Full snapshot write (used for initial write / final write)
    writeSnapshot() { dirty = true; return flush(); },

    // Update one row — instant in-memory update, periodic disk flush
    setRow(rowId, newRow) { rows[rowId] = newRow; scheduleFlush(); },

    // Force flush to disk now (call before exiting or between phases)
    flushNow() {
      if (flushTimer) { clearTimeout(flushTimer); flushTimer = null; }
      dirty = true;
      return flush();
    },

    getRows: () => rows,
  };
}

// Merge a base row with enrichment overrides (Email, Status, Source)
function composeCsvRowData(baseRow, overrides = {}) {
  const row = { ...baseRow };
  CSV_APPEND_COLUMNS.forEach((c) => { row[c] = overrides[c] ?? ''; });
  row[REPORT_COLUMNS[0]] = overrides[REPORT_COLUMNS[0]] ?? '';
  row[REPORT_COLUMNS[1]] = overrides[REPORT_COLUMNS[1]] ?? '';
  return row;
}

// Load existing CSV rows from a file (for rerun support)
async function loadExistingCsvRows(filePath) {
  return parseCsvFile(filePath);
}

module.exports = { buildCsvColumnOrder, createCsvSnapshotWriter, composeCsvRowData, loadExistingCsvRows };
