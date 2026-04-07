/**
 * services/upload/csv-writer.js — Creates and updates the output CSV file.
 *
 * Provides a snapshot writer that can update rows one-at-a-time
 * and flush the entire CSV to disk. Used for real-time progress.
 */

const fs = require('fs/promises');
const { serializeCsv } = require('./csv-serializer');
const { CSV_APPEND_COLUMNS, REPORT_COLUMNS } = require('./constants');
const { buildCsvColumnOrder } = require('./csv-columns');
const { parseCsvFile } = require('./csv-parser');

// Create a writer that can update individual rows and save to disk
function createCsvSnapshotWriter(filePath, columns, initialRows) {
  const rows = initialRows.slice();
  let writeQueue = Promise.resolve();
  const flush = () => {
    const data = serializeCsv(columns, rows);
    writeQueue = writeQueue.then(() => fs.writeFile(filePath, data, 'utf-8'));
    return writeQueue;
  };
  return {
    writeSnapshot: flush,
    async setRow(rowId, newRow) { rows[rowId] = newRow; await flush(); },
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
