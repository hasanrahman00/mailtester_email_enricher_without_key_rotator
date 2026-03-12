// Builds CSV column order and maintains an append-friendly snapshot writer for job outputs.
import fs from 'fs/promises';
import { OUTPUT_COLUMNS, CSV_APPEND_COLUMNS, WEBSITE_ONE_COLUMN } from './upload.constants.js';

export function buildCsvColumnOrder(headers = [], columnMap = null) {
  if (!Array.isArray(headers) || headers.length === 0) {
    return [...OUTPUT_COLUMNS, ...CSV_APPEND_COLUMNS];
  }

  const [FIRST_NAME_COLUMN, LAST_NAME_COLUMN, WEBSITE_COLUMN] = OUTPUT_COLUMNS;
  const baseColumns = [];
  const seen = new Set();

  const pushColumn = (column) => {
    if (!column || seen.has(column)) {
      return;
    }
    seen.add(column);
    baseColumns.push(column);
  };

  const mapHeader = (header) => {
    if (columnMap?.firstNameKey && header === columnMap.firstNameKey) {
      return FIRST_NAME_COLUMN;
    }
    if (columnMap?.lastNameKey && header === columnMap.lastNameKey) {
      return LAST_NAME_COLUMN;
    }
    if (columnMap?.websiteKey && header === columnMap.websiteKey) {
      return WEBSITE_COLUMN;
    }
    if (columnMap?.websiteOneKey && header === columnMap.websiteOneKey) {
      return WEBSITE_ONE_COLUMN;
    }
    return header;
  };

  headers.forEach((header) => pushColumn(mapHeader(header)));
  OUTPUT_COLUMNS.forEach((column) => pushColumn(column));

  const withoutAppendColumns = baseColumns.filter((column) => !CSV_APPEND_COLUMNS.includes(column));
  return [...withoutAppendColumns, ...CSV_APPEND_COLUMNS];
}

export function createCsvSnapshotWriter(filePath, columns, initialRows) {
  let rows = initialRows.slice();
  let writeQueue = Promise.resolve();

  const scheduleWrite = () => {
    const payload = serializeCsv(columns, rows);
    writeQueue = writeQueue.then(() => fs.writeFile(filePath, payload, 'utf-8'));
    return writeQueue;
  };

  return {
    async writeSnapshot() {
      await scheduleWrite();
    },
    async setRow(rowId, newRow) {
      rows[rowId] = newRow;
      await scheduleWrite();
    },
  };
}

export function composeCsvRowData(baseRow, overrides = {}) {
  const row = { ...baseRow };
  CSV_APPEND_COLUMNS.forEach((column) => {
    row[column] = overrides[column] ?? '';
  });
  return row;
}

function serializeCsv(columns, rows) {
  const headerLine = columns.map((column) => escapeCsvValue(column)).join(',');
  const bodyLines = rows.map((row) => columns.map((column) => escapeCsvValue(row?.[column] ?? '')).join(','));
  const lines = [headerLine, ...bodyLines];
  return `${lines.join('\n')}\n`;
}

function escapeCsvValue(value) {
  if (value === null || value === undefined) {
    return '';
  }
  const stringValue = String(value);
  if (/[",\n\r]/.test(stringValue)) {
    return `"${stringValue.replace(/"/g, '""')}"`;
  }
  return stringValue;
}
