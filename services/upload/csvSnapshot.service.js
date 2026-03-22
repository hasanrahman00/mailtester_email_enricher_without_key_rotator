// Builds CSV column order and maintains an append-friendly snapshot writer for job outputs.
import fs from 'fs/promises';
import { OUTPUT_COLUMNS, CSV_APPEND_COLUMNS, WEBSITE_ONE_COLUMN, REPORT_COLUMNS } from './upload.constants.js';

export function buildCsvColumnOrder(headers = [], columnMap = null) {
  if (!Array.isArray(headers) || headers.length === 0) {
    return [...OUTPUT_COLUMNS, ...CSV_APPEND_COLUMNS, ...REPORT_COLUMNS];
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

  // Remove append columns from their original position (if the input had them)
  const withoutAppendColumns = baseColumns.filter((column) => !CSV_APPEND_COLUMNS.includes(column) && !REPORT_COLUMNS.includes(column));

  // Insert Email, Status & Source right after Title
  const titleIndex = withoutAppendColumns.indexOf('Title');
  const insertAt = titleIndex !== -1 ? titleIndex + 1 : withoutAppendColumns.length;
  withoutAppendColumns.splice(insertAt, 0, ...CSV_APPEND_COLUMNS);

  // Report Name + Ratio Percentage always go at the very end
  withoutAppendColumns.push(...REPORT_COLUMNS);

  return withoutAppendColumns;
}

export async function loadExistingCsvRows(filePath, columns) {
  try {
    let content = await fs.readFile(filePath, 'utf-8');
    // Strip UTF-8 BOM if present
    if (content.charCodeAt(0) === 0xFEFF) content = content.slice(1);
    const records = splitCsvRecords(content);
    if (records.length < 2) return null;
    const headers = parseCsvLine(records[0]);
    const rows = records.slice(1).map((record) => {
      const values = parseCsvLine(record);
      const row = {};
      headers.forEach((h, i) => { row[h] = values[i] ?? ''; });
      return row;
    });
    return rows;
  } catch {
    return null;
  }
}

/**
 * Splits raw CSV text into logical records (rows), respecting quoted fields
 * that may contain newlines. Returns an array of record strings.
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
          i++; // skip escaped quote
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
        // End of record (handle \r\n by trimming trailing \r)
        if (current.endsWith('\r')) current = current.slice(0, -1);
        if (current.trim()) records.push(current);
        current = '';
      } else {
        current += ch;
      }
    }
  }
  // Flush last record
  if (current.endsWith('\r')) current = current.slice(0, -1);
  if (current.trim()) records.push(current);

  return records;
}

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
    getRows() {
      return rows;
    },
  };
}

export function composeCsvRowData(baseRow, overrides = {}) {
  const row = { ...baseRow };
  CSV_APPEND_COLUMNS.forEach((column) => {
    row[column] = overrides[column] ?? '';
  });
  row[REPORT_COLUMNS[0]] = overrides[REPORT_COLUMNS[0]] ?? '';
  row[REPORT_COLUMNS[1]] = overrides[REPORT_COLUMNS[1]] ?? '';
  return row;
}

// ──── DEBUG: if you see this log, the UPDATED csvSnapshot is loaded ────
console.log('[csvSnapshot] ✅ UPDATED csvSnapshot.service.js loaded (BOM + CRLF version)');

function serializeCsv(columns, rows) {
  const BOM = '\uFEFF';
  console.log(`[csvSnapshot] serializeCsv called — columns: ${columns.length}, rows: ${rows.length}`);
  const headerLine = columns.map((column) => escapeCsvValue(column)).join(',');
  console.log(`[csvSnapshot] headerLine first 80 chars: ${headerLine.slice(0, 80)}`);
  console.log(`[csvSnapshot] headerLine starts with quote: ${headerLine[0] === '"'}`);
  const bodyLines = rows.map((row) => columns.map((column) => escapeCsvValue(row?.[column] ?? '')).join(','));
  const lines = [headerLine, ...bodyLines];
  const result = `${BOM}${lines.join('\r\n')}\r\n`;
  console.log(`[csvSnapshot] output byte 0 is BOM: ${result.charCodeAt(0) === 0xFEFF}`);
  return result;
}

function escapeCsvValue(value) {
  if (value === null || value === undefined) {
    return '';
  }
  // Replace embedded newlines with space — prevents multi-line fields
  const stringValue = String(value).replace(/[\r\n]+/g, ' ');
  if (/[",]/.test(stringValue)) {
    return `"${stringValue.replace(/"/g, '""')}"`;
  }
  return stringValue;
}