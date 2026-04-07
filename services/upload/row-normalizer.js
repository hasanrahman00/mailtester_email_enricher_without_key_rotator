/**
 * services/upload/row-normalizer.js — Converts raw rows into clean contacts.
 *
 * Uses row-sanitizer for individual rows and column-resolver for headers.
 * Handles multi-section files (files with multiple header rows).
 */

const { COLUMN_ALIASES } = require('./constants');
const { normalizeKey } = require('./workbook-parser');
const { resolveColumns } = require('./column-resolver');
const { sanitizeRow } = require('./row-sanitizer');

// Check if a raw row array is actually another header row
function isHeaderRow(rowValues) {
  if (!Array.isArray(rowValues)) return false;
  const norm = rowValues.map((v) => normalizeKey(v));
  return COLUMN_ALIASES.firstName.some((a) => norm.includes(normalizeKey(a)))
    && COLUMN_ALIASES.lastName.some((a) => norm.includes(normalizeKey(a)))
    && COLUMN_ALIASES.website.some((a) => norm.includes(normalizeKey(a)));
}

// Process all rows into normalized contact objects
function normalizeRows(rows, initialColMap, headerRowIndex, initialHeaders) {
  const normalized = [];
  let headers = [...initialHeaders];
  let colMap = { ...initialColMap };
  let rowCounter = 0;

  rows.forEach((rowValues, index) => {
    // If this row looks like a header, re-resolve columns
    if (isHeaderRow(rowValues)) {
      headers = rowValues.map((c, i) => String(c || '').trim() || `column_${i + 1}`);
      colMap = resolveColumns(headers);
      return;
    }
    // Convert array to object using headers
    const rowObj = headers.reduce((acc, h, i) => { acc[h] = rowValues[i] ?? ''; return acc; }, {});
    const result = sanitizeRow(rowObj, colMap);
    if (!result) return;

    normalized.push({
      rowId: rowCounter++,
      rowNumber: headerRowIndex + 2 + index,
      ...result,
    });
  });

  return normalized;
}

module.exports = { normalizeRows };
