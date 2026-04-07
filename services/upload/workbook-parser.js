/**
 * services/upload/workbook-parser.js — Parses uploaded spreadsheet files.
 *
 * Reads CSV/XLS/XLSX files using the xlsx library.
 * Auto-detects the header row by looking for First Name, Last Name, Website.
 * Returns { rows, headers, headerRowIndex }.
 */

const XLSX = require('xlsx');
const { COLUMN_ALIASES } = require('./constants');

// Normalize a string for comparison (lowercase, no special chars)
function normalizeKey(value) {
  return String(value || '').toLowerCase().replace(/[^a-z0-9]/g, '');
}

// Check if a row of cells contains all required column aliases
function containsAlias(normalizedCells, aliasList) {
  return aliasList.some((alias) => normalizedCells.includes(normalizeKey(alias)));
}

// Find which row in the matrix is the header row
function detectHeaderRow(matrix) {
  for (let i = 0; i < matrix.length; i++) {
    const row = matrix[i];
    if (!Array.isArray(row)) continue;
    const cells = row.map((c) => normalizeKey(c));
    if (containsAlias(cells, COLUMN_ALIASES.firstName) &&
        containsAlias(cells, COLUMN_ALIASES.lastName) &&
        containsAlias(cells, COLUMN_ALIASES.website)) {
      const headers = row.map((c, idx) => String(c || '').trim() || `column_${idx + 1}`);
      return { headerRowIndex: i, headers };
    }
  }
  throw new Error('Could not locate required columns (First Name, Last Name, Website).');
}

// Main parser: read file, find headers, return data rows
async function parseWorkbook(filePath) {
  const workbook = XLSX.readFile(filePath, { cellDates: false });
  if (!workbook.SheetNames.length) throw new Error('File has no sheets.');
  const sheet = workbook.Sheets[workbook.SheetNames[0]];
  const matrix = XLSX.utils.sheet_to_json(sheet, { header: 1, defval: '' });
  if (!matrix.length) throw new Error('File is empty.');
  const { headerRowIndex, headers } = detectHeaderRow(matrix);
  const dataRows = matrix.slice(headerRowIndex + 1)
    .filter((row) => Array.isArray(row) && row.some((c) => String(c || '').trim()));
  if (!dataRows.length) throw new Error('No data rows found under the header.');
  return { rows: dataRows, headers, headerRowIndex };
}

module.exports = { parseWorkbook, detectHeaderRow, normalizeKey };
