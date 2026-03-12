// Reads the spreadsheet, detects headers, and returns normalized row matrices for downstream steps.
import XLSX from 'xlsx';
import { COLUMN_ALIASES } from './upload.constants.js';
import { normalizeKey } from './normalization.utils.js';

export async function parseWorkbook(filePath) {
  try {
    const workbook = XLSX.readFile(filePath, { cellDates: false });
    if (!workbook.SheetNames.length) {
      throw new Error('Uploaded file does not contain any sheets.');
    }
    const worksheet = workbook.Sheets[workbook.SheetNames[0]];
    const matrix = XLSX.utils.sheet_to_json(worksheet, { header: 1, defval: '' });
    if (!matrix.length) {
      throw new Error('Uploaded file is empty.');
    }
    const { headerRowIndex, headers } = detectHeaderRow(matrix);
    const dataRows = matrix
      .slice(headerRowIndex + 1)
      .filter((row) => Array.isArray(row) && row.some((cell) => String(cell || '').trim() !== ''));
    if (!dataRows.length) {
      throw new Error('Uploaded file does not contain any data rows under the header.');
    }
    return { rows: dataRows, headers, headerRowIndex };
  } catch (error) {
    throw new Error(`Failed to parse uploaded file: ${error.message}`);
  }
}

export function detectHeaderRow(matrix) {
  for (let rowIndex = 0; rowIndex < matrix.length; rowIndex += 1) {
    const row = matrix[rowIndex];
    if (!Array.isArray(row)) {
      continue;
    }
    const normalizedCells = row.map((cell) => normalizeKey(cell));
    if (
      containsAlias(normalizedCells, COLUMN_ALIASES.firstName) &&
      containsAlias(normalizedCells, COLUMN_ALIASES.lastName) &&
      containsAlias(normalizedCells, COLUMN_ALIASES.website)
    ) {
      const headers = row.map((cell, idx) => (String(cell || '').trim() ? String(cell) : `column_${idx + 1}`));
      return { headerRowIndex: rowIndex, headers };
    }
  }
  throw new Error('Could not locate required columns (First Name, Last Name, Website). Ensure the file contains a header row.');
}

function containsAlias(normalizedCells, aliasList) {
  return aliasList.some((alias) => normalizedCells.includes(normalizeKey(alias)));
}
