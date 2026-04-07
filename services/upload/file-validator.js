/**
 * services/upload/file-validator.js — Validates uploaded files.
 *
 * Checks file extension and row count before processing.
 * Throws an error if validation fails.
 */

const path = require('path');
const { ALLOWED_EXTENSIONS, MAX_ROWS } = require('./constants');

// Check that file extension is .csv, .xls, or .xlsx
function validateExtension(filename) {
  const ext = path.extname(filename || '').toLowerCase();
  if (!ALLOWED_EXTENSIONS.includes(ext)) {
    throw new Error('Unsupported file type. Please upload a CSV, XLS, or XLSX file.');
  }
}

// Check that row count doesn't exceed limit (if limit is set)
function enforceRowLimit(rowCount) {
  if (!Number.isFinite(MAX_ROWS)) return;
  if (rowCount > MAX_ROWS) {
    throw new Error(`Row limit exceeded. Maximum supported rows: ${MAX_ROWS}.`);
  }
}

module.exports = { validateExtension, enforceRowLimit };
