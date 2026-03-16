// Guards uploads by enforcing file extensions and spreadsheet row limits.
import path from 'path';
import { ALLOWED_EXTENSIONS, MAX_ROWS } from './upload.constants.js';

export function validateExtension(filename) {
  const ext = path.extname(filename || '').toLowerCase();
  if (!ALLOWED_EXTENSIONS.includes(ext)) {
    throw new Error('Unsupported file type. Please upload a CSV, XLS, or XLSX file.');
  }
}

export function enforceRowLimit(rowCount) {
  if (!Number.isFinite(MAX_ROWS)) {
    return;
  }
  if (rowCount > MAX_ROWS) {
    throw new Error(`Row limit exceeded. Maximum supported rows: ${MAX_ROWS}.`);
  }
}
