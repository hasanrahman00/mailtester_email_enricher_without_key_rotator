/**
 * services/upload/csv-columns.js — Builds output CSV column order.
 *
 * Determines which columns appear in the output CSV and in what order.
 * Email/Status/Source are inserted right after the Job Title column.
 * If no title column exists, they go after Last Name.
 */

const { OUTPUT_COLUMNS, CSV_APPEND_COLUMNS, WEBSITE_ONE_COLUMN, REPORT_COLUMNS } = require('./constants');

// Common names for the job title column in CSV files
const TITLE_NAMES = ['title', 'job title', 'jobtitle', 'job_title', 'position', 'role', 'designation'];

function buildCsvColumnOrder(headers = [], columnMap = null) {
  if (!headers.length) return [...OUTPUT_COLUMNS, ...CSV_APPEND_COLUMNS, ...REPORT_COLUMNS];

  const [FIRST, LAST, WEB] = OUTPUT_COLUMNS;
  const seen = new Set();
  const base = [];

  const push = (c) => { if (c && !seen.has(c)) { seen.add(c); base.push(c); } };

  // Map input headers to standard names
  const mapH = (h) => {
    if (columnMap?.firstNameKey === h) return FIRST;
    if (columnMap?.lastNameKey === h) return LAST;
    if (columnMap?.websiteKey === h) return WEB;
    if (columnMap?.websiteOneKey === h) return WEBSITE_ONE_COLUMN;
    return h;
  };

  headers.forEach((h) => push(mapH(h)));
  OUTPUT_COLUMNS.forEach(push);

  // Remove append + report columns from original position (we'll re-insert them)
  const clean = base.filter((c) => !CSV_APPEND_COLUMNS.includes(c) && !REPORT_COLUMNS.includes(c));

  // Find the Job Title column (try multiple common names)
  let insertAt = -1;
  for (let i = 0; i < clean.length; i++) {
    if (TITLE_NAMES.includes(clean[i].toLowerCase().trim())) {
      insertAt = i + 1; // insert AFTER the title column
      break;
    }
  }

  // Fallback: insert after Last Name
  if (insertAt === -1) {
    const lastIdx = clean.indexOf(LAST);
    insertAt = lastIdx !== -1 ? lastIdx + 1 : clean.length;
  }

  // Insert Email, Status, Source right after title (or last name)
  clean.splice(insertAt, 0, ...CSV_APPEND_COLUMNS);

  // Report columns always at the very end
  clean.push(...REPORT_COLUMNS);
  return clean;
}

module.exports = { buildCsvColumnOrder };
