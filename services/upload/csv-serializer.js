/**
 * services/upload/csv-serializer.js — Converts rows to a CSV string.
 *
 * Handles escaping commas, quotes, and newlines in cell values.
 * Adds UTF-8 BOM for Excel compatibility and uses CRLF line endings.
 */

// Escape a single CSV cell value
function escapeCsvValue(value) {
  if (value === null || value === undefined) return '';
  // Replace embedded newlines with space
  const str = String(value).replace(/[\r\n]+/g, ' ');
  // Wrap in quotes if it contains commas or quotes
  if (/[",]/.test(str)) return `"${str.replace(/"/g, '""')}"`;
  return str;
}

/**
 * Serialize columns + rows into a full CSV string.
 * Includes BOM for Excel UTF-8 support and CRLF line endings.
 */
function serializeCsv(columns, rows) {
  const BOM = '\uFEFF';
  const header = columns.map(escapeCsvValue).join(',');
  const body = rows.map((row) =>
    columns.map((col) => escapeCsvValue(row?.[col] ?? '')).join(',')
  );
  return `${BOM}${[header, ...body].join('\r\n')}\r\n`;
}

module.exports = { serializeCsv, escapeCsvValue };
