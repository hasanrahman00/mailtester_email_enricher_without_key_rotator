/**
 * services/upload/csv-parser.js — Parses an existing CSV file into row objects.
 *
 * Used when rerunning a job to load previously-written results.
 * Handles quoted fields, BOM characters, and CRLF line endings.
 */

const fs = require('fs/promises');

// Parse a single CSV line (handles quoted fields with commas)
function parseCsvLine(line) {
  if (line.endsWith('\r')) line = line.slice(0, -1);
  const vals = [];
  let cur = '';
  let inQ = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQ) {
      if (ch === '"' && line[i + 1] === '"') { cur += '"'; i++; }
      else if (ch === '"') inQ = false;
      else cur += ch;
    } else {
      if (ch === '"') inQ = true;
      else if (ch === ',') { vals.push(cur); cur = ''; }
      else cur += ch;
    }
  }
  vals.push(cur);
  return vals;
}

// Read a CSV file and return an array of row objects
async function parseCsvFile(filePath) {
  try {
    let content = await fs.readFile(filePath, 'utf-8');
    if (content.charCodeAt(0) === 0xFEFF) content = content.slice(1); // strip BOM
    const lines = content.split('\n').filter((l) => l.trim());
    if (lines.length < 2) return null;
    const headers = parseCsvLine(lines[0]);
    return lines.slice(1).map((line) => {
      const vals = parseCsvLine(line);
      const row = {};
      headers.forEach((h, i) => { row[h] = vals[i] ?? ''; });
      return row;
    });
  } catch { return null; }
}

module.exports = { parseCsvFile, parseCsvLine };
