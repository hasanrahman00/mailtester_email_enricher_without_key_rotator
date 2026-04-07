/**
 * services/job/report-generator.js — Generates summary report in CSV output.
 *
 * Computes stats from the final CSV rows (total profiles, emails found,
 * valid count, catch-all count, main vs waterfall source breakdown).
 * Writes these into the Report Name / Ratio Percentage columns.
 */

const { REPORT_COLUMNS } = require('../upload/constants');

// Calculate report stats from all CSV rows
function generateReport(rows) {
  let total = rows.length, found = 0, valid = 0, catchAll = 0, main = 0, waterfall = 0;

  for (const row of rows) {
    if (!(row?.Email || '').trim()) continue;
    found++;
    const status = (row.Status || '').trim().toLowerCase().replace(/[\s-]+/g, '_');
    if (status === 'valid') valid++;
    if (status === 'catch_all' || status === 'catchall') catchAll++;
    const source = (row.Source || '').trim().toLowerCase();
    if (source === 'main') main++;
    if (source === 'waterfall') waterfall++;
  }

  const pct = (n, d) => d > 0 ? `${((n / d) * 100).toFixed(1)}%` : '0.0%';

  return [
    { name: 'Total Profiles', ratio: `${total}` },
    { name: 'Emails Found', ratio: `${found}/${total} (${pct(found, total)})` },
    { name: 'Valid', ratio: `${valid}/${found} (${pct(valid, found)})` },
    { name: 'Catch-All', ratio: `${catchAll}/${found} (${pct(catchAll, found)})` },
    { name: 'Main', ratio: `${main}/${found} (${pct(main, found)})` },
    { name: 'Waterfall', ratio: `${waterfall}/${found} (${pct(waterfall, found)})` },
  ];
}

// Write report entries into the last two columns of rows 0-5
async function writeReportToRows(csvWriter) {
  const rows = csvWriter.getRows();
  const entries = generateReport(rows);
  const [nameCol, ratioCol] = REPORT_COLUMNS;

  for (let i = 0; i < entries.length && i < rows.length; i++) {
    rows[i][nameCol] = entries[i].name;
    rows[i][ratioCol] = entries[i].ratio;
    csvWriter.setRow(i, rows[i]);
  }
  await csvWriter.flushNow();
}

module.exports = { generateReport, writeReportToRows };
