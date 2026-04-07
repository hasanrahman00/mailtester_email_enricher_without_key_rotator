/**
 * services/upload/xlsx-builder.js — Builds a styled .xlsx file from CSV data.
 *
 * Sheet 1 "Results" — contact data with color-coded rows:
 *   Header    → dark blue background, white bold text
 *   Valid     → light green background
 *   Catch-All → light amber/yellow background
 *   Other     → light gray background
 *
 * Sheet 2 "Summary" — report stats (Total Profiles, Emails Found, etc.)
 *
 * Report Name / Ratio Percentage columns are excluded from the Results sheet
 * and shown only in the Summary sheet for a cleaner layout.
 */

const ExcelJS = require('exceljs');
const { REPORT_COLUMNS } = require('./constants');

// ── Color palette ──
const COLORS = {
  headerBg: '1F2937',      // dark blue-gray
  headerFont: 'FFFFFF',    // white
  validBg: 'DCFCE7',       // light green
  catchAllBg: 'FEF3C7',    // light amber
  otherBg: 'F3F4F6',       // light gray
  borderColor: 'D1D5DB',   // gray border
  summaryBg: 'EFF6FF',     // light blue for summary values
};

const DATA_FONT = { name: 'Calibri', size: 11 };
const HEADER_FONT = { name: 'Calibri', size: 11, bold: true, color: { argb: COLORS.headerFont } };
const SUMMARY_LABEL_FONT = { name: 'Calibri', size: 12, bold: true };
const SUMMARY_VALUE_FONT = { name: 'Calibri', size: 12 };

const THIN_BORDER = {
  top: { style: 'thin', color: { argb: COLORS.borderColor } },
  bottom: { style: 'thin', color: { argb: COLORS.borderColor } },
  left: { style: 'thin', color: { argb: COLORS.borderColor } },
  right: { style: 'thin', color: { argb: COLORS.borderColor } },
};

// Row background based on Status value
function getRowFill(status) {
  const s = (status || '').trim().toLowerCase();
  if (s === 'valid') return { type: 'pattern', pattern: 'solid', fgColor: { argb: COLORS.validBg } };
  if (s === 'catch_all') return { type: 'pattern', pattern: 'solid', fgColor: { argb: COLORS.catchAllBg } };
  if (s) return { type: 'pattern', pattern: 'solid', fgColor: { argb: COLORS.otherBg } };
  return null;
}

/**
 * Build a styled xlsx Buffer from columns + row objects.
 * @param {string[]} columns - Column headers in order
 * @param {object[]} rows    - Array of { [column]: value } objects
 * @returns {Promise<Buffer>} xlsx file as a Buffer
 */
async function buildXlsx(columns, rows) {
  const wb = new ExcelJS.Workbook();
  wb.creator = 'Email Enricher';

  // ── Extract report data from rows before building sheets ──
  const [reportNameCol, reportRatioCol] = REPORT_COLUMNS;
  const reportEntries = [];
  for (const row of rows) {
    const name = (row[reportNameCol] || '').trim();
    const ratio = (row[reportRatioCol] || '').trim();
    if (name) reportEntries.push({ name, ratio });
  }

  // ── Filter out Report columns from the Results sheet ──
  const resultCols = columns.filter((c) => !REPORT_COLUMNS.includes(c));

  // ════════════════════════════════════════════
  // Sheet 1: Results (contact data, color-coded)
  // ════════════════════════════════════════════
  const ws = wb.addWorksheet('Results');
  const statusIdx = resultCols.indexOf('Status');

  // Header row
  const headerRow = ws.addRow(resultCols);
  headerRow.eachCell((cell) => {
    cell.font = HEADER_FONT;
    cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: COLORS.headerBg } };
    cell.border = THIN_BORDER;
    cell.alignment = { vertical: 'middle', horizontal: 'center' };
  });
  headerRow.height = 22;

  // Data rows
  for (const row of rows) {
    const values = resultCols.map((col) => row?.[col] ?? '');
    const dataRow = ws.addRow(values);
    const status = statusIdx >= 0 ? values[statusIdx] : '';
    const fill = getRowFill(status);

    dataRow.eachCell({ includeEmpty: true }, (cell) => {
      cell.font = DATA_FONT;
      cell.border = THIN_BORDER;
      cell.alignment = { vertical: 'middle' };
      if (fill) cell.fill = fill;
    });
  }

  // Auto-fit column widths
  ws.columns.forEach((col, i) => {
    const header = resultCols[i] || '';
    let maxLen = header.length;
    for (const row of rows) {
      const val = String(row?.[header] ?? '');
      if (val.length > maxLen) maxLen = val.length;
    }
    col.width = Math.min(40, Math.max(10, maxLen + 3));
  });

  // Freeze header + auto-filter
  ws.views = [{ state: 'frozen', ySplit: 1 }];
  ws.autoFilter = { from: { row: 1, column: 1 }, to: { row: rows.length + 1, column: resultCols.length } };

  // ════════════════════════════════════════════
  // Sheet 2: Summary (report stats)
  // ════════════════════════════════════════════
  if (reportEntries.length) {
    const ss = wb.addWorksheet('Summary');

    // Header
    const sumHeader = ss.addRow(['Report Name', 'Ratio / Percentage']);
    sumHeader.eachCell((cell) => {
      cell.font = HEADER_FONT;
      cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: COLORS.headerBg } };
      cell.border = THIN_BORDER;
      cell.alignment = { vertical: 'middle', horizontal: 'center' };
    });
    sumHeader.height = 22;

    // Report entries
    for (const entry of reportEntries) {
      const r = ss.addRow([entry.name, entry.ratio]);
      r.getCell(1).font = SUMMARY_LABEL_FONT;
      r.getCell(1).border = THIN_BORDER;
      r.getCell(2).font = SUMMARY_VALUE_FONT;
      r.getCell(2).border = THIN_BORDER;
      r.getCell(2).fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: COLORS.summaryBg } };
      r.getCell(2).alignment = { horizontal: 'right' };
    }

    ss.getColumn(1).width = 22;
    ss.getColumn(2).width = 30;
  }

  return wb.xlsx.writeBuffer();
}

module.exports = { buildXlsx };
