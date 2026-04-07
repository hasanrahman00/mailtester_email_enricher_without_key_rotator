/**
 * api/controllers/download.controller.js — Download enriched results.
 *
 * GET /v1/scraper/enricher/download/:jobId          → xlsx (default)
 * GET /v1/scraper/enricher/download/:jobId?format=csv → csv
 *
 * The xlsx version has styled rows:
 *   Header  → dark blue, white bold text
 *   Valid   → green background
 *   Catch-All → amber background
 *   Other   → gray background
 */

const fs = require('fs/promises');
const path = require('path');
const { getTempRootDir, readMetadata } = require('../../utils/storage');
const { parseCsvFile } = require('../../services/upload/csv-parser');
const { buildXlsx } = require('../../services/upload/xlsx-builder');

async function downloadJobResult(req, res) {
  const { jobId } = req.params;
  const format = (req.query.format || 'xlsx').toLowerCase();

  try {
    const root = await getTempRootDir();
    const jobDir = path.join(root, jobId);
    const metadata = await readMetadata(jobDir);
    const outputFilename = metadata?.outputFilename;

    if (!outputFilename) {
      return res.status(404).json({ error: 'Result file not found.' });
    }

    const outputPath = path.join(jobDir, outputFilename);
    await fs.access(outputPath);

    // Build download filename from original upload name
    const originalName = metadata.originalFilename || outputFilename;
    const baseName = path.basename(originalName, path.extname(originalName));

    // ── CSV download ──
    if (format === 'csv') {
      return res.download(outputPath, `${baseName}.csv`);
    }

    // ── XLSX download (default) — styled with color-coded rows ──
    const rows = await parseCsvFile(outputPath);
    if (!rows || !rows.length) {
      return res.download(outputPath, `${baseName}.csv`);
    }

    // Get column order from the first row's keys
    const columns = Object.keys(rows[0]);
    const buffer = await buildXlsx(columns, rows);

    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', `attachment; filename="${baseName}.xlsx"`);
    return res.send(Buffer.from(buffer));
  } catch (err) {
    console.error('Download error:', err);
    return res.status(404).json({ error: 'Result file not found.' });
  }
}

module.exports = { downloadJobResult };
