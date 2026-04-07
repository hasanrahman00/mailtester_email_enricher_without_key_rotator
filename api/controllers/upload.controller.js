/**
 * api/controllers/upload.controller.js — File upload endpoint.
 *
 * POST /v1/scraper/enricher/upload
 * Accepts a CSV/XLS/XLSX file, responds immediately with jobId,
 * then processes enrichment in the background.
 */

const { processUploadedFile } = require('../../services/job/job-processor');
const { validateExtension } = require('../../services/upload/file-validator');

async function uploadContactsFile(req, res) {
  // Validate file was provided
  if (!req.file) return res.status(400).json({ error: 'File is required.' });
  if (!req.jobContext) return res.status(500).json({ error: 'Upload context missing.' });

  // Quick extension check before responding
  try { validateExtension(req.file.originalname); }
  catch (err) { return res.status(400).json({ error: err.message }); }

  const userId = req.headers['x-user-id'] || req.body?.userId || 'anonymous';
  const jobId = req.jobContext.jobId;

  // Respond immediately — UI shows the job row right away
  res.status(202).json({
    jobId,
    status: 'processing',
    downloadUrl: `/v1/scraper/enricher/download/${jobId}`,
  });

  // Process in background (errors are logged, not thrown)
  processUploadedFile({
    jobId,
    jobDir: req.jobContext.jobDir,
    file: req.file,
    userId,
  }).catch((err) => console.error(`Upload error for ${jobId}:`, err.message));
}

module.exports = { uploadContactsFile };
