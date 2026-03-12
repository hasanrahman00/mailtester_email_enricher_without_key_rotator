import fs from 'fs/promises';
import path from 'path';
import { enrichContacts } from '../services/enricher.service.js';
import { processUploadedFile } from '../services/uploadProcessor.service.js';
import { getTempRootDir, readMetadata } from '../utils/storage.js';

/**
 * Controller to handle POST /v1/scraper/enricher/start requests.
 * Validates the input and returns enriched contact results.
 */
export async function startEnricher(req, res) {
  const { contacts } = req.body;
  if (!Array.isArray(contacts) || contacts.length === 0) {
    return res.status(400).json({ error: 'contacts array is required' });
  }
  try {
    const results = await enrichContacts(contacts);
    return res.json({ results });
  } catch (error) {
    console.error('Enricher controller error:', error);
    return res.status(500).json({ error: 'Internal server error' });
  }
}

export async function uploadContactsFile(req, res) {
  if (!req.file) {
    return res.status(400).json({ error: 'File is required.' });
  }
  if (!req.jobContext) {
    return res.status(500).json({ error: 'Upload context missing job metadata.' });
  }

  const userId = req.headers['x-user-id'] || req.body?.userId || 'anonymous';
  const jobId = req.jobContext.jobId;
  const downloadUrl = `/v1/scraper/enricher/download/${jobId}`;
  let responded = false;

  processUploadedFile({
    jobId,
    jobDir: req.jobContext.jobDir,
    file: req.file,
    userId,
    onReady: async ({ metadata }) => {
      if (responded) {
        return;
      }
      responded = true;
      res.status(202).json({
        jobId,
        status: metadata.status || 'processing',
        totals: metadata.totals || null,
        progress: metadata.progress || null,
        downloadUrl,
      });
    },
  })
    .then((payload) => {
      if (!responded) {
        responded = true;
        res.json(payload);
      }
    })
    .catch((error) => {
      console.error('Upload processing error:', error);
      if (!responded) {
        responded = true;
        res.status(400).json({ error: error.message });
      }
    });
}

export async function downloadJobResult(req, res) {
  const { jobId } = req.params;
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
    return res.download(outputPath, outputFilename);
  } catch (error) {
    console.error('Download error:', error);
    return res.status(404).json({ error: 'Result file not found.' });
  }
}