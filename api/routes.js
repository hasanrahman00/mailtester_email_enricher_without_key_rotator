/**
 * api/routes.js — All API route definitions.
 *
 * Maps HTTP endpoints to controllers.
 * All routes prefixed with /v1/scraper/enricher/.
 */

const express = require('express');
const { startEnricher } = require('./controllers/enricher.controller');
const { uploadContactsFile } = require('./controllers/upload.controller');
const { downloadJobResult } = require('./controllers/download.controller');
const { listJobs, getJobStatus } = require('./controllers/job.controller');
const { stopJob, pauseJob, rerunJobController, deleteJob, fetchJobLogs } = require('./controllers/job-actions.controller');
const { prepareJobContext, uploadSingleFile } = require('./middleware/upload.middleware');
const { getKeyScheduler } = require('../clients/key-scheduler');

const router = express.Router();

// Enrichment endpoints
router.post('/v1/scraper/enricher/start', startEnricher);
router.post('/v1/scraper/enricher/upload', prepareJobContext, uploadSingleFile, uploadContactsFile);
router.get('/v1/scraper/enricher/download/:jobId', downloadJobResult);

// Job management endpoints
router.get('/v1/scraper/enricher/jobs', listJobs);
router.get('/v1/scraper/enricher/jobs/:jobId', getJobStatus);
router.post('/v1/scraper/enricher/jobs/:jobId/stop', stopJob);
router.post('/v1/scraper/enricher/jobs/:jobId/pause', pauseJob);
router.post('/v1/scraper/enricher/jobs/:jobId/rerun', rerunJobController);
router.delete('/v1/scraper/enricher/jobs/:jobId', deleteJob);
router.get('/v1/scraper/enricher/jobs/:jobId/logs', fetchJobLogs);

// API key health check
router.get('/v1/scraper/enricher/key-status', (_req, res) => {
  try {
    const s = getKeyScheduler();
    const status = s.getStatus();
    const rate = s.getRate();
    const rps = status.reduce((sum, k) => sum + 1000 / k.intervalMs, 0);
    res.json({
      keys: status,
      rate,
      combined: { maxReqPerSec: Math.round(rps * 10) / 10, maxReqPerMin: Math.round(rps * 60), totalDailyRemaining: status.reduce((sum, k) => sum + k.dailyRemaining, 0) },
    });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

module.exports = router;
