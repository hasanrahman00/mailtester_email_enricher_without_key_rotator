import express from 'express';
import { startEnricher, uploadContactsFile, downloadJobResult } from '../controllers/enricher.controller.js';
import { getJobStatus, listJobs, stopJob, pauseJob, rerunJobController, deleteJob, fetchJobLogs } from '../controllers/job.controller.js';
import { prepareJobContext, uploadSingleFile } from '../middlewares/jobUpload.middleware.js';
import { getKeyScheduler } from '../clients/keyManager.js';

const router = express.Router();

router.post('/v1/scraper/enricher/start', startEnricher);
router.post('/v1/scraper/enricher/upload', prepareJobContext, uploadSingleFile, uploadContactsFile);
router.get('/v1/scraper/enricher/download/:jobId', downloadJobResult);
router.get('/v1/scraper/enricher/jobs', listJobs);
router.get('/v1/scraper/enricher/jobs/:jobId', getJobStatus);
router.post('/v1/scraper/enricher/jobs/:jobId/stop', stopJob);
router.post('/v1/scraper/enricher/jobs/:jobId/pause', pauseJob);
router.post('/v1/scraper/enricher/jobs/:jobId/rerun', rerunJobController);
router.delete('/v1/scraper/enricher/jobs/:jobId', deleteJob);
router.get('/v1/scraper/enricher/jobs/:jobId/logs', fetchJobLogs);

router.get('/v1/scraper/enricher/key-status', (_req, res) => {
  try {
    const scheduler = getKeyScheduler();
    const status = scheduler.getStatus();
    const rps = status.reduce((s, k) => s + 1000 / k.intervalMs, 0);
    res.json({
      keys: status,
      combined: { maxReqPerSec: Math.round(rps * 10) / 10, maxReqPerMin: Math.round(rps * 60), totalDailyRemaining: status.reduce((s, k) => s + k.dailyRemaining, 0) },
    });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

export default router;
