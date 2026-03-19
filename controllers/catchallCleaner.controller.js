import {
  startCatchAllCleaner,
  requestCleanerStop,
  getCleanerState,
  getAllCleanerStates,
  getCleanerLogs,
  isCleanerRunning,
} from '../services/bounceban/catchallCleaner.service.js';
import { isConfigured } from '../services/bounceban/bounceban.client.js';

/**
 * POST /v1/scraper/enricher/jobs/:jobId/catchall-cleaner/run
 */
export async function startCleaner(req, res) {
  const { jobId } = req.params;
  if (!jobId) return res.status(400).json({ error: 'jobId required' });

  if (!isConfigured()) {
    return res.status(500).json({ error: 'BOUNCEBAN_API_KEY not configured in .env' });
  }

  if (isCleanerRunning(jobId)) {
    return res.status(409).json({ error: 'Catch-all cleaner already running for this job' });
  }

  // Fire and forget — respond immediately
  res.status(202).json({ message: 'Catch-all cleaner started', jobId });

  startCatchAllCleaner(jobId).catch((err) => {
    console.error(`[CatchAll] Error for job ${jobId}:`, err.message);
  });
}

/**
 * POST /v1/scraper/enricher/jobs/:jobId/catchall-cleaner/stop
 */
export async function stopCleaner(req, res) {
  const { jobId } = req.params;
  if (!jobId) return res.status(400).json({ error: 'jobId required' });

  requestCleanerStop(jobId);
  return res.json({ message: `Stop signal sent to catch-all cleaner for ${jobId}` });
}

/**
 * GET /v1/scraper/enricher/jobs/:jobId/catchall-cleaner/status
 */
export async function cleanerStatus(req, res) {
  const { jobId } = req.params;
  if (!jobId) return res.status(400).json({ error: 'jobId required' });

  const state = getCleanerState(jobId);
  if (!state) {
    return res.json({ jobId, status: 'idle', counts: null, logs: [] });
  }

  return res.json({
    jobId,
    status: state.stop && state.status === 'running' ? 'stopping' : state.status,
    counts: { ...state.counts },
    logs: state.logs.slice(-200),
  });
}

/**
 * GET /v1/scraper/enricher/jobs/:jobId/catchall-cleaner/logs
 */
export async function cleanerLogs(req, res) {
  const { jobId } = req.params;
  if (!jobId) return res.status(400).json({ error: 'jobId required' });

  const logs = getCleanerLogs(jobId);
  return res.json({ jobId, logs: logs.slice(-500) });
}

/**
 * GET /v1/scraper/enricher/catchall-cleaner/states
 */
export async function allCleanerStates(_req, res) {
  return res.json({ states: getAllCleanerStates() });
}

/**
 * GET /v1/scraper/enricher/bounceban-status
 */
export async function bounceBanStatus(_req, res) {
  return res.json({
    configured: isConfigured(),
  });
}