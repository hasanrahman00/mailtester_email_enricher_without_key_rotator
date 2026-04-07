/**
 * api/controllers/job.controller.js — Job listing and status endpoints.
 *
 * GET /jobs        — List all jobs with their current status
 * GET /jobs/:jobId — Get a single job's full metadata
 */

const path = require('path');
const { getTempRootDir, listJobDirectories, readMetadata } = require('../../utils/storage');
const { isStopRequested, isPauseRequested, getActiveJobIds } = require('../../services/job/job-state');

// List all jobs, sorted by creation date (newest first)
async function listJobs(req, res) {
  try {
    const limit = Math.max(1, Math.min(Number(req.query.limit) || 50, 200));
    const dirs = await listJobDirectories();
    const metas = await Promise.all(dirs.map(readMetadata));
    const activeIds = getActiveJobIds();
    const seen = new Set();

    const jobs = metas.filter(Boolean).map((m) => {
      seen.add(m.jobId);
      const liveStatus = activeIds.has(m.jobId)
        ? (isStopRequested(m.jobId) ? 'stopping' : isPauseRequested(m.jobId) ? 'pausing' : 'run')
        : m.status || 'run';
      return {
        jobId: m.jobId, userId: m.userId, originalFilename: m.originalFilename,
        status: liveStatus, createdAt: m.createdAt, completedAt: m.completedAt || null,
        totals: m.totals || null, progress: m.progress || null, downloadUrl: m.downloadUrl || null,
        resultCount: typeof m.resultCount === 'number' ? m.resultCount : m.progress?.processedContacts || 0,
      };
    });

    // Add placeholders for active jobs not yet on disk
    for (const id of activeIds) {
      if (!seen.has(id)) {
        jobs.push({ jobId: id, originalFilename: 'Processing...', status: 'run', createdAt: new Date().toISOString() });
      }
    }

    jobs.sort((a, b) => new Date(b.createdAt || 0) - new Date(a.createdAt || 0));
    res.json({ jobs: jobs.slice(0, limit) });
  } catch { res.status(500).json({ error: 'Unable to load jobs' }); }
}

// Get a single job's metadata
async function getJobStatus(req, res) {
  try {
    const root = await getTempRootDir();
    const meta = await readMetadata(path.join(root, req.params.jobId));
    if (!meta) return res.status(404).json({ error: 'Job not found' });
    res.json(meta);
  } catch { res.status(500).json({ error: 'Unable to fetch job' }); }
}

module.exports = { listJobs, getJobStatus };
