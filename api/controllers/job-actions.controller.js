/**
 * api/controllers/job-actions.controller.js — Job stop, pause, rerun, delete, logs.
 *
 * POST /jobs/:jobId/stop   — Signal a running job to stop
 * POST /jobs/:jobId/pause  — Signal a running job to pause
 * POST /jobs/:jobId/rerun  — Restart a stopped/paused job
 * DELETE /jobs/:jobId       — Delete a job and its files
 * GET /jobs/:jobId/logs     — Get job log lines
 */

const path = require('path');
const { getTempRootDir, readMetadata, writeMetadata, removeDirectory } = require('../../utils/storage');
const { requestJobStop, requestJobPause, getJobLogs, clearJobLogs, getActiveJobIds } = require('../../services/job/job-state');
const { rerunJob } = require('../../services/job/job-rerunner');

async function stopJob(req, res) {
  const { jobId } = req.params;
  requestJobStop(jobId);
  const root = await getTempRootDir();
  const meta = await readMetadata(path.join(root, jobId));
  // Write transitional status — finalizeJob writes the final 'stop' once workers drain
  if (meta?.status === 'run') await writeMetadata(path.join(root, jobId), { ...meta, status: 'stopping' });
  res.json({ message: `Stop signal sent to ${jobId}` });
}

async function pauseJob(req, res) {
  const { jobId } = req.params;
  requestJobPause(jobId);
  const root = await getTempRootDir();
  const meta = await readMetadata(path.join(root, jobId));
  // Write transitional status — finalizeJob writes the final 'pause' once workers drain
  if (meta?.status === 'run') await writeMetadata(path.join(root, jobId), { ...meta, status: 'pausing' });
  res.json({ message: `Pause signal sent to ${jobId}` });
}

async function rerunJobController(req, res) {
  const { jobId } = req.params;
  if (getActiveJobIds().has(jobId)) return res.status(409).json({ error: 'Job is already running' });
  const root = await getTempRootDir();
  const jobDir = path.join(root, jobId);
  const meta = await readMetadata(jobDir);
  if (!meta) return res.status(404).json({ error: 'Job not found' });
  if (!meta.storedFilename) return res.status(400).json({ error: 'Original file not available' });
  rerunJob({ jobId, jobDir }).catch((e) => console.error(`Rerun failed: ${e.message}`));
  res.status(202).json({ message: 'Rerun started', jobId });
}

async function deleteJob(req, res) {
  const { jobId } = req.params;
  if (getActiveJobIds().has(jobId)) requestJobStop(jobId);
  const root = await getTempRootDir();
  await removeDirectory(path.join(root, jobId));
  clearJobLogs(jobId);
  res.json({ message: `Job ${jobId} deleted` });
}

async function fetchJobLogs(req, res) {
  const logs = getJobLogs(req.params.jobId);
  res.json({ jobId: req.params.jobId, logs, count: logs.length });
}

module.exports = { stopJob, pauseJob, rerunJobController, deleteJob, fetchJobLogs };
