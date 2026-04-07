/**
 * services/job/job-state.js — In-memory job tracking.
 *
 * Tracks which jobs are running, which have stop/pause signals,
 * and stores per-job log lines. All state lives in memory —
 * it's lost on restart, but that's fine since jobs resume via "rerun".
 */

const activeJobs = new Set();     // Currently running job IDs
const stopSignals = new Set();    // Jobs that should stop
const pauseSignals = new Set();   // Jobs that should pause
const jobLogs = new Map();        // jobId -> [log lines]

const MAX_LOGS = 500;             // Max log lines per job

// Mark a job as actively running
function markJobActive(jobId) { if (jobId) activeJobs.add(jobId); }

// Mark a job as completed (clears all signals)
function markJobComplete(jobId) {
  if (!jobId) return;
  activeJobs.delete(jobId);
  stopSignals.delete(jobId);
  pauseSignals.delete(jobId);
}

// Get all currently active job IDs
function getActiveJobIds() { return new Set(activeJobs); }

// Signal a job to stop
function requestJobStop(jobId) { if (jobId) stopSignals.add(jobId); }
function isStopRequested(jobId) { return stopSignals.has(jobId); }

// Signal a job to pause
function requestJobPause(jobId) { if (jobId) pauseSignals.add(jobId); }
function isPauseRequested(jobId) { return pauseSignals.has(jobId); }

// Add a log line to a job's log history
function appendJobLog(jobId, message) {
  if (!jobId) return;
  if (!jobLogs.has(jobId)) jobLogs.set(jobId, []);
  const lines = jobLogs.get(jobId);
  lines.push(`[${new Date().toISOString().slice(11, 23)}] ${message}`);
  if (lines.length > MAX_LOGS) lines.splice(0, lines.length - MAX_LOGS);
}

// Get all log lines for a job
function getJobLogs(jobId) { return jobLogs.get(jobId) || []; }

// Clear log history for a deleted job
function clearJobLogs(jobId) { jobLogs.delete(jobId); }

module.exports = {
  markJobActive, markJobComplete, getActiveJobIds,
  requestJobStop, isStopRequested, requestJobPause, isPauseRequested,
  appendJobLog, getJobLogs, clearJobLogs,
};
