const activeJobs = new Set();
const stopSignals = new Set();
const jobLogs = new Map(); // jobId -> string[]

const MAX_LOG_LINES = 500;

export function markJobActive(jobId) {
  if (jobId) activeJobs.add(jobId);
}

export function markJobComplete(jobId) {
  if (jobId) {
    activeJobs.delete(jobId);
    stopSignals.delete(jobId);
  }
}

export function getActiveJobIds() {
  return new Set(activeJobs);
}

export function requestJobStop(jobId) {
  if (jobId) stopSignals.add(jobId);
}

export function isStopRequested(jobId) {
  return stopSignals.has(jobId);
}

export function appendJobLog(jobId, message) {
  if (!jobId) return;
  if (!jobLogs.has(jobId)) jobLogs.set(jobId, []);
  const lines = jobLogs.get(jobId);
  const ts = new Date().toISOString().slice(11, 23);
  lines.push(`[${ts}] ${message}`);
  if (lines.length > MAX_LOG_LINES) lines.splice(0, lines.length - MAX_LOG_LINES);
}

export function getJobLogs(jobId) {
  return jobLogs.get(jobId) || [];
}

export function clearJobLogs(jobId) {
  jobLogs.delete(jobId);
}
