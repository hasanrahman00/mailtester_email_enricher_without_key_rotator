import path from 'path';
import { getTempRootDir, listJobDirectories, readMetadata, removeDirectory } from '../utils/storage.js';
import { requestJobStop, getJobLogs, clearJobLogs, getActiveJobIds } from '../services/jobState.service.js';

export async function listJobs(req, res) {
  const limit = Math.max(1, Math.min(Number(req.query.limit) || 50, 200));
  try {
    const jobDirs = await listJobDirectories();
    const metadataEntries = await Promise.all(jobDirs.map((dir) => readMetadata(dir)));
    const activeIds = getActiveJobIds();
    const jobs = metadataEntries.filter(Boolean).map((meta) => ({
      jobId: meta.jobId, userId: meta.userId, originalFilename: meta.originalFilename,
      status: activeIds.has(meta.jobId) && meta.status === 'processing' ? 'processing' : meta.status || 'processing',
      createdAt: meta.createdAt, completedAt: meta.completedAt || null,
      totals: meta.totals || null, progress: meta.progress || null,
      downloadUrl: meta.downloadUrl || null,
      resultCount: typeof meta.resultCount === 'number' ? meta.resultCount : meta.progress?.processedContacts || 0,
    })).sort((a, b) => new Date(b.createdAt || 0) - new Date(a.createdAt || 0)).slice(0, limit);
    return res.json({ jobs });
  } catch (error) {
    return res.status(500).json({ error: 'Unable to load jobs' });
  }
}

export async function getJobStatus(req, res) {
  const { jobId } = req.params;
  if (!jobId) return res.status(400).json({ error: 'jobId required' });
  try {
    const root = await getTempRootDir();
    const jobDir = path.join(root, jobId);
    const metadata = await readMetadata(jobDir);
    if (!metadata) return res.status(404).json({ error: 'Job not found' });
    return res.json(metadata);
  } catch (error) {
    return res.status(500).json({ error: 'Unable to fetch job status' });
  }
}

export async function stopJob(req, res) {
  const { jobId } = req.params;
  if (!jobId) return res.status(400).json({ error: 'jobId required' });
  try {
    requestJobStop(jobId);
    // Update metadata to reflect stopped status
    const root = await getTempRootDir();
    const jobDir = path.join(root, jobId);
    const metadata = await readMetadata(jobDir);
    if (metadata && metadata.status === 'processing') {
      const { writeMetadata } = await import('../utils/storage.js');
      await writeMetadata(jobDir, { ...metadata, status: 'stopped', stoppedAt: new Date().toISOString() });
    }
    return res.json({ message: `Stop signal sent to job ${jobId}` });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
}

export async function deleteJob(req, res) {
  const { jobId } = req.params;
  if (!jobId) return res.status(400).json({ error: 'jobId required' });
  try {
    const active = getActiveJobIds();
    if (active.has(jobId)) {
      requestJobStop(jobId);
    }
    const root = await getTempRootDir();
    const jobDir = path.join(root, jobId);
    await removeDirectory(jobDir);
    clearJobLogs(jobId);
    return res.json({ message: `Job ${jobId} deleted` });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
}

export async function fetchJobLogs(req, res) {
  const { jobId } = req.params;
  if (!jobId) return res.status(400).json({ error: 'jobId required' });
  const logs = getJobLogs(jobId);
  return res.json({ jobId, logs, count: logs.length });
}
