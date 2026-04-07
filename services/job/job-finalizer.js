/**
 * services/job/job-finalizer.js — Writes final job status to metadata.
 *
 * After enrichment completes (or halts), this writes the appropriate
 * status (done, stop, pause) and timestamps to the job metadata.
 */

async function finalizeJob(meta, batch, writeMeta, resultCount) {
  const finalMeta = { ...meta, resultCount };

  if (batch.haltType === 'stop') {
    await writeMeta({ ...finalMeta, status: 'stop', stoppedAt: new Date().toISOString(), unprocessedRowIds: batch.unprocessedRowIds });
    return 'stop';
  }

  if (batch.haltType === 'pause' || batch.haltType === 'limit') {
    await writeMeta({
      ...finalMeta, status: 'pause', haltType: batch.haltType,
      haltReason: batch.haltReason || batch.haltType,
      pausedAt: new Date().toISOString(), unprocessedRowIds: batch.unprocessedRowIds,
    });
    return 'pause';
  }

  await writeMeta({ ...finalMeta, status: 'done', completedAt: new Date().toISOString() });
  return 'done';
}

module.exports = { finalizeJob };
