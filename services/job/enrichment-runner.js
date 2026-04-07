/**
 * services/job/enrichment-runner.js — Shared enrichment execution logic.
 *
 * Called by both job-processor (new jobs) and job-rerunner (reruns).
 * Runs enrichment, updates CSV rows and progress in real-time.
 */

const { normalizeDeliveryStatus } = require('../../utils/status-codes');
const { composeCsvRowData } = require('../upload/csv-writer');
const { enrichContacts } = require('../enricher/enricher');
const { normalizeStatusBucket } = require('../upload/progress-tracker');

function resolveSource(d) { return !d ? '' : d === 'Website' ? 'main' : 'waterfall'; }

async function runEnrichment({ contacts, jobId, progress, meta, rowLookup, csv, writeMeta }) {
  // Throttle metadata writes — flush at most once every 500ms
  // This prevents EPERM errors on Windows from rapid file locking
  let metaDirty = false;
  let metaTimer = null;
  const flushMeta = async () => {
    if (!metaDirty) return;
    metaDirty = false;
    meta.progress = { ...progress };
    meta.resultCount = progress.processedContacts;
    meta.lastUpdate = new Date().toISOString();
    await writeMeta(meta);
  };
  const scheduleMetaFlush = () => {
    metaDirty = true;
    if (!metaTimer) metaTimer = setTimeout(async () => { metaTimer = null; await flushMeta(); }, 500);
  };

  const updateProgress = async (status, replaces) => {
    if (replaces) {
      const old = normalizeStatusBucket(replaces);
      progress.statusCounts[old] = Math.max(0, (progress.statusCounts[old] || 0) - 1);
    } else { progress.processedContacts++; }
    progress.statusCounts[normalizeStatusBucket(status)] = (progress.statusCounts[normalizeStatusBucket(status)] || 0) + 1;
    scheduleMetaFlush();
  };

  if (!contacts.length) return { results: [], haltType: null, unprocessedRowIds: [] };

  const result = await enrichContacts(contacts, {
    jobId,
    onResult: async (r) => {
      const row = rowLookup.get(r?.contact?.rowId);
      if (row) {
        await csv.setRow(r.contact.rowId, composeCsvRowData(row.sanitizedRow, {
          Email: r.bestEmail || '',
          Status: normalizeDeliveryStatus(r.status),
          Source: r.bestEmail ? resolveSource(r.domainUsed) : '',
        }));
      }
      await updateProgress(r.status, r._replaces);
    },
  });

  // Final flush — ensure last progress update is persisted
  if (metaTimer) { clearTimeout(metaTimer); metaTimer = null; }
  metaDirty = true;
  await flushMeta();

  return result;
}

module.exports = { runEnrichment };
