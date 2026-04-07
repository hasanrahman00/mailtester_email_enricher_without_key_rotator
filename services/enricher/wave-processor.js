/**
 * services/enricher/wave-processor.js — Batched priority-pool processor.
 *
 * Contacts are processed in batches (~concurrency × 10).
 * Within each batch, a streaming priority pool runs all 9 combos:
 *   - Bucket 0 drains first → natural wave spreading across domains
 *   - Rejected contacts flow to bucket[N+1] immediately — ZERO dead time
 *   - Domain cache persists across ALL batches (hits resolve instantly)
 *
 * Why batches + priority pool:
 *   - Small batches (3000) → each contact flows through all 9 combos in ~3 min
 *   - Priority pool → zero idle time between combos (no wave barrier wait)
 *   - Domain cache → later batches resolve much faster (many cache hits)
 *   - combo[1] only fires AFTER combo[0] response returns (1-5s domain gap)
 */

const config = require('../../config/env');
const { appendJobLog } = require('../job/job-state');
const { isStopRequested, isPauseRequested } = require('../job/job-state');
const { createDomainCache } = require('./domain-cache');
const { runPriorityPool } = require('./wave-pool');
const { finalizeContacts } = require('./contact-finalizer');

const MAX_COMBOS = 9;

async function processContactsInBatches(contacts, { verifyEmail, generatePatterns, maxCombos = MAX_COMBOS, onResult, jobId }) {
  const concurrency = Math.max(1, Number(config.comboBatchSize) || 1);
  const log = (msg) => { if (jobId) appendJobLog(jobId, msg); };
  const domainCache = createDomainCache();

  // Batch size: large enough to fill workers + build cache, small enough for fast resolution
  const BATCH_SIZE = Math.max(concurrency * 10, 2000);

  const states = contacts.map((c) => ({
    contact: c, patterns: (generatePatterns(c) || []).slice(0, maxCombos),
    currentComboIndex: 0, done: false, bestEmail: null, status: null,
    details: {}, resultsPerCombo: [], spamBlockCount: 0,
  }));

  log(`[Start] ${states.length} contacts | ${concurrency} workers | max ${maxCombos} combos | batch ${BATCH_SIZE}`);

  let haltType = '', haltReason = '';

  // Process in batches — priority pool handles all combos within each batch
  for (let start = 0; start < states.length; start += BATCH_SIZE) {
    if (haltType) break;
    if (jobId && (isStopRequested(jobId) || isPauseRequested(jobId))) break;

    const batch = states.slice(start, start + BATCH_SIZE);
    const batchNum = Math.floor(start / BATCH_SIZE) + 1;
    const totalBatches = Math.ceil(states.length / BATCH_SIZE);
    log(`[Batch ${batchNum}/${totalBatches}] ${batch.length} contacts`);

    const result = await runPriorityPool(batch, {
      verifyEmail, concurrency, domainCache, maxCombos, onResult, jobId, log,
    });

    if (result.haltType) { haltType = result.haltType; haltReason = result.haltReason; }
  }

  const unprocessedRowIds = await finalizeContacts(states, haltType, onResult, log);

  return {
    results: states.map((s) => ({ contact: s.contact, bestEmail: s.bestEmail, status: s.status, details: s.details, resultsPerCombo: s.resultsPerCombo })),
    haltType, haltReason, unprocessedRowIds,
  };
}

module.exports = { processContactsInBatches };
