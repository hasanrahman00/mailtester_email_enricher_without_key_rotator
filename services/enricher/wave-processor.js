/**
 * services/enricher/wave-processor.js — Streaming priority-pool processor.
 *
 * All contacts enter a priority pool with 9 buckets (one per combo index).
 * Workers always pick from the LOWEST non-empty bucket:
 *   - Bucket 0 drains first → natural wave spreading across domains
 *   - Rejected contacts move to bucket[N+1] immediately
 *   - Domain cache hits resolve instantly (no API call)
 *   - ZERO idle time between combos → maximum throughput
 *
 * Domain protection:
 *   ✅ combo[1] only fires AFTER combo[0]'s response returns (1-5s gap)
 *   ✅ Lower combos have strict priority → domains spread naturally
 *   ✅ Domain cache prevents repeat API calls for catch-all/no-mx/timeout
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

  const states = contacts.map((c) => ({
    contact: c, patterns: (generatePatterns(c) || []).slice(0, maxCombos),
    currentComboIndex: 0, done: false, bestEmail: null, status: null,
    details: {}, resultsPerCombo: [], spamBlockCount: 0,
  }));

  log(`[Start] ${states.length} contacts | ${concurrency} workers | max ${maxCombos} combos`);

  const { haltType, haltReason } = await runPriorityPool(states, {
    verifyEmail, concurrency, domainCache, maxCombos, onResult, jobId, log,
  });

  const unprocessedRowIds = await finalizeContacts(states, haltType, onResult, log);

  return {
    results: states.map((s) => ({ contact: s.contact, bestEmail: s.bestEmail, status: s.status, details: s.details, resultsPerCombo: s.resultsPerCombo })),
    haltType, haltReason, unprocessedRowIds,
  };
}

module.exports = { processContactsInBatches };
