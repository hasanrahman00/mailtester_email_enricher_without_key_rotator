/**
 * services/enricher/wave-processor.js — Continuous pipeline processor.
 *
 * NO wave barriers. Each contact tries combos independently:
 *   combo[0] → rejected? → combo[1] → rejected? → combo[2] → ... → not_found
 * All contacts run in parallel through the pipeline.
 * Workers continuously fire at max rate — zero dead time between combos.
 */

const config = require('../../config/env');
const { appendJobLog } = require('../job/job-state');
const { createDomainCache } = require('./domain-cache');
const { runWavePool } = require('./wave-pool');
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

  log(`[Pipeline] ${states.length} contacts, ${concurrency} workers, up to ${maxCombos} combos each`);

  // Single continuous run — no wave barriers
  const result = await runWavePool(states, { verifyEmail, concurrency, domainCache, onResult, jobId, log });
  const { haltType, haltReason } = result;

  const unprocessedRowIds = await finalizeContacts(states, haltType, onResult, log);

  return {
    results: states.map((s) => ({ contact: s.contact, bestEmail: s.bestEmail, status: s.status, details: s.details, resultsPerCombo: s.resultsPerCombo })),
    haltType, haltReason, unprocessedRowIds,
  };
}

module.exports = { processContactsInBatches };
