/**
 * services/enricher/enricher.js — Multi-pass email enrichment orchestrator.
 *
 * Runs up to 3 passes: Website -> Website_one -> Website_two.
 * Each pass uses wave-processor for barrier-based verification.
 */

const { verifyEmail } = require('../../clients/mailtester.client');
const { generatePatterns } = require('../../utils/email-patterns');
const { DELIVERY_STATUS } = require('../../utils/status-codes');
const { processContactsInBatches } = require('./wave-processor');
const { runPass } = require('./pass-runner');
const { priorShouldDefer, buildMergedResult } = require('./pass-logic');

const MAX_COMBOS = 9;
const SHARED = { verifyEmail, generatePatterns, maxCombos: MAX_COMBOS };

async function enrichContacts(contacts, options = {}) {
  if (!contacts?.length) return { results: [], haltType: null, unprocessedRowIds: [] };
  const { jobId } = options;

  // Pass 1: Website domain (all contacts)
  const d1OnResult = options.onResult
    ? async (r) => await options.onResult({ ...r, domainUsed: 'Website', notes: '' })
    : undefined;
  const d1 = await processContactsInBatches(contacts, { ...SHARED, onResult: d1OnResult, jobId });
  const unprocessed = [...d1.unprocessedRowIds];
  if (d1.haltType) return { results: d1.results, haltType: d1.haltType, haltReason: d1.haltReason, unprocessedRowIds: unprocessed };

  // Pass 2: Website_one (non-valid contacts with domain2)
  const p2 = await runPass(contacts, d1.results, 'domain2', 'Website_one', options, SHARED, jobId);
  unprocessed.push(...p2.unprocessed);
  if (p2.haltType) return { results: p2.intermediate, haltType: p2.haltType, haltReason: p2.haltReason, unprocessedRowIds: unprocessed };

  // Pass 3: Website_two (still non-valid with domain3)
  const p3 = await runPass(contacts, p2.intermediate, 'domain3', 'Website_two', options, SHARED, jobId, p2.domainMap, p2.statusMap);
  unprocessed.push(...p3.unprocessed);
  return { results: p3.intermediate, haltType: p3.haltType || null, haltReason: p3.haltReason || null, unprocessedRowIds: unprocessed };
}

module.exports = { enrichContacts };
