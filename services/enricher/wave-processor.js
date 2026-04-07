/**
 * services/enricher/wave-processor.js — Chunked wave-barrier processor.
 *
 * Processes contacts in CHUNKS (default: concurrency × 10).
 * Within each chunk, strict wave barriers:
 *   Wave 0: combo[0] for all chunk contacts → BARRIER
 *   Wave 1: combo[1] for remaining → BARRIER
 *   ...up to 9 patterns
 * Then moves to next chunk. Domain cache persists across all chunks.
 *
 * This gives:
 *   ✅ Wave barriers prevent spam blocks (attempts spread across domains)
 *   ✅ Contacts resolve fast (~3-4 min per chunk vs 20+ min for all)
 *   ✅ UI shows steady progress as each chunk completes
 *   ✅ Domain cache builds across chunks
 */

const config = require('../../config/env');
const { appendJobLog } = require('../job/job-state');
const { isStopRequested, isPauseRequested } = require('../job/job-state');
const { createDomainCache } = require('./domain-cache');
const { runWavePool } = require('./wave-pool');
const { finalizeContacts } = require('./contact-finalizer');

const MAX_COMBOS = 9;

async function processContactsInBatches(contacts, { verifyEmail, generatePatterns, maxCombos = MAX_COMBOS, onResult, jobId }) {
  const concurrency = Math.max(1, Number(config.comboBatchSize) || 1);
  const log = (msg) => { if (jobId) appendJobLog(jobId, msg); };
  const domainCache = createDomainCache();

  // Chunk size: enough to fill workers and build cache, small enough for fast resolution
  const CHUNK_SIZE = Math.max(concurrency * 10, 1000);

  const states = contacts.map((c) => ({
    contact: c, patterns: (generatePatterns(c) || []).slice(0, maxCombos),
    currentComboIndex: 0, done: false, bestEmail: null, status: null,
    details: {}, resultsPerCombo: [], spamBlockCount: 0,
  }));

  let haltType = '', haltReason = '';

  // Process in chunks
  for (let start = 0; start < states.length; start += CHUNK_SIZE) {
    if (haltType) break;
    if (jobId && (isStopRequested(jobId) || isPauseRequested(jobId))) break;

    const chunk = states.slice(start, start + CHUNK_SIZE);
    const chunkNum = Math.floor(start / CHUNK_SIZE) + 1;
    const totalChunks = Math.ceil(states.length / CHUNK_SIZE);
    log(`[Chunk ${chunkNum}/${totalChunks}] ${chunk.length} contacts, ${concurrency} workers`);

    // Wave barriers within the chunk
    for (let wave = 0; wave < maxCombos; wave++) {
      const waveQueue = chunk.filter((s) => !s.done && s.currentComboIndex === wave && wave < s.patterns.length);
      if (!waveQueue.length) break;

      log(`[Chunk ${chunkNum} Wave ${wave}] ${waveQueue.length} contacts`);

      const result = await runWavePool(waveQueue, { wave, verifyEmail, concurrency, domainCache, onResult, jobId, log });
      if (result.haltType) { haltType = result.haltType; haltReason = result.haltReason; break; }
      for (const s of waveQueue) { if (!s.done) s.currentComboIndex++; }
    }
  }

  const unprocessedRowIds = await finalizeContacts(states, haltType, onResult, log);

  return {
    results: states.map((s) => ({ contact: s.contact, bestEmail: s.bestEmail, status: s.status, details: s.details, resultsPerCombo: s.resultsPerCombo })),
    haltType, haltReason, unprocessedRowIds,
  };
}

module.exports = { processContactsInBatches };
