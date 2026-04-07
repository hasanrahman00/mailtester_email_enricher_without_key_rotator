/**
 * services/enricher/wave-pool.js — Breadth-first continuous pipeline.
 *
 * Combines the best of both approaches:
 *   ✅ Breadth-first: combo[0] for all contacts BEFORE combo[1] for any
 *   ✅ No wave barriers: workers never idle, zero dead time
 *   ✅ Domain cache fills fast: one catch-all covers all contacts on that domain
 *
 * Uses priority buckets: bucket[0] = contacts needing combo[0],
 * bucket[1] = contacts needing combo[1], etc.
 * Workers always pick from the lowest bucket first.
 */

const { DailyLimitExhaustedError } = require('../../clients/mailtester.client');
const { isStopRequested, isPauseRequested } = require('../job/job-state');
const { classifyResult, buildPayload, extractDomain } = require('./contact-handler');

async function runWavePool(queue, { verifyEmail, concurrency, domainCache, onResult, jobId, log }) {
  let halted = false, haltType = '', haltReason = '';
  let inFlight = 0;

  // Priority buckets — index = combo number
  // Contacts at combo[0] are ALWAYS picked before combo[1], etc.
  const maxCombo = queue.reduce((m, s) => Math.max(m, s.patterns.length), 0);
  const buckets = Array.from({ length: maxCombo }, () => []);

  // Seed bucket[0] with all contacts
  for (const s of queue) {
    if (!s.done && s.patterns.length > 0) buckets[0].push(s);
  }

  let ioQueue = Promise.resolve();

  await new Promise((done) => {
    function checkHalt() {
      if (halted) return true;
      if (jobId && isStopRequested(jobId)) { halted = true; haltType = 'stop'; haltReason = 'Stopped by user'; return true; }
      if (jobId && isPauseRequested(jobId)) { halted = true; haltType = 'pause'; haltReason = 'Paused by user'; return true; }
      return false;
    }

    // Pick from lowest combo bucket first (breadth-first)
    function pick() {
      if (checkHalt()) return null;
      for (let b = 0; b < buckets.length; b++) {
        while (buckets[b].length) {
          const s = buckets[b].shift();
          if (!s.done && s.currentComboIndex < s.patterns.length) return s;
        }
      }
      return null;
    }

    function launch() {
      while (inFlight < concurrency) {
        const state = pick();
        if (!state) { if (inFlight === 0) done(); return; }
        inFlight++;
        processOne(state).then(() => { inFlight--; launch(); }).catch(() => { inFlight--; launch(); });
      }
    }

    async function processOne(state) {
      const wave = state.currentComboIndex;
      const email = state.patterns[wave];
      if (!email) return;
      const domain = extractDomain(email);

      // ── Domain cache hit — no API call ──
      const cached = domainCache.get(domain);
      if (cached) {
        state.bestEmail = cached.bestEmailFn ? cached.bestEmailFn(state) : null;
        state.status = cached.status;
        state.details = { ...cached.details };
        state.done = true;
        state.resultsPerCombo.push({ email, code: 'cached', message: `${cached.status} (cache)` });
        if (!halted && onResult) {
          const payload = buildPayload(state);
          ioQueue = ioQueue.then(() => onResult(payload)).catch(() => {});
        }
        return;
      }

      // ── API call ──
      let result;
      try { result = await verifyEmail(email); }
      catch (err) {
        if (err instanceof DailyLimitExhaustedError && !halted) { halted = true; haltType = 'limit'; haltReason = err.message; }
        if (halted) return;
        result = { code: null, message: null, error: err.message };
      }
      if (halted) return;

      // ── Classify result (synchronous) ──
      classifyResult(state, wave, result, domainCache, log);

      // ── Queue I/O (CSV write) without blocking ──
      if (state.done && onResult) {
        const payload = buildPayload(state);
        ioQueue = ioQueue.then(() => onResult(payload)).catch(() => {});
      }

      // ── Rejected? Push to next combo bucket (breadth-first priority) ──
      if (!state.done) {
        state.currentComboIndex++;
        const nextBucket = state.currentComboIndex;
        if (nextBucket < buckets.length) {
          buckets[nextBucket].push(state);
        }
      }
    }

    if (!queue.length) done(); else launch();
  });

  await ioQueue;
  return { haltType, haltReason };
}

module.exports = { runWavePool };
