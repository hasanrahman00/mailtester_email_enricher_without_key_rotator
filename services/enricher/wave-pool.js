/**
 * services/enricher/wave-pool.js — Streaming priority pool.
 *
 * Replaces strict wave barriers with priority buckets:
 *   Bucket 0 (combo[0]) → highest priority, picked first
 *   Bucket 1 (combo[1]) → picked when bucket 0 is empty
 *   ...up to maxCombos
 *
 * Workers always pick from the LOWEST non-empty bucket.
 * Rejected contacts move to the next bucket immediately.
 * Domain cache hits resolve instantly (no API call).
 *
 * Why this is safe from spam blocks:
 *   - A contact only enters bucket[N+1] AFTER combo[N]'s response returns
 *     (1-5s natural gap per domain)
 *   - Lower combos always have priority → 95%+ workers on combo[0] before
 *     any reach combo[1] → natural wave spreading
 *   - Zero idle time between waves → maximum throughput
 */

const { DailyLimitExhaustedError } = require('../../clients/mailtester.client');
const { isStopRequested, isPauseRequested } = require('../job/job-state');
const { classifyResult, buildPayload, extractDomain } = require('./contact-handler');

async function runPriorityPool(states, { verifyEmail, concurrency, domainCache, maxCombos, onResult, jobId, log }) {
  // Priority buckets — bucket[i] holds states ready for combo index i
  // Cursor-based: O(1) pick, no array shifting
  const buckets = Array.from({ length: maxCombos }, () => ({ items: [], cursor: 0 }));

  // All contacts with patterns start in bucket 0
  for (const s of states) {
    if (s.patterns.length > 0) buckets[0].items.push(s);
  }

  let inFlight = 0, halted = false, haltType = '', haltReason = '';
  let ioQueue = Promise.resolve();

  await new Promise((done) => {
    function pick() {
      if (halted) return null;
      if (jobId && isStopRequested(jobId)) { halted = true; haltType = 'stop'; haltReason = 'Stopped by user'; return null; }
      if (jobId && isPauseRequested(jobId)) { halted = true; haltType = 'pause'; haltReason = 'Paused by user'; return null; }

      // Pick from lowest non-empty bucket (natural domain spreading)
      for (let i = 0; i < maxCombos; i++) {
        const b = buckets[i];
        while (b.cursor < b.items.length) {
          const s = b.items[b.cursor++];
          if (!s.done && s.currentComboIndex === i) return s;
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
      if (!email) { state.done = true; return; }
      const domain = extractDomain(email);

      // ── Domain cache hit — instant resolution, no API call ──
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

      // ── Classify (synchronous — no I/O) ──
      classifyResult(state, wave, result, domainCache, log);

      if (state.done) {
        // Resolved (valid / catch-all / no-mx / timeout / rate-limited) — queue I/O
        if (onResult) {
          const payload = buildPayload(state);
          ioQueue = ioQueue.then(() => onResult(payload)).catch(() => {});
        }
      } else {
        // Rejected — advance to next combo bucket
        state.currentComboIndex++;
        if (state.currentComboIndex < state.patterns.length) {
          buckets[state.currentComboIndex].items.push(state);
        } else {
          // Exhausted all combos — mark as not_found immediately
          state.bestEmail = null;
          state.status = 'not_found';
          state.details = { reason: 'All candidates rejected' };
          state.done = true;
          if (onResult) {
            const payload = buildPayload(state);
            ioQueue = ioQueue.then(() => onResult(payload)).catch(() => {});
          }
        }
      }
    }

    if (buckets[0].items.length === 0) done(); else launch();
  });

  // Drain any remaining I/O writes
  await ioQueue;
  return { haltType, haltReason };
}

module.exports = { runPriorityPool };
