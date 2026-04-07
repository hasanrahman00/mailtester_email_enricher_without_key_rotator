/**
 * services/enricher/wave-pool.js — Runs one wave of concurrent API calls.
 *
 * Processes all queued contacts at one combo index simultaneously.
 * Workers fire at max rate, I/O (CSV writes) queued separately.
 * Wave barriers protect against spam blocks by spreading attempts
 * across different domains between combo retries.
 */

const { DailyLimitExhaustedError } = require('../../clients/mailtester.client');
const { isStopRequested, isPauseRequested } = require('../job/job-state');
const { classifyResult, buildPayload, extractDomain } = require('./contact-handler');

async function runWavePool(queue, { wave, verifyEmail, concurrency, domainCache, onResult, jobId, log }) {
  let cursor = 0, inFlight = 0, halted = false, haltType = '', haltReason = '';
  let ioQueue = Promise.resolve();

  await new Promise((done) => {
    function pick() {
      if (halted) return null;
      if (jobId && isStopRequested(jobId)) { halted = true; haltType = 'stop'; haltReason = 'Stopped by user'; return null; }
      if (jobId && isPauseRequested(jobId)) { halted = true; haltType = 'pause'; haltReason = 'Paused by user'; return null; }
      while (cursor < queue.length) { const s = queue[cursor++]; if (!s.done) return s; }
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
      const email = state.patterns[wave];
      if (!email) return;
      const domain = extractDomain(email);

      // Domain cache hit — no API call
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

      // API call
      let result;
      try { result = await verifyEmail(email); }
      catch (err) {
        if (err instanceof DailyLimitExhaustedError && !halted) { halted = true; haltType = 'limit'; haltReason = err.message; }
        if (halted) return;
        result = { code: null, message: null, error: err.message };
      }
      if (halted) return;

      // Classify (synchronous)
      classifyResult(state, wave, result, domainCache, log);

      // Queue I/O without blocking worker
      if (state.done && onResult) {
        const payload = buildPayload(state);
        ioQueue = ioQueue.then(() => onResult(payload)).catch(() => {});
      }
    }

    if (!queue.length) done(); else launch();
  });

  await ioQueue;
  return { haltType, haltReason };
}

module.exports = { runWavePool };
