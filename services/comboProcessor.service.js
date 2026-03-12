import { config } from '../config/env.js';
import { DELIVERY_STATUS } from './upload/status.utils.js';
import { DailyLimitExhaustedError } from '../clients/mailtester.client.js';
import { isStopRequested, appendJobLog } from './jobState.service.js';

const MAX_COMBOS_DEFAULT = 8;
const MAX_RATE_LIMIT_RETRIES_PER_COMBO = 2;
const RATE_LIMIT_POOL_PAUSE_MIN_MS = 30_000;
const RATE_LIMIT_POOL_PAUSE_MAX_MS = 60_000;

function randomPoolPause() {
  return RATE_LIMIT_POOL_PAUSE_MIN_MS + Math.floor(Math.random() * (RATE_LIMIT_POOL_PAUSE_MAX_MS - RATE_LIMIT_POOL_PAUSE_MIN_MS));
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function processContactsInBatches(contacts, {
  verifyEmail,
  generatePatterns,
  maxCombos = MAX_COMBOS_DEFAULT,
  onResult,
  jobId,
}) {
  const concurrency = Math.max(1, Number(config.comboBatchSize) || 1);
  const log = (msg) => { if (jobId) appendJobLog(jobId, msg); };

  const states = contacts.map((contact) => ({
    contact,
    patterns: generatePatterns(contact) || [],
    currentComboIndex: 0,
    done: false,
    bestEmail: null,
    status: null,
    details: {},
    resultsPerCombo: [],
    rateLimitRetries: 0,
  }));

  log(`Starting ${states.length} contacts, concurrency=${concurrency}`);

  let nextScan = 0;
  let inFlight = 0;
  let halted = false;
  let haltReason = '';

  await new Promise((resolveAll) => {
    function pickNextPending() {
      if (halted) return null;
      // Check stop signal
      if (jobId && isStopRequested(jobId)) {
        if (!halted) {
          halted = true;
          haltReason = 'Job stopped by user';
          log('STOP signal received — halting pool');
        }
        return null;
      }
      for (let i = 0; i < states.length; i++) {
        const idx = (nextScan + i) % states.length;
        const s = states[idx];
        if (!s.done && s.currentComboIndex < maxCombos && s.currentComboIndex < s.patterns.length) {
          nextScan = (idx + 1) % states.length;
          return s;
        }
      }
      return null;
    }

    function tryLaunch() {
      while (inFlight < concurrency) {
        const state = pickNextPending();
        if (!state) {
          if (inFlight === 0) resolveAll();
          return;
        }
        inFlight++;
        processOneState(state)
          .then(() => { inFlight--; tryLaunch(); })
          .catch(() => { inFlight--; tryLaunch(); });
      }
    }

    async function processOneState(state) {
      try {
        await advanceState(state, verifyEmail, maxCombos, onResult, log);
      } catch (err) {
        if (err instanceof DailyLimitExhaustedError) {
          if (!halted) {
            halted = true;
            haltReason = err.message;
            log(`DAILY LIMIT REACHED — ${err.message}`);
          }
        }
      }
    }

    if (states.length === 0) resolveAll();
    else tryLaunch();
  });

  // Finalize remaining
  for (const state of states) {
    if (!state.done) {
      if (halted) {
        state.bestEmail = null;
        // Use RATE_LIMITED, not NOT_FOUND — so the CSV shows the real reason
        state.status = haltReason.includes('stopped') ? DELIVERY_STATUS.NOT_FOUND : DELIVERY_STATUS.RATE_LIMITED;
        state.details = { reason: haltReason };
        state.done = true;
        if (onResult) await onResult(buildResultPayload(state));
      } else {
        await finalizeState(state, onResult, log);
      }
    }
  }

  const doneCount = states.filter((s) => s.done).length;
  log(`Completed: ${doneCount}/${states.length} contacts`);

  return states.map((state) => ({
    contact: state.contact,
    bestEmail: state.bestEmail,
    status: state.status,
    details: state.details,
    resultsPerCombo: state.resultsPerCombo,
  }));
}

async function advanceState(state, verifyEmail, maxCombos, notify, log) {
  if (state.done) return;
  if (state.currentComboIndex >= maxCombos || state.currentComboIndex >= state.patterns.length) {
    await finalizeState(state, notify, log);
    return;
  }

  const email = state.patterns[state.currentComboIndex];
  let result;
  try {
    result = await verifyEmail(email);
  } catch (error) {
    if (error instanceof DailyLimitExhaustedError) throw error;
    result = { code: null, message: null, error: error.message };
  }

  // Rate-limit: don't advance combo, pause and retry
  if (result?._rateLimited) {
    state.rateLimitRetries += 1;
    if (state.rateLimitRetries <= MAX_RATE_LIMIT_RETRIES_PER_COMBO) {
      const pauseMs = randomPoolPause();
      log(`Rate-limited on ${email}, pausing ${Math.round(pauseMs / 1000)}s (retry ${state.rateLimitRetries}/${MAX_RATE_LIMIT_RETRIES_PER_COMBO})`);
      await sleep(pauseMs);
      return;
    }
    log(`Rate-limit retries exhausted for ${email}, marking as rate_limited`);
    state.rateLimitRetries = 0;
    state.bestEmail = null;
    state.status = DELIVERY_STATUS.RATE_LIMITED;
    state.details = { reason: 'SPAM Block persisted after all retries' };
    state.done = true;
    state.resultsPerCombo.push({ email, code: result?.code ?? null, message: 'SPAM Block', error: 'Rate limit' });
    if (notify) await notify(buildResultPayload(state));
    return;
  }

  state.rateLimitRetries = 0;
  state.resultsPerCombo.push({
    email,
    code: result?.code ?? null,
    message: result?.message ?? null,
    error: result?.error ?? null,
  });

  if (isMissingMxRecords(result)) {
    state.bestEmail = null;
    state.status = DELIVERY_STATUS.NOT_FOUND;
    state.details = { reason: 'Domain missing MX records' };
    state.done = true;
    log(`${email} → No MX`);
    if (notify) await notify(buildResultPayload(state));
    return;
  }
  if (isTimeout(result)) {
    state.bestEmail = null;
    state.status = DELIVERY_STATUS.NOT_FOUND;
    state.details = { reason: 'Domain lookup timed out' };
    state.done = true;
    log(`${email} → Timeout`);
    if (notify) await notify(buildResultPayload(state));
    return;
  }
  if (isCatchAll(result)) {
    state.bestEmail = state.patterns[0] || null;
    state.status = DELIVERY_STATUS.CATCH_ALL;
    state.details = { reason: 'Domain reported Catch-All' };
    state.done = true;
    log(`${email} → Catch-All`);
    if (notify) await notify(buildResultPayload(state));
    return;
  }
  if (result?.code === 'ok') {
    state.bestEmail = email;
    state.status = DELIVERY_STATUS.VALID;
    state.details = { code: result.code, message: result.message };
    state.done = true;
    log(`${email} → Valid ✓`);
    if (notify) await notify(buildResultPayload(state));
    return;
  }

  log(`${email} → Rejected (combo ${state.currentComboIndex + 1}/${state.patterns.length})`);
  state.currentComboIndex += 1;
}

async function finalizeState(state, notify, log) {
  if (state.done) return;
  const allCatchAll = state.resultsPerCombo.length > 0 &&
    state.resultsPerCombo.every((e) => e.message === 'Catch-All');

  if (allCatchAll) {
    state.bestEmail = state.patterns[2] || state.patterns[0] || null;
    state.status = DELIVERY_STATUS.CATCH_ALL;
    state.details = { reason: 'All candidates returned Catch-All' };
  } else {
    const firstError = state.resultsPerCombo.find((e) => e.error)?.error;
    state.bestEmail = null;
    state.status = DELIVERY_STATUS.NOT_FOUND;
    state.details = { reason: 'All candidates rejected', ...(firstError ? { lastError: firstError } : {}) };
  }
  state.done = true;
  if (log) log(`Finalized ${state.contact.firstName} ${state.contact.lastName} → ${state.status}`);
  if (notify) await notify(buildResultPayload(state));
}

function buildResultPayload(state) {
  return { contact: state.contact, bestEmail: state.bestEmail, status: state.status, details: state.details, resultsPerCombo: state.resultsPerCombo };
}

function isMissingMxRecords(r) {
  return [r?.code, r?.message, r?.raw?.code, r?.raw?.message, r?.raw?.reason, r?.error]
    .some((v) => typeof v === 'string' && /mx/i.test(v) && /no |not |missing|without/i.test(v));
}
function isTimeout(r) {
  return [r?.code, r?.message, r?.raw?.code, r?.raw?.message, r?.raw?.reason, r?.error]
    .some((v) => typeof v === 'string' && /timeout|timed out|etimedout|econnaborted/i.test(v));
}
function isCatchAll(r) {
  return [r?.code, r?.message, r?.raw?.code, r?.raw?.message, r?.raw?.reason]
    .some((v) => typeof v === 'string' && v.toLowerCase().includes('catch-all'));
}
