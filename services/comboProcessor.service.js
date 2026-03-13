import { config } from '../config/env.js';
import { DELIVERY_STATUS } from './upload/status.utils.js';
import { DailyLimitExhaustedError } from '../clients/mailtester.client.js';
import { isStopRequested, isPauseRequested, appendJobLog } from './jobState.service.js';

/**
 * ── Wave-Based (Barrier) Combo Processor ──
 *
 * Processes contacts using STRICT WAVE BARRIERS:
 *
 *   Wave 0: combo[0] for ALL contacts  → BARRIER → drop done contacts
 *   Wave 1: combo[1] for remaining     → BARRIER → drop done contacts
 *   Wave 2: combo[2] for remaining     → BARRIER → drop done contacts
 *   ...up to maxCombos (all 9 patterns)
 *
 * At any moment, each contact has at most ONE pattern in flight.
 * No contact ever has combo[1] tried before ALL contacts finish combo[0].
 *
 * Why this prevents SPAM Blocks:
 *   10,000 rows with 500 at acme.com → acme.com hit once every ~20 requests
 *   At 17 req/sec → one acme.com probe every ~1.2 seconds (safe)
 *   Between waves → entire wave duration as natural rest per domain
 *
 * SPAM Block handling:
 *   OLD: 12 retries, 6-12 min wasted, then halt entire job
 *   NEW: Skip instantly, advance to next combo in next wave.
 *        If blocked 2 consecutive waves → mark rate_limited (domain issue)
 *
 * Domain cache:
 *   Catch-All or No MX for one contact → apply to ALL contacts on that domain
 */

const MAX_COMBOS_DEFAULT = 8;

export async function processContactsInBatches(contacts, {
  verifyEmail,
  generatePatterns,
  maxCombos = MAX_COMBOS_DEFAULT,
  onResult,
  jobId,
}) {
  const concurrency = Math.max(1, Number(config.comboBatchSize) || 1);
  const log = (msg) => { if (jobId) appendJobLog(jobId, msg); };

  // ── Build initial states ──
  const states = contacts.map((contact) => ({
    contact,
    patterns: generatePatterns(contact) || [],
    currentComboIndex: 0,
    done: false,
    bestEmail: null,
    status: null,
    details: {},
    resultsPerCombo: [],
    spamBlockCount: 0,  // consecutive SPAM blocks across waves
  }));

  log(`[Barrier] Starting ${states.length} contacts, concurrency=${concurrency}, maxCombos=${maxCombos}`);

  // ── Domain-level cache ──
  // Catch-All or No MX applies to every contact on that domain.
  const domainCache = new Map();  // domain → { status, details, bestEmailFn }

  let haltType = '';
  let haltReason = '';
  const unprocessedRowIds = [];

  // ────────────────────────────────────────────
  //  WAVE LOOP — one combo pattern per wave
  // ────────────────────────────────────────────
  for (let wave = 0; wave < maxCombos; wave++) {

    // Build queue: contacts that still need processing at this combo index
    const waveQueue = [];
    for (const state of states) {
      if (!state.done && state.currentComboIndex === wave && wave < state.patterns.length) {
        waveQueue.push(state);
      }
    }

    if (waveQueue.length === 0) {
      log(`[Barrier] Wave ${wave}: no contacts remaining — done early`);
      break;
    }

    log(`[Barrier] Wave ${wave}: ${waveQueue.length} contacts queued`);

    // ── Apply domain cache before making any API calls ──
    const apiQueue = [];
    for (const state of waveQueue) {
      const domain = extractDomain(state.patterns[wave]);
      const cached = domainCache.get(domain);

      if (cached) {
        state.bestEmail = cached.bestEmailFn ? cached.bestEmailFn(state) : null;
        state.status = cached.status;
        state.details = { ...cached.details };
        state.done = true;
        state.resultsPerCombo.push({
          email: state.patterns[wave],
          code: 'cached',
          message: `${cached.status} (domain cache)`,
          error: null,
        });
        log(`${state.patterns[wave]} → ${cached.status} (domain cache)`);
        if (onResult) await onResult(buildResultPayload(state));
      } else {
        apiQueue.push(state);
      }
    }

    const cachedCount = waveQueue.length - apiQueue.length;
    if (cachedCount > 0) {
      log(`[Barrier] Wave ${wave}: ${cachedCount} resolved from domain cache, ${apiQueue.length} need API`);
    }

    if (apiQueue.length === 0) continue;

    // ── Process this wave with concurrent workers ──
    const waveResult = await runWavePool(apiQueue, {
      wave,
      verifyEmail,
      concurrency,
      domainCache,
      onResult,
      jobId,
      log,
    });

    // Check for job-level halts (stop/pause/daily limit)
    if (waveResult.haltType) {
      haltType = waveResult.haltType;
      haltReason = waveResult.haltReason;
      log(`[Barrier] Wave ${wave} halted: ${haltType}`);
      break;
    }

    // ── Advance combo index for contacts that need next wave ──
    for (const state of apiQueue) {
      if (!state.done) {
        // Rejected or SPAM-skipped → move to next combo for next wave
        state.currentComboIndex += 1;
      }
    }

    // ── BARRIER — wave complete, log stats ──
    const doneThisWave = waveQueue.filter((s) => s.done).length;
    const totalRemaining = states.filter((s) => !s.done).length;
    log(`[Barrier] Wave ${wave} complete: ${doneThisWave} resolved, ${totalRemaining} remaining`);
  }

  // ────────────────────────────────────────────
  //  Finalize remaining contacts
  // ────────────────────────────────────────────
  for (const state of states) {
    if (!state.done) {
      if (haltType) {
        // ALL halt types: track as unprocessed for rerun
        state.done = true;
        const rowId = state.contact?.rowId;
        if (typeof rowId === 'number') unprocessedRowIds.push(rowId);
      } else {
        await finalizeState(state, onResult, log);
      }
    }
  }

  const doneCount = states.filter((s) => s.done).length;
  log(`[Barrier] Completed: ${doneCount}/${states.length} contacts`);

  return {
    results: states.map((state) => ({
      contact: state.contact,
      bestEmail: state.bestEmail,
      status: state.status,
      details: state.details,
      resultsPerCombo: state.resultsPerCombo,
    })),
    haltType,
    haltReason,
    unprocessedRowIds,
  };
}

// ────────────────────────────────────────────────────────
//  Wave pool: process all contacts at one combo index
// ────────────────────────────────────────────────────────

async function runWavePool(queue, { wave, verifyEmail, concurrency, domainCache, onResult, jobId, log }) {
  let cursor = 0;
  let inFlight = 0;
  let halted = false;
  let haltType = '';
  let haltReason = '';

  await new Promise((resolveAll) => {

    function pickNext() {
      if (halted) return null;

      // Check stop/pause signals
      if (jobId && isStopRequested(jobId)) {
        halted = true;
        haltType = 'stop';
        haltReason = 'Job stopped by user';
        log('[Barrier] STOP signal received');
        return null;
      }
      if (jobId && isPauseRequested(jobId)) {
        halted = true;
        haltType = 'pause';
        haltReason = 'Job paused by user';
        log('[Barrier] PAUSE signal received');
        return null;
      }

      while (cursor < queue.length) {
        const state = queue[cursor++];
        if (!state.done) return state;
      }
      return null;
    }

    function tryLaunch() {
      while (inFlight < concurrency) {
        const state = pickNext();
        if (!state) {
          if (inFlight === 0) resolveAll();
          return;
        }
        inFlight++;
        processOneContact(state)
          .then(() => { inFlight--; tryLaunch(); })
          .catch(() => { inFlight--; tryLaunch(); });
      }
    }

    async function processOneContact(state) {
      const email = state.patterns[wave];
      const domain = extractDomain(email);
      let result;

      try {
        result = await verifyEmail(email);
      } catch (err) {
        if (err instanceof DailyLimitExhaustedError) {
          if (!halted) {
            halted = true;
            haltType = 'limit';
            haltReason = err.message;
            log(`[Barrier] DAILY LIMIT — ${err.message}`);
          }
          return;
        }
        result = { code: null, message: null, error: err.message };
      }

      // ── SPAM Block: skip and move on ──
      if (result?._rateLimited) {
        state.spamBlockCount += 1;
        state.resultsPerCombo.push({
          email,
          code: result.code ?? null,
          message: result.message ?? 'SPAM Block',
          error: `SPAM Block (consecutive: ${state.spamBlockCount})`,
        });

        if (state.spamBlockCount >= 2) {
          // Blocked 2 consecutive waves → domain is persistently blocking
          state.bestEmail = null;
          state.status = DELIVERY_STATUS.RATE_LIMITED;
          state.details = {
            reason: `SPAM Block on ${domain} persisted across waves`,
            code: result.code || null,
            message: result.message || null,
          };
          state.done = true;
          log(`${email} → SPAM Block (2nd consecutive) — rate_limited`);
          if (onResult) await onResult(buildResultPayload(state));
        } else {
          // First SPAM Block → skip, will try next combo in next wave
          log(`${email} → SPAM Block — skipping to next wave`);
          // state.done stays false, combo index will be advanced after wave
        }
        return;
      }

      // ── Normal response — reset SPAM counter ──
      state.spamBlockCount = 0;

      state.resultsPerCombo.push({
        email,
        code: result?.code ?? null,
        message: result?.message ?? null,
        error: result?.error ?? null,
      });

      // ── No MX — cache for entire domain ──
      if (isMissingMxRecords(result)) {
        state.bestEmail = null;
        state.status = DELIVERY_STATUS.MX_NOT_FOUND;
        state.details = { reason: 'Domain missing MX records' };
        state.done = true;
        domainCache.set(domain, {
          status: DELIVERY_STATUS.MX_NOT_FOUND,
          details: { reason: 'Domain missing MX records (cached)' },
          bestEmailFn: null,
        });
        log(`${email} → No MX (caching ${domain})`);
        if (onResult) await onResult(buildResultPayload(state));
        return;
      }

      // ── Timeout ──
      if (isTimeout(result)) {
        state.bestEmail = null;
        state.status = DELIVERY_STATUS.ERROR;
        state.details = { reason: 'Domain lookup timed out' };
        state.done = true;
        log(`${email} → Timeout`);
        if (onResult) await onResult(buildResultPayload(state));
        return;
      }

      // ── Catch-All — cache for entire domain ──
      if (isCatchAll(result)) {
        state.bestEmail = state.patterns[0] || null;
        state.status = DELIVERY_STATUS.CATCH_ALL;
        state.details = { reason: 'Domain reported Catch-All' };
        state.done = true;
        domainCache.set(domain, {
          status: DELIVERY_STATUS.CATCH_ALL,
          details: { reason: 'Domain reported Catch-All (cached)' },
          bestEmailFn: (s) => s.patterns[0] || null,
        });
        log(`${email} → Catch-All (caching ${domain})`);
        if (onResult) await onResult(buildResultPayload(state));
        return;
      }

      // ── Valid ──
      if (result?.code === 'ok') {
        state.bestEmail = email;
        state.status = DELIVERY_STATUS.VALID;
        state.details = { code: result.code, message: result.message };
        state.done = true;
        log(`${email} → Valid ✓`);
        if (onResult) await onResult(buildResultPayload(state));
        return;
      }

      // ── Rejected — stays in pool for next wave ──
      log(`${email} → Rejected (wave ${wave})`);
      // state.done stays false, combo index advanced after wave completes
    }

    if (queue.length === 0) resolveAll();
    else tryLaunch();
  });

  return { haltType, haltReason };
}

// ────────────────────────────────────────────────────────
//  Helpers (unchanged from original)
// ────────────────────────────────────────────────────────

async function finalizeState(state, notify, log) {
  if (state.done) return;
  const allCatchAll = state.resultsPerCombo.length > 0 &&
    state.resultsPerCombo.every((e) =>
      e.message === 'Catch-All' ||
      (typeof e.message === 'string' && e.message.includes('catch_all'))
    );

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

function extractDomain(email) {
  if (!email) return '';
  const at = email.lastIndexOf('@');
  return at >= 0 ? email.slice(at + 1).toLowerCase() : '';
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