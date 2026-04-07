/**
 * services/enricher/contact-handler.js — Classifies API results for contacts.
 *
 * classifyResult() is synchronous — updates state, caches domain, no I/O.
 * buildPayload() creates the result object for onResult callbacks.
 * The wave-pool handles I/O queueing separately.
 */

const { DELIVERY_STATUS } = require('../../utils/status-codes');
const { isMissingMx, isTimeout, isCatchAll } = require('./response-classifier');

function extractDomain(email) {
  if (!email) return '';
  const at = email.lastIndexOf('@');
  return at >= 0 ? email.slice(at + 1).toLowerCase() : '';
}

function buildPayload(s) {
  return { contact: s.contact, bestEmail: s.bestEmail, status: s.status, details: s.details, resultsPerCombo: s.resultsPerCombo };
}

/**
 * Classify one API result — updates state synchronously, no async I/O.
 * After calling this, check state.done:
 *   true  → contact resolved (valid/catch-all/error/etc.), call onResult
 *   false → rejected, advance to next combo
 */
function classifyResult(state, wave, result, domainCache, log) {
  const email = state.patterns[wave];
  const domain = extractDomain(email);

  // SPAM Block — keep default combo as best guess
  if (result?._rateLimited) {
    state.spamBlockCount++;
    state.resultsPerCombo.push({ email, code: result.code, message: result.message, error: `SPAM Block (${state.spamBlockCount})` });
    if (state.spamBlockCount >= 2) {
      state.bestEmail = state.patterns[0] || null;
      state.status = DELIVERY_STATUS.RATE_LIMITED;
      state.details = { reason: `SPAM Block on ${domain} persisted` };
      state.done = true;
    } else {
      log(`${email} -> SPAM Block, skipping to next combo`);
    }
    return;
  }

  state.spamBlockCount = 0;
  state.resultsPerCombo.push({ email, code: result?.code, message: result?.message, error: result?.error });

  // No MX
  if (isMissingMx(result)) {
    state.bestEmail = null; state.status = DELIVERY_STATUS.MX_NOT_FOUND;
    state.details = { reason: 'Domain missing MX records' }; state.done = true;
    domainCache.setNoMx(domain); return;
  }
  // Timeout — keep default combo as best guess
  if (isTimeout(result)) {
    state.bestEmail = state.patterns[0] || null; state.status = DELIVERY_STATUS.ERROR;
    state.details = { reason: 'Domain lookup timed out' }; state.done = true;
    domainCache.setTimeout(domain); return;
  }
  // Catch-All
  if (isCatchAll(result)) {
    state.bestEmail = state.patterns[0] || null; state.status = DELIVERY_STATUS.CATCH_ALL;
    state.details = { reason: 'Domain reported Catch-All' }; state.done = true;
    domainCache.setCatchAll(domain); return;
  }
  // Valid
  if (result?.code === 'ok') {
    state.bestEmail = email; state.status = DELIVERY_STATUS.VALID;
    state.details = { code: result.code, message: result.message }; state.done = true;
    return;
  }
  // Rejected — stays in pipeline for next combo
  log(`${email} -> Rejected (combo ${wave})`);
}

module.exports = { classifyResult, buildPayload, extractDomain };
