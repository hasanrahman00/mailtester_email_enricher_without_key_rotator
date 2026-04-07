/**
 * services/enricher/pass-logic.js — Multi-pass overwrite rules.
 *
 * The enricher runs up to 3 passes (Website, Website_one, Website_two).
 * This module decides when a later pass should overwrite an earlier result.
 *
 * Rules:
 *   - Valid always overwrites anything
 *   - Domain failures (no_domain, mx_not_found, error, rate_limited)
 *     get overwritten by ANY result from the next pass
 *   - Working domain results (not_found, catch_all) only get
 *     overwritten by "valid" from the next pass
 */

const { DELIVERY_STATUS } = require('../../utils/status-codes');

// Statuses that mean the domain itself can't receive email
const DOMAIN_FAILURES = new Set([
  DELIVERY_STATUS.NO_DOMAIN,
  DELIVERY_STATUS.MX_NOT_FOUND,
  DELIVERY_STATUS.ERROR,
  DELIVERY_STATUS.RATE_LIMITED,
]);

/**
 * Should the prior pass result be replaced by the new pass result?
 * @param {string} priorDomain  - domain from the prior column
 * @param {string} priorStatus  - status from the prior pass
 * @param {string} newStatus    - status from the current pass
 */
function priorShouldDefer(priorDomain, priorStatus, newStatus) {
  // No prior domain or domain-level failure = always defer
  if (!priorDomain || DOMAIN_FAILURES.has(priorStatus)) return true;
  // not_found + new catch_all = defer (catch_all at least provides an email)
  if (priorStatus === DELIVERY_STATUS.NOT_FOUND && newStatus === DELIVERY_STATUS.CATCH_ALL) return true;
  return false;
}

// Build a merged result when a later pass overwrites an earlier one
function buildMergedResult(passResult, priorResult, domainLabel, isValid) {
  const word = domainLabel === 'Website_one' ? 'second' : 'third';
  return {
    contact: passResult.contact,
    bestEmail: passResult.bestEmail,
    status: passResult.status,
    details: passResult.details,
    resultsPerCombo: [...(priorResult.resultsPerCombo || []), ...(passResult.resultsPerCombo || [])],
    domainUsed: domainLabel,
    notes: isValid ? `Valid on ${word} domain` : `Fallback to ${word} domain`,
  };
}

module.exports = { priorShouldDefer, buildMergedResult, DOMAIN_FAILURES };
