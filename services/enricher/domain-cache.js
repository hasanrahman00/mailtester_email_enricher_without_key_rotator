/**
 * services/enricher/domain-cache.js — Caches domain-level results.
 *
 * When we discover a domain is Catch-All or has no MX records,
 * we cache it so all other contacts on that domain skip the API call.
 * This saves thousands of API calls on large files.
 */

const { DELIVERY_STATUS } = require('../../utils/status-codes');

// Create a new domain cache instance
function createDomainCache() {
  const cache = new Map(); // domain -> { status, details, bestEmailFn }

  return {
    // Check if a domain result is already cached
    get(domain) {
      return cache.get(domain) || null;
    },

    // Cache a "No MX" result for a domain
    setNoMx(domain) {
      cache.set(domain, {
        status: DELIVERY_STATUS.MX_NOT_FOUND,
        details: { reason: 'Domain missing MX records (cached)' },
        bestEmailFn: null,
      });
    },

    // Cache a "Timeout" result — bestEmail = first pattern (best guess)
    setTimeout(domain) {
      cache.set(domain, {
        status: DELIVERY_STATUS.ERROR,
        details: { reason: 'Domain lookup timed out (cached)' },
        bestEmailFn: (state) => state.patterns[0] || null,
      });
    },

    // Cache a "Catch-All" result — bestEmail = first pattern
    setCatchAll(domain) {
      cache.set(domain, {
        status: DELIVERY_STATUS.CATCH_ALL,
        details: { reason: 'Domain reported Catch-All (cached)' },
        bestEmailFn: (state) => state.patterns[0] || null,
      });
    },
  };
}

module.exports = { createDomainCache };
