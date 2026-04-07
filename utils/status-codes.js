/**
 * utils/status-codes.js — All possible email delivery statuses.
 *
 * These are the final statuses assigned to each email after verification.
 * Used throughout the app to keep status strings consistent.
 */

// The 7 possible delivery statuses
const DELIVERY_STATUS = {
  VALID: 'valid',           // Email exists and accepts mail
  CATCH_ALL: 'catch_all',   // Domain accepts all emails (risky)
  NOT_FOUND: 'not_found',   // Email was rejected by the server
  NO_DOMAIN: 'no_domain',   // No domain was provided for this contact
  RATE_LIMITED: 'rate_limited', // Server blocked us (spam protection)
  MX_NOT_FOUND: 'mx_not_found', // Domain has no mail server
  ERROR: 'error',           // Something went wrong (timeout, etc.)
};

// Map various API response strings to our standard statuses
const STATUS_ALIAS_MAP = new Map([
  ['valid', DELIVERY_STATUS.VALID],
  ['catch_all', DELIVERY_STATUS.CATCH_ALL],
  ['catchall', DELIVERY_STATUS.CATCH_ALL],
  ['catchall_default', DELIVERY_STATUS.CATCH_ALL],
  ['catch-all', DELIVERY_STATUS.CATCH_ALL],
  ['not_found', DELIVERY_STATUS.NOT_FOUND],
  ['not_found_valid_emails', DELIVERY_STATUS.NOT_FOUND],
  ['valid_email_not_found', DELIVERY_STATUS.NOT_FOUND],
  ['skipped_missing_fields', DELIVERY_STATUS.NOT_FOUND],
  ['no_domain', DELIVERY_STATUS.NO_DOMAIN],
  ['mx_not_found', DELIVERY_STATUS.MX_NOT_FOUND],
  ['error', DELIVERY_STATUS.ERROR],
  ['other', DELIVERY_STATUS.ERROR],
  ['rate_limited', DELIVERY_STATUS.RATE_LIMITED],
  ['spam_block', DELIVERY_STATUS.RATE_LIMITED],
  ['daily_limit_reached', DELIVERY_STATUS.RATE_LIMITED],
]);

/**
 * Converts any status string into one of our standard statuses.
 * Example: "Catch-All" => "catch_all", "spam_block" => "rate_limited"
 */
function normalizeDeliveryStatus(status) {
  if (!status) return DELIVERY_STATUS.NOT_FOUND;
  const normalized = String(status).trim().toLowerCase().replace(/[\s-]+/g, '_');
  if (STATUS_ALIAS_MAP.has(normalized)) return STATUS_ALIAS_MAP.get(normalized);
  if (normalized.includes('catch')) return DELIVERY_STATUS.CATCH_ALL;
  if (normalized.includes('rate') || normalized.includes('limit') || normalized.includes('spam')) return DELIVERY_STATUS.RATE_LIMITED;
  if (normalized.includes('mx')) return DELIVERY_STATUS.MX_NOT_FOUND;
  return DELIVERY_STATUS.NOT_FOUND;
}

module.exports = { DELIVERY_STATUS, normalizeDeliveryStatus };
