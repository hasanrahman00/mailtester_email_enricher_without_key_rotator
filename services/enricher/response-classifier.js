/**
 * services/enricher/response-classifier.js — Classifies MailTester API responses.
 *
 * Looks at various fields in the response to determine if the result is:
 * - Missing MX records (domain can't receive email)
 * - Timeout (server didn't respond in time)
 * - Catch-All (domain accepts all emails)
 */

// Check multiple fields in a response for a pattern
function checkFields(result, regex) {
  const fields = [
    result?.code, result?.message,
    result?.raw?.code, result?.raw?.message,
    result?.raw?.reason, result?.error,
  ];
  return fields.some((v) => typeof v === 'string' && regex.test(v));
}

// Domain has no MX records — can never receive email
function isMissingMx(result) {
  return checkFields(result, /mx/i) && checkFields(result, /no |not |missing|without/i);
}

// Server timed out — temporary issue
function isTimeout(result) {
  return checkFields(result, /timeout|timed out|etimedout|econnaborted/i);
}

// Domain accepts all emails (catch-all server)
function isCatchAll(result) {
  const fields = [
    result?.code, result?.message,
    result?.raw?.code, result?.raw?.message, result?.raw?.reason,
  ];
  return fields.some((v) => typeof v === 'string' && v.toLowerCase().includes('catch-all'));
}

module.exports = { isMissingMx, isTimeout, isCatchAll };
