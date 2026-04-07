/**
 * clients/mailtester.client.js — Calls the MailTester Ninja API.
 *
 * Verifies a single email address. Handles rate-limit responses
 * with 1 quick retry. If still blocked, returns _rateLimited flag
 * so the wave processor can handle it with natural domain rest.
 */

const axios = require('axios');
const config = require('../config/env');
const { acquireKey } = require('./key-scheduler');
const sleep = require('../utils/sleep');

// Patterns that indicate the target server blocked our probe
const RATE_LIMIT_PATTERNS = /spam.block|limited/i;
const MAX_RETRIES = 1;          // 1 quick retry on rate-limit
const RETRY_PAUSE_MS = 5_000;   // 5 seconds between retries

// Custom error for when all daily API credits are used up
class DailyLimitExhaustedError extends Error {
  constructor(msg) { super(msg); this.name = 'DailyLimitExhaustedError'; }
}

// Check if the API response means "rate limited"
function isRateLimitResponse(data) {
  if (!data) return false;
  const fields = [data.message, data.code, data.raw?.message, data.raw?.code];
  return fields.some((v) => typeof v === 'string' && RATE_LIMIT_PATTERNS.test(v));
}

// Verify one email via MailTester API
async function verifyEmail(email) {
  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    let key;
    try {
      key = await acquireKey();
    } catch (err) {
      if (err.message?.includes('exhausted')) throw new DailyLimitExhaustedError(err.message);
      throw err;
    }
    try {
      const url = `${config.mailTesterBaseUrl}?email=${encodeURIComponent(email)}&key=${encodeURIComponent(key)}`;
      const { data } = await axios.get(url);
      // Rate-limited? Retry once, then return with flag
      if (isRateLimitResponse(data)) {
        if (attempt < MAX_RETRIES) { await sleep(RETRY_PAUSE_MS); continue; }
        return { email, code: data.code, message: data.message, raw: data, _rateLimited: true };
      }
      return { email, code: data.code || null, message: data.message || null, raw: data };
    } catch (error) {
      return { email, code: null, message: null, raw: error.response?.data, error: error.message };
    }
  }
  return { email, code: null, message: null, raw: null, error: 'Retry exhaustion' };
}

module.exports = { verifyEmail, DailyLimitExhaustedError };
