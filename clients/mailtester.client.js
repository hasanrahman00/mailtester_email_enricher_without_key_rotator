import axios from 'axios';
import { config } from '../config/env.js';
import { acquireKey } from './keyManager.js';

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ── MailTester rate-limit response patterns ──
//
// From the API docs, possible message values:
//   "Accepted" | "Limited" | "Rejected" | "Catch-All" |
//   "No Mx" | "Mx Error" | "Timeout" | "SPAM Block"
//
// "SPAM Block" and "Limited" mean: target mail server refused the probe.
// These come back as HTTP 200 with code "ko".
const RATE_LIMIT_PATTERNS = /spam.block|limited/i;

// ── Retry config (wave-compatible) ──
//
// OLD: 3 retries × 30-60s sleep = up to 3 min wasted per email.
//      Then comboProcessor retried 2× more = 12 total attempts, ~8 min.
//      One worker blocked for 8 min while others kept hammering same domain.
//
// NEW: 1 quick retry with 5s pause. If still blocked, return _rateLimited
//      immediately. The wave barrier gives the domain natural rest
//      (entire wave duration = minutes) before next attempt.
//
// The wave barrier is the retry mechanism, not the client.
const MAX_RATE_LIMIT_RETRIES = 1;
const RATE_LIMIT_PAUSE_MS = 5_000;   // 5 seconds

/**
 * Custom error for daily API key exhaustion.
 */
export class DailyLimitExhaustedError extends Error {
  constructor(message) {
    super(message);
    this.name = 'DailyLimitExhaustedError';
  }
}

/**
 * Check if a MailTester API response is a rate-limit signal.
 */
function isRateLimitResponse(data) {
  if (!data) return false;
  const candidates = [data.message, data.code, data.raw?.message, data.raw?.code];
  return candidates.some((v) => typeof v === 'string' && RATE_LIMIT_PATTERNS.test(v));
}

/**
 * Verifies an email address using MailTester Ninja.
 *
 * SPAM Block → 1 quick retry (5s) → still blocked? → return _rateLimited
 * The wave barrier handles the real retry with natural domain rest.
 */
export async function verifyEmail(email) {
  for (let attempt = 0; attempt <= MAX_RATE_LIMIT_RETRIES; attempt++) {
    let key;
    try {
      key = await acquireKey();
    } catch (err) {
      if (err.message && err.message.includes('exhausted')) {
        throw new DailyLimitExhaustedError(err.message);
      }
      throw err;
    }

    try {
      const url = `${config.mailTesterBaseUrl}?email=${encodeURIComponent(email)}&key=${encodeURIComponent(key)}`;
      console.log('[MailTester] Verifying', { email, key: key.slice(0, 10) + '...', attempt });

      const response = await axios.get(url);
      const data = response.data || {};

      console.log('[MailTester] Result', { email, code: data.code, message: data.message });

      // ── Rate-limit detection ──
      if (isRateLimitResponse(data)) {
        if (attempt < MAX_RATE_LIMIT_RETRIES) {
          console.warn(`[MailTester] Rate-limited (${data.message}), quick retry in ${RATE_LIMIT_PAUSE_MS / 1000}s`, { email });
          await sleep(RATE_LIMIT_PAUSE_MS);
          continue;
        }

        // Still blocked — return immediately, let wave barrier handle rest
        console.warn(`[MailTester] Rate-limited (${data.message}), returning to wave processor`, { email });
        return {
          email,
          code: data.code || null,
          message: data.message || null,
          raw: data,
          _rateLimited: true,
        };
      }

      // ── Normal response ──
      return {
        email,
        code: data.code || null,
        message: data.message || null,
        raw: data,
      };
    } catch (error) {
      console.error('[MailTester] Request failed', { email, error: error.message });
      return {
        email,
        code: null,
        message: null,
        raw: error.response?.data,
        error: error.message,
      };
    }
  }

  return { email, code: null, message: null, raw: null, error: 'Retry exhaustion' };
}