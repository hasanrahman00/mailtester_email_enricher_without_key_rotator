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
// "SPAM Block" and "Limited" mean: you're sending too fast, slow down.
// These come back as HTTP 200 with code "ko" — they look like rejections
// but they're actually "try again later" signals.
const RATE_LIMIT_PATTERNS = /spam.block|limited/i;

// Retry config for SPAM Block responses.
// Each retry: pause 30–60s (random), acquire a fresh key, try the SAME email.
const MAX_RATE_LIMIT_RETRIES = 3;
const RATE_LIMIT_MIN_PAUSE_MS = 30_000;  // 30s
const RATE_LIMIT_MAX_PAUSE_MS = 60_000;  // 60s

function randomPause() {
  return RATE_LIMIT_MIN_PAUSE_MS + Math.floor(Math.random() * (RATE_LIMIT_MAX_PAUSE_MS - RATE_LIMIT_MIN_PAUSE_MS));
}

/**
 * Custom error for daily API key exhaustion.
 * comboProcessor catches this to PAUSE the job instead of
 * marking remaining contacts as NOT_FOUND.
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
 * ── What changed from the old version ──
 *
 * OLD: SPAM Block response → code "ko" → no pattern matched →
 *      combo index advanced → all 8 combos wasted → NOT_FOUND.
 *
 * OLD: acquireKey() throws "exhausted" → catch returns {code:null} →
 *      all 22 slots fail instantly → entire job marked NOT_FOUND.
 *
 * NEW: SPAM Block → retry 3× with 30-60s random pause using fresh keys.
 *      If still blocked, return with _rateLimited marker so
 *      comboProcessor does NOT advance the combo.
 *
 * NEW: Daily exhaustion → throw DailyLimitExhaustedError →
 *      comboProcessor pauses the job, doesn't burn contacts.
 */
export async function verifyEmail(email) {
  for (let attempt = 0; attempt <= MAX_RATE_LIMIT_RETRIES; attempt++) {
    // ── Acquire a key ──
    let key;
    try {
      key = await acquireKey();
    } catch (err) {
      // Daily cap reached — throw special error so the job pauses.
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
          const pauseMs = randomPause();
          console.warn(`[MailTester] Rate-limited (${data.message}), retry ${attempt + 1}/${MAX_RATE_LIMIT_RETRIES} in ${Math.round(pauseMs / 1000)}s`, { email });
          await sleep(pauseMs);
          continue;  // Retry same email with a new key
        }

        // All retries failed — return with marker so comboProcessor
        // does NOT waste the combo.
        console.error('[MailTester] Rate-limit persists after retries', { email, message: data.message });
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