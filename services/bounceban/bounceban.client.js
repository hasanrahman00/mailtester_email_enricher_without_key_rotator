/**
 * BounceBan API Client — Waterfall Edition
 *
 * Uses the waterfall endpoint which holds the connection open and returns
 * the result directly — no submit + poll loop needed.
 *
 * Endpoint: https://api-waterfall.bounceban.com/v1/verify/single
 *   - Waits up to 80s internally before returning 408
 *   - 408 retries within 30 min are FREE (no credit charged)
 *   - Supports thousands of concurrent connections
 *
 * Concurrency: semaphore caps at MAX_CONCURRENT (100) parallel open
 * connections. All callers beyond that queue and are served as slots free up.
 */

import axios from 'axios';

const WATERFALL_URL      = 'https://api-waterfall.bounceban.com';
const API_KEY            = () => process.env.BOUNCEBAN_API_KEY || '';

const MAX_CONCURRENT     = 100;       // max parallel open connections to BounceBan
const WATERFALL_TIMEOUT  = 85_000;    // axios timeout — 80s (BounceBan) + 5s buffer
const MAX_RETRIES        = 5;         // retries on 408 (free within 30 min window)
const RETRY_DELAY_MS     = 2_000;     // pause before each 408 retry
const RATE_LIMIT_DELAY   = 5_000;     // pause on 429 before retry

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

// ── Semaphore — caps parallel open HTTP connections ──────────────────────────
// When 100 slots are in use, new callers wait in a queue.
// As soon as any slot finishes (success or error), the next waiter is unblocked.

let activeCount = 0;
const waitQueue = [];

function acquireSlot() {
  if (activeCount < MAX_CONCURRENT) {
    activeCount++;
    return Promise.resolve();
  }
  return new Promise((resolve) => waitQueue.push(resolve));
}

function releaseSlot() {
  if (waitQueue.length > 0) {
    const next = waitQueue.shift();
    next(); // unblock next waiter — activeCount stays the same
  } else {
    activeCount--;
  }
}

// ── Main verify function ─────────────────────────────────────────────────────

/**
 * Verify a single email via the BounceBan waterfall endpoint.
 *
 * Acquires a concurrency slot, fires the request, retries on 408 up to
 * MAX_RETRIES times (free within 30 min), then releases the slot.
 *
 * Returns { email, result, raw }
 *   result: "deliverable" | "undeliverable" | "risky" | "unknown" | "error"
 */
export async function verifyEmail(email) {
  await acquireSlot();

  try {
    const key = API_KEY();
    if (!key) throw new Error('BOUNCEBAN_API_KEY not set');

    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      try {
        const { data } = await axios.get(`${WATERFALL_URL}/v1/verify/single`, {
          params: { email },
          headers: { Authorization: key },
          timeout: WATERFALL_TIMEOUT,
        });

        // Terminal result returned directly — no polling needed
        if (data.result && !isPending(data.result)) {
          return { email, result: normalizeResult(data.result), raw: data };
        }

        // Response came back but result is still pending — treat as unknown
        return { email, result: 'unknown', raw: data };

      } catch (err) {
        const status = err.response?.status;

        if (status === 408) {
          // BounceBan timed out internally — retry is FREE within 30 min
          if (attempt < MAX_RETRIES) {
            await sleep(RETRY_DELAY_MS);
            continue;
          }
          return { email, result: 'unknown', raw: { timeout: true, attempts: attempt } };
        }

        if (status === 429) {
          // Rate limited — pause and retry
          await sleep(RATE_LIMIT_DELAY);
          continue;
        }

        // Any other error — don't retry
        return { email, result: 'error', raw: { error: err.message, status } };
      }
    }

    return { email, result: 'unknown', raw: { timeout: true } };

  } finally {
    releaseSlot();
  }
}

// ── Helpers ──────────────────────────────────────────────────────────────────

function isPending(r) {
  const v = String(r).toLowerCase().trim();
  return v === 'pending' || v === 'processing' || v === 'queued' || v === 'in_progress';
}

function normalizeResult(raw) {
  if (!raw) return 'unknown';
  const r = String(raw).toLowerCase().trim();
  if (r === 'deliverable' || r === 'valid')                    return 'deliverable';
  if (r === 'undeliverable' || r === 'invalid')                return 'undeliverable';
  if (r === 'risky' || r === 'catch-all' || r === 'catchall' ||
      r === 'accept_all' || r === 'accept-all')                return 'risky';
  if (r === 'unknown')                                         return 'unknown';
  return r;
}

/**
 * Check if the API key is configured.
 */
export function isConfigured() {
  return Boolean(API_KEY());
}