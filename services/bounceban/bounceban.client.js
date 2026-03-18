/**
 * BounceBan API Client
 *
 * Two-step verification:
 *   1. GET /v1/verify/single?email=xxx  → { id, result?, ... }
 *   2. GET /v1/verify/single/status?id=xxx → { result: "deliverable"|"undeliverable"|... }
 *
 * Rate limits: 100 req/s per endpoint.
 * Uses a serial queue to guarantee 10ms spacing even under concurrency.
 *
 * Poll behaviour: polls indefinitely (up to ABSOLUTE_TIMEOUT_MS) rather than
 * giving up after N polls. Slow verifications are kept alive while other rows
 * process concurrently — they resolve whenever BounceBan finally responds.
 */

import axios from 'axios';

const BASE_URL = () => process.env.BOUNCEBAN_BASE_URL || 'https://api.bounceban.com';
const API_KEY  = () => process.env.BOUNCEBAN_API_KEY  || '';

const SUBMIT_INTERVAL_MS   = 10;               // ≤100 req/s on submit endpoint
const STATUS_INTERVAL_MS   = 10;               // ≤100 req/s on status endpoint
const STATUS_POLL_DELAY    = 1500;             // wait before first status poll (ms)
const STATUS_POLL_INTERVAL = 1500;             // base interval between polls (ms)
const ABSOLUTE_TIMEOUT_MS  = 10 * 60 * 1000;  // 10 min hard ceiling per email

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

// ── Serial queue-based throttle ──────────────────────────────────────────────
// Promise chain guarantees each request waits >= N ms after the previous one,
// regardless of how many concurrent callers are waiting.

let submitQueue = Promise.resolve();
let statusQueue = Promise.resolve();

function enqueueSubmit() {
  let release;
  const ticket = new Promise((r) => { release = r; });
  const prev = submitQueue;
  submitQueue = ticket;
  return prev.then(() => sleep(SUBMIT_INTERVAL_MS)).then(() => release);
}

function enqueueStatus() {
  let release;
  const ticket = new Promise((r) => { release = r; });
  const prev = statusQueue;
  statusQueue = ticket;
  return prev.then(() => sleep(STATUS_INTERVAL_MS)).then(() => release);
}

/**
 * Submit a single email for verification.
 */
export async function submitVerification(email) {
  const release = await enqueueSubmit();
  try {
    const key = API_KEY();
    if (!key) throw new Error('BOUNCEBAN_API_KEY not set');

    const { data } = await axios.get(`${BASE_URL()}/v1/verify/single`, {
      params: { email },
      headers: { Authorization: key },
      timeout: 30_000,
    });
    return data;
  } finally {
    release();
  }
}

/**
 * Poll verification result by id.
 */
export async function pollVerificationStatus(id) {
  const release = await enqueueStatus();
  try {
    const key = API_KEY();
    const { data } = await axios.get(`${BASE_URL()}/v1/verify/single/status`, {
      params: { id },
      headers: { Authorization: key },
      timeout: 30_000,
    });
    return data;
  } finally {
    release();
  }
}

/**
 * High-level: submit + poll until a terminal result is available.
 *
 * Never gives up early — keeps polling until BounceBan returns a terminal
 * result or the ABSOLUTE_TIMEOUT_MS wall-clock limit is reached.
 * All slow verifications run concurrently alongside other rows.
 *
 * Returns { email, result, raw }
 *   result: "deliverable" | "undeliverable" | "risky" | "unknown" | "error"
 */
export async function verifyEmail(email) {
  const deadline = Date.now() + ABSOLUTE_TIMEOUT_MS;

  try {
    const submitRes = await submitVerification(email);

    // Immediate terminal result (some emails resolve on submit)
    if (submitRes.result && !isPending(submitRes.result)) {
      return { email, result: normalizeResult(submitRes.result), raw: submitRes };
    }

    const verificationId = submitRes.id;
    if (!verificationId) {
      // No ID and no immediate result — treat as unknown
      return { email, result: normalizeResult(submitRes.result || 'unknown'), raw: submitRes };
    }

    // Wait before first poll
    await sleep(STATUS_POLL_DELAY);

    // Poll indefinitely until terminal result or absolute deadline
    let pollCount = 0;
    while (Date.now() < deadline) {
      pollCount++;
      const statusRes = await pollVerificationStatus(verificationId);
      const result = statusRes.result || statusRes.status;

      if (result && !isPending(result)) {
        return { email, result: normalizeResult(result), raw: statusRes };
      }

      // Exponential back-off capped at 5 s for very long-running checks
      const wait = Math.min(STATUS_POLL_INTERVAL * Math.pow(1.2, Math.min(pollCount - 1, 8)), 5000);
      await sleep(wait);
    }

    // Absolute timeout reached
    return { email, result: 'unknown', raw: { timeout: true, pollCount } };

  } catch (err) {
    if (err.response?.status === 408) {
      return { email, result: 'unknown', raw: { timeout: true, status: 408 } };
    }
    return { email, result: 'error', raw: { error: err.message, status: err.response?.status } };
  }
}

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