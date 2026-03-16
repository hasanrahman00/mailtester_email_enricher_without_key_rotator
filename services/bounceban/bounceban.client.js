/**
 * BounceBan API Client
 *
 * Two-step verification:
 *   1. GET /v1/verify/single?email=xxx  → { id, result?, ... }
 *   2. GET /v1/verify/single/status?id=xxx → { result: "deliverable"|"undeliverable"|... }
 *
 * Rate limits: 100 req/s per endpoint.
 * Uses a serial queue to guarantee 10ms spacing even under concurrency.
 */

import axios from 'axios';

const BASE_URL = () => process.env.BOUNCEBAN_BASE_URL || 'https://api.bounceban.com';
const API_KEY  = () => process.env.BOUNCEBAN_API_KEY  || '';

const SUBMIT_INTERVAL_MS   = 10;   // ≤100 req/s
const STATUS_INTERVAL_MS   = 10;
const STATUS_POLL_DELAY    = 2000; // wait before first status poll
const STATUS_POLL_MAX      = 30;   // max polls (~60 s)
const STATUS_POLL_INTERVAL = 2000;

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

// ── Serial queue-based throttle ──────────────────────────────────────────────
// A simple promise chain that guarantees each request waits at least N ms after
// the previous one, regardless of how many concurrent callers are waiting.
// This replaces the broken timestamp-based throttle that got bypassed by
// concurrent Promise.allSettled calls all reading the same lastTime.

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
 * Returns { email, result, raw }
 *   result: "deliverable" | "undeliverable" | "risky" | "unknown" | "error"
 */
export async function verifyEmail(email) {
  try {
    const submitRes = await submitVerification(email);

    // Immediate terminal result
    if (submitRes.result && !isPending(submitRes.result)) {
      return { email, result: normalizeResult(submitRes.result), raw: submitRes };
    }

    const verificationId = submitRes.id;
    if (!verificationId) {
      return { email, result: normalizeResult(submitRes.result || 'unknown'), raw: submitRes };
    }

    // Poll for status
    await sleep(STATUS_POLL_DELAY);

    for (let i = 0; i < STATUS_POLL_MAX; i++) {
      const statusRes = await pollVerificationStatus(verificationId);
      const result = statusRes.result || statusRes.status;

      if (result && !isPending(result)) {
        return { email, result: normalizeResult(result), raw: statusRes };
      }
      await sleep(STATUS_POLL_INTERVAL);
    }

    return { email, result: 'unknown', raw: { timeout: true } };
  } catch (err) {
    if (err.response?.status === 408) {
      return { email, result: 'unknown', raw: { timeout: true, status: 408 } };
    }
    return { email, result: 'error', raw: { error: err.message, status: err.response?.status } };
  }
}

function isPending(r) {
  const v = String(r).toLowerCase().trim();
  return v === 'pending' || v === 'processing';
}

function normalizeResult(raw) {
  if (!raw) return 'unknown';
  const r = String(raw).toLowerCase().trim();
  if (r === 'deliverable' || r === 'valid') return 'deliverable';
  if (r === 'undeliverable' || r === 'invalid') return 'undeliverable';
  if (r === 'risky' || r === 'catch-all' || r === 'catchall' || r === 'accept_all' || r === 'accept-all') return 'risky';
  if (r === 'unknown') return 'unknown';
  return r;
}

/**
 * Check if the API key is configured.
 */
export function isConfigured() {
  return Boolean(API_KEY());
}