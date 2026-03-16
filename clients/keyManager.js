import { config } from '../config/env.js';

/**
 * In-memory key scheduler.
 *
 * Replaces the external key-rotation microservice (MongoDB + Redis + BullMQ).
 * Zero network overhead — just timestamp math in the Node.js event loop.
 *
 * ── How it works ──
 *
 *   All concurrent callers funnel through a single promise chain (_queue).
 *   Each acquireKey() call:
 *
 *     1. Finds which key becomes available soonest
 *        (nextAvailableAt = lastUsed + intervalMs).
 *     2. Reserves the slot by setting lastUsed = max(now, nextAvailableAt).
 *     3. Returns { key, waitMs } — caller sleeps waitMs then fires the API.
 *
 *   Because the queue is FIFO and each reservation advances lastUsed into
 *   the future, no two callers ever get the same time slot on the same key.
 *   This is mathematically impossible, not probabilistically unlikely.
 *
 * ── Scaling ──
 *
 *   Add more keys in .env → scheduler auto-interleaves them.
 *
 *   1 ultimate:     5.88 req/s   =   353/min
 *   1 ultimate_2x: 11.36 req/s   =   682/min
 *   ───────────────────────────────────────────
 *   combined:      17.24 req/s   = 1,035/min
 *
 *   2 ultimate + 2 ultimate_2x:
 *                  34.48 req/s   = 2,069/min
 *
 *   ...and so on.  Each key independently tracks its own lastUsed and
 *   dailyUsed counters.  The scheduler always picks the soonest-available.
 */

const DAY_MS = 86_400_000;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

class KeyScheduler {
  constructor(keyConfigs) {
    if (!keyConfigs || keyConfigs.length === 0) {
      throw new Error(
        'No API keys configured.  Set MAILTESTER_KEYS in your .env file.\n' +
        'Format: MAILTESTER_KEYS=sub_xxx:ultimate,sub_yyy:ultimate_2x'
      );
    }

    this._slots = keyConfigs.map((cfg) => ({
      key: cfg.key,
      plan: cfg.plan,
      intervalMs: cfg.intervalMs,
      dailyLimit: cfg.dailyLimit,
      lastUsed: 0,     // epoch ms of last reserved slot
      usedToday: 0,    // requests used in current 24h window
      dayStart: Date.now(),
    }));

    // FIFO promise chain — serializes all acquireKey() calls.
    // Each link does <0.1ms of math, so the queue never blocks.
    this._queue = Promise.resolve();

    const totalPerSec = this._slots.reduce((s, k) => s + 1000 / k.intervalMs, 0);
    console.log('[KeyScheduler] Ready', {
      keys: this._slots.map((s) => `${s.plan} (${s.intervalMs}ms)`),
      maxReqPerSec: totalPerSec.toFixed(1),
      maxReqPerMin: Math.round(totalPerSec * 60),
    });
  }

  /**
   * Reserve the next available key slot.
   *
   * Returns a promise that resolves to { key, plan, waitMs }.
   * The caller must sleep(waitMs) before using the key.
   */
  acquireKey() {
    return new Promise((resolve, reject) => {
      this._queue = this._queue.then(() => {
        const now = Date.now();

        // Reset daily counters for keys whose 24h window has elapsed.
        for (const slot of this._slots) {
          if (now - slot.dayStart >= DAY_MS) {
            if (slot.usedToday > 0) {
              console.log('[KeyScheduler] Daily reset', {
                plan: slot.plan,
                used: slot.usedToday,
                limit: slot.dailyLimit,
              });
            }
            slot.usedToday = 0;
            slot.dayStart = now;
          }
        }

        // Find the key that becomes available soonest.
        let bestSlot = null;
        let bestAvailableAt = Infinity;

        for (const slot of this._slots) {
          if (slot.usedToday >= slot.dailyLimit) continue;
          const availableAt = Math.max(now, slot.lastUsed + slot.intervalMs);
          if (availableAt < bestAvailableAt) {
            bestAvailableAt = availableAt;
            bestSlot = slot;
          }
        }

        if (!bestSlot) {
          reject(new Error(
            'All API keys exhausted for today.  Daily limits: ' +
            this._slots.map((s) => `${s.plan}: ${s.usedToday}/${s.dailyLimit}`).join(', ')
          ));
          return;
        }

        // Reserve the slot by advancing lastUsed to the future timestamp.
        // This prevents the next queued caller from double-booking.
        bestSlot.lastUsed = bestAvailableAt;
        bestSlot.usedToday += 1;

        const waitMs = Math.max(0, bestAvailableAt - Date.now());

        resolve({ key: bestSlot.key, plan: bestSlot.plan, waitMs });
      }).catch(reject);
    });
  }

  /**
   * Snapshot of all keys' current state (for monitoring / health endpoint).
   */
  getStatus() {
    const now = Date.now();
    return this._slots.map((slot) => ({
      plan: slot.plan,
      intervalMs: slot.intervalMs,
      usedToday: slot.usedToday,
      dailyLimit: slot.dailyLimit,
      dailyRemaining: Math.max(0, slot.dailyLimit - slot.usedToday),
      nextAvailableInMs: Math.max(0, slot.lastUsed + slot.intervalMs - now),
    }));
  }
}

// ── Singleton ──
// All concurrent verifyEmail() calls share the same scheduler instance.
let _instance = null;

export function getKeyScheduler() {
  if (!_instance) {
    _instance = new KeyScheduler(config.keys);
  }
  return _instance;
}

/**
 * Acquire a key and sleep until it's safe to use.
 *
 * This is the only function the rest of the app needs to call.
 * By the time this returns, the key is ready — fire immediately.
 *
 * @returns {Promise<string>} The MailTester API key string.
 */
export async function acquireKey() {
  const scheduler = getKeyScheduler();
  const { key, waitMs } = await scheduler.acquireKey();

  if (waitMs > 0) {
    await sleep(waitMs);
  }

  return key;
}
