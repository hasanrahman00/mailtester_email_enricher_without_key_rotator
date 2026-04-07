/**
 * clients/key-scheduler-core.js — KeyScheduler class.
 *
 * FIFO queue that picks the soonest-available key.
 * Each key tracks its own timing and daily usage.
 * Add more keys in .env to scale throughput automatically.
 */

const DAY_MS = 86_400_000;

class KeyScheduler {
  constructor(keyConfigs) {
    if (!keyConfigs?.length) throw new Error('No API keys configured. Set MAILTESTER_KEYS in .env');
    this._slots = keyConfigs.map((cfg) => ({
      key: cfg.key, plan: cfg.plan, intervalMs: cfg.intervalMs,
      dailyLimit: cfg.dailyLimit, lastUsed: 0, usedToday: 0, dayStart: Date.now(),
    }));
    this._queue = Promise.resolve();
    this._minCount = 0;       // combos in current minute
    this._minStart = 0;       // when current minute started
    this._lastMinCount = 0;   // combos from the previous completed minute
    this._totalCombos = 0;    // lifetime combo counter
  }

  // Reserve the next available key slot
  acquireKey() {
    return new Promise((resolve, reject) => {
      this._queue = this._queue.then(() => {
        const now = Date.now();
        // Reset daily counters if 24h elapsed
        for (const s of this._slots) {
          if (now - s.dayStart >= DAY_MS) { s.usedToday = 0; s.dayStart = now; }
        }
        // Find soonest-available key
        let best = null, bestAt = Infinity;
        for (const s of this._slots) {
          if (s.usedToday >= s.dailyLimit) continue;
          const at = Math.max(now, s.lastUsed + s.intervalMs);
          if (at < bestAt) { bestAt = at; best = s; }
        }
        if (!best) return reject(new Error('All API keys exhausted for today'));
        best.lastUsed = bestAt;
        best.usedToday++;
        // Track combo rate — fixed 1-minute window that resets
        const tick = Date.now();
        this._totalCombos++;
        if (!this._minStart) this._minStart = tick;
        if (tick - this._minStart >= 60_000) {
          this._lastMinCount = this._minCount;
          this._minCount = 0;
          this._minStart = tick;
        }
        this._minCount++;
        resolve({ key: best.key, plan: best.plan, waitMs: Math.max(0, bestAt - Date.now()) });
      }).catch(reject);
    });
  }

  // Current status of all keys (for health endpoint)
  getStatus() {
    const now = Date.now();
    return this._slots.map((s) => ({
      plan: s.plan, intervalMs: s.intervalMs, usedToday: s.usedToday,
      dailyLimit: s.dailyLimit, dailyRemaining: Math.max(0, s.dailyLimit - s.usedToday),
      nextAvailableInMs: Math.max(0, s.lastUsed + s.intervalMs - now),
    }));
  }

  // Real-time combo rate — fixed 1-minute window, resets every 60s
  getRate() {
    const now = Date.now();
    // If minute has elapsed since last tick, reset
    if (this._minStart && now - this._minStart >= 60_000) {
      this._lastMinCount = this._minCount;
      this._minCount = 0;
      this._minStart = now;
    }
    // Show current minute's count while active, previous minute's count if current is still building
    const combosThisMin = this._minCount;
    const combosLastMin = this._lastMinCount;
    const elapsed = Math.min(60, (now - this._minStart) / 1000) || 1;
    return { combosThisMin, combosLastMin, totalCombos: this._totalCombos, elapsedSec: Math.round(elapsed) };
  }
}

module.exports = { KeyScheduler };
