/**
 * config/env.js — Loads and parses environment variables.
 *
 * Reads MAILTESTER_KEYS from .env and calculates rate limits.
 * Concurrency auto-scales based on number of keys — more keys = more parallel requests.
 *
 * API rate limits (from MailTester docs):
 *   Pro:      11 emails / 10s → 1 per 860ms, 100K/day
 *   Ultimate: 57 emails / 10s → 1 per 170ms, 500K/day
 *   QTY multipliers: Pro x2 = 200K/day, Ultimate x2 = 1M/day
 */

// Exact rates from MailTester API docs + small safety buffer
const PLAN_SPECS = {
  pro:         { intervalMs: 870, dailyLimit: 100_000 },
  ultimate:    { intervalMs: 176, dailyLimit: 500_000 },
  ultimate_2x: { intervalMs: 88,  dailyLimit: 1_000_000 },
};

// Parse "key1:plan1,key2:plan2" format from .env
function parseKeys() {
  const raw = process.env.MAILTESTER_KEYS || '';
  if (!raw.trim()) return [];

  return raw.split(',').map((e) => e.trim()).filter(Boolean).map((entry) => {
    const idx = entry.indexOf(':');
    const key = (idx > 0 ? entry.slice(0, idx) : entry).replace(/[{}]/g, '').trim();
    const plan = (idx > 0 ? entry.slice(idx + 1) : 'ultimate').toLowerCase().trim();
    const spec = PLAN_SPECS[plan] || PLAN_SPECS.ultimate;
    return key ? { key, plan, intervalMs: spec.intervalMs, dailyLimit: spec.dailyLimit } : null;
  }).filter(Boolean);
}

const keys = parseKeys();

// Auto-scale concurrency based on keys
// Workers must keep firing at rate-limit speed even while waiting for slow API responses.
// MailTester counts REQUESTS SENT, not responses — so we pipeline aggressively.
// Real API latency can be 3-5s. During that wait, the key becomes available again
// multiple times — we need enough idle workers to fill every slot.
// Extra workers are FREE — they just sit in the acquireKey queue, no CPU/memory cost.
const AVG_RESPONSE_MS = 5000; // worst-case real API latency
function calcConcurrency(keyList) {
  if (!keyList.length) return 1;
  let total = 0;
  for (const k of keyList) total += Math.ceil(AVG_RESPONSE_MS / k.intervalMs);
  // 1.5x headroom — ensures keys NEVER starve waiting for a free worker
  return Math.max(Math.ceil(total * 1.5), keyList.length);
}

const autoConcurrency = calcConcurrency(keys);

// Log throughput info on startup
if (keys.length) {
  const rps = keys.reduce((s, k) => s + 1000 / k.intervalMs, 0);
  const daily = keys.reduce((s, k) => s + k.dailyLimit, 0);
  console.log(`[Config] ${keys.length} key(s) | ${rps.toFixed(1)} req/s | ${Math.round(rps * 60)}/min | ${(daily / 1000).toFixed(0)}K/day | concurrency: ${autoConcurrency}`);
} else {
  console.warn('[Config] No MAILTESTER_KEYS in .env');
}

// Export the config object used everywhere in the app
module.exports = {
  mailTesterBaseUrl: process.env.MAILTESTER_BASE_URL || 'https://happy.mailtester.ninja/ninja',
  keys,
  comboBatchSize: autoConcurrency,
  port: Number(process.env.PORT) || 3000,
};
