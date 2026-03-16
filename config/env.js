const PLAN_SPECS = {
  pro:         { intervalMs: 955, dailyLimit: 100_000 },
  ultimate:    { intervalMs: 184, dailyLimit: 500_000 },
  ultimate_2x: { intervalMs: 92,  dailyLimit: 1_000_000 },
};

function parseKeys() {
  const raw = process.env.MAILTESTER_KEYS || '';
  if (!raw.trim()) return [];
  return raw.split(',').map((e) => e.trim()).filter(Boolean).map((entry) => {
    const idx = entry.indexOf(':');
    const rawKey = idx > 0 ? entry.slice(0, idx) : entry;
    const rawPlan = idx > 0 ? entry.slice(idx + 1) : 'ultimate';
    const key = rawKey.replace(/[{}]/g, '').trim();
    const plan = (rawPlan || 'ultimate').toLowerCase().trim();
    const spec = PLAN_SPECS[plan] || PLAN_SPECS.ultimate;
    return key ? { key, plan, intervalMs: spec.intervalMs, dailyLimit: spec.dailyLimit } : null;
  }).filter(Boolean);
}

const keys = parseKeys();
if (keys.length) {
  const rps = keys.reduce((s, k) => s + 1000 / k.intervalMs, 0);
  console.log(`[Config] ${keys.length} key(s): ${rps.toFixed(1)} req/s, ${Math.round(rps * 60)}/min`);
} else {
  console.warn('[Config] No MAILTESTER_KEYS in .env');
}

export const config = {
  mailTesterBaseUrl: process.env.MAILTESTER_BASE_URL || 'https://happy.mailtester.ninja/ninja',
  keys,
  comboBatchSize: Number(process.env.COMBO_BATCH_SIZE) || 22,
  port: Number(process.env.PORT) || 3000,
};

// ── BounceBan configuration ──
export const bounceBanConfig = {
  apiKey: process.env.BOUNCEBAN_API_KEY || '',
  baseUrl: process.env.BOUNCEBAN_BASE_URL || 'https://api.bounceban.com',
};

if (bounceBanConfig.apiKey) {
  console.log('[Config] BounceBan API key configured (catch-all cleaner ready)');
} else {
  console.warn('[Config] No BOUNCEBAN_API_KEY in .env — catch-all cleaner disabled');
}