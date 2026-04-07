/**
 * clients/key-scheduler.js — Manages API key rate limiting.
 *
 * All callers enter a FIFO queue. Scheduler picks the key
 * that becomes available soonest. Multiple keys interleave automatically.
 */

const config = require('../config/env');
const sleep = require('../utils/sleep');
const { KeyScheduler } = require('./key-scheduler-core');

// Singleton — all requests share one scheduler
let _instance = null;

function getKeyScheduler() {
  if (!_instance) _instance = new KeyScheduler(config.keys);
  return _instance;
}

// High-level: get a key, wait if needed, return ready to use
async function acquireKey() {
  const { key, waitMs } = await getKeyScheduler().acquireKey();
  if (waitMs > 0) await sleep(waitMs);
  return key;
}

module.exports = { getKeyScheduler, acquireKey };
