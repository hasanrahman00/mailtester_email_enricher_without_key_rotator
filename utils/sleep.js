/**
 * utils/sleep.js — Pauses execution for a given number of milliseconds.
 *
 * Usage:
 *   const sleep = require('./sleep');
 *   await sleep(1000); // waits 1 second
 */

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

module.exports = sleep;
