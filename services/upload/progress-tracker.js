/**
 * services/upload/progress-tracker.js — Tracks enrichment progress.
 *
 * Creates a snapshot of progress counts (valid, catch_all, not_found, etc.)
 * that gets updated in real-time as emails are verified.
 */

const { DELIVERY_STATUS, normalizeDeliveryStatus } = require('../../utils/status-codes');

// Create initial progress object with zero counts
function createProgressSnapshot(totalContacts, skippedRows) {
  return {
    totalContacts,
    processedContacts: 0,
    statusCounts: {
      [DELIVERY_STATUS.VALID]: 0,
      [DELIVERY_STATUS.CATCH_ALL]: 0,
      [DELIVERY_STATUS.NOT_FOUND]: 0,
      [DELIVERY_STATUS.RATE_LIMITED]: 0,
      [DELIVERY_STATUS.MX_NOT_FOUND]: 0,
      [DELIVERY_STATUS.ERROR]: 0,
      skipped: skippedRows,
    },
  };
}

// Normalize a status string to its standard bucket name
function normalizeStatusBucket(status) {
  return normalizeDeliveryStatus(status);
}

module.exports = { createProgressSnapshot, normalizeStatusBucket };
