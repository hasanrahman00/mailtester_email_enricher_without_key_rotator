import { DELIVERY_STATUS, normalizeDeliveryStatus } from './status.utils.js';

export function createProgressSnapshot(totalContacts, skippedRows) {
  return {
    totalContacts,
    processedContacts: 0,
    statusCounts: {
      [DELIVERY_STATUS.VALID]: 0,
      [DELIVERY_STATUS.CATCH_ALL]: 0,
      [DELIVERY_STATUS.NOT_FOUND]: 0,
      [DELIVERY_STATUS.RATE_LIMITED]: 0,
      skipped: skippedRows,
    },
  };
}

export function normalizeStatusBucket(status) {
  return normalizeDeliveryStatus(status);
}
