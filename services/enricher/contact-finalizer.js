/**
 * services/enricher/contact-finalizer.js — Finalizes contacts after all waves.
 *
 * Contacts that weren't resolved during waves get a final status:
 * - no_domain: no domain was provided
 * - not_found: all patterns were rejected
 * - unprocessed: job was halted before completion
 */

const { DELIVERY_STATUS } = require('../../utils/status-codes');
const { buildPayload } = require('./contact-handler');

async function finalizeContacts(states, haltType, onResult, log) {
  const unprocessedRowIds = [];

  for (const state of states) {
    if (state.done) continue;

    // Job was halted — mark as unprocessed
    if (haltType) {
      state.done = true;
      if (typeof state.contact?.rowId === 'number') unprocessedRowIds.push(state.contact.rowId);
      continue;
    }

    // No patterns = no domain provided
    if (state.patterns.length === 0) {
      state.status = DELIVERY_STATUS.NO_DOMAIN;
      state.details = { reason: 'No domain provided' };
      state.done = true;
      if (onResult) await onResult(buildPayload(state));
      continue;
    }

    // All patterns tried and rejected
    state.bestEmail = null;
    state.status = DELIVERY_STATUS.NOT_FOUND;
    state.details = { reason: 'All candidates rejected' };
    state.done = true;
    if (log) log(`Finalized ${state.contact.firstName} ${state.contact.lastName} -> ${state.status}`);
    if (onResult) await onResult(buildPayload(state));
  }

  return unprocessedRowIds;
}

module.exports = { finalizeContacts };
