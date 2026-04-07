/**
 * services/upload/result-builder.js — Builds final API result payloads.
 *
 * Merges normalized rows with enrichment results. Handles skipped rows
 * (existing email, missing fields) by building placeholder results.
 */

const { DELIVERY_STATUS } = require('../../utils/status-codes');

// Build the final results array from normalized rows + enrichment data
function buildResultSets(normalizedRows, enrichmentResults) {
  const apiResults = [];
  let enrichIdx = 0;

  normalizedRows.forEach((row) => {
    // Skipped rows (existing email or missing data)
    if (!row.contact) {
      apiResults.push({
        firstName: row.profile.firstName,
        lastName: row.profile.lastName,
        domain: row.profile.domain,
        bestEmail: row.existingEmail || null,
        status: row.existingEmail ? DELIVERY_STATUS.VALID : DELIVERY_STATUS.NOT_FOUND,
        details: { reason: row.skipReason },
        allCheckedCandidates: [],
        domainUsed: '',
        notes: row.skipReason || '',
      });
      return;
    }

    // Enriched rows — pull from enrichment results
    const result = enrichmentResults[enrichIdx] || {
      firstName: row.profile.firstName,
      lastName: row.profile.lastName,
      domain: row.profile.domain,
      bestEmail: null,
      status: DELIVERY_STATUS.NOT_FOUND,
      details: { reason: 'Unexpected processing mismatch' },
      allCheckedCandidates: [],
      domainUsed: '',
      notes: '',
    };
    enrichIdx++;
    apiResults.push(result);
  });

  return { apiResults };
}

module.exports = { buildResultSets };
