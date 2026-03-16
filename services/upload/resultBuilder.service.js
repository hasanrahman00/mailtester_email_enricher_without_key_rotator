// Shapes final API payloads by merging normalized rows with enrichment results and summaries.
import { DELIVERY_STATUS } from './status.utils.js';

export function buildResultSets(normalizedRows, enrichmentResults) {
  const apiResults = [];
  let enrichmentIndex = 0;

  normalizedRows.forEach((row) => {
    if (!row.contact) {
      const skipResult = {
        firstName: row.profile.firstName,
        lastName: row.profile.lastName,
        domain: row.profile.domain,
        bestEmail: row.existingEmail || null,
        status: row.existingEmail ? DELIVERY_STATUS.VALID : DELIVERY_STATUS.NOT_FOUND,
        details: { reason: row.skipReason },
        allCheckedCandidates: [],
        domainUsed: '',
        notes: row.skipReason || '',
      };
      apiResults.push(skipResult);
      return;
    }

    const result = enrichmentResults[enrichmentIndex] || defaultResult(row.profile);
    enrichmentIndex += 1;
    apiResults.push(result);
  });

  return { apiResults };
}

function defaultResult(profile) {
  return {
    firstName: profile.firstName,
    lastName: profile.lastName,
    domain: profile.domain,
    bestEmail: null,
    status: DELIVERY_STATUS.NOT_FOUND,
    details: { reason: 'Unexpected processing mismatch' },
    allCheckedCandidates: [],
    domainUsed: '',
    notes: '',
  };
}
