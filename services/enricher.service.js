import { generatePatterns } from '../utils/emailPatterns.js';
import { verifyEmail } from '../clients/mailtester.client.js';
import { processContactsInBatches } from './comboProcessor.service.js';
import { DELIVERY_STATUS } from './upload/status.utils.js';

const MAX_COMBOS = 8;

export async function enrichContacts(contacts, options = {}) {
  if (!Array.isArray(contacts) || contacts.length === 0) return { results: [], haltType: null, unprocessedRowIds: [] };
  const { jobId } = options;
  const dualDomainRowIds = new Set(contacts.filter((c) => c.domain2).map((c) => c.rowId));
  const finalResults = new Array(contacts.length);

  const d1OnResult = async (result) => {
    const rowId = result?.contact?.rowId;
    if (dualDomainRowIds.has(rowId)) {
      if (result.status === DELIVERY_STATUS.VALID && options.onResult) {
        await options.onResult({ ...result, domainUsed: 'Website', notes: '' });
      }
    } else if (options.onResult) {
      await options.onResult({ ...result, domainUsed: 'Website', notes: '' });
    }
  };

  const d1Batch = await processContactsInBatches(contacts, {
    verifyEmail, generatePatterns, maxCombos: MAX_COMBOS, onResult: d1OnResult, jobId,
  });
  const d1Processed = d1Batch.results;
  const unprocessedRowIds = [...d1Batch.unprocessedRowIds];

  // If halted during first pass, stop here
  if (d1Batch.haltType) {
    return { results: finalResults.filter(Boolean), haltType: d1Batch.haltType, haltReason: d1Batch.haltReason, unprocessedRowIds };
  }

  const d2Candidates = [];
  contacts.forEach((contact, index) => {
    const d1 = d1Processed[index];
    if (!contact.domain2 || d1.status === DELIVERY_STATUS.VALID) {
      finalResults[index] = { firstName: d1.contact.firstName, lastName: d1.contact.lastName, domain: d1.contact.domain, bestEmail: d1.bestEmail, status: d1.status, details: d1.details, allCheckedCandidates: d1.resultsPerCombo, domainUsed: 'Website', notes: '' };
    } else {
      d2Candidates.push({ originalIndex: index, contact, d1Result: d1 });
    }
  });

  if (d2Candidates.length > 0) {
    const d2Contacts = d2Candidates.map(({ contact }) => ({ ...contact, domain: contact.domain2 }));
    const d2Batch = await processContactsInBatches(d2Contacts, { verifyEmail, generatePatterns, maxCombos: MAX_COMBOS, jobId });
    const d2Processed = d2Batch.results;
    unprocessedRowIds.push(...d2Batch.unprocessedRowIds);

    for (let i = 0; i < d2Candidates.length; i++) {
      const { originalIndex, contact, d1Result } = d2Candidates[i];
      const d2 = d2Processed[i];
      const base = { firstName: contact.firstName, lastName: contact.lastName, domain: contact.domain };
      const combos = [...(d1Result.resultsPerCombo || []), ...(d2?.resultsPerCombo || [])];
      let merged;

      if (d1Result.status === DELIVERY_STATUS.CATCH_ALL && d2?.status === DELIVERY_STATUS.VALID) {
        merged = { ...base, bestEmail: d2.bestEmail, status: DELIVERY_STATUS.VALID, details: d2.details, allCheckedCandidates: combos, domainUsed: 'Website_one', notes: 'Valid on second domain' };
      } else if (d1Result.status === DELIVERY_STATUS.CATCH_ALL) {
        merged = { ...base, bestEmail: d1Result.bestEmail, status: DELIVERY_STATUS.CATCH_ALL, details: { reason: 'Primary catch-all' }, allCheckedCandidates: combos, domainUsed: 'Website', notes: 'Catch-all primary' };
      } else if (d2?.status === DELIVERY_STATUS.VALID) {
        merged = { ...base, bestEmail: d2.bestEmail, status: DELIVERY_STATUS.VALID, details: d2.details, allCheckedCandidates: combos, domainUsed: 'Website_one', notes: 'Valid on second domain' };
      } else if (d2?.status === DELIVERY_STATUS.CATCH_ALL) {
        merged = { ...base, bestEmail: d2.bestEmail, status: DELIVERY_STATUS.CATCH_ALL, details: d2.details, allCheckedCandidates: combos, domainUsed: 'Website_one', notes: 'D2 catch-all' };
      } else {
        merged = { ...base, bestEmail: null, status: DELIVERY_STATUS.NOT_FOUND, details: { reason: 'Both domains exhausted' }, allCheckedCandidates: combos, domainUsed: '', notes: '' };
      }

      finalResults[originalIndex] = merged;
      if (options.onResult) {
        await options.onResult({ contact, bestEmail: merged.bestEmail, status: merged.status, details: merged.details, resultsPerCombo: merged.allCheckedCandidates || [], domainUsed: merged.domainUsed, notes: merged.notes });
      }
    }

    if (d2Batch.haltType) {
      return { results: finalResults.filter(Boolean), haltType: d2Batch.haltType, haltReason: d2Batch.haltReason, unprocessedRowIds };
    }
  }

  return { results: finalResults, haltType: null, haltReason: null, unprocessedRowIds };
}
