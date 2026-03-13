import { generatePatterns } from '../utils/emailPatterns.js';
import { verifyEmail } from '../clients/mailtester.client.js';
import { processContactsInBatches } from './comboProcessor.service.js';
import { DELIVERY_STATUS } from './upload/status.utils.js';

const MAX_COMBOS = 8;

/**
 * Serial two-pass enrichment:
 *
 *   Pass 1 — ALL contacts against Website (domain).
 *            Every result fires onResult immediately — no suppression.
 *
 *   Pass 2 — Contacts with Website_one that were NOT valid on Website.
 *            Overwrite rules:
 *              A) D2 valid         → always overwrite
 *              B) D1 empty domain  → overwrite with any D2 result
 *              C) otherwise        → silent, D1 stays
 */
export async function enrichContacts(contacts, options = {}) {
  if (!Array.isArray(contacts) || contacts.length === 0) {
    return { results: [], haltType: null, unprocessedRowIds: [] };
  }
  const { jobId } = options;

  // ── Pass 1: ALL contacts against Website ──

  const d1OnResult = options.onResult
    ? async (result) => {
        await options.onResult({ ...result, domainUsed: 'Website', notes: '' });
      }
    : undefined;

  const d1Batch = await processContactsInBatches(contacts, {
    verifyEmail,
    generatePatterns,
    maxCombos: MAX_COMBOS,
    onResult: d1OnResult,
    jobId,
  });

  const d1Results = d1Batch.results;
  const unprocessedRowIds = [...d1Batch.unprocessedRowIds];

  if (d1Batch.haltType) {
    return {
      results: d1Results,
      haltType: d1Batch.haltType,
      haltReason: d1Batch.haltReason,
      unprocessedRowIds,
    };
  }

  // ── Filter for Pass 2: has domain2 AND D1 was NOT valid ──

  const d2Candidates = [];
  contacts.forEach((contact, index) => {
    if (contact.domain2 && d1Results[index].status !== DELIVERY_STATUS.VALID) {
      d2Candidates.push({
        originalIndex: index,
        contact,
        d1Result: d1Results[index],
      });
    }
  });

  if (d2Candidates.length === 0) {
    return {
      results: d1Results,
      haltType: null,
      haltReason: null,
      unprocessedRowIds,
    };
  }

  // ── Pass 2: non-valid contacts against Website_one ──

  const d2Contacts = d2Candidates.map(({ contact }) => ({
    ...contact,
    domain: contact.domain2,
  }));

  // Track each contact's original domain and D1 status for the overwrite gate
  const d1InfoByRowId = new Map(
    d2Candidates.map(({ contact, d1Result }) => [
      contact.rowId,
      { originalDomain: contact.domain, d1Status: d1Result.status },
    ]),
  );

  const d2OnResult = options.onResult
    ? async (result) => {
        const rowId = result?.contact?.rowId;
        const info = d1InfoByRowId.get(rowId);
        if (!info) return;

        const d1HadEmptyDomain = !info.originalDomain;
        const d2IsValid = result.status === DELIVERY_STATUS.VALID;

        if (d2IsValid || d1HadEmptyDomain) {
          const notes = d2IsValid
            ? 'Valid on second domain'
            : 'Fallback to second domain';

          await options.onResult({
            ...result,
            domainUsed: 'Website_one',
            notes,
            _replaces: info.d1Status,
          });
        }
        // else: D1 had real domain AND D2 not valid → silent, D1 stays
      }
    : undefined;

  const d2Batch = await processContactsInBatches(d2Contacts, {
    verifyEmail,
    generatePatterns,
    maxCombos: MAX_COMBOS,
    onResult: d2OnResult,
    jobId,
  });

  unprocessedRowIds.push(...d2Batch.unprocessedRowIds);

  // ── Build final results: apply same overwrite logic ──

  const finalResults = d1Results.map((r) => ({ ...r }));

  d2Candidates.forEach(({ originalIndex, contact, d1Result }, i) => {
    const d2 = d2Batch.results[i];
    if (!d2) return;

    const d1HadEmptyDomain = !contact.domain;
    const d2IsValid = d2.status === DELIVERY_STATUS.VALID;

    if (d2IsValid || d1HadEmptyDomain) {
      finalResults[originalIndex] = {
        contact: d2.contact,
        bestEmail: d2.bestEmail,
        status: d2.status,
        details: d2.details,
        resultsPerCombo: [
          ...(d1Result.resultsPerCombo || []),
          ...(d2.resultsPerCombo || []),
        ],
        domainUsed: 'Website_one',
        notes: d2IsValid
          ? 'Valid on second domain'
          : 'Fallback to second domain',
      };
    }
  });

  return {
    results: finalResults,
    haltType: d2Batch.haltType || null,
    haltReason: d2Batch.haltReason || null,
    unprocessedRowIds,
  };
}