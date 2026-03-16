import { generatePatterns } from '../utils/emailPatterns.js';
import { verifyEmail } from '../clients/mailtester.client.js';
import { processContactsInBatches } from './comboProcessor.service.js';
import { DELIVERY_STATUS } from './upload/status.utils.js';

const MAX_COMBOS = 8;

/**
 * Serial three-pass enrichment — Website → Website_one → Website_two
 *
 * Each pass is a fully independent barrier wave run. Pass N must complete
 * all waves for all its contacts before Pass N+1 starts. There is no
 * interleaving — the `await` on each processContactsInBatches call
 * enforces strict sequential execution.
 *
 * ── Pass 1 — Website (domain) — ALL contacts ──────────────────────────
 *   Every contact enters the barrier regardless of whether Website is empty.
 *   Empty domain → generatePatterns returns [] → Wave 0 has no queue →
 *   contact finalizes as no_domain (not not_found — it was never attempted).
 *   no_domain contacts are still eligible for Pass 2 and Pass 3.
 *   Valid contacts are dropped permanently after Pass 1.
 *
 * ── Pass 2 — Website_one (domain2) — non-valid contacts with domain2 ──
 *   Eligible: Pass 1 status !== valid AND contact.domain2 is not empty.
 *   This includes no_domain, not_found, catch_all, mx_not_found, error,
 *   rate_limited — anything except valid.
 *
 *   Overwrite gate:
 *     A) Pass 2 valid                    → always overwrite Pass 1 result
 *     B) Pass 1 was no_domain (empty)    → overwrite with any Pass 2 result
 *     C) Pass 1 had a real domain        → silent, Pass 1 result stays
 *        (catch_all from Pass 1 is protected from non-valid Pass 2)
 *
 *   IMPORTANT: no early return when d2Candidates is empty. Contacts with
 *   only domain3 (no domain2) must still reach Pass 3.
 *
 * ── Pass 3 — Website_two (domain3) — still-non-valid contacts with domain3
 *   Eligible: intermediate status !== valid AND contact.domain3 is not empty.
 *   Includes contacts that skipped Pass 2 entirely (had no domain2).
 *
 *   Overwrite gate:
 *     A) Pass 3 valid                    → always overwrite best-so-far
 *     B) contact had no domain2 at all   → overwrite with any Pass 3 result
 *     C) contact had a real domain2      → silent, best-so-far stays
 *        (catch_all from Pass 2 is protected from non-valid Pass 3)
 *
 * ── Catch-all bestEmail priority ──────────────────────────────────────
 *   Each pass uses the domain column it owns. patterns[0] = first@<that domain>.
 *   Priority naturally falls: Website → Website_one → Website_two.
 *   The overwrite gate prevents a catch-all from an earlier pass being
 *   replaced by a non-valid result from a later pass.
 */
export async function enrichContacts(contacts, options = {}) {
  if (!Array.isArray(contacts) || contacts.length === 0) {
    return { results: [], haltType: null, unprocessedRowIds: [] };
  }
  const { jobId } = options;

  // ─────────────────────────────────────────────────────────────────────
  // Pass 1 — Website domain — ALL contacts
  // ─────────────────────────────────────────────────────────────────────

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

  // ─────────────────────────────────────────────────────────────────────
  // Filter for Pass 2 — has domain2 AND Pass 1 was not valid
  //
  // NO early return here even if d2Candidates is empty.
  // Contacts with only domain3 must still reach Pass 3 below.
  // ─────────────────────────────────────────────────────────────────────

  const d2Candidates = [];
  contacts.forEach((contact, index) => {
    if (contact.domain2 && d1Results[index].status !== DELIVERY_STATUS.VALID) {
      d2Candidates.push({ originalIndex: index, contact, d1Result: d1Results[index] });
    }
  });

  // ─────────────────────────────────────────────────────────────────────
  // Pass 2 — Website_one domain (block skipped when no candidates)
  // ─────────────────────────────────────────────────────────────────────

  let d2Batch = { results: [], haltType: null, haltReason: null, unprocessedRowIds: [] };

  if (d2Candidates.length > 0) {
    const d2Contacts = d2Candidates.map(({ contact }) => ({
      ...contact,
      domain: contact.domain2,
    }));

    // Track Pass 1 domain per rowId — needed for the overwrite gate in onResult
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
            await options.onResult({
              ...result,
              domainUsed: 'Website_one',
              notes: d2IsValid ? 'Valid on second domain' : 'Fallback to second domain',
              _replaces: info.d1Status,
            });
          }
          // else: Pass 1 had a real domain and Pass 2 not valid → silent, Pass 1 stays
        }
      : undefined;

    d2Batch = await processContactsInBatches(d2Contacts, {
      verifyEmail,
      generatePatterns,
      maxCombos: MAX_COMBOS,
      onResult: d2OnResult,
      jobId,
    });

    unprocessedRowIds.push(...d2Batch.unprocessedRowIds);
  }

  // Surface halt after Pass 2 with partial results
  if (d2Batch.haltType) {
    const partialResults = d1Results.map((r) => ({ ...r }));
    d2Candidates.forEach(({ originalIndex, contact, d1Result }, i) => {
      const d2 = d2Batch.results[i];
      if (!d2) return;
      const d2IsValid = d2.status === DELIVERY_STATUS.VALID;
      const d1HadEmptyDomain = !contact.domain;
      if (d2IsValid || d1HadEmptyDomain) {
        partialResults[originalIndex] = buildMergedResult(d2, d1Result, 'Website_one', d2IsValid);
      }
    });
    return {
      results: partialResults,
      haltType: d2Batch.haltType,
      haltReason: d2Batch.haltReason,
      unprocessedRowIds,
    };
  }

  // ─────────────────────────────────────────────────────────────────────
  // Build intermediate results after Pass 2.
  // Record domain2 per originalIndex — needed for the Pass 3 overwrite gate.
  // ─────────────────────────────────────────────────────────────────────

  const intermediateResults = d1Results.map((r) => ({ ...r }));
  const d2DomainByOriginalIndex = new Map();

  d2Candidates.forEach(({ originalIndex, contact, d1Result }, i) => {
    const d2 = d2Batch.results[i];
    if (!d2) return;

    const d1HadEmptyDomain = !contact.domain;
    const d2IsValid = d2.status === DELIVERY_STATUS.VALID;

    if (d2IsValid || d1HadEmptyDomain) {
      intermediateResults[originalIndex] = buildMergedResult(d2, d1Result, 'Website_one', d2IsValid);
    }

    // Always record domain2 so Pass 3 gate can check if it existed
    d2DomainByOriginalIndex.set(originalIndex, contact.domain2 || '');
  });

  // ─────────────────────────────────────────────────────────────────────
  // Filter for Pass 3 — has domain3 AND intermediate result is not valid.
  // Also catches contacts that had no domain2 (skipped Pass 2 entirely).
  // ─────────────────────────────────────────────────────────────────────

  const d3Candidates = [];
  contacts.forEach((contact, index) => {
    if (
      contact.domain3 &&
      intermediateResults[index].status !== DELIVERY_STATUS.VALID
    ) {
      d3Candidates.push({
        originalIndex: index,
        contact,
        intermediateResult: intermediateResults[index],
        // domain2 for this contact — empty string when contact had no domain2
        priorDomain: d2DomainByOriginalIndex.get(index) ?? contact.domain2 ?? '',
      });
    }
  });

  if (d3Candidates.length === 0) {
    return {
      results: intermediateResults,
      haltType: null,
      haltReason: null,
      unprocessedRowIds,
    };
  }

  // ─────────────────────────────────────────────────────────────────────
  // Pass 3 — Website_two domain
  // ─────────────────────────────────────────────────────────────────────

  const d3Contacts = d3Candidates.map(({ contact }) => ({
    ...contact,
    domain: contact.domain3,
  }));

  // Map rowId → priorDomain so the onResult gate can check if domain2 existed
  const priorDomainByRowId = new Map(
    d3Candidates.map(({ contact, priorDomain }) => [contact.rowId, priorDomain]),
  );

  const originalIndexByRowId = new Map(
    d3Candidates.map(({ contact, originalIndex }) => [contact.rowId, originalIndex]),
  );

  const d3OnResult = options.onResult
    ? async (result) => {
        const rowId = result?.contact?.rowId;
        const priorDomain = priorDomainByRowId.get(rowId);
        if (priorDomain === undefined) return;

        const priorHadEmptyDomain = !priorDomain;
        const d3IsValid = result.status === DELIVERY_STATUS.VALID;

        if (d3IsValid || priorHadEmptyDomain) {
          const origIdx = originalIndexByRowId.get(rowId);
          await options.onResult({
            ...result,
            domainUsed: 'Website_two',
            notes: d3IsValid ? 'Valid on third domain' : 'Fallback to third domain',
            _replaces: origIdx !== undefined ? intermediateResults[origIdx]?.status : undefined,
          });
        }
        // else: contact had real domain2 and Pass 3 not valid → silent, intermediate stays
      }
    : undefined;

  const d3Batch = await processContactsInBatches(d3Contacts, {
    verifyEmail,
    generatePatterns,
    maxCombos: MAX_COMBOS,
    onResult: d3OnResult,
    jobId,
  });

  unprocessedRowIds.push(...d3Batch.unprocessedRowIds);

  // ─────────────────────────────────────────────────────────────────────
  // Build final results — apply Pass 3 overwrite logic
  // ─────────────────────────────────────────────────────────────────────

  const finalResults = intermediateResults.map((r) => ({ ...r }));

  d3Candidates.forEach(({ originalIndex, contact, intermediateResult }, i) => {
    const d3 = d3Batch.results[i];
    if (!d3) return;

    const priorHadEmptyDomain = !contact.domain2;
    const d3IsValid = d3.status === DELIVERY_STATUS.VALID;

    if (d3IsValid || priorHadEmptyDomain) {
      finalResults[originalIndex] = buildMergedResult(d3, intermediateResult, 'Website_two', d3IsValid);
    }
  });

  return {
    results: finalResults,
    haltType: d3Batch.haltType || null,
    haltReason: d3Batch.haltReason || null,
    unprocessedRowIds,
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// Shared helper — merge a pass result into the running best result
// ─────────────────────────────────────────────────────────────────────────────

function buildMergedResult(passResult, priorResult, domainLabel, isValid) {
  const domainWord = domainLabel === 'Website_one' ? 'second' : 'third';
  return {
    contact: passResult.contact,
    bestEmail: passResult.bestEmail,
    status: passResult.status,
    details: passResult.details,
    resultsPerCombo: [
      ...(priorResult.resultsPerCombo || []),
      ...(passResult.resultsPerCombo || []),
    ],
    domainUsed: domainLabel,
    notes: isValid ? `Valid on ${domainWord} domain` : `Fallback to ${domainWord} domain`,
  };
}