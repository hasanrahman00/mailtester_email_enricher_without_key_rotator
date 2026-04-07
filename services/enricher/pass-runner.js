/**
 * services/enricher/pass-runner.js — Runs a single fallback pass (Pass 2 or 3).
 *
 * Filters eligible contacts, runs wave processor, applies overwrite logic,
 * and returns intermediate results for the next pass.
 */

const { DELIVERY_STATUS } = require('../../utils/status-codes');
const { processContactsInBatches } = require('./wave-processor');
const { priorShouldDefer, buildMergedResult } = require('./pass-logic');

async function runPass(contacts, priorResults, domainKey, domainLabel, options, shared, jobId, prevDomainMap, prevStatusMap) {
  const intermediate = priorResults.map((r) => ({ ...r }));
  const domainMap = new Map(), statusMap = new Map();

  // Filter: has the target domain AND prior result wasn't valid
  const candidates = [];
  contacts.forEach((c, i) => {
    const priorDomain = prevDomainMap?.get(i) ?? (domainKey === 'domain3' ? (c.domain2 || '') : c.domain);
    const priorStatus = prevStatusMap?.get(i) ?? priorResults[i].status;
    if (c[domainKey] && priorResults[i].status !== DELIVERY_STATUS.VALID)
      candidates.push({ idx: i, contact: c, prior: priorResults[i], priorDomain, priorStatus });
  });

  if (!candidates.length) return { intermediate, haltType: null, unprocessed: [], domainMap, statusMap };

  const passContacts = candidates.map(({ contact: c }) => ({ ...c, domain: c[domainKey] }));
  const infoMap = new Map(candidates.map(({ contact: c, prior, priorDomain }) => [c.rowId, { origDomain: priorDomain, priorStatus: prior.status }]));

  const onResult = options.onResult ? async (r) => {
    const info = infoMap.get(r?.contact?.rowId); if (!info) return;
    const isValid = r.status === DELIVERY_STATUS.VALID;
    if (isValid || priorShouldDefer(info.origDomain, info.priorStatus, r.status)) {
      const word = domainLabel === 'Website_one' ? 'second' : 'third';
      await options.onResult({ ...r, domainUsed: domainLabel, notes: isValid ? `Valid on ${word} domain` : `Fallback to ${word} domain`, _replaces: info.priorStatus });
    }
  } : undefined;

  const batch = await processContactsInBatches(passContacts, { ...shared, onResult, jobId });

  // Merge results
  candidates.forEach(({ idx, contact: c, prior }, i) => {
    const r = batch.results[i]; if (!r) return;
    const isValid = r.status === DELIVERY_STATUS.VALID;
    if (isValid || priorShouldDefer(c.domain, prior.status, r.status))
      intermediate[idx] = buildMergedResult(r, prior, domainLabel, isValid);
    domainMap.set(idx, c[domainKey] || '');
    statusMap.set(idx, r.status);
  });

  return { intermediate, haltType: batch.haltType, haltReason: batch.haltReason, unprocessed: batch.unprocessedRowIds, domainMap, statusMap };
}

module.exports = { runPass };
