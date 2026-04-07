/**
 * services/upload/row-sanitizer.js — Cleans and validates a single row.
 *
 * Extracts names, domains, existing emails from a row object.
 * Decides if the row should be enriched or skipped.
 */

const { cleanName, cleanDomain } = require('../../utils/data-cleaner');
const { OUTPUT_COLUMNS, WEBSITE_ONE_COLUMN, WEBSITE_TWO_COLUMN } = require('./constants');

const [FIRST, LAST, WEB] = OUTPUT_COLUMNS;
const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

function firstToken(v) { return v ? v.replace(/-/g, ' ').split(/\s+/).filter(Boolean)[0] || '' : ''; }
function lastToken(v) { const t = v ? v.replace(/-/g, ' ').split(/\s+/).filter(Boolean) : []; return t[t.length - 1] || ''; }

function mapHeader(h, cm) {
  if (cm.firstNameKey === h) return FIRST;
  if (cm.lastNameKey === h) return LAST;
  if (cm.websiteKey === h) return WEB;
  if (cm.websiteOneKey === h) return WEBSITE_ONE_COLUMN;
  if (cm.websiteTwoKey === h) return WEBSITE_TWO_COLUMN;
  return h;
}

function sanitizeRow(rowObj, colMap) {
  const firstName = firstToken(cleanName(rowObj[colMap.firstNameKey]));
  const lastName = lastToken(cleanName(rowObj[colMap.lastNameKey]));
  const domain = cleanDomain(rowObj[colMap.websiteKey]);
  const domain2 = cleanDomain(colMap.websiteOneKey ? rowObj[colMap.websiteOneKey] : '');
  const domain3 = cleanDomain(colMap.websiteTwoKey ? rowObj[colMap.websiteTwoKey] : '');
  const rawEmail = colMap.emailKey ? String(rowObj[colMap.emailKey] ?? '').trim() : '';
  const existingEmail = rawEmail && EMAIL_RE.test(rawEmail) ? rawEmail : '';
  const existingStatus = colMap.statusKey ? (rowObj[colMap.statusKey] || '').trim().toLowerCase().replace(/[\s-]+/g, '_') : '';

  // Build sanitized row
  const sanitizedRow = {};
  Object.keys(rowObj || {}).forEach((h) => {
    const out = mapHeader(h, colMap);
    if (out === FIRST) sanitizedRow[out] = firstName;
    else if (out === LAST) sanitizedRow[out] = lastName;
    else if (out === WEB) sanitizedRow[out] = domain;
    else if (out === WEBSITE_ONE_COLUMN) sanitizedRow[out] = domain2;
    else if (out === WEBSITE_TWO_COLUMN) sanitizedRow[out] = domain3;
    else sanitizedRow[out] = rowObj[h] ?? '';
  });
  sanitizedRow[FIRST] = sanitizedRow[FIRST] ?? firstName;
  sanitizedRow[LAST] = sanitizedRow[LAST] ?? lastName;
  sanitizedRow[WEB] = sanitizedRow[WEB] ?? domain;

  const profile = { firstName, lastName, domain };
  if (domain2) profile.domain2 = domain2;
  if (domain3) profile.domain3 = domain3;
  const empty = !firstName && !lastName && !domain && !domain2 && !domain3;

  if (empty && !existingEmail) return null;
  if (existingEmail) return { sanitizedRow, contact: null, skipReason: 'Existing email', profile, existingEmail, existingStatus };
  if (!domain && !domain2 && !domain3) return { sanitizedRow, contact: null, skipReason: 'Missing domain', profile, existingEmail: '' };
  if (!firstName && !lastName) return { sanitizedRow, contact: null, skipReason: 'Missing name', profile, existingEmail: '' };
  return { sanitizedRow, contact: profile, skipReason: null, profile, existingEmail: '' };
}

module.exports = { sanitizeRow };
