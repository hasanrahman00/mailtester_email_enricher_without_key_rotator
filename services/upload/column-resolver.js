/**
 * services/upload/column-resolver.js — Maps CSV headers to standard columns.
 *
 * Given headers like "First", "Last Name", "Domain", finds which column
 * corresponds to firstName, lastName, website, etc.
 */

const { COLUMN_ALIASES } = require('./constants');
const { normalizeKey } = require('./workbook-parser');

// Find a column by checking against a list of aliases
function findColumnKey(normalizedMap, candidates) {
  for (const candidate of candidates) {
    const key = normalizeKey(candidate);
    if (normalizedMap.has(key)) return normalizedMap.get(key);
  }
  return null;
}

/**
 * Resolve which headers map to which standard columns.
 * Throws if required columns (firstName, lastName, website) are missing.
 */
function resolveColumns(headers) {
  // Build a map: normalized header -> original header
  const map = new Map();
  headers.forEach((h) => map.set(normalizeKey(h), h));

  const firstNameKey = findColumnKey(map, COLUMN_ALIASES.firstName);
  const lastNameKey = findColumnKey(map, COLUMN_ALIASES.lastName);
  const websiteKey = findColumnKey(map, COLUMN_ALIASES.website);
  const websiteOneKey = findColumnKey(map, COLUMN_ALIASES.websiteOne || []);
  const websiteTwoKey = findColumnKey(map, COLUMN_ALIASES.websiteTwo || []);
  const emailKey = findColumnKey(map, COLUMN_ALIASES.email || []);
  const statusKey = findColumnKey(map, COLUMN_ALIASES.status || []);

  if (!firstNameKey || !lastNameKey || !websiteKey) {
    throw new Error('File must include First Name, Last Name, and Website columns.');
  }

  return { firstNameKey, lastNameKey, websiteKey, websiteOneKey, websiteTwoKey, emailKey, statusKey };
}

module.exports = { resolveColumns };
