// Converts worksheet rows into sanitized contact profiles while tracking skip reasons.
import { cleanName, cleanDomain } from '../../utils/dataCleaner.js';
import { COLUMN_ALIASES, OUTPUT_COLUMNS, WEBSITE_ONE_COLUMN } from './upload.constants.js';
import { normalizeKey } from './normalization.utils.js';

const [FIRST_NAME_COLUMN, LAST_NAME_COLUMN, WEBSITE_COLUMN] = OUTPUT_COLUMNS;

export function resolveColumns(headers) {
  const normalizedMap = new Map();
  headers.forEach((header) => {
    normalizedMap.set(normalizeKey(header), header);
  });

  const firstNameKey = findColumnKey(normalizedMap, COLUMN_ALIASES.firstName);
  const lastNameKey = findColumnKey(normalizedMap, COLUMN_ALIASES.lastName);
  const websiteKey = findColumnKey(normalizedMap, COLUMN_ALIASES.website);
  const websiteOneKey = findColumnKey(normalizedMap, COLUMN_ALIASES.websiteOne || []);
  const emailKey = findColumnKey(normalizedMap, COLUMN_ALIASES.email || []);

  if (!firstNameKey || !lastNameKey || !websiteKey) {
    throw new Error('File must include First Name, Last Name, and Website columns.');
  }

  return { firstNameKey, lastNameKey, websiteKey, websiteOneKey, emailKey };
}

export function normalizeRows(rows, initialColumnMap, headerRowIndex, initialHeaders) {
  const normalized = [];
  let currentHeaders = [...initialHeaders];
  let currentColumnMap = { ...initialColumnMap };
  let rowCounter = 0;

  rows.forEach((rowValues, index) => {
    const rowNumber = headerRowIndex + 2 + index;

    if (isHeaderRowArray(rowValues)) {
      currentHeaders = sanitizeHeadersFromRow(rowValues);
      currentColumnMap = resolveColumns(currentHeaders);
      return;
    }

    const rowObject = convertRowToObject(currentHeaders, rowValues);
    const sanitized = sanitizeRow(rowObject, currentColumnMap);

    if (!sanitized) {
      return;
    }

    const rowId = rowCounter;
    rowCounter += 1;

    normalized.push({
      rowId,
      rowNumber,
      sanitizedRow: sanitized.sanitizedRow,
      contact: sanitized.contact,
      skipReason: sanitized.skipReason,
      profile: sanitized.profile,
      existingEmail: sanitized.existingEmail || '',
    });
  });

  return normalized;
}

export function sanitizeRow(rowObject, columnMap) {
  const rawFirst = rowObject[columnMap.firstNameKey];
  const rawLast = rowObject[columnMap.lastNameKey];
  const rawDomain = rowObject[columnMap.websiteKey];
  const rawDomainTwo = columnMap.websiteOneKey ? rowObject[columnMap.websiteOneKey] : '';
  const rawEmail = columnMap.emailKey ? rowObject[columnMap.emailKey] : '';

  const firstName = keepFirstToken(cleanName(rawFirst));
  const lastName = keepLastToken(cleanName(rawLast));
  const domain = cleanDomain(rawDomain);
  const domain2 = cleanDomain(rawDomainTwo);
  const existingEmail = extractEmail(rawEmail);

  const sanitizedRow = {};
  Object.keys(rowObject || {}).forEach((header) => {
    const outputHeader = mapHeaderToOutputHeader(header, columnMap);
    if (outputHeader === FIRST_NAME_COLUMN) {
      sanitizedRow[outputHeader] = firstName;
      return;
    }
    if (outputHeader === LAST_NAME_COLUMN) {
      sanitizedRow[outputHeader] = lastName;
      return;
    }
    if (outputHeader === WEBSITE_COLUMN) {
      sanitizedRow[outputHeader] = domain;
      return;
    }
    if (outputHeader === WEBSITE_ONE_COLUMN) {
      sanitizedRow[outputHeader] = domain2;
      return;
    }
    sanitizedRow[outputHeader] = rowObject[header] ?? '';
  });

  sanitizedRow[FIRST_NAME_COLUMN] = sanitizedRow[FIRST_NAME_COLUMN] ?? firstName;
  sanitizedRow[LAST_NAME_COLUMN] = sanitizedRow[LAST_NAME_COLUMN] ?? lastName;
  sanitizedRow[WEBSITE_COLUMN] = sanitizedRow[WEBSITE_COLUMN] ?? domain;

  const profile = { firstName, lastName, domain };
  if (domain2) {
    profile.domain2 = domain2;
  }
  const emptyProfile = !firstName && !lastName && !domain && !domain2;

  if (emptyProfile && !existingEmail) {
    return null;
  }

  if (existingEmail) {
    return {
      sanitizedRow,
      contact: null,
      skipReason: 'Existing email provided',
      profile,
      existingEmail,
    };
  }

  if (!domain && !domain2) {
    return {
      sanitizedRow,
      contact: null,
      skipReason: 'Missing website/domain',
      profile,
      existingEmail: '',
    };
  }

  if (!firstName && !lastName) {
    return {
      sanitizedRow,
      contact: null,
      skipReason: 'Missing first and last name',
      profile,
      existingEmail: '',
    };
  }

  return { sanitizedRow, contact: profile, skipReason: null, profile, existingEmail: '' };
}

function mapHeaderToOutputHeader(header, columnMap) {
  if (columnMap?.firstNameKey && header === columnMap.firstNameKey) {
    return FIRST_NAME_COLUMN;
  }
  if (columnMap?.lastNameKey && header === columnMap.lastNameKey) {
    return LAST_NAME_COLUMN;
  }
  if (columnMap?.websiteKey && header === columnMap.websiteKey) {
    return WEBSITE_COLUMN;
  }
  if (columnMap?.websiteOneKey && header === columnMap.websiteOneKey) {
    return WEBSITE_ONE_COLUMN;
  }
  return header;
}

function keepFirstToken(value) {
  if (!value) {
    return '';
  }
  const tokens = tokenizeName(value);
  return tokens[0] || '';
}

function keepLastToken(value) {
  if (!value) {
    return '';
  }
  const tokens = tokenizeName(value);
  return tokens[tokens.length - 1] || '';
}

function tokenizeName(value) {
  return value
    .replace(/-/g, ' ')
    .split(/\s+/)
    .map((token) => token.trim())
    .filter(Boolean);
}

function extractEmail(value) {
  const email = String(value ?? '').trim();
  if (!email) {
    return '';
  }
  const EMAIL_REGEX = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  return EMAIL_REGEX.test(email) ? email : '';
}

function findColumnKey(normalizedHeaderMap, candidates) {
  for (const candidate of candidates) {
    const normalizedCandidate = normalizeKey(candidate);
    if (normalizedHeaderMap.has(normalizedCandidate)) {
      return normalizedHeaderMap.get(normalizedCandidate);
    }
  }
  return null;
}

function convertRowToObject(headers, rowValues) {
  return headers.reduce((acc, header, idx) => {
    acc[header] = rowValues[idx] ?? '';
    return acc;
  }, {});
}

function isHeaderRowArray(rowValues) {
  if (!Array.isArray(rowValues)) {
    return false;
  }
  const normalizedValues = rowValues.map((value) => normalizeKey(value));
  const hasFirst = COLUMN_ALIASES.firstName.some((alias) => normalizedValues.includes(normalizeKey(alias)));
  const hasLast = COLUMN_ALIASES.lastName.some((alias) => normalizedValues.includes(normalizeKey(alias)));
  const hasDomain = COLUMN_ALIASES.website.some((alias) => normalizedValues.includes(normalizeKey(alias)));
  return hasFirst && hasLast && hasDomain;
}

function sanitizeHeadersFromRow(rowValues) {
  return rowValues.map((cell, idx) => (String(cell || '').trim() ? String(cell) : `column_${idx + 1}`));
}