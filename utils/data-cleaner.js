/**
 * utils/data-cleaner.js — Sanitizes names and domains from user input.
 *
 * Names: removes special chars, converts to Title Case.
 * Domains: strips protocols, www, paths, and normalizes.
 */

// Only allow letters, spaces, hyphens, and apostrophes in names
const NAME_REGEX = /[^a-zA-Z\s'-]/g;

// Clean a name: remove junk chars, normalize spaces, title case
function cleanName(value) {
  const cleaned = String(value ?? '').replace(NAME_REGEX, '').replace(/\s+/g, ' ').trim();
  if (!cleaned) return '';
  return cleaned.toLowerCase().split(' ')
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');
}

// Clean a domain: strip http/www/paths, lowercase, remove junk
function cleanDomain(value) {
  let domain = String(value ?? '').trim().toLowerCase();
  if (!domain) return '';
  if (domain.includes('@')) domain = domain.split('@').pop();
  domain = domain.replace(/^https?:\/\//, '').replace(/^www\./, '');
  domain = domain.split(/[\/\s]/)[0].split('?')[0];
  return domain.replace(/[^a-z0-9.-]/g, '');
}

module.exports = { cleanName, cleanDomain };
