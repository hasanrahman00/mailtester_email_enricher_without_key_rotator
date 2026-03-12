const NAME_REGEX = /[^a-zA-Z\s'-]/g;

export function cleanName(value) {
  const cleaned = String(value ?? '')
    .replace(NAME_REGEX, '')
    .replace(/\s+/g, ' ')
    .trim();
  return toTitleCase(cleaned);
}

export function cleanDomain(value) {
  let domain = String(value ?? '')
    .trim()
    .toLowerCase();
  if (!domain) {
    return '';
  }
  if (domain.includes('@')) {
    domain = domain.split('@').pop();
  }
  domain = domain.replace(/^https?:\/\//, '');
  domain = domain.replace(/^www\./, '');
  domain = domain.split(/[\/\s]/)[0];
  domain = domain.split('?')[0];
  domain = domain.replace(/[^a-z0-9.-]/g, '');
  return domain;
}

function toTitleCase(value) {
  if (!value) {
    return '';
  }
  return value
    .toLowerCase()
    .split(' ')
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
    .join(' ');
}
