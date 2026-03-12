// Generate candidate email addresses for a contact, gracefully handling
// cases where only a first or last name is present.
export function generatePatterns({ firstName, lastName, domain }) {
  const clean = (str) => String(str || '').trim().toLowerCase().replace(/\s+/g, '');
  const first = clean(firstName);
  const last = clean(lastName);
  const f = first.charAt(0);
  const l = last.charAt(0);
  const base = clean(domain);

  if (!base) {
    return [];
  }

  const ordered = [];
  const add = (condition, localPart) => {
    if (!condition) {
      return;
    }
    const local = String(localPart || '').replace(/^[._-]+|[._-]+$/g, '');
    if (local) {
      ordered.push(`${local}@${base}`);
    }
  };

  const hasFirst = Boolean(first);
  const hasLast = Boolean(last);

  add(hasFirst, `${first}`);
  add(hasFirst && hasLast, `${first}.${last}`);
  add(hasFirst && hasLast, `${first}${l}`);
  add(hasFirst && hasLast, `${first}${last}`);
  add(hasFirst && hasLast, `${f}${last}`);
  add(hasFirst && hasLast, `${first}.${l}`);
  add(hasFirst && hasLast, `${first}_${last}`);
  add(hasFirst && hasLast, `${last}${f}`);
  add(hasLast, `${last}`);

  return Array.from(new Set(ordered));
}