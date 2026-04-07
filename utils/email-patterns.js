/**
 * utils/email-patterns.js — Generates candidate email addresses.
 *
 * Given a first name, last name, and domain, produces up to 9
 * common B2B email patterns ranked by real-world usage data.
 *
 * Priority order is based on Interseller's analysis of 5M+ companies
 * and Fortune 500 email pattern studies:
 *
 *   Pattern          | Enterprise (1k+) | Mid (51-200) | Small (<50)
 *   -----------------+------------------+--------------+------------
 *   first.last@      | 48-56%           | 30%          | 10-13%
 *   flast@           | 22-35%           | 42%          | 13-27%
 *   first@           |  3-7%            | 17%          | 42-71%
 *   firstlast@       | ~5%              | ~5%          | ~5%
 *   first_last@      | ~4%              | ~3%          | ~2%
 *
 * The remaining patterns (last.first@, f.last@, first-last@, etc.)
 * each cover <2% but together catch another ~10% of companies.
 */

function generatePatterns({ firstName, lastName, domain }) {
  // Normalize inputs to lowercase, strip spaces
  const clean = (s) => String(s || '').trim().toLowerCase().replace(/\s+/g, '');
  const first = clean(firstName);
  const last  = clean(lastName);
  const f     = first.charAt(0);   // first initial  (j)
  const l     = last.charAt(0);    // last initial    (d)
  const base  = clean(domain);

  // No domain = no patterns possible
  if (!base) return [];

  const ordered = [];

  // Helper: add pattern only when condition is true and local part is valid
  const add = (condition, localPart) => {
    if (!condition) return;
    const local = String(localPart || '').replace(/^[._-]+|[._-]+$/g, '');
    if (local) ordered.push(`${local}@${base}`);
  };

  const hasFirst = Boolean(first);
  const hasLast  = Boolean(last);
  const hasBoth  = hasFirst && hasLast;

  // ── Tier 1: Top 3 patterns cover ~80% of all companies ──
  add(hasBoth,  `${first}.${last}`);   // 1.  john.doe@      (~35% overall, #1 enterprise)
  add(hasBoth,  `${f}${last}`);        // 2.  jdoe@          (~20% overall, #1 mid-market)
  add(hasFirst, first);                // 3.  john@          (~15% overall, #1 small biz)

  // ── Tier 2: Next 4 patterns catch another ~15% ──
  add(hasBoth, `${first}${last}`);     // 4.  johndoe@       (~5%)
  add(hasBoth, `${first}_${last}`);    // 5.  john_doe@      (~4%)
  add(hasBoth, `${first}${l}`);        // 6.  johnd@         (~3%)
  add(hasBoth, `${last}.${first}`);    // 7.  doe.john@      (~2-3%, common in EU/Asia)

  // ── Tier 3: Long-tail patterns (kept to 2 for 9-combo limit) ──
  add(hasBoth, `${f}.${last}`);        // 8.  j.doe@         (~1-2%)
  add(hasBoth, `${first}-${last}`);    // 9.  john-doe@      (~1%)

  // Remove duplicates while keeping priority order
  return Array.from(new Set(ordered));
}

module.exports = { generatePatterns };
