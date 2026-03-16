// Shared helpers for normalizing header values to lightweight keys.
export function normalizeKey(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/[^a-z0-9]/g, '');
}
