export const DELIVERY_STATUS = {
  VALID: 'valid',
  CATCH_ALL: 'catch_all',
  NOT_FOUND: 'not_found',
  NO_DOMAIN: 'no_domain',
  RATE_LIMITED: 'rate_limited',
  MX_NOT_FOUND: 'mx_not_found',
  ERROR: 'error',
};

const STATUS_ALIAS_MAP = new Map([
  ['valid', DELIVERY_STATUS.VALID],
  ['catch_all', DELIVERY_STATUS.CATCH_ALL],
  ['catchall', DELIVERY_STATUS.CATCH_ALL],
  ['catchall_default', DELIVERY_STATUS.CATCH_ALL],
  ['catch-all', DELIVERY_STATUS.CATCH_ALL],
  ['not_found', DELIVERY_STATUS.NOT_FOUND],
  ['not_found_valid_emails', DELIVERY_STATUS.NOT_FOUND],
  ['valid_email_not_found', DELIVERY_STATUS.NOT_FOUND],
  ['skipped_missing_fields', DELIVERY_STATUS.NOT_FOUND],
  ['no_domain', DELIVERY_STATUS.NO_DOMAIN],
  ['mx_not_found', DELIVERY_STATUS.MX_NOT_FOUND],
  ['error', DELIVERY_STATUS.ERROR],
  ['other', DELIVERY_STATUS.ERROR],
  ['rate_limited', DELIVERY_STATUS.RATE_LIMITED],
  ['spam_block', DELIVERY_STATUS.RATE_LIMITED],
  ['daily_limit_reached', DELIVERY_STATUS.RATE_LIMITED],
]);

export function normalizeDeliveryStatus(status) {
  if (!status) return DELIVERY_STATUS.NOT_FOUND;
  const normalized = String(status).trim().toLowerCase().replace(/[\s-]+/g, '_');
  if (STATUS_ALIAS_MAP.has(normalized)) return STATUS_ALIAS_MAP.get(normalized);
  if (normalized.includes('catch')) return DELIVERY_STATUS.CATCH_ALL;
  if (normalized.includes('rate') || normalized.includes('limit') || normalized.includes('spam')) return DELIVERY_STATUS.RATE_LIMITED;
  if (normalized.includes('mx')) return DELIVERY_STATUS.MX_NOT_FOUND;
  if (normalized === DELIVERY_STATUS.VALID) return DELIVERY_STATUS.VALID;
  return DELIVERY_STATUS.NOT_FOUND;
}