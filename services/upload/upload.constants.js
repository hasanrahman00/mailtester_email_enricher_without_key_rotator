// Centralized constants for upload limits and column aliases shared across upload helpers.
export const ALLOWED_EXTENSIONS = ['.csv', '.xls', '.xlsx'];

// Set to null to disable row limits.
export const MAX_ROWS = null;

export const COLUMN_ALIASES = {
  firstName: ['first name', 'firstname', 'first'],
  lastName: ['last name', 'lastname', 'last'],
  website: ['website', 'domain', 'company website', 'company domain'],
  websiteOne: ['website_one', 'website one', 'websiteone', 'website 1', 'second domain', 'domain 2', '2nd domain'],
  email: ['email', 'e-mail', 'work email', 'business email'],
};

export const OUTPUT_COLUMNS = ['First Name', 'Last Name', 'Website'];

export const WEBSITE_ONE_COLUMN = 'Website_one';

export const CSV_APPEND_COLUMNS = ['Email', 'Status', 'Domain Used', 'Notes'];
