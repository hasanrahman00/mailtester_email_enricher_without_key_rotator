/**
 * services/upload/constants.js — Shared constants for file uploads.
 *
 * Defines allowed file types, column name aliases, and output column names.
 * Change COLUMN_ALIASES to support different CSV header names.
 */

// Supported file extensions for upload
const ALLOWED_EXTENSIONS = ['.csv', '.xls', '.xlsx'];

// Set to a number to limit rows, or null for unlimited
const MAX_ROWS = null;

// Maps various header names to our standard column names
const COLUMN_ALIASES = {
  firstName: ['first name', 'firstname', 'first'],
  lastName: ['last name', 'lastname', 'last'],
  website: ['website', 'domain', 'company website', 'company domain'],
  websiteOne: ['website_one', 'website one', 'websiteone', 'website 1', 'second domain', 'domain 2', '2nd domain'],
  websiteTwo: ['website_two', 'website two', 'websitetwo', 'website 2', 'third domain', 'domain 3', '3rd domain'],
  email: ['email', 'e-mail', 'work email', 'business email'],
  status: ['status'],
};

// Standard output column names
const OUTPUT_COLUMNS = ['First Name', 'Last Name', 'Website'];
const WEBSITE_ONE_COLUMN = 'Website_one';
const WEBSITE_TWO_COLUMN = 'Website_two';

// Columns added to the output CSV
const CSV_APPEND_COLUMNS = ['Email', 'Status', 'Source'];

// Report summary columns (always at the end)
const REPORT_COLUMNS = ['Report Name', 'Ratio Percentage'];

module.exports = {
  ALLOWED_EXTENSIONS, MAX_ROWS, COLUMN_ALIASES, OUTPUT_COLUMNS,
  WEBSITE_ONE_COLUMN, WEBSITE_TWO_COLUMN, CSV_APPEND_COLUMNS, REPORT_COLUMNS,
};
