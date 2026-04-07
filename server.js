/**
 * server.js — Application entry point.
 *
 * Sets up Express, serves the frontend UI, mounts API routes,
 * and handles graceful shutdown.
 */

require('dotenv/config');
const express = require('express');
const path = require('path');
const routes = require('./api/routes');
const config = require('./config/env');
const { getTempRootDir } = require('./utils/storage');

const app = express();
const publicDir = path.join(process.cwd(), 'public');

// Parse JSON request bodies
app.use(express.json());

// Serve the frontend UI at /email-enricher
app.use('/email-enricher', express.static(publicDir));
app.get('/', (req, res) => res.redirect('/email-enricher'));

// Mount all API routes
app.use(routes);

// Global error handler
app.use((err, req, res, next) => {
  console.error('Unhandled error:', err);
  res.status(500).json({ error: 'Internal server error' });
});

// Start the server
const port = config.port || 3004;
const server = app.listen(port, async () => {
  await getTempRootDir(); // ensure temp directory exists
  console.log(`Server running on port ${port}`);
});

// Graceful shutdown on SIGTERM/SIGINT
function shutdown(sig) {
  console.log(`${sig} received, shutting down...`);
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(1), 5000);
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

module.exports = app;
