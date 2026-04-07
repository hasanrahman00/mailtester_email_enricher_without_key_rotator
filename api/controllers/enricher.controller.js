/**
 * api/controllers/enricher.controller.js — JSON API enrichment endpoint.
 *
 * POST /v1/scraper/enricher/start
 * Accepts a JSON body with { contacts: [...] } and returns enriched results.
 */

const { enrichContacts } = require('../../services/enricher/enricher');

async function startEnricher(req, res) {
  const { contacts } = req.body;

  // Validate: contacts must be a non-empty array
  if (!Array.isArray(contacts) || !contacts.length) {
    return res.status(400).json({ error: 'contacts array is required' });
  }

  try {
    const results = await enrichContacts(contacts);
    return res.json({ results });
  } catch (err) {
    console.error('Enricher error:', err);
    return res.status(500).json({ error: 'Internal server error' });
  }
}

module.exports = { startEnricher };
