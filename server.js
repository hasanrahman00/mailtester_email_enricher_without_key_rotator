import 'dotenv/config';
import express from 'express';
import path from 'path';
import enricherRoute from './routes/enricher.route.js';
import { config } from './config/env.js';
import { getTempRootDir } from './utils/storage.js';

const app = express();
const publicDir = path.join(process.cwd(), 'public');

app.use(express.json());
app.use('/email-enricher', express.static(publicDir));
app.get('/', (req, res) => res.redirect('/email-enricher'));
app.use(enricherRoute);

app.use((err, req, res, next) => {
  console.error('Unhandled error:', err);
  res.status(500).json({ error: 'Internal server error' });
});

const port = config.port || 3000;
const server = app.listen(port, async () => {
  await getTempRootDir();
  console.log(`Server running on port ${port}`);
});

function shutdown(sig) {
  console.log(`${sig} received, shutting down...`);
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(1), 5000);
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

export default app;
