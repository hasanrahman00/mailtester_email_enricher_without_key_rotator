# Email Enricher API

> Repository: `alhamdulillah_sass_emil_enricher`

Minimal Express service that generates candidate email addresses for a list of contacts, verifies them with MailTester Ninja, and returns the best match per contact.

## Features
- POST endpoint at `/v1/scraper/enricher/start` that accepts a batch of contacts
- Deterministic email pattern generator covering common naming conventions
- MailTester Ninja client that relies on the key-rotation microservice for pacing
- Catch-all handling rules to surface the most useful fallback when no address validates
- Centralized error handling and JSON-only responses

## Getting Started
1. **Install dependencies**
   ```bash
   npm install
   ```
2. **Set environment variables** (see below). You can use a process manager, `.env`, or your shell profile.
3. **Run the server**
   ```bash
   npm start
   ```
   The server listens on `PORT` (defaults to `3000`).

## Configuration
| Variable | Default | Purpose |
| --- | --- | --- |
| `MAILTESTER_BASE_URL` | `https://happy.mailtester.ninja/ninja` | MailTester Ninja endpoint used for validation |
| `KEY_PROVIDER_URL` | `https://api.daddy-leads.com/mailtester/key/available` | Internal service that returns a MailTester key |
| `COMBO_BATCH_SIZE` | `25` | Number of contacts processed concurrently per pattern wave |
| `PORT` | `3000` | HTTP port for the Express server |

## API Usage
**Endpoint**: `POST /v1/scraper/enricher/start`

**Request body**
```json
{
  "contacts": [
    {
      "firstName": "Ada",
      "lastName": "Lovelace",
      "domain": "example.com"

### File Upload Workflow

| Endpoint | Method | Description |
| --- | --- | --- |
| `/v1/scraper/enricher/upload` | `POST` (multipart) | Accepts a CSV/XLS/XLSX upload (field `file`) plus optional `X-User-Id` header, validates the dataset, runs enrichment in configurable batches, and returns `{ jobId, downloadUrl, outputFile, results }`. |
| `/v1/scraper/enricher/download/:jobId` | `GET` | Streams the processed CSV that includes only `First Name`, `Last Name`, `Website`, and the appended `Email` + `Status` columns. |

Key rules enforced by the upload pipeline:

- Allowed file types: `.csv`, `.xls`, `.xlsx`.
- Maximum 10,000 data rows per upload.
- Required columns: `First Name`, `Last Name`, and `Website` (case-insensitive); other columns are preserved.
- First/last names and website domains are cleaned automatically before processing. Invalid rows trigger a descriptive error.
- Rows with no website/domain or with neither first nor last name are skipped automatically (status `skipped_missing_fields`). Rows that contain at least one of the name fields still run through enrichment using single-name combo patterns.
- Temporary job directories live under `tempUploads/` with metadata (jobId, userId, timestamps). A background scheduler purges artifacts older than 24 hours unless the job is currently running.
- The output CSV is created immediately and appended in real time as each row finishes, so users can download partial results while the job is still running.

### Frontend testing surface

Run `npm start` and open `http://localhost:3000/` to access a lightweight UI (served from `public/`). The page lets you:

1. Provide a user ID (mirrors the future JWT claim).
2. Upload a CSV/XLS/XLSX file.
3. Kick off the enrichment job and inspect the JSON response inline.
4. Download the enriched CSV through a single click once the backend finishes.
    }
  ]
}
```

**Response body**
middlewares/  # Upload + job context middleware
```json
{
  comboProcessor.service.js  # batch combo engine
  uploadProcessor.service.js # file parsing + CSV writer
  "results": [
public/       # Static upload UI (HTML/CSS/JS)
    {
      "firstName": "Ada",
      "lastName": "Lovelace",
      "domain": "example.com",
      "bestEmail": "ada.lovelace@example.com",
      "status": "valid",
      "details": {
        "code": "ok",
        "message": "deliverable"
      },
      "allCheckedCandidates": [
        {
          "email": "ada.lovelace@example.com",
          "code": "ok",
          "message": "deliverable"
        }
      ]
    }
  ]
}
```

**cURL example**
```bash
curl -X POST http://localhost:3000/v1/scraper/enricher/start \
  -H "Content-Type: application/json" \
  -d '{
    "contacts": [
      {"firstName": "Ada", "lastName": "Lovelace", "domain": "example.com"}
    ]
  }'
```

## Project Structure
```
config/       # env + runtime configuration
clients/      # MailTester + key provider integrations
controllers/  # Express controllers
routes/       # Express routers
services/     # Enrichment logic
utils/        # Helpers (patterns, rate limiter)
server.js     # Express bootstrap + listener
```

## Notes
- API key fetching is cached per process to avoid overloading the provider.
- Request pacing is delegated to the key-rotation microservice, which only hands out keys when it is safe to call MailTester.
- The key provider may return `subscriptionId` values wrapped in `{}`; the app now strips those braces automatically before calling MailTester.
- Uploaded files are stored temporarily under `tempUploads/` and deleted automatically after 24 hours by the cleanup scheduler.
