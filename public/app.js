/**
 * public/app.js — Frontend logic for the Email Enricher UI.
 *
 * Handles file uploads, job polling, logs drawer, and issues drawer.
 * No catchall cleaner — enrichment only.
 */

const USER_ID = 'demo-user';
const POLL_MS = 3000;

const $ = (sel) => document.querySelector(sel);
const form = $('#upload-form');
const fileInput = $('#file-input');
const submitBtn = $('#submit-btn');
const banner = $('#status-banner');
const tbody = $('#jobs-tbody');
const emptyEl = $('#empty-state');
const dropzone = $('#dropzone');
const fileLabel = $('#file-name');
const refreshBtn = $('#refresh-btn');
const apiStatus = $('#api-status');
const apiText = $('#api-text');
const logsOverlay = $('#logs-overlay');
const logsTitle = $('#logs-title');
const logsContent = $('#logs-content');
const logsClose = $('#logs-close');
const issuesOverlay = $('#issues-overlay');
const issuesTitle = $('#issues-title');
const issuesContent = $('#issues-content');
const issuesClose = $('#issues-close');

let jobs = [];
let pollTimer = null;
let logsPollTimer = null;
let pollGraceCount = 0;
const POLL_GRACE_CYCLES = 3;

// ── Helpers ──
function showBanner(type, msg) {
  if (!banner) return;
  banner.textContent = msg;
  banner.className = 'banner' + (msg ? ` ${type} visible` : '');
}

function esc(str) { const d = document.createElement('div'); d.textContent = str; return d.innerHTML; }

function fmtDate(val) {
  if (!val) return '--';
  const d = new Date(val);
  return isNaN(d) ? '--' : d.toLocaleString(undefined, { month: 'short', day: '2-digit', hour: '2-digit', minute: '2-digit' });
}

function fmtStatus(s) {
  return { run: 'Running', done: 'Done', stop: 'Stopped', stopping: 'Stopping...', pause: 'Paused', pausing: 'Pausing...', failed: 'Failed' }[s] || 'Pending';
}

// ── Drag & Drop ──
if (dropzone) {
  ['dragenter', 'dragover'].forEach((e) => dropzone.addEventListener(e, (ev) => { ev.preventDefault(); dropzone.classList.add('dragover'); }));
  ['dragleave', 'drop'].forEach((e) => dropzone.addEventListener(e, (ev) => {
    ev.preventDefault(); dropzone.classList.remove('dragover');
    if (e === 'drop' && ev.dataTransfer?.files?.length) {
      const dt = new DataTransfer();
      Array.from(ev.dataTransfer.files).forEach((f) => dt.items.add(f));
      fileInput.files = dt.files;
      updateFileLabel();
    }
  }));
}
if (fileInput) fileInput.addEventListener('change', updateFileLabel);
function updateFileLabel() { if (fileLabel) fileLabel.textContent = fileInput?.files?.[0]?.name || 'CSV / XLS / XLSX'; }

// ── Upload ──
if (form) form.addEventListener('submit', async (e) => {
  e.preventDefault();
  if (!fileInput?.files?.length) return showBanner('error', 'Select a file first.');
  submitBtn.disabled = true; submitBtn.textContent = 'Uploading...';
  showBanner('info', 'Uploading and starting job...');
  const fd = new FormData(); fd.append('file', fileInput.files[0]);
  try {
    const res = await fetch('/v1/scraper/enricher/upload', { method: 'POST', headers: { 'x-user-id': USER_ID }, body: fd });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || 'Upload failed');
    showBanner('success', `Job ${data.jobId} started.`);
    await loadJobs(); pollGraceCount = 0; startPolling();
  } catch (err) { showBanner('error', err.message); }
  finally {
    submitBtn.disabled = false;
    submitBtn.innerHTML = '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="5 3 19 12 5 21 5 3"/></svg> Run Enrichment';
  }
});

// ── Jobs Table ──
async function loadJobs() {
  try {
    // Fetch jobs and rate data in parallel so render has fresh combos/min
    const [jobsRes, rateRes] = await Promise.all([
      fetch('/v1/scraper/enricher/jobs'),
      fetch('/v1/scraper/enricher/key-status').catch(() => null),
    ]);
    if (!jobsRes.ok) return;
    const data = await jobsRes.json();
    if (rateRes?.ok) {
      const rateData = await rateRes.json();
      liveRate = rateData.rate || liveRate;
      updateApiStatusBadge(rateData);
    }
    const serverJobs = data.jobs || [];
    const serverMap = new Map(serverJobs.map((j) => [j.jobId, j]));
    const merged = [...serverJobs];
    for (const local of jobs) { if (!serverMap.has(local.jobId) && local.status === 'run') merged.push(local); }
    jobs = merged;
    renderJobs();
    const hasRunning = jobs.some((j) => ['run', 'stopping', 'pausing'].includes(j.status));
    if (hasRunning) { pollGraceCount = 0; startPolling(); }
    else { pollGraceCount++; if (pollGraceCount >= POLL_GRACE_CYCLES) stopPolling(); else startPolling(); }
    syncApiPoll();
  } catch { /* retry next poll */ }
}

function renderJobs() {
  if (!tbody) return;
  const countEl = document.getElementById('jobs-count');
  if (countEl) countEl.textContent = jobs.length || '';
  if (!jobs.length) { tbody.innerHTML = ''; if (emptyEl) { emptyEl.style.display = 'block'; } return; }
  if (emptyEl) emptyEl.style.display = 'none';

  tbody.innerHTML = jobs.map((job) => {
    const total = job.totals?.totalRows ?? job.progress?.totalContacts ?? 0;
    const processed = job.resultCount ?? job.progress?.processedContacts ?? 0;
    const pct = total ? Math.round((processed / total) * 100) : (job.status === 'done' ? 100 : 0);
    const c = job.progress?.statusCounts || {};
    const valid = (c.valid || 0) + (c.verified || 0);
    const catchAll = (c.catch_all || 0) + (c.risky || 0);
    const notFound = (c.not_found || 0) + (c.no_domain || 0);
    const rateLimited = c.rate_limited || 0;
    const mxNotFound = c.mx_not_found || 0;
    const errors = c.error || 0;
    const issueCount = rateLimited + mxNotFound + errors;
    const isRunning = job.status === 'run';
    const isStopping = job.status === 'stopping' || job.status === 'pausing';
    const canRerun = job.status === 'stop' || job.status === 'pause' || job.status === 'failed';

    return `<tr data-job="${job.jobId}">
      <td><div class="file-cell"><strong>${esc(job.originalFilename || 'Untitled')}</strong><span class="meta">${fmtDate(job.createdAt)}</span></div></td>
      <td><span class="pill ${job.status || 'pending'}">${fmtStatus(job.status)}</span></td>
      <td class="col-num">${total}</td>
      <td><div class="progress-cell"><div class="progress-track"><div class="progress-fill" style="width:${pct}%"></div></div><span class="progress-text">${processed}/${total} (${pct}%)</span>${isRunning && liveRate.combosThisMin > 0 ? `<span class="rate-badge">${liveRate.combosThisMin} combos / ${liveRate.elapsedSec}s</span>` : ''}</div></td>
      <td class="col-num"><span class="stat-num ${valid ? 'green' : 'muted'}">${valid}</span></td>
      <td class="col-num"><span class="stat-num ${catchAll ? 'amber' : 'muted'}">${catchAll}</span></td>
      <td class="col-num"><span class="stat-num ${notFound ? 'red' : 'muted'}">${notFound}</span></td>
      <td class="col-num"><button class="other-stat-btn ${(mxNotFound + errors) > 0 ? 'has-issues' : 'muted'}" onclick="showIssues('${job.jobId}')" title="MX Not Found + Errors">${mxNotFound + errors}</button></td>
      <td class="col-num"><span class="stat-num ${rateLimited ? 'orange' : 'muted'}">${rateLimited}</span></td>
      <td><div class="action-row">
        ${isRunning ? `<button class="btn-action pause" onclick="pauseJob('${job.jobId}')">Pause</button>` : ''}
        ${isRunning ? `<button class="btn-action stop" onclick="stopJob('${job.jobId}')">Stop</button>` : ''}
        ${isStopping ? `<span class="btn-action stopping-label">${fmtStatus(job.status)}</span>` : ''}
        ${canRerun ? `<button class="btn-action rerun" onclick="rerunJob('${job.jobId}')">Rerun</button>` : ''}
        ${job.downloadUrl ? `<a class="btn-action download" href="${job.downloadUrl}" download>Download</a>` : ''}
        <button class="btn-action issues" onclick="showIssues('${job.jobId}')">Issues${issueCount > 0 ? ` <span class="issue-badge">${issueCount}</span>` : ''}</button>
        <button class="btn-action logs" onclick="openLogs('${job.jobId}')">Logs</button>
        <button class="btn-action danger" onclick="deleteJob('${job.jobId}')">Delete</button>
      </div></td>
    </tr>`;
  }).join('');
}

// ── Polling ──
function startPolling() { if (!pollTimer) pollTimer = setInterval(loadJobs, POLL_MS); }
function stopPolling() { if (pollTimer) { clearInterval(pollTimer); pollTimer = null; } }

// ── Job Actions ──
window.stopJob = async (id) => { await fetch(`/v1/scraper/enricher/jobs/${id}/stop`, { method: 'POST' }); showBanner('info', 'Stopping...'); loadJobs(); };
window.pauseJob = async (id) => { await fetch(`/v1/scraper/enricher/jobs/${id}/pause`, { method: 'POST' }); showBanner('info', 'Pausing...'); loadJobs(); };
window.rerunJob = async (id) => {
  const res = await fetch(`/v1/scraper/enricher/jobs/${id}/rerun`, { method: 'POST' });
  const data = await res.json();
  if (!res.ok) return showBanner('error', data.error || 'Rerun failed');
  showBanner('success', `Rerun started.`); loadJobs(); pollGraceCount = 0; startPolling();
};
window.deleteJob = async (id) => {
  if (!confirm('Delete this job and its output files?')) return;
  await fetch(`/v1/scraper/enricher/jobs/${id}`, { method: 'DELETE' });
  jobs = jobs.filter((j) => j.jobId !== id); renderJobs();
};

// ── Issues Drawer ──
window.showIssues = (jobId) => {
  const job = jobs.find((j) => j.jobId === jobId);
  if (!job) return;
  const c = job.progress?.statusCounts || {};
  const total = job.progress?.totalContacts || 0;
  const pct = (n) => total > 0 ? `${Math.round((n / total) * 100)}%` : '--';
  const issues = [
    { label: 'MX Not Found', desc: 'Domain has no MX records', key: 'mx_not_found', color: 'orange' },
    { label: 'Rate Limited', desc: 'Spam block or daily limit', key: 'rate_limited', color: 'amber' },
    { label: 'Errors', desc: 'Timeout or connection failure', key: 'error', color: 'red' },
  ];
  const totalIssues = issues.reduce((s, r) => s + (c[r.key] || 0), 0);
  const clean = [
    { label: 'Verified', key: 'valid', color: 'green' },
    { label: 'Risky', key: 'catch_all', color: 'amber' },
    { label: 'Not Found', key: 'not_found', color: 'red' },
    { label: 'No Domain', key: 'no_domain', color: 'muted' },
  ];
  const totalClean = clean.reduce((s, r) => s + (c[r.key] || 0), 0);

  issuesContent.innerHTML = `
    <div class="issues-summary">
      <div class="issues-summary-card issues-card-bad"><div class="issues-card-num">${totalIssues}</div><div class="issues-card-label">Total Issues</div><div class="issues-card-pct">${pct(totalIssues)}</div></div>
      <div class="issues-summary-card issues-card-good"><div class="issues-card-num">${totalClean}</div><div class="issues-card-label">Processed OK</div><div class="issues-card-pct">${pct(totalClean)}</div></div>
    </div>
    <div class="issues-section-title">Issue Breakdown</div>
    <table class="issues-table"><thead><tr><th>Type</th><th>Description</th><th style="text-align:right">Count</th><th style="text-align:right">%</th></tr></thead><tbody>
      ${issues.map(({ label, desc, key, color }) => { const n = c[key] || 0; return `<tr><td><span class="issue-type-label ${color}">${label}</span></td><td class="issue-desc">${desc}</td><td style="text-align:right"><span class="stat-num ${n ? color : 'muted'}">${n}</span></td><td style="text-align:right"><span class="stat-num muted">${pct(n)}</span></td></tr>`; }).join('')}
      <tr class="issues-total"><td colspan="2"><strong>Total Issues</strong></td><td style="text-align:right"><strong>${totalIssues}</strong></td><td style="text-align:right"><strong>${pct(totalIssues)}</strong></td></tr>
    </tbody></table>
    <div class="issues-section-title" style="margin-top:20px">Results Summary</div>
    <table class="issues-table"><thead><tr><th>Status</th><th style="text-align:right">Count</th><th style="text-align:right">%</th></tr></thead><tbody>
      ${clean.map(({ label, key, color }) => { const n = c[key] || 0; return `<tr><td><span class="stat-num ${n ? color : 'muted'}">${label}</span></td><td style="text-align:right"><span class="stat-num ${n ? color : 'muted'}">${n}</span></td><td style="text-align:right"><span class="stat-num muted">${pct(n)}</span></td></tr>`; }).join('')}
    </tbody></table>
    <div class="issues-job-meta">Job: <span>${esc(job.originalFilename || jobId)}</span> &middot; Total: <span>${total}</span></div>`;
  issuesOverlay.hidden = false;
};
issuesClose?.addEventListener('click', () => { issuesOverlay.hidden = true; });
issuesOverlay?.addEventListener('click', (e) => { if (e.target === issuesOverlay) issuesOverlay.hidden = true; });

// ── Logs Drawer ──
window.openLogs = (jobId) => {
  logsTitle.textContent = `Logs - ${jobId.slice(0, 8)}...`;
  logsContent.textContent = 'Loading...';
  logsOverlay.hidden = false;
  fetchLogs(jobId);
  logsPollTimer = setInterval(() => fetchLogs(jobId), 2000);
};
async function fetchLogs(jobId) {
  try {
    const res = await fetch(`/v1/scraper/enricher/jobs/${jobId}/logs`);
    const data = await res.json();
    logsContent.textContent = (data.logs || []).join('\n') || 'No logs yet.';
    logsContent.scrollTop = logsContent.scrollHeight;
  } catch { logsContent.textContent = 'Failed to load logs.'; }
}
logsClose?.addEventListener('click', closeLogs);
logsOverlay?.addEventListener('click', (e) => { if (e.target === logsOverlay) closeLogs(); });
function closeLogs() { logsOverlay.hidden = true; if (logsPollTimer) { clearInterval(logsPollTimer); logsPollTimer = null; } }

// ── API Status Badge ──
let liveRate = { combosLastMin: 0, totalCombos: 0, avgPerSec: 0 };

function updateApiStatusBadge(data) {
  if (!data) return;
  const remaining = data.combined?.totalDailyRemaining ?? 0;
  const rpm = data.combined?.maxReqPerMin ?? 0;
  liveRate = data.rate || liveRate;
  const live = liveRate.combosThisMin || 0;
  apiStatus.classList.remove('ok', 'warn', 'err');
  if (remaining <= 0) { apiStatus.classList.add('err'); apiText.textContent = 'Keys exhausted'; }
  else if (remaining < 50000) { apiStatus.classList.add('warn'); apiText.textContent = `${(remaining / 1000).toFixed(0)}K remaining`; }
  else {
    apiStatus.classList.add('ok');
    apiText.textContent = live > 0
      ? `${live} combos / ${liveRate.elapsedSec}s | ${(remaining / 1000).toFixed(0)}K left`
      : `${rpm}/min max | ${(remaining / 1000).toFixed(0)}K left`;
  }
}

async function checkApiStatus() {
  try {
    const res = await fetch('/v1/scraper/enricher/key-status');
    const data = await res.json();
    updateApiStatusBadge(data);
  } catch { apiStatus.classList.add('err'); apiText.textContent = 'No keys'; }
}

// ── Refresh Button ──
if (refreshBtn) refreshBtn.addEventListener('click', loadJobs);

// ── Boot ──
let apiPollTimer = null;
function syncApiPoll() {
  const hasRunning = jobs.some((j) => ['run', 'stopping', 'pausing'].includes(j.status));
  const interval = hasRunning ? 5000 : 30000;  // 5s while running, 30s idle
  if (apiPollTimer) clearInterval(apiPollTimer);
  apiPollTimer = setInterval(checkApiStatus, interval);
}
(async () => { await loadJobs(); syncApiPoll(); })();
