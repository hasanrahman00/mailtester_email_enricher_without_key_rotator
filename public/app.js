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
const issuesTbody = $('#issues-tbody');
const issuesClose = $('#issues-close');

// ── Catch-all Cleaner DOM refs ──
const catchallOverlay = $('#catchall-overlay');
const catchallTitle = $('#catchall-title');
const catchallClose = $('#catchall-close');
const catchallStatusPill = $('#catchall-status-pill');
const catchallRunBtn = $('#catchall-run-btn');
const catchallStopBtn = $('#catchall-stop-btn');
const catchallLogs = $('#catchall-logs');

let jobs = [];
let pollTimer = null;
let logsPollTimer = null;
let openLogJobId = null;
let pollGraceCount = 0;         // consecutive polls with no running jobs
const POLL_GRACE_CYCLES = 3;    // keep polling this many extra cycles after last "run"

// ── Catch-all Cleaner state ──
let cleanerStates = {};          // jobId → { status, counts, logs }
let openCleanerJobId = null;
let cleanerPollTimer = null;

// ── Icons (inline SVG) ──
const ICONS = {
  stop: '<svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor"><rect x="6" y="6" width="12" height="12" rx="1"/></svg>',
  pause: '<svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor"><rect x="6" y="4" width="4" height="16" rx="1"/><rect x="14" y="4" width="4" height="16" rx="1"/></svg>',
  rerun: '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="1 4 1 10 7 10"/><path d="M3.51 15a9 9 0 1 0 .49-4.5"/></svg>',
  issues: '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg>',
  trash: '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/></svg>',
  download: '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>',
  logs: '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/></svg>',
};

// ── Drag & Drop ──
if (dropzone) {
  ['dragenter', 'dragover'].forEach((e) => dropzone.addEventListener(e, (ev) => { ev.preventDefault(); dropzone.classList.add('dragover'); }));
  ['dragleave', 'drop'].forEach((e) => dropzone.addEventListener(e, (ev) => {
    ev.preventDefault();
    dropzone.classList.remove('dragover');
    if (e === 'drop' && ev.dataTransfer?.files?.length) {
      const dt = new DataTransfer();
      Array.from(ev.dataTransfer.files).forEach((f) => dt.items.add(f));
      fileInput.files = dt.files;
      updateFileLabel();
    }
  }));
}
if (fileInput) fileInput.addEventListener('change', updateFileLabel);
function updateFileLabel() {
  if (fileLabel) fileLabel.textContent = fileInput?.files?.[0]?.name || 'CSV · XLS · XLSX';
}

// ── Upload ──
if (form) form.addEventListener('submit', async (e) => {
  e.preventDefault();
  if (!fileInput?.files?.length) return showBanner('error', 'Select a file first.');
  submitBtn.disabled = true;
  submitBtn.textContent = 'Uploading…';
  showBanner('info', 'Uploading and starting job...');
  const fd = new FormData();
  fd.append('file', fileInput.files[0]);
  try {
    const res = await fetch('/v1/scraper/enricher/upload', { method: 'POST', headers: { 'x-user-id': USER_ID }, body: fd });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || 'Upload failed');
    showBanner('success', `Job ${data.jobId} started.`);
    await loadJobs();
    pollGraceCount = 0;
    startPolling();
  } catch (err) {
    showBanner('error', err.message);
  } finally {
    submitBtn.disabled = false;
    submitBtn.innerHTML = '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="5 3 19 12 5 21 5 3"/></svg> Run Enrichment';
  }
});

// ── Jobs Table ──
async function loadJobs() {
  try {
    const res = await fetch('/v1/scraper/enricher/jobs');
    if (!res.ok) return;
    const data = await res.json();
    const serverJobs = data.jobs || [];

    // Merge: build a map from server data, then layer in any locally-known
    // running jobs that the server might have transiently missed.
    const serverMap = new Map(serverJobs.map((j) => [j.jobId, j]));
    const merged = [...serverJobs];

    for (const local of jobs) {
      if (!serverMap.has(local.jobId) && local.status === 'run') {
        // Server didn't return this running job (transient read failure).
        // Keep it visible so the UI doesn't flicker.
        merged.push(local);
      }
    }

    jobs = merged;
    renderJobs();

    const hasRunning = jobs.some((j) => j.status === 'run');
    if (hasRunning) {
      pollGraceCount = 0;
      startPolling();
    } else {
      pollGraceCount += 1;
      if (pollGraceCount >= POLL_GRACE_CYCLES) {
        stopPolling();
      } else {
        startPolling();   // keep polling through the grace window
      }
    }
  } catch (err) { /* retry next poll */ }
}

function renderJobs() {
  if (!tbody) return;
  if (!jobs.length) {
    tbody.innerHTML = '';
    if (emptyEl) { emptyEl.style.display = 'block'; emptyEl.textContent = 'No jobs yet. Upload a file to start.'; }
    return;
  }
  if (emptyEl) emptyEl.style.display = 'none';

  tbody.innerHTML = jobs.map((job) => {
    const total = job.totals?.totalRows ?? job.progress?.totalContacts ?? 0;
    const processed = job.resultCount ?? job.progress?.processedContacts ?? 0;
    const pct = total ? Math.round((processed / total) * 100) : (job.status === 'done' ? 100 : 0);
    const counts = job.progress?.statusCounts || {};
    const valid = counts.valid || 0;
    const catchAll = counts.catch_all || 0;
    const notFound = counts.not_found || 0;
    const rateLimited = counts.rate_limited || 0;
    const mxNotFound = counts.mx_not_found || 0;
    const errors = counts.error || 0;
    const issueCount = rateLimited + mxNotFound + errors;
    const isRunning = job.status === 'run';
    const canRerun = job.status === 'stop' || job.status === 'pause';

    // Catch-all cleaner state for this job
    const cleanerSt = cleanerStates[job.jobId];
    const cleanerBtnClass = cleanerSt?.status === 'running' ? 'running' : cleanerSt?.status === 'done' ? 'done' : cleanerSt?.status === 'error' ? 'error' : '';
    const cleanerBtnLabel = cleanerSt?.status === 'running' ? 'Cleaning...' : 'Catch-all Cleaner';

    return `<tr data-job="${job.jobId}">
      <td><div class="file-cell"><strong>${esc(job.originalFilename || 'Untitled')}</strong><span class="meta">${fmtDate(job.createdAt)}</span></div></td>
      <td><span class="pill ${job.status || 'pending'}">${fmtStatus(job.status)}</span></td>
      <td class="col-num">${total}</td>
      <td><div class="progress-cell"><div class="progress-track"><div class="progress-fill" style="width:${pct}%"></div></div><span class="progress-text">${processed}/${total} (${pct}%)</span></div></td>
      <td class="col-num"><span class="stat-num ${valid ? 'green' : 'muted'}">${valid}</span></td>
      <td class="col-num"><span class="stat-num ${catchAll ? 'amber' : 'muted'}">${catchAll}</span></td>
      <td class="col-num"><span class="stat-num ${notFound ? 'red' : 'muted'}">${notFound}</span></td>
      <td class="col-num"><button class="other-stat-btn ${(mxNotFound + errors) > 0 ? 'has-issues' : 'muted'}" onclick="showIssues('${job.jobId}')" title="MX Not Found + Timeouts/Errors">${mxNotFound + errors}</button></td>
      <td class="col-num"><span class="stat-num ${rateLimited ? 'orange' : 'muted'}">${rateLimited}</span></td>
      <td><div class="action-row">
        ${catchAll > 0 || cleanerSt ? `<button class="btn-action catchall-btn ${cleanerBtnClass}" onclick="openCatchallCleaner('${job.jobId}')">${cleanerBtnLabel}</button>` : ''}
        ${isRunning ? `<button class="btn-action pause" onclick="pauseJob('${job.jobId}')">Pause</button>` : ''}
        ${isRunning ? `<button class="btn-action stop" onclick="stopJob('${job.jobId}')">Stop</button>` : ''}
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
function startPolling() {
  if (pollTimer) return;
  pollTimer = setInterval(loadJobs, POLL_MS);
}
function stopPolling() {
  if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
}

// ── Job Actions ──
window.stopJob = async (jobId) => {
  try {
    await fetch(`/v1/scraper/enricher/jobs/${jobId}/stop`, { method: 'POST' });
    showBanner('info', `Stopping job ${jobId}...`);
    await loadJobs();
  } catch (err) { showBanner('error', err.message); }
};

window.pauseJob = async (jobId) => {
  try {
    await fetch(`/v1/scraper/enricher/jobs/${jobId}/pause`, { method: 'POST' });
    showBanner('info', `Pausing job ${jobId}...`);
    await loadJobs();
  } catch (err) { showBanner('error', err.message); }
};

window.rerunJob = async (jobId) => {
  try {
    const res = await fetch(`/v1/scraper/enricher/jobs/${jobId}/rerun`, { method: 'POST' });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || 'Rerun failed');
    showBanner('success', `Rerun started for job ${jobId}.`);
    await loadJobs();
    pollGraceCount = 0;
    startPolling();
  } catch (err) { showBanner('error', err.message); }
};

window.deleteJob = async (jobId) => {
  if (!confirm('Delete this job and its output files?')) return;
  try {
    await fetch(`/v1/scraper/enricher/jobs/${jobId}`, { method: 'DELETE' });
    jobs = jobs.filter((j) => j.jobId !== jobId);
    renderJobs();
  } catch (err) { showBanner('error', err.message); }
};

// ── Issues Drawer ──
window.showIssues = (jobId) => {
  const job = jobs.find((j) => j.jobId === jobId);
  if (!job) return;
  const counts = job.progress?.statusCounts || {};
  const total = job.progress?.totalContacts || 0;
  issuesTitle.textContent = `Issue Statistics`;

  const issueRows = [
    { label: 'MX Record Not Found', desc: 'Domain has no MX records', key: 'mx_not_found', color: 'orange', icon: '⚡' },
    { label: 'API / Rate Limited',  desc: 'Spam block or daily limit hit', key: 'rate_limited', color: 'amber', icon: '⏱' },
    { label: 'Processing Errors',   desc: 'Timeout or connection failure', key: 'error', color: 'red', icon: '✕' },
  ];

  const totalIssues = issueRows.reduce((s, r) => s + (counts[r.key] || 0), 0);
  const cleanRows = [
    { label: 'Valid',     key: 'valid',     color: 'green' },
    { label: 'Catch-All', key: 'catch_all', color: 'amber' },
    { label: 'Not Found', key: 'not_found', color: 'red' },
    { label: 'Skipped',   key: 'skipped',   color: 'muted' },
  ];
  const totalClean = cleanRows.reduce((s, r) => s + (counts[r.key] || 0), 0);

  const pct = (n) => total > 0 ? `${Math.round((n / total) * 100)}%` : '—';

  const issuesContent = $('#issues-content');
  issuesContent.innerHTML = `
    <div class="issues-summary">
      <div class="issues-summary-card issues-card-bad">
        <div class="issues-card-num">${totalIssues}</div>
        <div class="issues-card-label">Total Issues</div>
        <div class="issues-card-pct">${pct(totalIssues)} of contacts</div>
      </div>
      <div class="issues-summary-card issues-card-good">
        <div class="issues-card-num">${totalClean}</div>
        <div class="issues-card-label">Processed OK</div>
        <div class="issues-card-pct">${pct(totalClean)} of contacts</div>
      </div>
    </div>

    <div class="issues-section-title">Issue Breakdown</div>
    <table class="issues-table">
      <thead><tr><th>Type</th><th>Description</th><th style="text-align:right">Count</th><th style="text-align:right">% of Total</th></tr></thead>
      <tbody>
        ${issueRows.map(({ label, desc, key, color, icon }) => {
          const count = counts[key] || 0;
          return `<tr class="${count > 0 ? 'issue-row-active' : ''}">
            <td><span class="issue-type-label ${color}">${icon} ${label}</span></td>
            <td class="issue-desc">${desc}</td>
            <td style="text-align:right"><span class="stat-num ${count > 0 ? color : 'muted'}">${count}</span></td>
            <td style="text-align:right"><span class="stat-num muted">${pct(count)}</span></td>
          </tr>`;
        }).join('')}
        <tr class="issues-total">
          <td colspan="2"><strong>Total Issues</strong></td>
          <td style="text-align:right"><strong>${totalIssues}</strong></td>
          <td style="text-align:right"><strong>${pct(totalIssues)}</strong></td>
        </tr>
      </tbody>
    </table>

    <div class="issues-section-title" style="margin-top:20px">Results Summary</div>
    <table class="issues-table">
      <thead><tr><th>Status</th><th style="text-align:right">Count</th><th style="text-align:right">% of Total</th></tr></thead>
      <tbody>
        ${cleanRows.map(({ label, key, color }) => {
          const count = counts[key] || 0;
          return `<tr>
            <td><span class="stat-num ${count > 0 ? color : 'muted'}">${label}</span></td>
            <td style="text-align:right"><span class="stat-num ${count > 0 ? color : 'muted'}">${count}</span></td>
            <td style="text-align:right"><span class="stat-num muted">${pct(count)}</span></td>
          </tr>`;
        }).join('')}
      </tbody>
    </table>
    <div class="issues-job-meta">Job: <span>${esc(job.originalFilename || jobId)}</span> &nbsp;·&nbsp; Total Contacts: <span>${total}</span></div>
  `;

  issuesOverlay.hidden = false;
};

issuesClose?.addEventListener('click', () => { issuesOverlay.hidden = true; });
issuesOverlay?.addEventListener('click', (e) => { if (e.target === issuesOverlay) issuesOverlay.hidden = true; });

// ── Logs Drawer ──
window.openLogs = (jobId) => {
  openLogJobId = jobId;
  logsTitle.textContent = `Logs — ${jobId.slice(0, 8)}...`;
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
function closeLogs() {
  logsOverlay.hidden = true;
  openLogJobId = null;
  if (logsPollTimer) { clearInterval(logsPollTimer); logsPollTimer = null; }
}

// ── API Status ──
async function checkApiStatus() {
  try {
    const res = await fetch('/v1/scraper/enricher/key-status');
    const data = await res.json();
    const remaining = data.combined?.totalDailyRemaining ?? 0;
    const rpm = data.combined?.maxReqPerMin ?? 0;
    apiStatus.classList.remove('ok', 'warn', 'err');
    if (remaining <= 0) { apiStatus.classList.add('err'); apiText.textContent = 'Keys exhausted'; }
    else if (remaining < 50000) { apiStatus.classList.add('warn'); apiText.textContent = `${(remaining / 1000).toFixed(0)}K remaining`; }
    else { apiStatus.classList.add('ok'); apiText.textContent = `${rpm}/min · ${(remaining / 1000).toFixed(0)}K left`; }
  } catch { apiStatus.classList.add('err'); apiText.textContent = 'No keys'; }
}

// ── Helpers ──
function showBanner(type, msg) {
  if (!banner) return;
  banner.textContent = msg;
  banner.classList.remove('success', 'error', 'info', 'visible');
  if (msg) { banner.classList.add(type, 'visible'); }
}

function esc(str) {
  const d = document.createElement('div');
  d.textContent = str;
  return d.innerHTML;
}

function fmtDate(val) {
  if (!val) return '—';
  const d = new Date(val);
  if (isNaN(d)) return '—';
  return d.toLocaleString(undefined, { month: 'short', day: '2-digit', hour: '2-digit', minute: '2-digit' });
}

function fmtStatus(s) {
  return { run: 'Running', done: 'Done', stop: 'Stopped', pause: 'Paused', failed: 'Failed' }[s] || 'Pending';
}

// ── Refresh ──
if (refreshBtn) refreshBtn.addEventListener('click', loadJobs);

// ── Catch-all Cleaner ──
window.openCatchallCleaner = (jobId) => {
  openCleanerJobId = jobId;
  catchallTitle.textContent = `Catch-all Cleaner — ${jobId.slice(0, 8)}…`;
  catchallOverlay.hidden = false;
  updateCleanerDrawer(jobId);
  fetchCleanerStatus(jobId);
  cleanerPollTimer = setInterval(() => fetchCleanerStatus(jobId), 2000);
};

catchallClose?.addEventListener('click', closeCatchallDrawer);
catchallOverlay?.addEventListener('click', (e) => { if (e.target === catchallOverlay) closeCatchallDrawer(); });

function closeCatchallDrawer() {
  catchallOverlay.hidden = true;
  openCleanerJobId = null;
  if (cleanerPollTimer) { clearInterval(cleanerPollTimer); cleanerPollTimer = null; }
}

async function fetchCleanerStatus(jobId) {
  try {
    const res = await fetch(`/v1/scraper/enricher/jobs/${jobId}/catchall-cleaner/status`);
    const data = await res.json();
    cleanerStates[jobId] = data;
    if (openCleanerJobId === jobId) updateCleanerDrawer(jobId);
    // Re-render job row to update button style
    renderJobs();
    // Stop polling if cleaner finished
    if (data.status !== 'running' && cleanerPollTimer && openCleanerJobId === jobId) {
      // Keep polling a few more cycles then stop
    }
  } catch { /* retry next tick */ }
}

async function fetchAllCleanerStates() {
  try {
    const res = await fetch('/v1/scraper/enricher/catchall-cleaner/states');
    const data = await res.json();
    cleanerStates = data.states || {};
  } catch { /* silent */ }
}

function updateCleanerDrawer(jobId) {
  const state = cleanerStates[jobId];
  const status = state?.status || 'idle';
  const counts = state?.counts || { total: 0, deliverable: 0, undeliverable: 0, comboValid: 0, comboInvalid: 0, comboSkipped: 0, error: 0, skipped: 0 };
  const logs = state?.logs || [];

  // Status pill
  catchallStatusPill.textContent = fmtCleanerStatus(status);
  catchallStatusPill.className = `pill ${status}`;

  // Buttons
  const isRunning = status === 'running';
  catchallRunBtn.style.display = isRunning ? 'none' : '';
  catchallStopBtn.style.display = isRunning ? '' : 'none';
  catchallRunBtn.disabled = isRunning;

  // Derived counts for UI
  const comboTried = (counts.comboValid || 0) + (counts.comboInvalid || 0);
  const unchanged = (counts.undeliverable || 0) + (counts.comboInvalid || 0) + (counts.comboSkipped || 0) + (counts.skipped || 0);
  const verified = (counts.deliverable || 0) + (counts.undeliverable || 0) + comboTried + (counts.comboSkipped || 0) + (counts.error || 0) + (counts.skipped || 0);

  // Counts
  $('#cc-total').textContent = counts.total;
  $('#cc-deliverable').textContent = counts.deliverable;
  $('#cc-combo-tried').textContent = comboTried;
  $('#cc-combo-valid').textContent = counts.comboValid;
  $('#cc-unchanged').textContent = unchanged;
  $('#cc-errors').textContent = counts.error;

  // Progress
  const total = counts.total || 0;
  const pct = total > 0 ? Math.round((verified / total) * 100) : 0;
  $('#cc-progress-text').textContent = `${verified} / ${total}`;
  $('#cc-progress-pct').textContent = `${pct}%`;
  $('#cc-progress-fill').style.width = `${pct}%`;

  // Logs
  catchallLogs.textContent = logs.join('\n') || 'No logs yet.';
  catchallLogs.scrollTop = catchallLogs.scrollHeight;
}

function fmtCleanerStatus(s) {
  return { running: 'Running', done: 'Done', stopped: 'Stopped', error: 'Error', idle: 'Idle' }[s] || 'Idle';
}

window.runCatchallCleaner = async () => {
  if (!openCleanerJobId) return;
  try {
    catchallRunBtn.disabled = true;
    catchallRunBtn.textContent = 'Starting…';
    const res = await fetch(`/v1/scraper/enricher/jobs/${openCleanerJobId}/catchall-cleaner/run`, { method: 'POST' });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || 'Failed to start cleaner');
    showBanner('success', `Catch-all cleaner started for job.`);
    // Immediately start polling
    fetchCleanerStatus(openCleanerJobId);
  } catch (err) {
    showBanner('error', err.message);
  } finally {
    catchallRunBtn.disabled = false;
    catchallRunBtn.innerHTML = '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="5 3 19 12 5 21 5 3"/></svg> Run Cleaner';
  }
};

window.stopCatchallCleaner = async () => {
  if (!openCleanerJobId) return;
  try {
    await fetch(`/v1/scraper/enricher/jobs/${openCleanerJobId}/catchall-cleaner/stop`, { method: 'POST' });
    showBanner('info', 'Stopping catch-all cleaner…');
  } catch (err) { showBanner('error', err.message); }
};

// ── Boot ──
(async () => {
  await fetchAllCleanerStates();
  await loadJobs();
  checkApiStatus();
  checkBounceBanStatus();
  setInterval(checkApiStatus, 30000);
  setInterval(checkBounceBanStatus, 30000);
  // Periodically refresh cleaner states for button indicators
  setInterval(async () => {
    await fetchAllCleanerStates();
    renderJobs();
  }, 5000);
})();

// ── BounceBan Status Badge ──
async function checkBounceBanStatus() {
  const bbStatus = $('#bb-status');
  const bbText = $('#bb-text');
  if (!bbStatus || !bbText) return;
  try {
    const res = await fetch('/v1/scraper/enricher/bounceban-status');
    const data = await res.json();
    bbStatus.classList.remove('ok', 'warn', 'err');
    if (data.configured) {
      bbStatus.classList.add('ok');
      bbText.textContent = 'BounceBan OK';
    } else {
      bbStatus.classList.add('err');
      bbText.textContent = 'BounceBan: No Key';
    }
  } catch {
    bbStatus.classList.remove('ok', 'warn', 'err');
    bbStatus.classList.add('err');
    bbText.textContent = 'BounceBan: Error';
  }
}