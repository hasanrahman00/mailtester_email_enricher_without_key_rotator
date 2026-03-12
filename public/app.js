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

let jobs = [];
let pollTimer = null;
let logsPollTimer = null;
let openLogJobId = null;

// ── Icons (inline SVG) ──
const ICONS = {
  stop: '<svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor"><rect x="6" y="6" width="12" height="12" rx="1"/></svg>',
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
    jobs = data.jobs || [];
    renderJobs();
    // Start polling if any job is processing
    if (jobs.some((j) => j.status === 'processing')) startPolling();
    else stopPolling();
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
    const pct = total ? Math.round((processed / total) * 100) : (job.status === 'completed' ? 100 : 0);
    const counts = job.progress?.statusCounts || {};
    const valid = counts.valid || 0;
    const catchAll = counts.catch_all || 0;
    const notFound = counts.not_found || 0;
    const rateLimited = counts.rate_limited || 0;
    const isProcessing = job.status === 'processing';

    return `<tr data-job="${job.jobId}">
      <td><div class="file-cell"><strong>${esc(job.originalFilename || 'Untitled')}</strong><span class="meta">${fmtDate(job.createdAt)}</span></div></td>
      <td><span class="pill ${job.status || 'pending'}">${fmtStatus(job.status)}</span></td>
      <td class="col-num">${total}</td>
      <td><div class="progress-cell"><div class="progress-track"><div class="progress-fill" style="width:${pct}%"></div></div><span class="progress-text">${processed}/${total} (${pct}%)</span></div></td>
      <td class="col-num"><span class="stat-num ${valid ? 'green' : 'muted'}">${valid}</span></td>
      <td class="col-num"><span class="stat-num ${catchAll ? 'amber' : 'muted'}">${catchAll}</span></td>
      <td class="col-num"><span class="stat-num ${notFound ? 'red' : 'muted'}">${notFound}</span></td>
      <td class="col-num"><span class="stat-num ${rateLimited ? 'orange' : 'muted'}">${rateLimited}</span></td>
      <td><div class="action-row">
        ${isProcessing ? `<button class="btn-icon stop" title="Stop" onclick="stopJob('${job.jobId}')">${ICONS.stop}</button>` : ''}
        ${job.downloadUrl ? `<a class="btn-icon" title="Download" href="${job.downloadUrl}" download>${ICONS.download}</a>` : ''}
        <button class="btn-icon" title="Logs" onclick="openLogs('${job.jobId}')">${ICONS.logs}</button>
        <button class="btn-icon danger" title="Delete" onclick="deleteJob('${job.jobId}')">${ICONS.trash}</button>
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

window.deleteJob = async (jobId) => {
  if (!confirm('Delete this job and its output files?')) return;
  try {
    await fetch(`/v1/scraper/enricher/jobs/${jobId}`, { method: 'DELETE' });
    jobs = jobs.filter((j) => j.jobId !== jobId);
    renderJobs();
  } catch (err) { showBanner('error', err.message); }
};

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
  return { completed: 'Done', processing: 'Running', failed: 'Failed', stopped: 'Stopped' }[s] || 'Pending';
}

// ── Refresh ──
if (refreshBtn) refreshBtn.addEventListener('click', loadJobs);

// ── Boot ──
(async () => {
  await loadJobs();
  checkApiStatus();
  setInterval(checkApiStatus, 30000);
})();
