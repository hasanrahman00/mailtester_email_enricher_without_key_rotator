/**
 * utils/storage.js — File system helpers for job directories.
 *
 * Each job gets its own folder inside "tempUploads/".
 * This module handles creating, reading, and cleaning up those folders.
 */

const fs = require('fs/promises');
const path = require('path');

// Root folder where all job data is stored
const TEMP_ROOT = path.join(process.cwd(), 'tempUploads');

// Create a directory (and parents) if it doesn't exist
async function ensureDir(dirPath) {
  await fs.mkdir(dirPath, { recursive: true });
}

// Make sure the root temp folder exists, return its path
async function getTempRootDir() {
  await ensureDir(TEMP_ROOT);
  return TEMP_ROOT;
}

// Create a new folder for a job, return its path
async function createJobDirectory(jobId) {
  const jobDir = path.join(await getTempRootDir(), jobId);
  await ensureDir(jobDir);
  return jobDir;
}

// Save job metadata as JSON
// On Windows, atomic rename often fails (EPERM) when antivirus or editors
// hold a lock on the file. We try atomic rename first, then fall back to
// a direct overwrite, and retry once on transient lock errors.
async function writeMetadata(jobDir, metadata) {
  const metaPath = path.join(jobDir, 'metadata.json');
  const data = JSON.stringify(metadata, null, 2);

  // Try atomic rename first (safe on Linux/Mac, often fails on Windows)
  try {
    const tmpPath = metaPath + '.tmp';
    await fs.writeFile(tmpPath, data, 'utf-8');
    await fs.rename(tmpPath, metaPath);
    return;
  } catch { /* fall through to direct write */ }

  // Fallback: direct overwrite (handles Windows file locking)
  try {
    await fs.writeFile(metaPath, data, 'utf-8');
  } catch {
    // One retry after a short wait for transient locks
    await new Promise((r) => setTimeout(r, 50));
    await fs.writeFile(metaPath, data, 'utf-8');
  }
}

// Read job metadata, returns null if not found
async function readMetadata(jobDir) {
  try {
    const raw = await fs.readFile(path.join(jobDir, 'metadata.json'), 'utf-8');
    return JSON.parse(raw);
  } catch { return null; }
}

// Delete an entire job directory
async function removeDirectory(jobDir) {
  await fs.rm(jobDir, { recursive: true, force: true });
}

// Get full path for a file inside a job folder
function buildJobFilePath(jobDir, fileName) {
  return path.join(jobDir, fileName);
}

// List all job directories inside tempUploads/
async function listJobDirectories() {
  const root = await getTempRootDir();
  const entries = await fs.readdir(root, { withFileTypes: true });
  return entries.filter((e) => e.isDirectory()).map((e) => path.join(root, e.name));
}

module.exports = {
  getTempRootDir, createJobDirectory, writeMetadata, readMetadata,
  removeDirectory, buildJobFilePath, listJobDirectories,
};
