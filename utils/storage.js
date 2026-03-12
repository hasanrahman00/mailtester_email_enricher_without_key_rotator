import fs from 'fs/promises';
import path from 'path';

const TEMP_ROOT = path.join(process.cwd(), 'tempUploads');

export async function ensureDirectoryExists(dirPath) {
  await fs.mkdir(dirPath, { recursive: true });
}

export async function getTempRootDir() {
  await ensureDirectoryExists(TEMP_ROOT);
  return TEMP_ROOT;
}

export async function createJobDirectory(jobId) {
  const root = await getTempRootDir();
  const jobDir = path.join(root, jobId);
  await ensureDirectoryExists(jobDir);
  return jobDir;
}

export async function writeMetadata(jobDir, metadata) {
  const metadataPath = path.join(jobDir, 'metadata.json');
  await fs.writeFile(metadataPath, JSON.stringify(metadata, null, 2), 'utf-8');
  return metadataPath;
}

export async function readMetadata(jobDir) {
  try {
    const metadataPath = path.join(jobDir, 'metadata.json');
    const buffer = await fs.readFile(metadataPath, 'utf-8');
    return JSON.parse(buffer);
  } catch (error) {
    return null;
  }
}

export async function removeDirectory(jobDir) {
  await fs.rm(jobDir, { recursive: true, force: true });
}

export function buildJobFilePath(jobDir, fileName) {
  return path.join(jobDir, fileName);
}

export async function listJobDirectories() {
  const root = await getTempRootDir();
  const entries = await fs.readdir(root, { withFileTypes: true });
  return entries.filter((entry) => entry.isDirectory()).map((entry) => path.join(root, entry.name));
}
