/**
 * api/middleware/upload.middleware.js — File upload handling.
 *
 * Uses multer to accept file uploads. Before multer runs,
 * we create a unique job directory so the file lands there.
 */

const multer = require('multer');
const { v4: uuidv4 } = require('uuid');
const { createJobDirectory } = require('../../utils/storage');

// Configure multer to save files into the job directory
const storage = multer.diskStorage({
  destination(req, file, cb) {
    if (!req.jobContext?.jobDir) return cb(new Error('Job directory missing'));
    cb(null, req.jobContext.jobDir);
  },
  filename(req, file, cb) {
    // Sanitize the original filename
    const safe = String(file.originalname || 'upload').replace(/[^a-z0-9.]/gi, '-').toLowerCase();
    cb(null, `${Date.now()}-${safe}`);
  },
});

const upload = multer({ storage, limits: { fileSize: 100 * 1024 * 1024 } }); // 100MB max

// Middleware: create a job folder BEFORE multer saves the file
async function prepareJobContext(req, res, next) {
  try {
    const jobId = uuidv4();
    const jobDir = await createJobDirectory(jobId);
    req.jobContext = { jobId, jobDir };
    next();
  } catch (err) { next(err); }
}

// Middleware: accept a single file field named "file"
const uploadSingleFile = upload.single('file');

module.exports = { prepareJobContext, uploadSingleFile };
