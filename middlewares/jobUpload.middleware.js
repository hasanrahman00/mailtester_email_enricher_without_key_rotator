import multer from 'multer';
import { v4 as uuidv4 } from 'uuid';
import { createJobDirectory } from '../utils/storage.js';

const storage = multer.diskStorage({
  destination(req, file, cb) {
    if (!req.jobContext?.jobDir) {
      cb(new Error('Upload context missing job directory'));
      return;
    }
    cb(null, req.jobContext.jobDir);
  },
  filename(req, file, cb) {
    const sanitizedName = String(file.originalname || 'upload')
      .replace(/[^a-z0-9.]/gi, '-')
      .toLowerCase();
    cb(null, `${Date.now()}-${sanitizedName}`);
  },
});

const upload = multer({
  storage,
  limits: {
    fileSize: 25 * 1024 * 1024,
  },
});

export async function prepareJobContext(req, res, next) {
  try {
    const jobId = uuidv4();
    const jobDir = await createJobDirectory(jobId);
    req.jobContext = { jobId, jobDir };
    next();
  } catch (error) {
    next(error);
  }
}

export const uploadSingleFile = upload.single('file');
