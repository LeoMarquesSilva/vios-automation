// src/jobs/store.js
export const jobStore = new Map(); 
// Estrutura: jobId -> { status: 'queued'|'running'|'done'|'error', result, error, startedAt, finishedAt }