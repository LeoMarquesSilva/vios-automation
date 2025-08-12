// src/server.js
import 'dotenv/config';
import express from 'express';
import morgan from 'morgan';
import path from 'path';
import fs from 'fs';
import crypto from 'crypto';

import { buildRouter } from './api/routes.js'; // opcional
import { runViosProcessosJob } from './jobs/viosProcessosJob.js';
import { readHistory } from './core/history.js';
import { jobStore } from './jobs/store.js'; // Map: jobId -> { status, ... }

const app = express();
app.set('trust proxy', true);

app.use(express.json({ limit: process.env.JSON_LIMIT || '500kb' }));
app.use(morgan(process.env.MORGAN_FORMAT || 'dev'));

// ---------- HEALTH (público) ----------
app.get('/health', (_req, res) => {
  res.json({ ok: true, status: 'up', service: 'vios-automation', ts: new Date().toISOString() });
});

// ---------- Middleware API Key (tudo abaixo exige x-api-key se API_KEY existir) ----------
app.use((req, res, next) => {
  if (process.env.API_KEY) {
    const key = req.headers['x-api-key'];
    if (key !== process.env.API_KEY) {
      return res.status(401).json({ ok: false, error: 'unauthorized' });
    }
  }
  next();
});

// ---------- Rotas existentes /api (se houver buildRouter) ----------
if (buildRouter) {
  app.use('/api', buildRouter());
}

// =========================================================
// CONFIG DE JOBS ASSÍNCRONOS
// =========================================================
const MAX_CONCURRENT_JOBS = parseInt(process.env.MAX_CONCURRENT_JOBS || '1', 10);
const RETENTION_MINUTES = parseInt(process.env.RETENTION_MINUTES || '120', 10); // tempo que mantém jobs finalizados
let activeJobs = 0;

function genId() {
  return crypto.randomBytes(9).toString('hex');
}

function nowISO() {
  return new Date().toISOString();
}

function launchJob(jobId) {
  const job = jobStore.get(jobId);
  if (!job || job.status !== 'queued') return;
  job.status = 'running';
  job.startedAt = nowISO();
  activeJobs++;

  (async () => {
    try {
      // Aqui você pode adaptar os parâmetros conforme o body enviado no POST /api/run-async
      const mode = job.payload?.mode || 'LAST_FULL_WEEK';
      const result = await runViosProcessosJob(
        { dateRangeMode: mode },
        { logger: (m) => console.log(`[job:${jobId}]`, m) }
      );
      job.status = 'done';
      job.result = {
        summary: result?.summary || null,
        finishedAt: nowISO()
      };
    } catch (err) {
      console.error(`[job:${jobId}] erro`, err);
      job.status = 'error';
      job.error = err.message || String(err);
      job.finishedAt = nowISO();
    } finally {
      if (!job.finishedAt) job.finishedAt = nowISO();
      activeJobs--;
      // Tenta iniciar próximo da fila
      scheduleNext();
    }
  })();
}

function scheduleNext() {
  if (activeJobs >= MAX_CONCURRENT_JOBS) return;
  for (const [id, job] of jobStore.entries()) {
    if (job.status === 'queued') {
      launchJob(id);
      break;
    }
  }
}

// Limpeza periódica de jobs finalizados
setInterval(() => {
  const cutoff = Date.now() - RETENTION_MINUTES * 60 * 1000;
  let removed = 0;
  for (const [id, job] of jobStore.entries()) {
    if (['done', 'error'].includes(job.status)) {
      const endTs = Date.parse(job.finishedAt || job.startedAt || job.createdAt || Date.now());
      if (endTs < cutoff) {
        jobStore.delete(id);
        removed++;
      }
    }
  }
  if (removed > 0) {
    console.log(`[jobs] limpeza: removidos ${removed} jobs antigos`);
  }
}, 60 * 1000).unref();

// =========================================================
// ENDPOINT ASSÍNCRONO: cria job e retorna imediatamente
// POST /api/run-async { "mode": "LAST_FULL_WEEK" }
// =========================================================
app.post('/api/run-async', (req, res) => {
  const mode = (req.body && req.body.mode) || 'LAST_FULL_WEEK';
  const jobId = genId();
  jobStore.set(jobId, {
    jobId,
    status: 'queued',
    createdAt: nowISO(),
    payload: { mode }
  });

  // Inicia se houver slot
  scheduleNext();

  res.status(202).json({
    ok: true,
    accepted: true,
    jobId,
    mode,
    status: 'queued',
    statusUrl: `/api/jobs/${jobId}`
  });
});

// =========================================================
// ENDPOINT STATUS DO JOB
// GET /api/jobs/:id
// =========================================================
app.get('/api/jobs/:id', (req, res) => {
  const job = jobStore.get(req.params.id);
  if (!job) {
    return res.status(404).json({ ok: false, error: 'not_found', jobId: req.params.id });
  }
  res.json({
    ok: true,
    jobId: req.params.id,
    status: job.status,
    createdAt: job.createdAt,
    startedAt: job.startedAt,
    finishedAt: job.finishedAt,
    payload: job.payload,
    result: job.result,
    error: job.error
  });
});

// (Opcional) listar jobs recentes
// GET /api/jobs?limit=20&status=done
app.get('/api/jobs', (req, res) => {
  const limit = Math.min(parseInt(req.query.limit || '50', 10), 200);
  const statusFilter = req.query.status;
  const entries = [];
  for (const [, job] of jobStore.entries()) {
    if (statusFilter && job.status !== statusFilter) continue;
    entries.push({
      jobId: job.jobId,
      status: job.status,
      createdAt: job.createdAt,
      startedAt: job.startedAt,
      finishedAt: job.finishedAt
    });
  }
  // Ordena por criação desc
  entries.sort((a, b) => (a.createdAt < b.createdAt ? 1 : -1));
  res.json({ ok: true, count: entries.slice(0, limit).length, items: entries.slice(0, limit) });
});

// =========================================================
// ENDPOINT SÍNCRONO EXISTENTE (mantido)
// POST /api/run
// =========================================================
app.post('/api/run', async (req, res) => {
  const mode = (req.body && req.body.mode) || 'LAST_FULL_WEEK';
  try {
    const result = await runViosProcessosJob(
      { dateRangeMode: mode },
      { logger: (m) => console.log('[manual-job]', m) }
    );
    return res.json({ ok: true, summary: result.summary });
  } catch (e) {
    console.error('[manual-job] Erro:', e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// Histórico (existente)
app.get('/api/history', (req, res) => {
  const limit = parseInt(req.query.limit, 10) || 100;
  const history = readHistory({ limit });
  res.json({ ok: true, count: history.length, items: history });
});

// Servir relatórios estáticos
const reportsDir = process.env.REPORTS_DIR
  ? path.join(process.env.REPORTS_DIR, 'reports')
  : path.join(process.cwd(), 'reports');

if (fs.existsSync(reportsDir)) {
  app.use('/reports', express.static(reportsDir));
}

// Raiz
app.get('/', (_req, res) => {
  res.json({
    ok: true,
    service: 'vios-automation',
    endpoints: [
      '/health (GET, público)',
      '/api/run (POST, síncrono)',
      '/api/run-async (POST, assíncrono)',
      '/api/jobs (GET, lista)',
      '/api/jobs/:id (GET, status)',
      '/api/history (GET)',
      '/reports (static)'
    ],
    concurrency: {
      max: MAX_CONCURRENT_JOBS,
      active: activeJobs
    }
  });
});

// 404
app.use((req, res) => {
  res.status(404).json({ ok: false, error: 'not_found', path: req.originalUrl });
});

// Tratadores globais
process.on('unhandledRejection', (err) => {
  console.error('[UNHANDLED_REJECTION]', err);
});
process.on('uncaughtException', (err) => {
  console.error('[UNCAUGHT_EXCEPTION]', err);
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Servidor ouvindo na porta ${PORT}`);
  console.log('Scheduler interno removido (uso de cron externo Render).');
  console.log(`MAX_CONCURRENT_JOBS=${MAX_CONCURRENT_JOBS} | RETENTION_MINUTES=${RETENTION_MINUTES}`);
});