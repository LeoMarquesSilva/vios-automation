// src/server.js
import 'dotenv/config';
import express from 'express';
import morgan from 'morgan';
import path from 'path';
import fs from 'fs';

import { buildRouter } from './api/routes.js'; // mantém sua rota existente (se existir)
import { runViosProcessosJob } from './jobs/viosProcessosJob.js';
import { readHistory } from './core/history.js';

// Se buildRouter não existir ou quiser simplificar, ajuste conforme sua base.

const app = express();
app.set('trust proxy', true);

app.use(express.json({ limit: process.env.JSON_LIMIT || '500kb' }));
app.use(morgan(process.env.MORGAN_FORMAT || 'dev'));

// Health público
app.get('/health', (_req, res) => {
  res.json({ ok: true, status: 'up', service: 'vios-automation', ts: new Date().toISOString() });
});

// Middleware de API Key (aplica em tudo que vier abaixo)
app.use((req, res, next) => {
  if (process.env.API_KEY) {
    const key = req.headers['x-api-key'];
    if (key !== process.env.API_KEY) {
      return res.status(401).json({ ok: false, error: 'unauthorized' });
    }
  }
  next();
});

// Rotas existentes /api
if (buildRouter) {
  app.use('/api', buildRouter());
}

// Endpoint manual para disparar job (POST /api/run)
// Body JSON: { "mode": "LAST_FULL_WEEK" } (opcional)
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

// Endpoint para ler histórico (GET /api/history?limit=50)
app.get('/api/history', (req, res) => {
  const limit = parseInt(req.query.limit, 10) || 100;
  const history = readHistory({ limit });
  res.json({ ok: true, count: history.length, items: history });
});

// Servir diretório de relatórios estáticos (se existir)
const reportsDir = process.env.REPORTS_DIR
  ? path.join(process.env.REPORTS_DIR, 'reports')
  : path.join(process.cwd(), 'reports');

if (fs.existsSync(reportsDir)) {
  app.use('/reports', express.static(reportsDir));
}

// Página raiz simples
app.get('/', (_req, res) => {
  res.json({
    ok: true,
    service: 'vios-automation',
    endpoints: [
      '/health',
      '/api/run (POST)',
      '/api/history (GET)',
      '/reports (static)'
    ]
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
});
