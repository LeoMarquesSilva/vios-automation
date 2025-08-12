// src/api/routes.js
import { Router } from 'express';
import { runViosProcessosJob } from '../jobs/viosProcessosJob.js';

// Armazenamento simples em memória para logs
const jobLogs = new Map();
function appendJobLog(jobId, line) {
  if (!jobId) return;
  if (!jobLogs.has(jobId)) jobLogs.set(jobId, []);
  jobLogs.get(jobId).push(line);
}

export function buildRouter() {
  const router = Router();

  // Health interno (opcional - já existe /health na raiz)
  router.get('/health', (req, res) => {
    res.json({ ok: true, ts: Date.now() });
  });

  // Executa job (rota duplicada com /api/run já existente, use só se precisar)
  router.post('/job/run', async (req, res) => {
    const payload = req.body || {};
    const { format } = req.query;
    const jobId = payload.jobId || `job_${Date.now()}`;

    try {
      const result = await runViosProcessosJob(payload, {
        logger: line => appendJobLog(jobId, line)
      });

      result.jobId = jobId;

      if (!result.ok) {
        return res.status(500).json(result);
      }

      if (format === 'csv') {
        if (!result.headers?.length) {
          return res.status(400).json({ ok:false, error:'Sem headers para CSV', jobId });
        }
        const sep = ';';
        const linhas = [
          result.headers.join(sep),
            ...result.data.map(row =>
              result.headers.map(h => {
                const val = (row[h] ?? '').toString().replace(/"/g,'""');
                return /[;"\n]/.test(val) ? `"${val}"` : val;
              }).join(sep)
            )
        ];
        res.setHeader('Content-Type','text/csv; charset=utf-8');
        res.setHeader('X-Job-Id', jobId);
        return res.send(linhas.join('\n'));
      }

      res.json(result);
    } catch (e) {
      res.status(500).json({ ok:false, error:e.message, jobId });
    }
  });

  // Logs
  router.get('/job/logs/:jobId', (req, res) => {
    const { jobId } = req.params;
    res.json({
      ok: true,
      jobId,
      logs: jobLogs.get(jobId) || []
    });
  });

  // Status simples
  router.get('/job/status', (req, res) => {
    res.json({ ok:true, ultimoJobIds: Array.from(jobLogs.keys()).slice(-5) });
  });

  return router;
}

export default buildRouter;
