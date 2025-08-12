import express from 'express';
import { runViosProcessosJob } from '../../jobs/viosProcessosJob.js';


const router = express.Router();

// Middleware simples de API Key
function apiAuth(req, res, next) {
  const key = req.header('x-api-key');
  if (!key || key !== process.env.API_KEY) {
    return res.status(401).json({ ok:false, error:'unauthorized' });
  }
  next();
}

// Armazenamento em memória de logs (opcional)
const jobLogs = new Map();
function appendJobLog(jobId, line) {
  if (!jobId) return;
  if (!jobLogs.has(jobId)) jobLogs.set(jobId, []);
  jobLogs.get(jobId).push(line);
}

// Executar job (JSON por padrão, CSV opcional via query ?format=csv)
router.post('/job/run', apiAuth, async (req, res) => {
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

// (Opcional) Recuperar logs depois
router.get('/job/logs/:jobId', apiAuth, (req, res) => {
  const { jobId } = req.params;
  res.json({
    ok: true,
    jobId,
    logs: jobLogs.get(jobId) || []
  });
});

export default router;
