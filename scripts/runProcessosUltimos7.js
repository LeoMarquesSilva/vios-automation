// scripts/runProcessosUltimos7.js
// Executa o job para os últimos 7 dias (até ontem). Útil para testes ou análises.
// Pode ser configurado como outro cron futuramente, se desejar.

import { runViosProcessosJob } from '../src/jobs/viosProcessosJob.js';
import { appendHistory } from '../src/core/history.js';

(async () => {
  const startedAt = new Date();
  console.log('[cron] Execução últimos 7 dias (YESTERDAY_7)');

  try {
    const result = await runViosProcessosJob(
      { dateRangeMode: 'YESTERDAY_7' },
      { logger: (m) => console.log('[job]', m) }
    );

    console.log('[cron] Summary:', result.summary);
    appendHistory({
      job: 'processos-ultimos7',
      mode: 'YESTERDAY_7',
      startedAt: startedAt.toISOString(),
      endedAt: new Date().toISOString(),
      ok: !!result.ok,
      summary: result.summary || null
    });

    process.exit(0); // Mesmo se ok false, não queremos falhar pipeline neste exemplo
  } catch (e) {
    console.error('[cron] Erro:', e);
    appendHistory({
      job: 'processos-ultimos7',
      mode: 'YESTERDAY_7',
      startedAt: startedAt.toISOString(),
      endedAt: new Date().toISOString(),
      ok: false,
      error: e.message
    });
    process.exit(1);
  }
})();
