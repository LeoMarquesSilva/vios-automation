// scripts/runProcessosManual.js
// Uso local ou em shell de debug.
// Exemplo: node scripts/runProcessosManual.js LAST_FULL_WEEK
// Modo default: LAST_FULL_WEEK

import { runViosProcessosJob } from '../src/jobs/viosProcessosJob.js';
import { appendHistory } from '../src/core/history.js';

const mode = process.argv[2] || 'LAST_FULL_WEEK';

(async () => {
  const startedAt = new Date();
  console.log('[manual] Rodando modo', mode);
  try {
    const result = await runViosProcessosJob(
      { dateRangeMode: mode },
      { logger: (m) => console.log('[job]', m) }
    );
    console.log('[manual] OK?', result.ok, 'Summary:', result.summary);
    appendHistory({
      job: 'processos-manual',
      mode,
      startedAt: startedAt.toISOString(),
      endedAt: new Date().toISOString(),
      ok: !!result.ok,
      summary: result.summary || null
    });
  } catch (e) {
    console.error('[manual] Erro ao rodar job:', e);
    appendHistory({
      job: 'processos-manual',
      mode,
      startedAt: startedAt.toISOString(),
      endedAt: new Date().toISOString(),
      ok: false,
      error: e.message
    });
    process.exit(1);
  }
})();
