// scripts/runProcessosSemanal.js
// Este script será chamado pelo Cron Job do Render.
// Ele executa o job semanal usando o modo LAST_FULL_WEEK.
//
// Certifique-se de que o arquivo src/jobs/viosProcessosJob.js exporta
// a função: runViosProcessosJob(params, ctx)
//
// Params esperados no exemplo: { dateRangeMode: 'LAST_FULL_WEEK' }

import { runViosProcessosJob } from '../src/jobs/viosProcessosJob.js';
import { appendHistory } from '../src/core/history.js';

async function main() {
  const startedAt = new Date();
  console.log('[cron] Execução semanal iniciada (LAST_FULL_WEEK)');

  try {
    const result = await runViosProcessosJob(
        
      { dateRangeMode: 'LAST_FULL_WEEK' },
      { logger: (m) => console.log('[job]', m) }
    );

    console.log('[cron] Summary:', JSON.stringify(result.summary || {}, null, 2));
    console.log('[cron] OK?', result.ok);
    console.log('[cron] Linhas:', result.summary?.linhas);

    // Grava histórico (opcional mas recomendado)
    appendHistory({
      job: 'processos-semanal',
      mode: 'LAST_FULL_WEEK',
      startedAt: startedAt.toISOString(),
      endedAt: new Date().toISOString(),
      ok: !!result.ok,
      summary: result.summary || null
    });

    const dur = ((Date.now() - startedAt.getTime()) / 1000).toFixed(1);
    console.log('[cron] Duração (s):', dur);
    process.exit(result.ok ? 0 : 2);
  } catch (e) {
    console.error('[cron] Falha geral:', e);

    appendHistory({
      job: 'processos-semanal',
      mode: 'LAST_FULL_WEEK',
      startedAt: startedAt.toISOString(),
      endedAt: new Date().toISOString(),
      ok: false,
      error: e.message
    });

    process.exit(1);
  }
}

main();
