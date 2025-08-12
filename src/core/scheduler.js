// src/core/scheduler.js
import cron from 'node-cron';
import { enqueueJob, appendLog } from './jobRunner.js';
import { runViosProcessosJob } from '../jobs/viosProcessosJob.js';

// Inicia todos os schedules configurados
export function initScheduler() {
  if (process.env.SCHEDULE_ENABLED !== '1') {
    console.log('[scheduler] Desativado (defina SCHEDULE_ENABLED=1 para ativar)');
    return;
  }

  const tz = process.env.TZ || 'America/Sao_Paulo';

  // Job semanal: toda segunda às 09:00 (semana cheia anterior)
  const semanalExpr = '0 9 * * 1';
  cron.schedule(semanalExpr, () => {
    console.log('[scheduler] Disparando job semanal (processos, LAST_FULL_WEEK)');
    enqueueJob(
      'processos-semanal',
      { dateRangeMode: 'LAST_FULL_WEEK' },
      async (payload, jobId) => {
        const logger = (m) => appendLog(jobId, m);
        return runViosProcessosJob(payload, { logger });
      }
    );
  }, { timezone: tz });
  console.log(`[scheduler] Agendado semanal: ${semanalExpr} TZ=${tz}`);

  // (Opcional) Job diário: descomente se quiser rodar todo dia às 07:30 pegando últimos 7 dias até ontem
  // const diarioExpr = '30 7 * * *';
  // cron.schedule(diarioExpr, () => {
  //   console.log('[scheduler] Disparando job diário (processos, YESTERDAY_7)');
  //   enqueueJob(
  //     'processos-diario',
  //     { dateRangeMode: 'YESTERDAY_7' },
  //     async (payload, jobId) => {
  //       const logger = (m) => appendLog(jobId, m);
  //       return runViosProcessosJob(payload, { logger });
  //     }
  //   );
  // }, { timezone: tz });
  // console.log(`[scheduler] Agendado diário: ${diarioExpr} TZ=${tz}`);
}
