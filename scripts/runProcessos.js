// scripts/runProcessos.js
import { runViosProcessosJob } from '../src/jobs/viosProcessosJob.js';

const opts = {
  // Ajuste se quiser:
  headless: false,            // ou true para testar sem abrir janela
  dateRangeMode: 'YESTERDAY_7',
  exportCsv: true
};

console.log('[runner] Iniciando job processos...');
const result = await runViosProcessosJob(opts, { logger: (l) => console.log(l) });
console.log('[runner] OK?', result.ok);
if (!result.ok) {
  console.error('[runner] ERRO:', result.error);
  process.exit(1);
}
console.log('[runner] Fonte:', result.summary.fonte, 'Linhas:', result.summary.linhas);
