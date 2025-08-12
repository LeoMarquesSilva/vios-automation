// src/jobs/viosProcessosJob.js
// Versão consolidada com ajustes para integração Render + scripts cron externos.
// Mantém toda a sua lógica Playwright, adiciona:
// - Uso de REPORTS_DIR
// - summary.range (ISO)
// - summary.files (lista de arquivos gerados)
// - Optional lock para evitar corrida
// - Resposta padronizada ok/erro

import { chromium } from 'playwright';
import fs from 'fs';
import path from 'path';
import 'dotenv/config';

/* =========================
   BASE / CONFIG
   ========================= */
const baseDir = process.env.REPORTS_DIR || process.cwd();
const reportsDir = path.join(baseDir, 'reports');
const locksDir = path.join(baseDir, 'locks');

const DEFAULT_CONFIG = {
baseUrl: process.env.VIOS_BASE_URL || 'https://bp.vios.com.br',
  headless: process.env.HEADLESS === 'false' ? false : true,
  waitAfterSearchMs: process.env.WAIT_AFTER_SEARCH
    ? parseInt(process.env.WAIT_AFTER_SEARCH, 10)
    : 8000,
  maxCsvWaitMs: 30000,
  dateRangeMode: process.env.DATE_RANGE_MODE || 'LAST_FULL_WEEK',
  valorMostrarMax: process.env.MAX_RECORDS || '9999999',
  selectLimitName: 'pesq[limit]',
  tipoRelatorioSelect: 'pesq[tprel]',
  tipoRelatorioAlvoTexto: 'CSV',
  limitarOptionsDiagnostico: 0,
  salvarCsvComo: 'processos-export.csv',
  salvarJsonComo: 'processos-export.json',
  fallbackDataTable: true,
  diagnosticoListarCampos: true,
  extraDownloadDelayMs: process.env.EXTRA_DOWNLOAD_DELAY
    ? parseInt(process.env.EXTRA_DOWNLOAD_DELAY, 10)
    : 0,
  webhook: {
    enabled: true,
    url: process.env.WEBHOOK_URL || 'https://ia-n8n.a8fvaf.easypanel.host/webhook-test/vios',
    mode: 'multipart',
    includeParsedJson: true,
    extraFields: { origem: 'processos', formato: 'csv' }
  }
};

function ensureDir() {
  if (!fs.existsSync(reportsDir)) fs.mkdirSync(reportsDir, { recursive: true });
  if (!fs.existsSync(locksDir)) fs.mkdirSync(locksDir, { recursive: true });
}
function ts() { return `[${new Date().toISOString()}]`; }

function makeLogger(logs, jobContext) {
  return (msg) => {
    const line = `${ts()} ${msg}`;
    logs.push(line);
    if (jobContext?.logger) {
      try { jobContext.logger(line); } catch(_) {}
    } else {
      console.log(line);
    }
  };
}

/* =========================
   DATAS
   ========================= */
function agoraSP() {
  const fmt = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/Sao_Paulo',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit'
  });
  const parts = fmt.formatToParts(new Date()).reduce((acc, p) => { acc[p.type] = p.value; return acc; }, {});
  return new Date(`${parts.year}-${parts.month}-${parts.day}T00:00:00-03:00`);
}

function formatDateBR(date) {
  const dd = String(date.getDate()).padStart(2,'0');
  const mm = String(date.getMonth()+1).padStart(2,'0');
  const yyyy = date.getFullYear();
  return `${dd}/${mm}/${yyyy}`;
}

function parseBRtoISO(br) {
  // dd/MM/yyyy -> yyyy-MM-dd
  const [d,m,y] = br.split('/');
  return `${y}-${m}-${d}`;
}

function calcRangeYesterday7() {
  const base = agoraSP();
  const fim = new Date(base);
  fim.setDate(fim.getDate() - 1);
  const inicio = new Date(fim);
  inicio.setDate(inicio.getDate() - 6);
  return { inicio: formatDateBR(inicio), fim: formatDateBR(fim), mode: 'YESTERDAY_7' };
}

function calcRangeLastFullWeek() {
  const today = agoraSP();
  const dow = today.getDay(); // 0=Dom
  const offsetToMonday = dow === 0 ? -6 : (1 - dow);
  const mondayCurrentWeek = new Date(today);
  mondayCurrentWeek.setDate(mondayCurrentWeek.getDate() + offsetToMonday);
  const mondayLastWeek = new Date(mondayCurrentWeek);
  mondayLastWeek.setDate(mondayLastWeek.getDate() - 7);
  const sundayLastWeek = new Date(mondayLastWeek);
  sundayLastWeek.setDate(sundayLastWeek.getDate() + 6);
  return { inicio: formatDateBR(mondayLastWeek), fim: formatDateBR(sundayLastWeek), mode: 'LAST_FULL_WEEK' };
}

function resolveInterval(cfg, override) {
  if (override?.inicio && override?.fim) {
    return { inicio: override.inicio, fim: override.fim, mode: 'OVERRIDE' };
  }
  if (cfg.dateRangeMode === 'YESTERDAY_7') return calcRangeYesterday7();
  return calcRangeLastFullWeek();
}

function hojeDDMMYYYY() {
  const d=new Date();
  return `${String(d.getDate()).padStart(2,'0')}/${String(d.getMonth()+1).padStart(2,'0')}/${d.getFullYear()}`;
}

/* =========================
   FUNÇÕES DE PÁGINA
   (mantidas do seu código original)
   ========================= */
async function listarCampos(page, cfg, log) { /* ... igual ao seu ... */ 
  if (!cfg.diagnosticoListarCampos) return;
  const info = await page.evaluate(() => {
    function attr(el, name) { return el.getAttribute(name) || ''; }
    const inputs = Array.from(document.querySelectorAll('input')).map(el => ({
      tag: el.tagName,
      type: attr(el,'type'),
      name: attr(el,'name'),
      id: attr(el,'id'),
      placeholder: attr(el,'placeholder'),
      value: el.value || '',
      classes: attr(el,'class')
    }));
    const selects = Array.from(document.querySelectorAll('select')).map(el => ({
      tag: el.tagName,
      name: attr(el,'name'),
      id: attr(el,'id'),
      classes: attr(el,'class'),
      options: Array.from(el.querySelectorAll('option')).map(o => ({
        value: attr(o,'value'),
        text: (o.textContent||'').trim()
      }))
    }));
    const radios = Array.from(document.querySelectorAll('input[type=radio],input[type=checkbox]')).map(el => {
      let labelTxt = '';
      const id = el.getAttribute('id');
      if (id) {
        const lab = document.querySelector(`label[for="${id}"]`);
          if (lab) labelTxt = lab.textContent || '';
      }
      if (!labelTxt) {
        const parentLab = el.closest('label');
        if (parentLab) labelTxt = parentLab.textContent || '';
      }
      return {
        type: el.getAttribute('type'),
        name: el.getAttribute('name'),
        id,
        label: (labelTxt||'').trim().replace(/\s+/g,' ')
      };
    });
    return { inputs, selects, radios };
  });
  log('DIAGNOSTICO LISTA_INPUTS = ' + info.inputs.length);
  info.inputs.forEach(i => {
    log(`INPUT name="${i.name}" id="${i.id}" type="${i.type}" placeholder="${i.placeholder}" val="${i.value}" class="${i.classes}"`);
  });
  log('DIAGNOSTICO LISTA_SELECTS = ' + info.selects.length);
  info.selects.forEach(s => {
    log(`SELECT name="${s.name}" id="${s.id}" class="${s.classes}" options=${s.options.length}`);
  });
  log('DIAGNOSTICO LISTA_RADIO_CHECK = ' + info.radios.length);
  return info;
}

async function setInputDate(page, selector, value) { /* ... */ 
  const el = page.locator(selector);
  await el.click({ clickCount:3 }).catch(()=>{});
  await el.fill('');
  await el.type(value, { delay:25 });
  await page.evaluate((sel) => {
    const i=document.querySelector(sel);
    if (i) {
      i.dispatchEvent(new Event('input',{bubbles:true}));
      i.dispatchEvent(new Event('change',{bubbles:true}));
      i.dispatchEvent(new Event('blur',{bubbles:true}));
    }
  }, selector);
}

async function preencherDatas(page, cfg, log) { /* ... igual ... */
  const candidatosInicio = [
    'input[name="pesq[idata]"]',
    'input[name="pesq[data_de]"]',
    'input[name="pesq[data_ini]"]',
    'input[name="pesq[data_inicio]"]'
  ];
  const candidatosFim = [
    'input[name="pesq[fdata]"]',
    'input[name="pesq[data_ate]"]',
    'input[name="pesq[data_fim]"]',
    'input[name="pesq[data_final]"]'
  ];
  const dataIni = cfg.dataInicio;
  const dataFim = cfg.dataFim || hojeDDMMYYYY();

  let usadoInicio=null, usadoFim=null;

  for (const sel of candidatosInicio) {
    if (await page.$(sel)) { await setInputDate(page, sel, dataIni); usadoInicio=sel; break; }
  }
  for (const sel of candidatosFim) {
    if (await page.$(sel)) { await setInputDate(page, sel, dataFim); usadoFim=sel; break; }
  }

  if (!usadoInicio && !usadoFim) {
    const result = await page.evaluate(({ dIni, dFim }) => {
      const poss = Array.from(document.querySelectorAll('input[type="text"]'))
        .filter(i => /data|dt/i.test(i.name||'') || /vios-mascara-data/.test(i.className||''));
      if (poss.length === 1) {
        poss[0].value = dIni;
        poss[0].dispatchEvent(new Event('input',{bubbles:true}));
        poss[0].dispatchEvent(new Event('change',{bubbles:true}));
        return { unico:true,inicio:true,fim:false };
      } else if (poss.length >= 2) {
        poss[0].value = dIni;
        poss[0].dispatchEvent(new Event('input',{bubbles:true}));
        poss[0].dispatchEvent(new Event('change',{bubbles:true}));
        poss[1].value = dFim;
        poss[1].dispatchEvent(new Event('input',{bubbles:true}));
        poss[1].dispatchEvent(new Event('change',{bubbles:true}));
        return { unico:false,inicio:true,fim:true };
      }
      return { unico:false,inicio:false,fim:false };
    }, { dIni: dataIni, dFim: dataFim });
    log(`Heurística fallback datas: inicio=${result.inicio} fim=${result.fim} unico=${result.unico}`);
  } else {
    log(`Datas preenchidas. InicioSel=${usadoInicio||'NA'} FimSel=${usadoFim||'NA'}`);
  }
}

async function selecionarTipoRelatorioCSV(page, cfg, log) { /* ... igual ... */ 
  const alvoTexto = (cfg.tipoRelatorioAlvoTexto||'CSV').toLowerCase();
  const r = await page.evaluate(({ selectName, alvoTexto }) => {
    const sel = document.querySelector(`select[name="${selectName}"]`);
    if (!sel) return { encontrado:false };
    const options = Array.from(sel.querySelectorAll('option'));
    let opt =
      options.find(o => (o.textContent||'').trim().toLowerCase() === alvoTexto) ||
      options.find(o => (o.value||'').trim().toLowerCase() === alvoTexto) ||
      options.find(o => (o.textContent||'').toLowerCase().includes(alvoTexto)) ||
      options.find(o => (o.value||'').toLowerCase().includes(alvoTexto));
    if (!opt) return { encontrado:true, aplicado:false, alvo:alvoTexto, disponiveis: options.map(o=>({ value:o.value, text:(o.textContent||'').trim() })) };
    sel.value = opt.value;
    sel.dispatchEvent(new Event('change',{bubbles:true}));
    return {
      encontrado:true,
      aplicado:true,
      value: opt.value,
      text: (opt.textContent||'').trim()
    };
  }, { selectName: cfg.tipoRelatorioSelect, alvoTexto });
  if (!r.encontrado) log(`TipoRel CSV: select ${cfg.tipoRelatorioSelect} não encontrado.`);
  else if (!r.aplicado) log(`TipoRel CSV: não encontrou option contendo "${cfg.tipoRelatorioAlvoTexto}".`);
  else log(`TipoRel CSV aplicado -> value="${r.value}" text="${r.text}"`);
}

async function selecionarLimit(page, cfg, log) { /* ... igual ... */ 
  const res = await page.evaluate(({ selName, prefer }) => {
    const sel = document.querySelector(`select[name="${selName}"]`);
    if (!sel) return { encontrado:false };
    const options = Array.from(sel.querySelectorAll('option')).map(o => ({
      value: o.value,
      text: (o.textContent||'').trim()
    }));
    let alvo = options.find(o => o.value === prefer || o.text === prefer);
    if (!alvo) {
      const nums = options.map(o => {
        const n = parseInt(o.value || o.text.replace(/\D+/g,''),10);
        return isNaN(n)?null:{ o, n };
      }).filter(Boolean).sort((a,b)=>b.n - a.n);
      if (nums.length) alvo = nums[0].o;
    }
    if (alvo) {
      sel.value = alvo.value;
      sel.dispatchEvent(new Event('change',{bubbles:true}));
      return { encontrado:true, aplicado:true, value:alvo.value };
    }
    return { encontrado:true, aplicado:false };
  }, { selName: cfg.selectLimitName, prefer: cfg.valorMostrarMax });
  if (!res.encontrado) log(`LIMIT: select ${cfg.selectLimitName} não encontrado.`);
  else if (!res.aplicado) log(`LIMIT: não foi possível aplicar valor preferido (${cfg.valorMostrarMax}).`);
  else log(`LIMIT: aplicado value=${res.value}`);
}

async function clicarPesquisar(page, log) { /* ... igual ... */ 
  const seletores = [
    'button:has-text("Pesquisar")',
    'input[type="submit"][value*="Pesquisar"]',
    'input[type="button"][value*="Pesquisar"]',
    'a:has-text("Pesquisar")',
    '#btnPesquisar',
    'button[name="pesquisar"]'
  ];
  for (const sel of seletores) {
    const el = page.locator(sel);
    if (await el.count() > 0) {
      log(`Clique Pesquisar via ${sel}`);
      await Promise.all([
        el.first().click().catch(()=>{}),
        page.waitForTimeout(300)
      ]);
      return true;
    }
  }
  log('Botão Pesquisar não encontrado.');
  return false;
}

async function esperarLinkCsv(page, cfg, log) { /* ... */ 
  try {
    await page.waitForFunction(() =>
      !!document.querySelector('#btnGeraRel a[href*="download.php"]')
    , { timeout: cfg.maxCsvWaitMs });
    log('Link CSV detectado.');
    return true;
  } catch {
    log('Timeout aguardando link CSV.');
    return false;
  }
}

async function extrairHrefCsv(page) { /* ... */ 
  return await page.evaluate(() => {
    const a = document.querySelector('#btnGeraRel a[href*="download.php"]');
    if (!a) return null;
    return {
      href: a.getAttribute('href'),
      title: a.getAttribute('title') || '',
      outer: a.outerHTML
    };
  });
}

async function baixarCsv(page, href, cfg, log) { /* ... igual ... */ 
  try {
    if (!href) return null;
    let finalHref = href;
    if (!/^https?:/i.test(finalHref)) {
      finalHref = finalHref.replace(/^\.\//,'/');
      const base = cfg.baseUrl.endsWith('/') ? cfg.baseUrl.slice(0,-1) : cfg.baseUrl;
      if (!finalHref.startsWith('/')) finalHref = '/' + finalHref;
      finalHref = base + finalHref;
    }
    log(`Baixando CSV de: ${finalHref}`);
    const resp = await page.request.get(finalHref);
    if (!resp.ok()) {
      log(`Falha download CSV status=${resp.status()}`);
      return null;
    }
    const body = await resp.body();
    const outPath = path.join(reportsDir, cfg.salvarCsvComo);
    fs.writeFileSync(outPath, body);
    log(`CSV salvo (${body.length} bytes) -> ${cfg.salvarCsvComo}`);
    return outPath;
  } catch(e) {
    log(`Erro download CSV: ${e.message}`);
    return null;
  }
}

function csvParaJson(conteudo) { /* ... igual ... */ 
  const linhas = conteudo.split(/\r?\n/).filter(l => l.trim().length);
  if (!linhas.length) return { headers:[], rows:[] };
  const parseLinha = (l) => {
    const cols=[]; let atual='', dentro=false;
    for (let i=0;i<l.length;i++) {
      const c=l[i];
      if (c === '"') {
        if (dentro && l[i+1] === '"') { atual+='"'; i++; }
        else dentro = !dentro;
      } else if (c === ';' && !dentro) {
        cols.push(atual); atual='';
      } else {
        atual += c;
      }
    }
    cols.push(atual);
    return cols.map(c=>c.trim());
  };
  const headers = parseLinha(linhas[0]);
  const rows = linhas.slice(1).map(l => {
    const cols = parseLinha(l);
    const o={};
    headers.forEach((h,i)=>o[h]=cols[i]||'');
    return o;
  });
  return { headers, rows };
}

async function fallbackDataTableExtracao(page, log) { /* ... */ 
  const dt = await page.evaluate(() => {
    const t = document.querySelector('#processos-lista');
    if (!t) return null;
    const headers = Array.from(t.querySelectorAll('thead tr th')).map(th => (th.innerText||'').trim());
    const bodyRows = Array.from(t.querySelectorAll('tbody tr')).map(tr => {
      return Array.from(tr.querySelectorAll('td')).map(td => (td.innerText||'').trim());
    }).filter(r => r.join('').trim().length);
    return { headers, rows: bodyRows };
  });
  if (dt) {
    log(`Fallback DataTable: ${dt.rows.length} linhas.`);
  } else {
    log('Nenhuma tabela principal encontrada no fallback.');
  }
  return dt;
}

async function enviarCsvWebhook(page, csvPath, parsed, cfg, log) { /* ... igual ... */ 
  if (!cfg.webhook.enabled) {
    log('Webhook desabilitado.');
    return;
  }
  try {
    const webhookUrl = cfg.webhook.url;
    if (!webhookUrl) {
      log('Webhook: URL não definida.');
      return;
    }
    const stats = fs.statSync(csvPath);
    const fileBuf = fs.readFileSync(csvPath);
    const fileName = path.basename(csvPath);

    if (cfg.webhook.mode === 'multipart') {
      const multipart = {
        file: { name: fileName, mimeType: 'text/csv', buffer: fileBuf },
        filename: fileName,
        size: String(stats.size),
        ...cfg.webhook.extraFields
      };
      const resp = await page.request.post(webhookUrl, { multipart });
      const txt = await resp.text();
      log(`Webhook multipart status=${resp.status()} lenResp=${txt.length}`);
      if (resp.status() >= 400) {
        log(`Webhook ERRO corpo="${txt.slice(0,300)}"`);
      }
    } else {
      const payload = {
        filename: fileName,
        size: stats.size,
        encoding: 'base64',
        data: fileBuf.toString('base64'),
        ...cfg.webhook.extraFields
      };
      if (cfg.webhook.includeParsedJson && parsed) {
        payload.parsed = {
          headers: parsed.headers,
            rows: parsed.rows
        };
      }
      const resp = await page.request.post(webhookUrl, {
        data: JSON.stringify(payload),
        headers: { 'Content-Type': 'application/json' }
      });
      const txt = await resp.text();
      log(`Webhook json status=${resp.status()} lenResp=${txt.length}`);
      if (resp.status() >= 400) {
        log(`Webhook ERRO corpo="${txt.slice(0,300)}"`);
      }
    }
  } catch(e) {
    log(`Webhook falhou: ${e.message}`);
  }
}

/* =========================
   LOCK SIMPLES (Opcional)
   ========================= */
function acquireLock(log) {
  ensureDir();
  const lockFile = path.join(locksDir, 'processos.lock');
  if (fs.existsSync(lockFile)) {
    const stat = fs.statSync(lockFile);
    const ageMs = Date.now() - stat.mtimeMs;
    if (ageMs < 60 * 60 * 1000) { // <1h
      log('LOCK: Já existe execução em andamento (ou lock recente). Abortando.');
      return false;
    } else {
      log('LOCK: Arquivo antigo encontrado. Prosseguindo e substituindo.');
    }
  }
  fs.writeFileSync(lockFile, String(Date.now()));
  return true;
}
function releaseLock(log) {
  const lockFile = path.join(locksDir, 'processos.lock');
  if (fs.existsSync(lockFile)) {
    try { fs.unlinkSync(lockFile); log('LOCK: liberado.'); } catch(e) { log('LOCK: falha ao remover: '+e.message); }
  }
}

/* =========================
   FUNÇÃO PRINCIPAL
   ========================= */
export async function runViosProcessosJob(payload = {}, jobContext = {}) {
  const logs = [];
  const log = makeLogger(logs, jobContext);
  const startedAt = Date.now();

  const cfg = {
    ...DEFAULT_CONFIG,
    usuario: process.env.VIOS_USER,
    senha: process.env.VIOS_PASS
  };

  // Overrides
  [
    'usuario','senha','headless','dateRangeMode','waitAfterSearchMs','maxCsvWaitMs',
    'fallbackDataTable','diagnosticoListarCampos','tipoRelatorioAlvoTexto','selectLimitName'
  ].forEach(k => {
    if (payload[k] !== undefined) cfg[k] = payload[k];
  });

  if (payload.webhookEnabled !== undefined) cfg.webhook.enabled = payload.webhookEnabled;
  if (payload.webhookUrl) cfg.webhook.url = payload.webhookUrl;
  if (payload.webhookMode) cfg.webhook.mode = payload.webhookMode;
  if (payload.webhookExtra) cfg.webhook.extraFields = { ...cfg.webhook.extraFields, ...payload.webhookExtra };

  const exportCsv = payload.exportCsv !== undefined ? payload.exportCsv : true;
  const saveFiles = payload.saveFiles !== undefined ? payload.saveFiles : true;
  const dedup = payload.dedup || false;

  if (!cfg.usuario || !cfg.senha) {
    return {
      ok: false,
      error: 'Credenciais ausentes (VIOS_USER / VIOS_PASS ou payload.usuario/senha)',
      logs,
      summary: {},
      data: []
    };
  }

  const intervalo = resolveInterval(cfg, payload.intervalo);
  cfg.dataInicio = intervalo.inicio;
  cfg.dataFim = intervalo.fim;

  // ISO range (para histórico / consumo machine-friendly)
  const isoStart = parseBRtoISO(cfg.dataInicio);
  const isoEnd = parseBRtoISO(cfg.dataFim);

  const summary = {
    intervalo: { inicio: cfg.dataInicio, fim: cfg.dataFim, mode: intervalo.mode },
    range: { startDate: isoStart, endDate: isoEnd, mode: intervalo.mode },
    fonte: null,
    linhas: 0,
    files: []
  };

  const files = [];
  if (saveFiles) ensureDir();

  if (!acquireLock(log)) {
    return {
      ok: false,
      error: 'Execução bloqueada (lock em vigor).',
      summary,
      logs
    };
  }

  let browser;
  try {
    log('Iniciando navegador...');
    browser = await chromium.launch({
      headless: cfg.headless,
      args: ['--no-sandbox','--disable-dev-shm-usage','--disable-blink-features=AutomationControlled']
    });
    const context = await browser.newContext({
      viewport: { width:1600, height:950 },
      userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
      acceptDownloads: true
    });
    const page = await context.newPage();

    // LOGIN
    log('Acessando base...');
    await page.goto(cfg.baseUrl, { waitUntil:'domcontentloaded', timeout:60000 });
    await page.waitForSelector('input[name="form[usuario]"]',{timeout:20000});
    await page.fill('input[name="form[usuario]"]', cfg.usuario);
    await page.fill('input[name="form[senha]"]', cfg.senha);
    await Promise.all([
      page.waitForNavigation({ waitUntil:'domcontentloaded', timeout:45000 }).catch(()=>{}),
      page.click('input[type="submit"][name="Entrar"], input[name="Entrar"], button:has-text("Entrar")')
    ]);
    log('Login OK: ' + page.url());

    // NAVEGA
    const target='?pag=sys/processos/processos-lista.php&menu_lateral=true';
    await page.goto(`${cfg.baseUrl}/${target}`, { waitUntil:'domcontentloaded', timeout:60000 });
    log('Página de processos carregada.');

    if (cfg.diagnosticoListarCampos) {
      const diag = await listarCampos(page, cfg, log);
      if (saveFiles && diag) {
        const fname = 'diagnostico-campos.json';
        fs.writeFileSync(path.join(reportsDir,fname), JSON.stringify(diag,null,2));
        files.push(fname);
      }
    }

    await preencherDatas(page, cfg, log);
    await selecionarTipoRelatorioCSV(page, cfg, log);
    await selecionarLimit(page, cfg, log);

    await clicarPesquisar(page, log);
    if (cfg.waitAfterSearchMs) {
      log(`Aguardando pós-busca ${cfg.waitAfterSearchMs}ms`);
      await page.waitForTimeout(cfg.waitAfterSearchMs);
    }

    let data = [];
    let headers = [];
    let fonte = null;

    // CSV
    if (exportCsv) {
      const temCsv = await esperarLinkCsv(page, cfg, log);
      if (temCsv) {
        const meta = await extrairHrefCsv(page);
        if (meta) {
          log(`Link CSV meta href=${meta.href} title="${meta.title}"`);
          if (saveFiles) {
            fs.writeFileSync(path.join(reportsDir,'csv-link-outer.html'), meta.outer, 'utf8');
            files.push('csv-link-outer.html');
          }
            const arquivo = await baixarCsv(page, meta.href, cfg, log);
          if (arquivo) {
            const conteudo = fs.readFileSync(arquivo, 'utf8');
            const parsed = csvParaJson(conteudo);
            data = parsed.rows;
            headers = parsed.headers;
            fonte = 'csv';
            summary.linhas = data.length;
            if (saveFiles) {
              fs.writeFileSync(path.join(reportsDir,cfg.salvarJsonComo), JSON.stringify(parsed,null,2));
              files.push(cfg.salvarJsonComo);
            }
            // Webhook
            await enviarCsvWebhook(page, arquivo, parsed, cfg, log);
            if (saveFiles) {
              files.push(path.basename(arquivo));
            }
          } else {
            log('Falha no download CSV (arquivo nulo).');
          }
        } else {
          log('Meta CSV não encontrada embora link detectado.');
        }
      } else {
        log('CSV não apareceu dentro do timeout.');
      }
    } else {
      log('exportCsv=false (pulado fluxo CSV).');
    }

    // FALLBACK
    if ((!data.length) && cfg.fallbackDataTable) {
      log('Iniciando fallback DataTable...');
      const dt = await fallbackDataTableExtracao(page, log);
      if (dt) {
        headers = dt.headers;
        data = dt.rows.map(r => {
          const obj = {};
          dt.headers.forEach((h,i)=>obj[h]=r[i]||'');
          return obj;
        });
        fonte = 'datatable';
        summary.linhas = data.length;
        if (saveFiles) {
          fs.writeFileSync(path.join(reportsDir,'processos-fallback.json'), JSON.stringify({ headers:dt.headers, rows:data }, null, 2));
          files.push('processos-fallback.json');
        }
      }
    }

    // DEDUP
    if (dedup && data.length) {
      log('Aplicando deduplicação (chave: primeira coluna ou "numero"/"Número")');
      const seen = new Set();
      const out = [];
      for (const row of data) {
        const key = row.numero || row['Número'] || row[headers[0]] || JSON.stringify(row);
        if (!seen.has(key)) {
          seen.add(key);
          out.push(row);
        }
      }
      data = out;
      summary.linhas = data.length;
      log(`Após dedup: ${data.length} linhas`);
    }

    summary.fonte = fonte || 'nenhuma';

    // Screenshot
    if (saveFiles) {
      try {
        const shot = 'processos-lista-export.png';
        await page.screenshot({ path: path.join(reportsDir, shot), fullPage:true });
        files.push(shot);
      } catch(e) {
        log('Screenshot falhou: ' + e.message);
      }
    }

    // Log file
    if (saveFiles) {
      const logFile = 'processos-execucao.log';
      fs.writeFileSync(path.join(reportsDir, logFile), logs.join('\n'));
      files.push(logFile);
    }

    const durationMs = Date.now() - startedAt;
    summary.files = files;
    const result = {
      ok: true,
      summary,
      data,
      headers,
      files: saveFiles ? files : [],
      logs,
      finishedAt: new Date().toISOString(),
      durationMs
    };
    log(`FINALIZADO. Fonte=${summary.fonte} Linhas=${summary.linhas} Duration=${durationMs}ms`);
    return result;
  } catch(e) {
    const durationMs = Date.now() - startedAt;
    const errMsg = e?.message || String(e);
    const failResult = {
      ok: false,
      error: errMsg,
      summary,
      data: [],
      headers: [],
      files: [],
      logs,
      finishedAt: new Date().toISOString(),
      durationMs
    };
    log('ERRO: ' + errMsg);
    return failResult;
  } finally {
    if (browser) {
      try { await browser.close(); } catch(_){}
    }
    releaseLock(log);
  }
}
