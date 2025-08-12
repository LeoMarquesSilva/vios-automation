// src/core/history.js
import fs from 'fs';
import path from 'path';

function resolveBaseDir() {
  return process.env.REPORTS_DIR || process.cwd();
}

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

export function appendHistory(entry) {
  try {
    const base = resolveBaseDir();
    const logsDir = path.join(base, 'logs');
    ensureDir(logsDir);
    const file = path.join(logsDir, 'job-history.jsonl');
    const enriched = {
      ...entry,
      ts: new Date().toISOString()
    };
    fs.appendFileSync(file, JSON.stringify(enriched) + '\n');
  } catch (e) {
    console.error('[history] Falha ao gravar histórico', e);
  }
}

export function readHistory({ limit = 100 } = {}) {
  try {
    const base = resolveBaseDir();
    const file = path.join(base, 'logs', 'job-history.jsonl');
    if (!fs.existsSync(file)) return [];
    const lines = fs.readFileSync(file, 'utf-8')
      .trim()
      .split('\n')
      .filter(Boolean);
    const slice = lines.slice(-limit);
    return slice
      .map(l => { try { return JSON.parse(l); } catch { return null; } })
      .filter(Boolean)
      .reverse(); // mais recentes primeiro
  } catch (e) {
    console.error('[history] Erro ao ler histórico', e);
    return [];
  }
}

export function purgeOldHistory({ days = 90 } = {}) {
  // Opcional: remover linhas antigas (não obrigatório)
  try {
    const cutoff = Date.now() - days * 86400_000;
    const base = resolveBaseDir();
    const file = path.join(base, 'logs', 'job-history.jsonl');
    if (!fs.existsSync(file)) return;
    const lines = fs.readFileSync(file, 'utf-8').split('\n').filter(Boolean);
    const kept = [];
    for (const line of lines) {
      try {
        const obj = JSON.parse(line);
        if (!obj.ts || Date.parse(obj.ts) >= cutoff) kept.push(line);
      } catch {
        // ignora linha inválida
      }
    }
    fs.writeFileSync(file, kept.join('\n') + (kept.length ? '\n' : ''));
  } catch (e) {
    console.error('[history] Erro purge:', e);
  }
}
