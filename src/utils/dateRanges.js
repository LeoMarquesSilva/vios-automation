// src/utils/dateRanges.js
// Gera intervalos de datas conforme "dateRangeMode".
// Ajuste esta lógica para refletir necessidade real.

function toISODate(d) {
  return d.toISOString().slice(0, 10);
}

export function computeDateRange(mode = 'LAST_FULL_WEEK') {
  const today = new Date(); // data atual (TZ conforme container)
  // Normalizamos para meia-noite local (simplificado)
  const base = new Date(today.getFullYear(), today.getMonth(), today.getDate());

  if (mode === 'LAST_FULL_WEEK') {
    // Semana cheia anterior (segunda a domingo) considerando segunda=1
    const day = base.getDay(); // 0=Dom
    // Offset até segunda desta semana
    const offsetToMonday = (day === 0 ? 6 : day - 1);
    const thisMonday = new Date(base);
    thisMonday.setDate(base.getDate() - offsetToMonday);
    const lastMonday = new Date(thisMonday);
    lastMonday.setDate(thisMonday.getDate() - 7);
    const lastSunday = new Date(lastMonday);
    lastSunday.setDate(lastMonday.getDate() + 6);
    return {
      mode,
      startDate: toISODate(lastMonday),
      endDate: toISODate(lastSunday)
    };
  }

  if (mode === 'YESTERDAY_7') {
    // Últimos 7 dias até ontem (7 dias completos)
    const yesterday = new Date(base);
    yesterday.setDate(base.getDate() - 1);
    const start = new Date(yesterday);
    start.setDate(yesterday.getDate() - 6);
    return {
      mode,
      startDate: toISODate(start),
      endDate: toISODate(yesterday)
    };
  }

  // Default fallback: ontem
  const yesterday = new Date(base);
  yesterday.setDate(base.getDate() - 1);
  return {
    mode: 'YESTERDAY',
    startDate: toISODate(yesterday),
    endDate: toISODate(yesterday)
  };
}
