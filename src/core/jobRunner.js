import { v4 as uuid } from 'uuid';

const state = {
  running: null,
  queue: [],
  history: [],
  logs: {},
  maxHistory: 30
};

function pushHistory(entry) {
  state.history.unshift(entry);
  if (state.history.length > state.maxHistory) state.history.pop();
}

export function getStatus() {
  return {
    running: state.running,
    queueLength: state.queue.length,
    lastResult: state.history[0] || null,
    history: state.history
  };
}

export function getLogs(jobId) {
  return state.logs[jobId] || [];
}

export function appendLog(jobId, line) {
  if (!state.logs[jobId]) state.logs[jobId] = [];
  const msg = `[${new Date().toISOString()}] ${line}`;
  state.logs[jobId].push(msg);
  if (state.logs[jobId].length > 5000) {
    state.logs[jobId].splice(0, state.logs[jobId].length - 5000);
  }
  return msg;
}

async function runNext() {
  if (state.running || state.queue.length === 0) return;
  const job = state.queue.shift();
  state.running = { id: job.id, type: job.type, startedAt: Date.now() };

  const finalize = (ok, error, summary) => {
    pushHistory({
      jobId: job.id,
      type: job.type,
      ok,
      error,
      finishedAt: Date.now(),
      startedAt: state.running.startedAt,
      durationMs: Date.now() - state.running.startedAt,
      summary
    });
    state.running = null;
    runNext();
  };

  try {
    const result = await job.execute(job.payload, job.id);
    job.resolve(result);
    finalize(result.ok !== false, result.error, result.summary || null);
  } catch (e) {
    job.reject(e);
    finalize(false, e.message, null);
  }
}

export function enqueueJob(type, payload, execute) {
  const id = uuid();
  return new Promise((resolve, reject) => {
    state.queue.push({ id, type, payload, execute, resolve, reject, enqueuedAt: Date.now() });
    runNext();
  });
}

export async function tryRunImmediate(type, payload, execute) {
  if (state.running || state.queue.length > 0) {
    return enqueueJob(type, payload, execute);
  }
  const id = uuid();
  state.running = { id, type, startedAt: Date.now() };
  try {
    const result = await execute(payload, id);
    pushHistory({
      jobId: id,
      type,
      ok: result.ok !== false,
      error: result.error,
      finishedAt: Date.now(),
      startedAt: state.running.startedAt,
      durationMs: Date.now() - state.running.startedAt,
      summary: result.summary || null
    });
    return result;
  } finally {
    state.running = null;
    runNext();
  }
}
