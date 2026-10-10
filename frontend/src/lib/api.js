export const SEED_URLS = (
  import.meta.env.VITE_SEED_URLS || 'http://localhost:4001,http://localhost:4002,http://localhost:4003'
)
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean);

export const toWs = (url) => url.replace(/^http/, 'ws');

export async function call(baseUrl, path, body, { timeout = 4000, method } = {}) {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeout);
  try {
    const res = await fetch(`${baseUrl}${path}`, {
      method: method || (body !== undefined ? 'POST' : 'GET'),
      headers: body !== undefined ? { 'Content-Type': 'application/json' } : undefined,
      body: body !== undefined ? JSON.stringify(body) : undefined,
      signal: ctrl.signal,
      // Bypass the HTTP cache: Chrome serializes identical in-flight GETs behind a cache lock,
      // which would hide effects like cache stampedes.
      cache: 'no-store',
    });
    const data = await res.json().catch(() => ({}));
    return { ok: res.ok, status: res.status, data };
  } finally {
    clearTimeout(timer);
  }
}

const PALETTE = ['#38bdf8', '#c084fc', '#f472b6', '#34d399', '#fbbf24', '#fb923c', '#a3e635', '#f87171'];

export function instanceColor(id = '') {
  const n = Number(String(id).match(/(\d+)$/)?.[1] ?? 1);
  return PALETTE[(n - 1) % PALETTE.length];
}

export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
