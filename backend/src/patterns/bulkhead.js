import { Router } from 'express';
import { INSTANCE_ID } from '../config.js';
import { redis } from '../redis.js';
import { lifecycle, rejectIfCrashed, sleep } from '../lifecycle.js';
import { emit } from '../events.js';

/**
 * Bulkheads: isolate resource pools so one failing dependency can't sink the whole ship.
 *
 * Each instance serves two endpoints, and has a limited number of concurrent "workers"
 * (think thread pool, DB/HTTP connection pool):
 *
 *   /inventory       - calls a fast, healthy dependency
 *   /recommendations - calls a dependency whose latency you control
 *
 * Modes:
 *   shared   - both endpoints draw from one pool of SHARED_SIZE workers. When
 *              recommendations get slow, its requests hold every worker and inventory
 *              requests queue behind them until they time out, although inventory is fine.
 *   bulkhead - each endpoint has its own pool. The slow dependency exhausts only its own
 *              compartment and gets rejected fast; inventory keeps its capacity.
 *
 * A request that can't get a worker within QUEUE_WAIT_MS is rejected (503).
 *
 * The load generator runs inside each instance (not in the browser): browsers cap
 * connections per host at ~6, which would queue requests client-side and hide the effect.
 */
const CONFIG_KEY = 'bulkhead:config';
const SHARED_SIZE = 8;
const BULKHEAD_SIZES = { inventory: 5, recommendations: 3 };
const QUEUE_WAIT_MS = 500;
const INVENTORY_MS = 30;
const HISTORY_MS = 15_000;

class Pool {
  constructor(name, size) {
    this.name = name;
    this.size = size;
    this.inUse = 0;
    this.waiters = [];
    this.holders = { inventory: 0, recommendations: 0 }; // who holds the workers (for the dashboard)
  }

  acquire(maxWaitMs, tag) {
    if (this.inUse < this.size) {
      this.inUse++;
      this.holders[tag]++;
      return Promise.resolve(true);
    }
    return new Promise((resolve) => {
      const waiter = { resolve, tag };
      waiter.timer = setTimeout(() => {
        this.waiters.splice(this.waiters.indexOf(waiter), 1);
        resolve(false);
      }, maxWaitMs);
      this.waiters.push(waiter);
    });
  }

  release(tag) {
    this.holders[tag]--;
    const next = this.waiters.shift();
    if (next) {
      clearTimeout(next.timer);
      this.holders[next.tag]++;
      next.resolve(true); // hand the worker over directly
    } else {
      this.inUse--;
    }
  }

  view() {
    return { name: this.name, size: this.size, inUse: this.inUse, queued: this.waiters.length, holders: { ...this.holders } };
  }
}

let config = { mode: 'shared', recsLatencyMs: 80, fastRps: 0, slowRps: 0 };
let pools = buildPools('shared');
const history = []; // { at, endpoint, result, ms }
const saturated = new Set(); // pool names currently at capacity (for edge-triggered events)

function buildPools(mode) {
  if (mode === 'bulkhead') {
    return {
      inventory: new Pool('inventory', BULKHEAD_SIZES.inventory),
      recommendations: new Pool('recommendations', BULKHEAD_SIZES.recommendations),
    };
  }
  const shared = new Pool('shared', SHARED_SIZE);
  return { inventory: shared, recommendations: shared };
}

async function refreshConfig() {
  const c = await redis.hgetall(CONFIG_KEY);
  const next = {
    mode: c.mode === 'bulkhead' ? 'bulkhead' : 'shared',
    recsLatencyMs: Number(c.recsLatencyMs ?? 80),
    fastRps: Number(c.fastRps ?? 0),
    slowRps: Number(c.slowRps ?? 0),
  };
  if (next.mode !== config.mode) {
    pools = buildPools(next.mode); // in-flight requests release into the pool they came from
    saturated.clear();
  }
  config = next;
}

function trackSaturation(pool) {
  const full = pool.inUse >= pool.size && pool.waiters.length > 0;
  if (full && !saturated.has(pool.name)) {
    saturated.add(pool.name);
    emit('bulkhead.saturated', { pool: pool.name, size: pool.size, mode: config.mode });
  } else if (!full && pool.inUse < pool.size && saturated.delete(pool.name)) {
    emit('bulkhead.recovered', { pool: pool.name, mode: config.mode });
  }
}

/** Serve one request for `endpoint` through its pool. */
async function handle(endpoint) {
  const started = Date.now();
  const pool = pools[endpoint];
  const got = await pool.acquire(QUEUE_WAIT_MS, endpoint);
  trackSaturation(pool);
  let result = 'rejected';
  if (got) {
    try {
      await sleep(endpoint === 'inventory' ? INVENTORY_MS + Math.random() * 20 : config.recsLatencyMs * (0.8 + Math.random() * 0.4));
      result = 'ok';
    } finally {
      pool.release(endpoint);
      trackSaturation(pool);
    }
  }
  const entry = { at: Date.now(), endpoint, result, ms: Date.now() - started };
  history.push(entry);
  while (history.length && history[0].at < Date.now() - HISTORY_MS) history.shift();
  return entry;
}

// ---- in-process load generator -----------------------------------------------------------
const credit = { inventory: 0, recommendations: 0 };
let lastTick = Date.now();

function loadTick() {
  const now = Date.now();
  const dt = (now - lastTick) / 1000;
  lastTick = now;
  if (lifecycle.crashed) return;
  credit.inventory += config.fastRps * dt;
  credit.recommendations += config.slowRps * dt;
  for (const endpoint of Object.keys(credit)) {
    while (credit[endpoint] >= 1) {
      credit[endpoint] -= 1;
      handle(endpoint).catch(() => {});
    }
  }
}

export async function startBulkhead() {
  await refreshConfig();
  setInterval(() => refreshConfig().catch(() => {}), 500);
  setInterval(loadTick, 50);
}

export const bulkheadRouter = Router();

bulkheadRouter.get('/call/:endpoint', rejectIfCrashed, async (req, res) => {
  const { endpoint } = req.params;
  if (!pools[endpoint]) return res.status(404).json({ error: 'unknown endpoint' });
  const entry = await handle(endpoint);
  res.status(entry.result === 'ok' ? 200 : 503).json({ ...entry, instance: INSTANCE_ID, mode: config.mode });
});

bulkheadRouter.get('/state', (req, res) => {
  const unique = [...new Set(Object.values(pools))];
  res.json({
    instance: INSTANCE_ID,
    crashed: lifecycle.crashed,
    config,
    pools: unique.map((p) => p.view()),
    history: history.filter((h) => h.at > Date.now() - HISTORY_MS),
    limits: { sharedSize: SHARED_SIZE, bulkheadSizes: BULKHEAD_SIZES, queueWaitMs: QUEUE_WAIT_MS },
    now: Date.now(),
  });
});

bulkheadRouter.post('/config', async (req, res) => {
  const b = req.body ?? {};
  const set = [];
  if (b.mode !== undefined) set.push('mode', b.mode === 'bulkhead' ? 'bulkhead' : 'shared');
  if (b.recsLatencyMs !== undefined) set.push('recsLatencyMs', Math.min(Math.max(Number(b.recsLatencyMs) || 0, 10), 5000));
  if (b.fastRps !== undefined) set.push('fastRps', Math.min(Math.max(Number(b.fastRps) || 0, 0), 30));
  if (b.slowRps !== undefined) set.push('slowRps', Math.min(Math.max(Number(b.slowRps) || 0, 0), 30));
  if (set.length) await redis.hset(CONFIG_KEY, ...set);
  await refreshConfig();
  emit('bulkhead.config', config);
  res.json(config);
});
