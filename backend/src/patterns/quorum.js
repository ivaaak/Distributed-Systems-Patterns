import { Router } from 'express';
import { INSTANCE_ID } from '../config.js';
import { redis } from '../redis.js';
import { lifecycle, rejectIfCrashed, sleep } from '../lifecycle.js';
import { emit } from '../events.js';
import { getInstanceMeta } from '../registry.js';

/**
 * Leaderless replication with tunable quorums (Dynamo / Cassandra style).
 *
 * Every instance holds a full replica of a small key-value store *in its own memory* (N =
 * number of registered instances). Any instance can coordinate a request:
 *
 *   write - stamp the value with a version (timestamp + node id, last-write-wins), send it to
 *           all N replicas in parallel and answer once W of them acknowledged. The rest still
 *           get it in the background; replicas that are down simply miss it.
 *   read  - ask all N replicas, answer with the newest version among the first R replies.
 *           With read repair on, stale replicas seen during the read get the newest version.
 *
 * If R + W > N every read quorum overlaps every write quorum, so a read always sees the
 * latest acknowledged write. If R + W <= N it can miss it (a stale read).
 * Anti-entropy is the background process that compares whole replicas and fixes them.
 */
const CONFIG_KEY = 'quorum:config';
const REPLICA_TIMEOUT_MS = 2500;
const SLOW_EXTRA_MS = 1200;

// ---- this instance's replica -------------------------------------------------------------
const store = new Map(); // key -> { value, ts, node }
let clock = 0;

const newer = (a, b) => Boolean(a) && (!b || a.ts > b.ts || (a.ts === b.ts && a.node > b.node));
const newest = (versions) => versions.reduce((best, v) => (newer(v, best) ? v : best), null);

async function getConfig() {
  const c = await redis.hgetall(CONFIG_KEY);
  return { w: Number(c.w ?? 2), r: Number(c.r ?? 2), readRepair: c.readRepair !== '0', slow: c.slow || null };
}

/** Simulated network + disk latency for one replica operation. */
async function replicaDelay() {
  const { slow } = await getConfig();
  await sleep(20 + Math.random() * 100 + (slow === INSTANCE_ID ? SLOW_EXTRA_MS : 0));
}

function applyLocal(key, version) {
  clock = Math.max(clock, version.ts);
  const applied = newer(version, store.get(key));
  if (applied) store.set(key, version);
  return applied;
}

// ---- coordinator -------------------------------------------------------------------------
async function getReplicas() {
  const meta = await getInstanceMeta();
  return Object.values(meta)
    .map((m) => ({ id: m.id, url: m.internalUrl ?? m.url }))
    .sort((a, b) => a.id.localeCompare(b.id));
}

async function callReplica(replica, method, key, body) {
  const started = Date.now();
  try {
    let data;
    if (replica.id === INSTANCE_ID) {
      await replicaDelay();
      if (lifecycle.crashed) throw new Error('crashed');
      data = method === 'PUT' ? { applied: applyLocal(key, body) } : { version: store.get(key) ?? null };
    } else {
      const r = await fetch(`${replica.url}/api/quorum/replica/${encodeURIComponent(key)}`, {
        method,
        headers: { 'content-type': 'application/json' },
        body: body ? JSON.stringify(body) : undefined,
        signal: AbortSignal.timeout(REPLICA_TIMEOUT_MS),
      });
      if (!r.ok) throw new Error(`status ${r.status}`);
      data = await r.json();
    }
    return { replica: replica.id, ok: true, ms: Date.now() - started, ...data };
  } catch (err) {
    return { replica: replica.id, ok: false, ms: Date.now() - started, error: err.message };
  }
}

/**
 * Resolve as soon as `need` calls succeeded (or can no longer succeed). `all` keeps
 * tracking the stragglers so callers can act on late replies.
 */
function quorum(calls, need) {
  const settled = [];
  let resolveEarly;
  const early = new Promise((r) => (resolveEarly = r));
  const check = () => {
    const oks = settled.filter((s) => s.ok).length;
    const fails = settled.length - oks;
    if (oks >= need || fails > calls.length - need) resolveEarly([...settled]);
  };
  const all = Promise.all(calls.map((p) => p.then((res) => { settled.push(res); check(); return res; })));
  if (need <= 0) resolveEarly([]);
  return { early, all };
}

const withPending = (replicas, settled) =>
  replicas.map((r) => settled.find((s) => s.replica === r.id) ?? { replica: r.id, pending: true });

export const quorumRouter = Router();

quorumRouter.put('/kv/:key', rejectIfCrashed, async (req, res) => {
  const { key } = req.params;
  const started = Date.now();
  const [cfg, replicas] = await Promise.all([getConfig(), getReplicas()]);
  const w = Math.min(cfg.w, replicas.length);
  clock = Math.max(Date.now(), clock + 1);
  const version = { value: String(req.body?.value ?? ''), ts: clock, node: INSTANCE_ID };

  const { early, all } = quorum(replicas.map((r) => callReplica(r, 'PUT', key, version)), w);
  const settled = await early;
  const acks = settled.filter((s) => s.ok).length;
  const ok = acks >= w;
  const body = { ok, key, version, w, n: replicas.length, acks, ms: Date.now() - started, replicas: withPending(replicas, settled), coordinator: INSTANCE_ID };
  emit(ok ? 'quorum.write' : 'quorum.write_failed', { key, value: version.value, w, acks, ms: body.ms });
  res.status(ok ? 200 : 503).json(ok ? body : { ...body, error: `only ${acks}/${w} replicas acknowledged` });

  all.then((results) => {
    const missed = results.filter((s) => !s.ok).map((s) => s.replica);
    if (missed.length) emit('quorum.replica_missed', { key, value: version.value, missed });
  });
});

quorumRouter.get('/kv/:key', rejectIfCrashed, async (req, res) => {
  const { key } = req.params;
  const started = Date.now();
  const [cfg, replicas] = await Promise.all([getConfig(), getReplicas()]);
  const r = Math.min(cfg.r, replicas.length);

  const { early, all } = quorum(replicas.map((rep) => callReplica(rep, 'GET', key)), r);
  const settled = await early;
  const replies = settled.filter((s) => s.ok);
  if (replies.length < r) {
    emit('quorum.read_failed', { key, r, replies: replies.length });
    return res.status(503).json({ error: `only ${replies.length}/${r} replicas answered`, key, r, replicas: withPending(replicas, settled) });
  }
  const chosen = newest(replies.map((s) => s.version).filter(Boolean));
  const body = { key, version: chosen, r, n: replicas.length, ms: Date.now() - started, replicas: withPending(replicas, settled), coordinator: INSTANCE_ID };
  emit('quorum.read', { key, value: chosen?.value ?? null, r, ms: body.ms });
  res.json(body);

  if (!cfg.readRepair) return;
  // Read repair: once every replica answered (or failed), push the newest version to stale ones.
  const results = await all;
  const latest = newest(results.filter((s) => s.ok).map((s) => s.version).filter(Boolean));
  const stale = results.filter((s) => s.ok && newer(latest, s.version));
  if (!stale.length) return;
  await Promise.all(stale.map((s) => callReplica(replicas.find((x) => x.id === s.replica), 'PUT', key, latest)));
  emit('quorum.read_repair', { key, value: latest.value, repaired: stale.map((s) => s.replica) });
});

/** Full-replica comparison: every live replica ends up with the newest version of every key. */
quorumRouter.post('/anti-entropy', rejectIfCrashed, async (req, res) => {
  const replicas = await getReplicas();
  const dumps = (await Promise.all(replicas.map((r) => fetchDump(r)))).filter((d) => d && !d.crashed);
  const keys = new Set(dumps.flatMap((d) => Object.keys(d.data)));
  const fixes = [];
  for (const key of keys) {
    const latest = newest(dumps.map((d) => d.data[key]).filter(Boolean));
    for (const d of dumps) if (newer(latest, d.data[key])) fixes.push({ replica: d.replica, key, latest });
  }
  await Promise.all(fixes.map((f) => callReplica(replicas.find((r) => r.id === f.replica), 'PUT', f.key, f.latest)));
  emit('quorum.anti_entropy', { repaired: fixes.length, replicas: dumps.map((d) => d.replica) });
  res.json({ repaired: fixes.map(({ replica, key }) => ({ replica, key })) });
});

// ---- replica endpoints (called by coordinators) --------------------------------------------
quorumRouter.get('/replica/:key', rejectIfCrashed, async (req, res) => {
  await replicaDelay();
  res.json({ version: store.get(req.params.key) ?? null });
});

quorumRouter.put('/replica/:key', rejectIfCrashed, async (req, res) => {
  await replicaDelay();
  res.json({ applied: applyLocal(req.params.key, req.body) });
});

/** Observer view of this replica (answers even when crashed, so the dashboard can show stale data). */
quorumRouter.get('/replica', (req, res) => {
  res.json({ replica: INSTANCE_ID, crashed: lifecycle.crashed, data: Object.fromEntries(store) });
});

quorumRouter.delete('/replica', (req, res) => {
  store.clear();
  res.json({ ok: true });
});

async function fetchDump(replica, method = 'GET') {
  if (replica.id === INSTANCE_ID) {
    if (method === 'DELETE') store.clear();
    return { replica: INSTANCE_ID, crashed: lifecycle.crashed, data: Object.fromEntries(store) };
  }
  try {
    const r = await fetch(`${replica.url}/api/quorum/replica`, { method, signal: AbortSignal.timeout(1500) });
    return await r.json();
  } catch {
    return null;
  }
}

// ---- dashboard -----------------------------------------------------------------------------
quorumRouter.get('/state', async (req, res) => {
  const [cfg, replicas] = await Promise.all([getConfig(), getReplicas()]);
  const dumps = await Promise.all(replicas.map((r) => fetchDump(r)));
  res.json({
    config: cfg,
    n: replicas.length,
    replicas: replicas.map((r, i) => ({ id: r.id, reachable: Boolean(dumps[i]), crashed: dumps[i]?.crashed ?? null, data: dumps[i]?.data ?? {} })),
  });
});

quorumRouter.post('/config', async (req, res) => {
  const b = req.body ?? {};
  const set = [];
  if (b.w !== undefined) set.push('w', Math.max(1, Number(b.w) || 1));
  if (b.r !== undefined) set.push('r', Math.max(1, Number(b.r) || 1));
  if (b.readRepair !== undefined) set.push('readRepair', b.readRepair ? '1' : '0');
  if (b.slow !== undefined) set.push('slow', b.slow || '');
  if (set.length) await redis.hset(CONFIG_KEY, ...set);
  const cfg = await getConfig();
  emit('quorum.config', cfg);
  res.json(cfg);
});

quorumRouter.post('/reset', async (req, res) => {
  const replicas = await getReplicas();
  await Promise.all(replicas.map((r) => fetchDump(r, 'DELETE')));
  emit('quorum.reset');
  res.json({ ok: true });
});
