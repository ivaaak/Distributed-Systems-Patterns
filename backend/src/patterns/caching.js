import { randomUUID } from 'node:crypto';
import { Router } from 'express';
import { INSTANCE_ID } from '../config.js';
import { redis, publish, subscribe, DEL_IF_OWNER } from '../redis.js';
import { rejectIfCrashed, sleep } from '../lifecycle.js';
import { emit } from '../events.js';

/**
 * Two-level cache-aside in front of a slow "database".
 *
 *   L1 - in-process Map on each instance (fastest, but N independent copies)
 *   L2 - shared Redis cache (`cache:l2:<id>`)
 *   DB - Redis hash `db:products` behind an artificial 300ms latency
 *
 * Writes go to the DB and delete the cached copies. The hard part is L1 on the *other*
 * instances: without a broadcast they keep serving the old value until their TTL expires.
 * With invalidation, the writer PUBLISHes the key on `cache:invalidate` and every instance
 * drops its L1 entry.
 *
 * Stampede protection: when a hot key expires, N concurrent readers all miss and all hit
 * the DB. With `protect`, only the reader that wins `SET cache:fill:<id> NX` queries the DB;
 * the rest wait for L2 to be filled.
 */
const DB_KEY = 'db:products';
const DB_QUERIES = 'db:queries';
const DB_LATENCY_MS = 300;
const L1_TTL_MS = 60_000;
const L2_TTL_S = 60;
const INVALIDATE = 'cache:invalidate';
const l2Key = (id) => `cache:l2:${id}`;
const fillLock = (id) => `cache:fill:${id}`;

const SEED = {
  p1: { name: 'Mechanical keyboard', price: 89 },
  p2: { name: 'Wireless mouse', price: 29 },
  p3: { name: '4K monitor', price: 349 },
};

const l1 = new Map(); // id -> { value, cachedAt }

async function dbRead(id) {
  await redis.incr(DB_QUERIES);
  emit('cache.db_read', { id });
  await sleep(DB_LATENCY_MS);
  const raw = await redis.hget(DB_KEY, id);
  return raw ? JSON.parse(raw) : null;
}

async function readThrough(id, protect) {
  const cached = l1.get(id);
  if (cached && Date.now() - cached.cachedAt < L1_TTL_MS) return { value: cached.value, source: 'L1' };

  const fromL2 = async () => {
    const raw = await redis.get(l2Key(id));
    return raw ? JSON.parse(raw) : null;
  };

  let value = await fromL2();
  let source = 'L2';

  if (!value && protect) {
    const token = randomUUID();
    if ((await redis.set(fillLock(id), token, 'PX', 3000, 'NX')) === 'OK') {
      value = await dbRead(id);
      await redis.set(l2Key(id), JSON.stringify(value), 'EX', L2_TTL_S);
      await redis.eval(DEL_IF_OWNER, 1, fillLock(id), token);
      source = 'DB';
    } else {
      // Someone else is filling the cache: wait for it instead of piling onto the DB.
      const deadline = Date.now() + 3000;
      while (!value && Date.now() < deadline) {
        await sleep(20);
        value = await fromL2();
      }
      source = 'L2 (waited)';
    }
  }
  if (!value) {
    value = await dbRead(id);
    await redis.set(l2Key(id), JSON.stringify(value), 'EX', L2_TTL_S);
    source = 'DB';
  }

  l1.set(id, { value, cachedAt: Date.now() });
  return { value, source };
}

export async function startCaching() {
  if (!(await redis.exists(DB_KEY))) await seed();
  await subscribe(INVALIDATE, ({ ids, by }) => {
    const dropped = ids === '*' ? (l1.clear(), ['*']) : ids.filter((id) => l1.delete(id));
    if (by !== INSTANCE_ID && dropped.length) emit('cache.invalidated', { ids: dropped, by });
  });
}

async function seed() {
  const pipeline = redis.pipeline();
  Object.entries(SEED).forEach(([id, p]) => pipeline.hset(DB_KEY, id, JSON.stringify({ id, ...p, version: 1 })));
  Object.keys(SEED).forEach((id) => pipeline.del(l2Key(id)));
  pipeline.set(DB_QUERIES, 0);
  await pipeline.exec();
}

export const cacheRouter = Router();

cacheRouter.get('/products/:id', rejectIfCrashed, async (req, res) => {
  const started = Date.now();
  const { value, source } = await readThrough(req.params.id, req.query.protect === '1');
  const result = { instance: INSTANCE_ID, source, value, ms: Date.now() - started };
  emit('cache.read', { id: req.params.id, source, version: value?.version, ms: result.ms });
  res.json(result);
});

cacheRouter.put('/products/:id', rejectIfCrashed, async (req, res) => {
  const { id } = req.params;
  const { price, invalidation = 'none' } = req.body ?? {};
  const current = JSON.parse((await redis.hget(DB_KEY, id)) || 'null');
  if (!current) return res.status(404).json({ error: 'unknown product' });

  const next = { ...current, price: Number(price), version: current.version + 1 };
  await redis.hset(DB_KEY, id, JSON.stringify(next));
  await redis.del(l2Key(id)); // cache-aside: write the source of truth, then delete cached copies
  l1.delete(id);
  if (invalidation === 'pubsub') await publish(INVALIDATE, { ids: [id], by: INSTANCE_ID });

  emit('cache.write', { id, price: next.price, version: next.version, invalidation });
  res.json({ instance: INSTANCE_ID, value: next, invalidation });
});

/** This instance's L1 contents, so the UI can show every instance's view side by side. */
cacheRouter.get('/l1', (req, res) => {
  const now = Date.now();
  res.json({
    instance: INSTANCE_ID,
    entries: Object.fromEntries(
      [...l1].map(([id, { value, cachedAt }]) => [id, { ...value, ageMs: now - cachedAt, expiresInMs: L1_TTL_MS - (now - cachedAt) }])
    ),
  });
});

cacheRouter.get('/db', async (req, res) => {
  const [raw, queries, l2] = await Promise.all([
    redis.hgetall(DB_KEY),
    redis.get(DB_QUERIES),
    Promise.all(Object.keys(SEED).map((id) => redis.get(l2Key(id)))),
  ]);
  res.json({
    products: Object.fromEntries(Object.entries(raw).map(([id, v]) => [id, JSON.parse(v)])),
    l2: Object.fromEntries(Object.keys(SEED).map((id, i) => [id, l2[i] ? JSON.parse(l2[i]) : null])),
    queries: Number(queries || 0),
  });
});

/** Make a key cold everywhere (L1 on all instances + L2) to set up a stampede. */
cacheRouter.post('/expire/:id', async (req, res) => {
  await redis.del(l2Key(req.params.id));
  await publish(INVALIDATE, { ids: [req.params.id], by: INSTANCE_ID });
  l1.delete(req.params.id);
  emit('cache.expired', { id: req.params.id });
  res.json({ ok: true });
});

cacheRouter.post('/reset', async (req, res) => {
  await seed();
  await publish(INVALIDATE, { ids: '*', by: INSTANCE_ID });
  emit('cache.reset');
  res.json({ ok: true });
});
