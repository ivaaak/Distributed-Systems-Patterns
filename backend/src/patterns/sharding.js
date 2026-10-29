import { createHash } from 'node:crypto';
import { Router } from 'express';
import { INSTANCE_ID } from '../config.js';
import { redis } from '../redis.js';
import { rejectIfCrashed } from '../lifecycle.js';
import { emit } from '../events.js';
import { getInstanceMeta } from '../registry.js';

/**
 * Sharding with consistent hashing + request routing.
 *
 * A key-value store is partitioned across the instances in the ring. Each instance
 * stores its partition in its own Redis hash (`shard:data:<id>`, think "its local disk").
 * Any instance can receive a request: it hashes the key, finds the owner, and either
 * serves it or forwards it to the owner over HTTP.
 *
 *   modulo - owner = members[hash % N]. Changing N remaps almost every key.
 *   ring   - each member owns VNODES points on a 32-bit ring; a key belongs to the first
 *            point clockwise from its hash. Adding/removing a member only remaps ~1/N keys.
 */
const VNODES = 32;
const STRATEGY_KEY = 'shard:strategy';
const MEMBERS_KEY = 'shard:members';
const dataKey = (id) => `shard:data:${id}`;

export const hash32 = (s) => createHash('md5').update(String(s)).digest().readUInt32BE(0);

export function buildRing(members) {
  const points = [];
  for (const instance of members) {
    for (let v = 0; v < VNODES; v++) points.push({ point: hash32(`${instance}#${v}`), instance, vnode: v });
  }
  return points.sort((a, b) => a.point - b.point);
}

function ownerOnRing(ring, h) {
  if (!ring.length) return null;
  let lo = 0;
  let hi = ring.length;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (ring[mid].point < h) lo = mid + 1;
    else hi = mid;
  }
  return ring[lo % ring.length].instance; // wrap around past the last point
}

const ownerByModulo = (members, h) => (members.length ? members[h % members.length] : null);

function ownerFn(strategy, members) {
  const sorted = [...members].sort();
  if (strategy === 'modulo') return (h) => ownerByModulo(sorted, h);
  const ring = buildRing(sorted);
  return (h) => ownerOnRing(ring, h);
}

async function getStrategy() {
  return (await redis.get(STRATEGY_KEY)) || 'ring';
}

async function getMembers() {
  const members = await redis.smembers(MEMBERS_KEY);
  if (members.length) return members.sort();
  return Object.keys(await getInstanceMeta()).sort(); // default: every registered instance
}

async function locate(key) {
  const [strategy, members] = await Promise.all([getStrategy(), getMembers()]);
  return { owner: ownerFn(strategy, members)(hash32(key)), strategy, members };
}

export const shardRouter = Router();

/** Routed read/write: serve locally if we own the key, otherwise forward to the owner. */
async function route(req, res, local) {
  const { key } = req.params;
  const forwardedBy = req.get('x-forwarded-by');
  const { owner, strategy } = await locate(key);

  if (!owner) return res.status(503).json({ error: 'ring is empty' });
  if (owner === INSTANCE_ID || forwardedBy) {
    const result = await local(key);
    emit('shard.served', { key, owner: INSTANCE_ID, forwardedBy, op: req.method, found: result.found });
    return res.json({ ...result, key, servedBy: INSTANCE_ID, owner, strategy, hops: forwardedBy ? [forwardedBy, INSTANCE_ID] : [INSTANCE_ID] });
  }

  const meta = (await getInstanceMeta())[owner];
  emit('shard.forwarded', { key, to: owner, op: req.method });
  try {
    const r = await fetch(`${meta.internalUrl ?? meta.url}/api/shard/kv/${encodeURIComponent(key)}`, {
      method: req.method,
      headers: { 'content-type': 'application/json', 'x-forwarded-by': INSTANCE_ID },
      body: req.method === 'GET' ? undefined : JSON.stringify(req.body ?? {}),
      signal: AbortSignal.timeout(2000),
    });
    const body = await r.json();
    res.status(r.status).json({ ...body, receivedBy: INSTANCE_ID });
  } catch (err) {
    emit('shard.owner_unreachable', { key, owner });
    res.status(502).json({ error: `owner ${owner} unreachable`, key, owner, receivedBy: INSTANCE_ID });
  }
}

shardRouter.get('/kv/:key', rejectIfCrashed, (req, res) =>
  route(req, res, async (key) => {
    const value = await redis.hget(dataKey(INSTANCE_ID), key);
    return { found: value !== null, value };
  })
);

shardRouter.put('/kv/:key', rejectIfCrashed, (req, res) =>
  route(req, res, async (key) => {
    await redis.hset(dataKey(INSTANCE_ID), key, String(req.body?.value ?? ''));
    return { found: true, value: req.body?.value, written: true };
  })
);

shardRouter.get('/state', async (req, res) => {
  const [strategy, members, meta] = await Promise.all([getStrategy(), getMembers(), getInstanceMeta()]);
  const registered = Object.keys(meta).sort();
  const shards = await Promise.all(registered.map((id) => redis.hkeys(dataKey(id))));
  const owner = ownerFn(strategy, members);

  const keys = [];
  registered.forEach((id, i) =>
    shards[i].forEach((key) => {
      const h = hash32(key);
      keys.push({ key, hash: h, storedOn: id, owner: owner(h) });
    })
  );
  keys.sort((a, b) => a.hash - b.hash);

  // "What if" we toggled each instance's membership: fraction of keys that would change owner.
  const moveFraction = (strat, from, to) => {
    if (!keys.length || !to.length) return null;
    const a = ownerFn(strat, from);
    const b = ownerFn(strat, to);
    return keys.filter((k) => a(k.hash) !== b(k.hash)).length / keys.length;
  };
  const whatIf = Object.fromEntries(
    registered.map((id) => {
      const next = members.includes(id) ? members.filter((m) => m !== id) : [...members, id];
      return [id, { action: members.includes(id) ? 'remove' : 'add', ring: moveFraction('ring', members, next), modulo: moveFraction('modulo', members, next) }];
    })
  );

  res.json({ strategy, members, registered, vnodes: VNODES, ring: buildRing(members), keys, whatIf });
});

shardRouter.post('/strategy', async (req, res) => {
  const strategy = req.body?.strategy === 'modulo' ? 'modulo' : 'ring';
  await redis.set(STRATEGY_KEY, strategy);
  emit('shard.strategy', { strategy });
  res.json({ strategy });
});

shardRouter.post('/members', async (req, res) => {
  const { instance, include } = req.body ?? {};
  const members = await getMembers();
  const next = include ? [...new Set([...members, instance])] : members.filter((m) => m !== instance);
  if (!next.length) return res.status(400).json({ error: 'ring needs at least one member' });
  await redis.multi().del(MEMBERS_KEY).sadd(MEMBERS_KEY, ...next).exec();
  emit(include ? 'shard.member_added' : 'shard.member_removed', { instance, members: next });
  res.json({ members: next });
});

/** Bulk-load demo keys directly into their owners' partitions. */
shardRouter.post('/seed', async (req, res) => {
  const count = Math.min(Number(req.body?.count) || 60, 300);
  const [strategy, members, meta] = await Promise.all([getStrategy(), getMembers(), getInstanceMeta()]);
  const ownerOf = ownerFn(strategy, members);
  const pipeline = redis.pipeline();
  Object.keys(meta).forEach((id) => pipeline.del(dataKey(id)));
  for (let i = 1; i <= count; i++) {
    const key = `user:${i}`;
    pipeline.hset(dataKey(ownerOf(hash32(key))), key, `User #${i}`);
  }
  await pipeline.exec();
  emit('shard.seeded', { count, strategy });
  res.json({ count });
});

/** Move every key that is stored on a node that no longer owns it. */
shardRouter.post('/rebalance', async (req, res) => {
  const [strategy, members, meta] = await Promise.all([getStrategy(), getMembers(), getInstanceMeta()]);
  const owner = ownerFn(strategy, members);
  const moves = [];
  for (const id of Object.keys(meta)) {
    const entries = await redis.hgetall(dataKey(id));
    for (const [key, value] of Object.entries(entries)) {
      const to = owner(hash32(key));
      if (to !== id) moves.push({ key, value, from: id, to });
    }
  }
  const pipeline = redis.pipeline();
  moves.forEach((m) => pipeline.hset(dataKey(m.to), m.key, m.value).hdel(dataKey(m.from), m.key));
  await pipeline.exec();
  emit('shard.rebalanced', { moved: moves.length, strategy });
  res.json({ moved: moves.length, moves: moves.map(({ key, from, to }) => ({ key, from, to })) });
});
