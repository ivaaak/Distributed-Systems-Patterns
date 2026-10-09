import { INSTANCE_ID, PUBLIC_URL, INTERNAL_URL } from './config.js';
import { redis } from './redis.js';
import { lifecycle } from './lifecycle.js';
import { emit } from './events.js';

/**
 * Service discovery via heartbeats: each instance refreshes a key with a short TTL.
 * If it stops (crash, partition, GC pause) the key expires and peers consider it dead.
 */
const HEARTBEAT_MS = 1000;
const TTL_MS = 3000;
const META_KEY = 'cluster:instances';
const aliveKey = (id) => `cluster:alive:${id}`;

let statsProvider = () => ({});

export function setStatsProvider(fn) {
  statsProvider = fn;
}

async function heartbeat() {
  if (lifecycle.crashed) return;
  await redis.set(aliveKey(INSTANCE_ID), JSON.stringify(statsProvider()), 'PX', TTL_MS);
}

export async function startRegistry() {
  await redis.hset(
    META_KEY,
    INSTANCE_ID,
    JSON.stringify({ id: INSTANCE_ID, url: PUBLIC_URL, internalUrl: INTERNAL_URL, startedAt: Date.now() })
  );
  await heartbeat();
  emit('instance.started', { url: PUBLIC_URL });
  setInterval(() => heartbeat().catch(() => {}), HEARTBEAT_MS);
  lifecycle.on('recover', () => heartbeat().catch(() => {}));
}

export async function deregister() {
  await redis.del(aliveKey(INSTANCE_ID));
}

/** id -> { id, url, internalUrl, startedAt } for every registered instance (alive or not). */
export async function getInstanceMeta() {
  const raw = await redis.hgetall(META_KEY);
  return Object.fromEntries(Object.entries(raw).map(([id, json]) => [id, JSON.parse(json)]));
}

export async function getClusterSnapshot() {
  const meta = await redis.hgetall(META_KEY);
  const ids = Object.keys(meta).sort();
  const [alive, leaderId, leaderTtl, epoch] = await Promise.all([
    ids.length ? redis.mget(ids.map(aliveKey)) : [],
    redis.get('leader:lock'),
    redis.pttl('leader:lock'),
    redis.get('leader:epoch'),
  ]);
  return {
    servedBy: INSTANCE_ID,
    ts: Date.now(),
    heartbeatTtlMs: TTL_MS,
    leader: leaderId ? { id: leaderId, ttlMs: leaderTtl, epoch: Number(epoch) } : null,
    instances: ids.map((id, i) => ({
      ...JSON.parse(meta[id]),
      alive: Boolean(alive[i]),
      stats: alive[i] ? JSON.parse(alive[i]) : null,
      isLeader: id === leaderId,
    })),
  };
}

/** Remove instances that are no longer heartbeating from the member list. */
export async function forgetDeadInstances() {
  const ids = await redis.hkeys(META_KEY);
  if (!ids.length) return [];
  const alive = await redis.mget(ids.map(aliveKey));
  const dead = ids.filter((_, i) => !alive[i]);
  if (dead.length) await redis.hdel(META_KEY, ...dead);
  return dead;
}
