import { randomUUID } from 'node:crypto';
import { Router } from 'express';
import { INSTANCE_ID } from '../config.js';
import { redis, publish, subscribe } from '../redis.js';
import { rejectIfCrashed } from '../lifecycle.js';
import { emit } from '../events.js';

/**
 * Sliding-window rate limiting: LIMIT requests per WINDOW_MS per key.
 *
 *   local  - each instance keeps its own in-memory window. Behind a load balancer with N
 *            instances a client effectively gets N x LIMIT.
 *   global - one sorted set in Redis, updated atomically by a Lua script, so the limit
 *            holds no matter which instance serves the request. Uses Redis' clock (TIME)
 *            so instances with skewed clocks still agree.
 */
export const LIMIT = 10;
export const WINDOW_MS = 10_000;
const RESET_CHANNEL = 'ratelimit:reset';

const SLIDING_WINDOW = `
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local window = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, now - window)
local count = redis.call('ZCARD', KEYS[1])
if count < limit then
  redis.call('ZADD', KEYS[1], now, ARGV[3])
  redis.call('PEXPIRE', KEYS[1], window)
  return {1, count + 1}
end
return {0, count}`;

const localWindows = new Map(); // key -> number[] timestamps

function hitLocal(key) {
  const now = Date.now();
  const hits = (localWindows.get(key) ?? []).filter((t) => t > now - WINDOW_MS);
  const allowed = hits.length < LIMIT;
  if (allowed) hits.push(now);
  localWindows.set(key, hits);
  return { allowed, count: hits.length };
}

async function hitGlobal(key) {
  const [allowed, count] = await redis.eval(
    SLIDING_WINDOW,
    1,
    `ratelimit:${key}`,
    WINDOW_MS,
    LIMIT,
    `${INSTANCE_ID}:${randomUUID()}`
  );
  return { allowed: allowed === 1, count };
}

export async function startRateLimiter() {
  await subscribe(RESET_CHANNEL, () => localWindows.clear());
}

export const rateLimitRouter = Router();

rateLimitRouter.post('/hit', rejectIfCrashed, async (req, res) => {
  const { mode = 'global', key = 'demo-user', reqId } = req.body ?? {};
  const { allowed, count } = mode === 'local' ? hitLocal(key) : await hitGlobal(key);
  const result = { reqId, instance: INSTANCE_ID, mode, allowed, count, limit: LIMIT, windowMs: WINDOW_MS, ts: Date.now() };
  emit('ratelimit.hit', result);
  res.status(allowed ? 200 : 429).json(result);
});

rateLimitRouter.post('/reset', async (req, res) => {
  const keys = await redis.keys('ratelimit:*');
  if (keys.length) await redis.del(...keys);
  await publish(RESET_CHANNEL, { by: INSTANCE_ID });
  emit('ratelimit.reset');
  res.json({ ok: true });
});

rateLimitRouter.get('/config', (req, res) => res.json({ limit: LIMIT, windowMs: WINDOW_MS }));
