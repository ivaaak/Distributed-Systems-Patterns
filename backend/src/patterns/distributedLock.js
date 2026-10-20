import { randomUUID } from 'node:crypto';
import { Router } from 'express';
import { INSTANCE_ID } from '../config.js';
import { redis, DEL_IF_OWNER } from '../redis.js';
import { rejectIfCrashed, sleep } from '../lifecycle.js';
import { emit } from '../events.js';

/**
 * Race conditions across instances.
 *
 * Every op does a read-modify-write on a shared counter with some "business logic"
 * latency in between. Without coordination, concurrent ops on different instances read
 * the same value and overwrite each other (lost updates).
 *
 * Modes:
 *   none   - GET, think, SET              -> lost updates
 *   lock   - acquire `SET lock NX PX` with a unique token, then GET/SET, release with
 *            compare-and-delete so we never delete someone else's lock
 *   atomic - INCR                         -> no lock needed when the op fits in one command
 */
const COUNTER_KEY = 'demo:counter';
const LOCK_KEY = 'lock:counter';
const LOCK_TTL_MS = 3000;
const ACQUIRE_TIMEOUT_MS = 10000;

async function acquire(token) {
  const deadline = Date.now() + ACQUIRE_TIMEOUT_MS;
  while (Date.now() < deadline) {
    if ((await redis.set(LOCK_KEY, token, 'PX', LOCK_TTL_MS, 'NX')) === 'OK') return true;
    await sleep(5 + Math.random() * 10); // jittered retry
  }
  return false;
}

export const lockRouter = Router();

lockRouter.post('/op', rejectIfCrashed, async (req, res) => {
  const { mode = 'none', opId = randomUUID().slice(0, 6) } = req.body ?? {};
  const start = Date.now();
  let lockedAt = null;
  let read;
  let wrote;

  if (mode === 'atomic') {
    wrote = await redis.incr(COUNTER_KEY);
    read = wrote - 1;
  } else {
    const token = `${INSTANCE_ID}:${opId}:${randomUUID()}`;
    if (mode === 'lock') {
      if (!(await acquire(token))) return res.status(503).json({ error: 'lock timeout', opId });
      lockedAt = Date.now();
      emit('lock.acquired', { opId, waitedMs: lockedAt - start });
    }
    try {
      read = Number((await redis.get(COUNTER_KEY)) || 0);
      await sleep(15 + Math.random() * 35); // simulated work between read and write
      wrote = read + 1;
      await redis.set(COUNTER_KEY, wrote);
    } finally {
      if (mode === 'lock') {
        await redis.eval(DEL_IF_OWNER, 1, LOCK_KEY, token);
        emit('lock.released', { opId });
      }
    }
  }

  const result = { opId, instance: INSTANCE_ID, mode, start, lockedAt, end: Date.now(), read, wrote };
  emit('counter.op', result);
  res.json(result);
});

lockRouter.post('/reset', async (req, res) => {
  await redis.set(COUNTER_KEY, 0);
  await redis.del(LOCK_KEY);
  emit('counter.reset');
  res.json({ counter: 0 });
});

lockRouter.get('/counter', async (req, res) => {
  res.json({ counter: Number((await redis.get(COUNTER_KEY)) || 0) });
});
