import { Router } from 'express';
import { INSTANCE_ID } from '../config.js';
import { redis, DEL_IF_OWNER } from '../redis.js';
import { lifecycle, sleep } from '../lifecycle.js';
import { emit } from '../events.js';

/**
 * Leader election with a lease: `SET leader:lock <id> NX PX <lease>`.
 * The leader renews its lease periodically; if it dies the key expires and another
 * instance wins. Each new term increments `leader:epoch`, which is used as a
 * *fencing token*: writes from a stale leader (e.g. one that was stuck in a long GC
 * pause and doesn't know it lost the lease) are rejected by the resource.
 */
const LOCK_KEY = 'leader:lock';
const EPOCH_KEY = 'leader:epoch';
const RESOURCE_KEY = 'leader:resource';
const LEASE_MS = 4000;
const RENEW_EVERY_MS = 1000;
const JOB_EVERY_MS = 2000;

const RENEW = `
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('pexpire', KEYS[1], ARGV[2])
end
return 0`;

// The protected resource only accepts writes carrying an epoch >= the highest it has seen.
const FENCED_WRITE = `
local current = tonumber(redis.call('hget', KEYS[1], 'epoch') or '0')
if tonumber(ARGV[1]) < current then
  return current
end
redis.call('hset', KEYS[1], 'epoch', ARGV[1], 'writer', ARGV[2], 'ts', ARGV[3])
redis.call('hincrby', KEYS[1], 'runs', 1)
return -1`;

const state = { isLeader: false, epoch: null, stalledUntil: 0 };

const isStalled = () => Date.now() < state.stalledUntil;

async function electionTick() {
  if (lifecycle.crashed || isStalled()) return;
  if (state.isLeader) {
    const renewed = await redis.eval(RENEW, 1, LOCK_KEY, INSTANCE_ID, LEASE_MS);
    if (!renewed) {
      state.isLeader = false;
      emit('leader.lost', { epoch: state.epoch, reason: 'lease expired' });
    }
    return;
  }
  const won = await redis.set(LOCK_KEY, INSTANCE_ID, 'PX', LEASE_MS, 'NX');
  if (won === 'OK') {
    state.isLeader = true;
    state.epoch = await redis.incr(EPOCH_KEY);
    emit('leader.elected', { epoch: state.epoch, leaseMs: LEASE_MS });
  }
}

/** The "singleton" job that only the leader should run (think: a cron / scheduler). */
async function leaderJob() {
  if (lifecycle.crashed || isStalled() || !state.isLeader) return;
  const result = await redis.eval(FENCED_WRITE, 1, RESOURCE_KEY, state.epoch, INSTANCE_ID, Date.now());
  if (result === -1) {
    emit('leader.job', { epoch: state.epoch, accepted: true });
  } else {
    emit('leader.fenced', { epoch: state.epoch, currentEpoch: result });
  }
}

export function startLeaderElection() {
  const loop = (fn, every) =>
    setInterval(() => fn().catch((err) => console.error(err.message)), every);
  loop(electionTick, RENEW_EVERY_MS);
  loop(leaderJob, JOB_EVERY_MS);
  electionTick().catch(() => {});
}

export const isLeader = () => state.isLeader;

export async function resignLeadership() {
  if (!state.isLeader) return false;
  state.isLeader = false;
  await redis.eval(DEL_IF_OWNER, 1, LOCK_KEY, INSTANCE_ID);
  emit('leader.resigned', { epoch: state.epoch });
  return true;
}

export const leaderRouter = Router();

leaderRouter.get('/state', async (req, res) => {
  const [leader, ttlMs, epoch, resource] = await Promise.all([
    redis.get(LOCK_KEY),
    redis.pttl(LOCK_KEY),
    redis.get(EPOCH_KEY),
    redis.hgetall(RESOURCE_KEY),
  ]);
  res.json({ leader, ttlMs, epoch: Number(epoch || 0), leaseMs: LEASE_MS, resource });
});

leaderRouter.post('/resign', async (req, res) => {
  res.json({ resigned: await resignLeadership() });
});

/**
 * Freeze this instance's leader loop (like a stop-the-world GC pause). When it wakes up
 * it still *believes* it is the leader and immediately runs its job with its old epoch
 * -> the fencing token rejects it.
 */
leaderRouter.post('/stall', async (req, res) => {
  const ms = Math.min(Number(req.body?.ms) || 6000, 20000);
  state.stalledUntil = Date.now() + ms;
  emit('leader.stalled', { ms, wasLeader: state.isLeader, epoch: state.epoch });
  res.json({ stalledMs: ms });
  await sleep(ms + 10);
  emit('leader.resumed', { stillThinksLeader: state.isLeader, epoch: state.epoch });
  await leaderJob().catch(() => {});
  await electionTick().catch(() => {});
});
