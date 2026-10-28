import { randomUUID } from 'node:crypto';
import { Router } from 'express';
import { INSTANCE_ID } from '../config.js';
import { redis } from '../redis.js';
import { lifecycle, rejectIfCrashed, sleep } from '../lifecycle.js';
import { emit } from '../events.js';

/**
 * Orchestrated saga with compensations and durable, resumable execution.
 *
 * A distributed "transaction" across services that can't share a DB transaction:
 *   reserveInventory -> chargePayment -> createShipment
 * If a step fails, the orchestrator runs the compensations of the completed steps in
 * reverse order (release, refund, cancel) so the system converges to a consistent state.
 *
 * Durability: the saga's progress lives in Redis (`saga:<id>`), not in the orchestrator's
 * memory. The orchestrator holds a renewable lease (`saga:<id>:lease`). If it crashes, the
 * lease expires and another instance resumes the saga from the last persisted step.
 * Every side effect is applied through an idempotent "apply once" script, so re-running a
 * step after a crash (between doing it and recording it) never double-applies it.
 */
const STEPS = [
  { name: 'reserveInventory', effect: ['stock', -1], compensation: 'releaseInventory', undo: ['stock', 1] },
  { name: 'chargePayment', effect: ['revenue', 'amount'], compensation: 'refundPayment', undo: ['revenue', '-amount'] },
  { name: 'createShipment', effect: ['shipments', 1], compensation: 'cancelShipment', undo: ['shipments', -1] },
];
const SERVICES = 'saga:services';
const ACTIVE = 'sagas:active';
const RECENT = 'sagas:recent';
const STATS = 'sagas:stats'; // totals since reset, for the consistency check
const LEASE_MS = 3000;
const sagaKey = (id) => `saga:${id}`;
const leaseKey = (id) => `saga:${id}:lease`;
const logKey = (id) => `saga:${id}:log`;
const effectsKey = (id) => `saga:${id}:effects`;

const APPLY_ONCE = `
if redis.call('sadd', KEYS[1], ARGV[1]) == 1 then
  redis.call('hincrby', KEYS[2], ARGV[2], ARGV[3])
  return 1
end
return 0`;

const RENEW = `
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('pexpire', KEYS[1], ARGV[2])
end
return 0`;

// Deltas are numbers, or 'amount' / '-amount' to use the order total.
const delta = (v, saga) => (v === 'amount' ? Number(saga.amount) : v === '-amount' ? -Number(saga.amount) : v);

const driving = new Set(); // sagas this instance is currently executing

async function log(id, entry) {
  const row = { ...entry, instance: INSTANCE_ID, ts: Date.now() };
  await redis.rpush(logKey(id), JSON.stringify(row));
  emit(`saga.${entry.phase}`, { id, step: entry.step });
}

const ownsLease = async (id) => (await redis.get(leaseKey(id))) === INSTANCE_ID;

/** Sleep for the step's work time; returns false if this instance "crashed" meanwhile. */
async function work(ms) {
  const deadline = Date.now() + ms;
  while (Date.now() < deadline) {
    if (lifecycle.crashed) return false;
    await sleep(50);
  }
  return !lifecycle.crashed;
}

async function drive(id) {
  if (driving.has(id)) return;
  driving.add(id);
  const renew = setInterval(() => {
    if (!lifecycle.crashed) redis.eval(RENEW, 1, leaseKey(id), INSTANCE_ID, LEASE_MS).catch(() => {});
  }, 1000);
  try {
    while (true) {
      if (lifecycle.crashed || !(await ownsLease(id))) return; // abandoned; someone else will resume
      const s = await redis.hgetall(sagaKey(id));
      const stepMs = Number(s.stepMs);

      if (s.status === 'running') {
        const i = Number(s.step);
        if (i >= STEPS.length) {
          await redis.multi().hset(sagaKey(id), 'status', 'completed').srem(ACTIVE, id).hincrby(STATS, 'completed', 1).exec();
          await log(id, { phase: 'completed' });
          return;
        }
        const step = STEPS[i];
        await log(id, { phase: 'step_started', step: step.name });
        if (!(await work(stepMs))) return;
        if (s.failAt === step.name) {
          await redis.hset(sagaKey(id), 'status', 'compensating', 'failedStep', i, 'comp', i - 1);
          await log(id, { phase: 'step_failed', step: step.name });
          continue;
        }
        await redis.eval(APPLY_ONCE, 2, effectsKey(id), SERVICES, step.name, step.effect[0], delta(step.effect[1], s));
        await redis.hset(sagaKey(id), 'step', i + 1);
        await log(id, { phase: 'step_done', step: step.name });
        continue;
      }

      if (s.status === 'compensating') {
        const c = Number(s.comp);
        if (c < 0) {
          await redis.multi().hset(sagaKey(id), 'status', 'compensated').srem(ACTIVE, id).hincrby(STATS, 'compensated', 1).exec();
          await log(id, { phase: 'compensated' });
          return;
        }
        const step = STEPS[c];
        await log(id, { phase: 'comp_started', step: step.compensation });
        if (!(await work(stepMs))) return;
        await redis.eval(APPLY_ONCE, 2, effectsKey(id), SERVICES, step.compensation, step.undo[0], delta(step.undo[1], s));
        await redis.hset(sagaKey(id), 'comp', c - 1);
        await log(id, { phase: 'comp_done', step: step.compensation });
        continue;
      }
      return; // completed / compensated
    }
  } finally {
    clearInterval(renew);
    driving.delete(id);
  }
}

/** Resume sagas whose orchestrator's lease has expired. */
async function recoveryTick() {
  if (lifecycle.crashed) return;
  for (const id of await redis.smembers(ACTIVE)) {
    if (driving.has(id)) continue;
    const previous = await redis.hget(sagaKey(id), 'owner');
    if ((await redis.set(leaseKey(id), INSTANCE_ID, 'PX', LEASE_MS, 'NX')) !== 'OK') continue;
    await redis.hset(sagaKey(id), 'owner', INSTANCE_ID);
    await redis.hincrby(sagaKey(id), 'takeovers', 1);
    await log(id, { phase: 'resumed', from: previous });
    drive(id).catch((err) => console.error('[saga]', err.message));
  }
}

export async function startSagas() {
  await redis.hsetnx(SERVICES, 'stock', 100);
  setInterval(() => recoveryTick().catch((err) => console.error('[saga]', err.message)), 1000);
}

export const sagaRouter = Router();

sagaRouter.post('/', rejectIfCrashed, async (req, res) => {
  const { failAt = 'none', stepMs = 1200, amount = 25 } = req.body ?? {};
  const id = `order-${randomUUID().slice(0, 4)}`;
  await redis
    .multi()
    .hset(sagaKey(id), { id, status: 'running', step: 0, failAt, stepMs: Math.min(Number(stepMs), 5000), amount, owner: INSTANCE_ID, createdAt: Date.now(), takeovers: 0 })
    .set(leaseKey(id), INSTANCE_ID, 'PX', LEASE_MS)
    .sadd(ACTIVE, id)
    .lpush(RECENT, id)
    .ltrim(RECENT, 0, 11)
    .exec();
  await log(id, { phase: 'started', failAt });
  drive(id).catch((err) => console.error('[saga]', err.message));
  res.json({ id, owner: INSTANCE_ID });
});

sagaRouter.get('/state', async (req, res) => {
  const ids = await redis.lrange(RECENT, 0, -1);
  const [services, stats, active, ...rows] = await Promise.all([
    redis.hgetall(SERVICES),
    redis.hgetall(STATS),
    redis.scard(ACTIVE),
    ...ids.map(async (id) => ({
      ...(await redis.hgetall(sagaKey(id))),
      log: (await redis.lrange(logKey(id), 0, -1)).map((r) => JSON.parse(r)),
    })),
  ]);
  res.json({
    steps: STEPS.map(({ name, compensation }) => ({ name, compensation })),
    services: { stock: Number(services.stock ?? 0), revenue: Number(services.revenue ?? 0), shipments: Number(services.shipments ?? 0) },
    totals: { completed: Number(stats.completed ?? 0), compensated: Number(stats.compensated ?? 0), active },
    sagas: rows.filter((r) => r.id),
  });
});

sagaRouter.post('/reset', async (req, res) => {
  const ids = await redis.lrange(RECENT, 0, -1);
  const keys = ids.flatMap((id) => [sagaKey(id), leaseKey(id), logKey(id), effectsKey(id)]);
  await redis.del(ACTIVE, RECENT, STATS, ...keys);
  await redis.hset(SERVICES, 'stock', 100, 'revenue', 0, 'shipments', 0);
  emit('saga.reset');
  res.json({ ok: true });
});
