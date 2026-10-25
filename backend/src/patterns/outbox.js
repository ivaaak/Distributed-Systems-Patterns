import { randomUUID } from 'node:crypto';
import { Router } from 'express';
import { INSTANCE_ID } from '../config.js';
import { redis, DEL_IF_OWNER } from '../redis.js';
import { lifecycle, rejectIfCrashed, sleep } from '../lifecycle.js';
import { emit } from '../events.js';

/**
 * Transactional outbox: the dual-write problem.
 *
 * Placing an order must (1) save the order in the service's database and (2) publish an
 * `OrderPlaced` event to the message broker so other services (shipping, billing) react.
 * Those are two different systems, so there's no transaction spanning both.
 *
 *   dual   - write the DB, then publish. A crash (or broker outage) between the two leaves
 *            an order that the rest of the system never hears about.
 *   outbox - write the order *and* the event into the same database in one transaction
 *            (MULTI/EXEC on `ob:orders` + `ob:outbox`). A relay later copies outbox rows to
 *            the broker and deletes them. The event can't be lost, but a relay crash between
 *            "publish" and "delete" re-publishes it: delivery is at-least-once, so consumers
 *            dedupe by event id.
 *
 * Every instance runs a relay tick; a short lock (`ob:relay:lock`) makes one of them drain
 * the outbox at a time, and if that one dies the lock expires and another takes over.
 */
const ORDERS = 'ob:orders'; // "the service database"
const OUTBOX = 'ob:outbox'; // outbox table, in the same database
const BROKER = 'ob:broker'; // "the message broker" (a Stream)
const FAULTS = 'ob:faults';
const RELAY_LOCK = 'ob:relay:lock';
const RELAY_LOCK_MS = 2000;
const RELAY_EVERY_MS = 400;
const RELAY_BATCH = 5;
const PUBLISH_MS = 120; // make draining visible

const faults = () => redis.hgetall(FAULTS);

/** Take a one-shot fault if it's armed (so exactly one request/relay trips it). */
const consumeFault = async (name) => (await redis.hdel(FAULTS, name)) === 1;

/** Simulated process death in the middle of a request: no response, no cleanup. */
function crashNow(req, where) {
  lifecycle.crash();
  emit('instance.crashed', { reason: where });
  // Never answer. (Resetting the connection right away makes browsers silently re-send the request.)
  if (req) setTimeout(() => req.socket.destroy(), 10_000);
}

async function publishToBroker(event) {
  await sleep(PUBLISH_MS);
  if ((await redis.hget(FAULTS, 'brokerDown')) === '1') throw new Error('broker unavailable');
  return redis.xadd(BROKER, 'MAXLEN', '~', 2000, '*', 'eventId', event.eventId, 'orderId', event.orderId, 'via', event.via, 'publishedBy', INSTANCE_ID);
}

// ---- relay ---------------------------------------------------------------------------------
async function relayTick() {
  if (lifecycle.crashed) return;
  const token = `${INSTANCE_ID}:${randomUUID()}`;
  if (!(await redis.set(RELAY_LOCK, token, 'PX', RELAY_LOCK_MS, 'NX'))) return;
  try {
    const rows = await redis.lrange(OUTBOX, 0, RELAY_BATCH - 1);
    for (const raw of rows) {
      if (lifecycle.crashed) return; // lock is left to expire, like a real crash
      const event = JSON.parse(raw);
      try {
        const streamId = await publishToBroker(event);
        emit('outbox.relayed', { eventId: event.eventId, orderId: event.orderId, streamId });
      } catch (err) {
        emit('outbox.relay_blocked', { eventId: event.eventId, error: err.message });
        return; // keep the row, retry on a later tick (preserves order)
      }
      if (await consumeFault('crashRelay')) {
        // Published but not yet marked as sent: the next relay will publish it again.
        crashNow(null, 'relay crashed after publish, before deleting the outbox row');
        return;
      }
      await redis.lrem(OUTBOX, 1, raw);
    }
  } finally {
    if (!lifecycle.crashed) await redis.eval(DEL_IF_OWNER, 1, RELAY_LOCK, token);
  }
}

export function startOutbox() {
  setInterval(() => relayTick().catch((err) => console.error('[outbox relay]', err.message)), RELAY_EVERY_MS);
}

// ---- API -----------------------------------------------------------------------------------
export const outboxRouter = Router();

outboxRouter.post('/orders', rejectIfCrashed, async (req, res) => {
  const mode = req.body?.mode === 'outbox' ? 'outbox' : 'dual';
  const orderId = `ord-${randomUUID().slice(0, 6)}`;
  const order = { orderId, mode, total: 10 + Math.floor(Math.random() * 90), instance: INSTANCE_ID, ts: Date.now() };
  const event = { eventId: `evt-${randomUUID().slice(0, 8)}`, type: 'OrderPlaced', orderId, via: mode };

  if (mode === 'outbox') {
    // One local transaction: the order and its event commit together or not at all.
    await redis.multi().hset(ORDERS, orderId, JSON.stringify(order)).rpush(OUTBOX, JSON.stringify(event)).exec();
    emit('outbox.committed', { orderId, eventId: event.eventId, mode });
    if (await consumeFault('crashAfterCommit')) return crashNow(req, 'crashed right after the DB commit');
    return res.status(201).json({ ...order, eventId: event.eventId, published: 'queued in outbox' });
  }

  // Dual write: two separate systems, no shared transaction.
  await redis.hset(ORDERS, orderId, JSON.stringify(order));
  emit('outbox.committed', { orderId, mode });
  if (await consumeFault('crashAfterCommit')) return crashNow(req, 'crashed between the DB commit and the publish');
  try {
    const streamId = await publishToBroker(event);
    emit('outbox.published', { orderId, eventId: event.eventId, streamId });
    res.status(201).json({ ...order, eventId: event.eventId, published: 'yes' });
  } catch (err) {
    emit('outbox.publish_failed', { orderId, error: err.message });
    res.status(502).json({ ...order, error: `order saved, but publishing failed: ${err.message}` });
  }
});

outboxRouter.get('/state', async (req, res) => {
  const [ordersRaw, outboxRaw, brokerRaw, f, relay] = await Promise.all([
    redis.hvals(ORDERS),
    redis.lrange(OUTBOX, 0, -1),
    redis.xrange(BROKER, '-', '+'),
    faults(),
    redis.get(RELAY_LOCK),
  ]);
  const broker = brokerRaw.map(([streamId, fields]) => {
    const e = { streamId };
    for (let i = 0; i < fields.length; i += 2) e[fields[i]] = fields[i + 1];
    return e;
  });
  const orders = ordersRaw.map((r) => JSON.parse(r)).sort((a, b) => a.ts - b.ts);
  res.json({
    orders,
    outbox: outboxRaw.map((r) => JSON.parse(r)),
    broker,
    faults: { brokerDown: f.brokerDown === '1', crashAfterCommit: f.crashAfterCommit === '1', crashRelay: f.crashRelay === '1' },
    relayHolder: relay?.split(':')[0] ?? null,
  });
});

outboxRouter.post('/faults', async (req, res) => {
  const allowed = ['brokerDown', 'crashAfterCommit', 'crashRelay'];
  const updates = Object.entries(req.body ?? {}).filter(([k]) => allowed.includes(k));
  for (const [k, on] of updates) {
    if (on) await redis.hset(FAULTS, k, '1');
    else await redis.hdel(FAULTS, k);
    emit('outbox.fault', { [k]: Boolean(on) });
  }
  res.json(await faults());
});

outboxRouter.post('/reset', async (req, res) => {
  await redis.del(ORDERS, OUTBOX, BROKER, FAULTS, RELAY_LOCK);
  emit('outbox.reset');
  res.json({ ok: true });
});
