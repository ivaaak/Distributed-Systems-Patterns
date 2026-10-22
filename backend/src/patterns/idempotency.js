import { randomUUID } from 'node:crypto';
import { Router } from 'express';
import { INSTANCE_ID } from '../config.js';
import { redis } from '../redis.js';
import { rejectIfCrashed, sleep } from '../lifecycle.js';
import { emit } from '../events.js';

/**
 * Idempotency keys for safe retries.
 *
 * Networks fail *after* the server did the work: the charge succeeded but the response
 * never arrived, so the client retries — possibly on a different instance. Without
 * protection, every retry is a new charge.
 *
 * With an `Idempotency-Key` header, the first request claims `idem:<key>` with SET NX
 * (status "processing"), does the work, then stores the response. Retries with the same
 * key get the stored response replayed; a retry that arrives while the original is still
 * running gets 409 and backs off. Because the record lives in Redis, it works no matter
 * which instance the retry lands on.
 */
const LEDGER = 'pay:ledger';
const RECORD_TTL_S = 24 * 3600;
const idemKey = (k) => `idem:${k}`;

export const paymentsRouter = Router();

paymentsRouter.post('/', rejectIfCrashed, async (req, res) => {
  const key = req.get('Idempotency-Key') || null;
  const { amount = 42, orderId = 'order', dropResponse = false, attempt = 1 } = req.body ?? {};
  emit('idem.request', { key, orderId, attempt });

  if (key) {
    const claimed = await redis.set(
      idemKey(key),
      JSON.stringify({ status: 'processing', instance: INSTANCE_ID }),
      'EX', RECORD_TTL_S, 'NX'
    );
    if (!claimed) {
      const record = JSON.parse(await redis.get(idemKey(key)));
      if (record.status === 'processing') {
        emit('idem.conflict', { key, attempt, owner: record.instance });
        return res.status(409).json({ error: 'a request with this key is still in flight', key, instance: INSTANCE_ID });
      }
      emit('idem.replayed', { key, attempt, chargeId: record.body.chargeId });
      res.set('Idempotent-Replayed', 'true');
      return res.status(record.statusCode).json({ ...record.body, replayed: true, replayedBy: INSTANCE_ID });
    }
  }

  // The actual side effect: talk to the card network, append to the ledger.
  await sleep(250 + Math.random() * 150);
  const charge = { chargeId: `ch_${randomUUID().slice(0, 8)}`, orderId, amount, key, attempt, instance: INSTANCE_ID, ts: Date.now() };
  await redis.rpush(LEDGER, JSON.stringify(charge));
  emit('idem.charged', charge);

  const body = { ...charge, status: 'charged' };
  if (key) await redis.set(idemKey(key), JSON.stringify({ status: 'done', statusCode: 200, body }), 'EX', RECORD_TTL_S);

  if (dropResponse) {
    // Simulate the response getting lost after the work is done: never answer, so the client
    // times out. (Resetting the connection instead makes browsers silently re-send the request
    // on their own — real, but it makes the demo nondeterministic.)
    emit('idem.response_lost', { chargeId: charge.chargeId, attempt });
    setTimeout(() => req.socket.destroy(), 10_000);
    return;
  }
  res.json({ ...body, instance: INSTANCE_ID });
});

paymentsRouter.get('/ledger', async (req, res) => {
  const rows = await redis.lrange(LEDGER, 0, -1);
  res.json({ charges: rows.map((r) => JSON.parse(r)) });
});

paymentsRouter.post('/reset', async (req, res) => {
  const keys = await redis.keys('idem:*');
  await redis.del(LEDGER, ...keys);
  emit('idem.reset');
  res.json({ ok: true });
});
