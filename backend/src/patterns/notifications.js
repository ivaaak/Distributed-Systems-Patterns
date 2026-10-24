import { randomUUID } from 'node:crypto';
import { Router } from 'express';
import { INSTANCE_ID } from '../config.js';
import { redis, publish, subscribe, DEL_IF_OWNER } from '../redis.js';
import { lifecycle, rejectIfCrashed } from '../lifecycle.js';
import { emit } from '../events.js';

/**
 * Multi-instance WebSocket notifications.
 *
 * Each client socket lives on exactly one instance, so an HTTP request that lands on
 * instance A can't write to a socket held by instance B. Redis Pub/Sub is the backplane:
 *   - broadcast: publish to `notif:broadcast`; every instance delivers to its local sockets.
 *   - direct:    look up `presence:<clientId>` -> owning instance, publish to that
 *                instance's private channel `notif:instance:<id>`.
 *   - local:     (the naive, broken approach) only deliver to sockets on the receiving instance.
 */
const BROADCAST_CHANNEL = 'notif:broadcast';
const instanceChannel = (id) => `notif:instance:${id}`;
const presenceKey = (clientId) => `presence:${clientId}`;
const PRESENCE_TTL_S = 30;

const clients = new Map(); // clientId -> { ws, name, connectedAt }

export const localClientCount = () => clients.size;

export function handleClientSocket(ws, url) {
  if (lifecycle.crashed) {
    ws.close(1013, 'instance crashed');
    return;
  }
  const clientId = url.searchParams.get('clientId') || randomUUID().slice(0, 8);
  const name = url.searchParams.get('name') || clientId;

  clients.get(clientId)?.ws.close(4000, 'replaced by newer connection');
  clients.set(clientId, { ws, name, connectedAt: Date.now() });
  redis.set(presenceKey(clientId), INSTANCE_ID, 'EX', PRESENCE_TTL_S).catch(() => {});
  emit('client.connected', { clientId, name });
  ws.send(JSON.stringify({ type: 'welcome', instance: INSTANCE_ID, clientId }));

  ws.on('close', () => {
    if (clients.get(clientId)?.ws !== ws) return;
    clients.delete(clientId);
    // A real crash can't clean up after itself, so leave presence to expire via TTL.
    if (lifecycle.crashed) return;
    redis.eval(DEL_IF_OWNER, 1, presenceKey(clientId), INSTANCE_ID).catch(() => {});
    emit('client.disconnected', { clientId, name });
  });
}

function deliver(notification, onlyClientId) {
  const payload = JSON.stringify({ type: 'notification', ...notification, deliveredBy: INSTANCE_ID });
  const delivered = [];
  for (const [clientId, { ws }] of clients) {
    if (onlyClientId && clientId !== onlyClientId) continue;
    if (ws.readyState !== ws.OPEN) continue;
    ws.send(payload);
    delivered.push(clientId);
  }
  return delivered;
}

export async function startNotifications() {
  await subscribe(BROADCAST_CHANNEL, (n) => {
    if (lifecycle.crashed) return;
    emit('notify.delivered', { nid: n.id, via: 'broadcast', clientIds: deliver(n) });
  });

  await subscribe(instanceChannel(INSTANCE_ID), (n) => {
    if (lifecycle.crashed) return;
    const clientIds = deliver(n, n.targetClientId);
    emit(clientIds.length ? 'notify.delivered' : 'notify.missed', {
      nid: n.id,
      via: 'direct',
      clientIds,
      targetClientId: n.targetClientId,
    });
  });

  // Keep presence alive for connected clients.
  setInterval(() => {
    if (lifecycle.crashed || !clients.size) return;
    const pipeline = redis.pipeline();
    for (const clientId of clients.keys()) {
      pipeline.set(presenceKey(clientId), INSTANCE_ID, 'EX', PRESENCE_TTL_S);
    }
    pipeline.exec().catch(() => {});
  }, 10_000);

  // Simulated crash: drop every socket abruptly (no close handshake).
  lifecycle.on('crash', () => {
    for (const { ws } of clients.values()) ws.terminate();
  });
}

export const notificationsRouter = Router();

notificationsRouter.post('/', rejectIfCrashed, async (req, res) => {
  const { mode = 'broadcast', message = 'Hello!', targetClientId } = req.body ?? {};
  const n = {
    id: randomUUID().slice(0, 8),
    mode,
    message: String(message).slice(0, 200),
    targetClientId,
    origin: INSTANCE_ID,
    ts: Date.now(),
  };
  emit('notify.received', { nid: n.id, mode, message: n.message, targetClientId });

  if (mode === 'local') {
    const clientIds = deliver(n);
    emit('notify.delivered', { nid: n.id, via: 'local', clientIds });
    return res.json({ ...n, delivered: clientIds.length });
  }

  if (mode === 'broadcast') {
    emit('notify.published', { nid: n.id, channel: BROADCAST_CHANNEL });
    const receivers = await publish(BROADCAST_CHANNEL, n);
    return res.json({ ...n, channel: BROADCAST_CHANNEL, subscribers: receivers });
  }

  if (mode === 'direct') {
    if (!targetClientId) return res.status(400).json({ error: 'targetClientId is required' });
    const owner = await redis.get(presenceKey(targetClientId));
    emit('notify.lookup', { nid: n.id, targetClientId, owner });
    if (!owner) return res.status(404).json({ error: `no presence for ${targetClientId}` });
    const channel = instanceChannel(owner);
    emit('notify.published', { nid: n.id, channel, owner });
    await publish(channel, n);
    return res.json({ ...n, channel, owner });
  }

  res.status(400).json({ error: `unknown mode ${mode}` });
});
