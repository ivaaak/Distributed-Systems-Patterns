import { INSTANCE_ID } from './config.js';
import { publish, subscribe } from './redis.js';

/**
 * Observability bus. Every instance publishes what it does to one Redis channel and
 * every instance forwards that channel to its monitor sockets, so a dashboard attached
 * to *any* instance sees the whole cluster.
 */
export const EVENTS_CHANNEL = 'system:events';

let seq = 0;

export function emit(type, data = {}) {
  const event = {
    id: `${INSTANCE_ID}:${Date.now()}:${seq++}`,
    ts: Date.now(),
    instance: INSTANCE_ID,
    type,
    data,
  };
  publish(EVENTS_CHANNEL, event).catch(() => {});
}

const monitors = new Set();

export function handleMonitorSocket(ws) {
  monitors.add(ws);
  ws.send(JSON.stringify({ type: 'hello', instance: INSTANCE_ID }));
  ws.on('close', () => monitors.delete(ws));
}

export async function startEventForwarding() {
  await subscribe(EVENTS_CHANNEL, (event) => {
    const msg = JSON.stringify({ type: 'event', event });
    for (const ws of monitors) {
      if (ws.readyState === ws.OPEN) ws.send(msg);
    }
  });
}
