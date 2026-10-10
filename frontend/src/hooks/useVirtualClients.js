import { useEffect, useMemo, useSyncExternalStore } from 'react';
import { toWs } from '../lib/api.js';
import { useCluster } from '../lib/cluster.jsx';

const NAMES = [
  'Ada', 'Linus', 'Grace', 'Alan', 'Barbara', 'Ken', 'Margaret', 'Dennis', 'Radia', 'Edsger',
  'Leslie', 'Frances', 'Tim', 'Hedy', 'Guido', 'Anita', 'Donald', 'Sophie', 'Brendan', 'Katherine',
];

/**
 * A pool of simulated browser clients, each holding a real WebSocket to one backend
 * instance. When their instance dies they reconnect through the "load balancer" to a
 * different one — exactly what a real client would do.
 */
class ClientPool {
  entries = new Map();
  listeners = new Set();
  snapshot = [];
  seq = 0;
  pick = () => null;

  subscribe = (fn) => {
    this.listeners.add(fn);
    return () => this.listeners.delete(fn);
  };

  getSnapshot = () => this.snapshot;

  changed() {
    this.snapshot = [...this.entries.values()].map((e) => e.state);
    this.listeners.forEach((fn) => fn());
  }

  patch(entry, patch) {
    entry.state = { ...entry.state, ...(typeof patch === 'function' ? patch(entry.state) : patch) };
    this.changed();
  }

  add(instance) {
    const n = this.seq++;
    const name = NAMES[n % NAMES.length] + (n >= NAMES.length ? ` ${Math.floor(n / NAMES.length) + 1}` : '');
    const id = `client-${n + 1}`;
    const entry = {
      ws: null,
      removed: false,
      state: { id, name, status: 'connecting', instanceId: null, inbox: [], reconnects: 0 },
    };
    this.entries.set(id, entry);
    this.connect(entry, instance ?? this.pick());
    return id;
  }

  connect(entry, instance) {
    if (entry.removed) return;
    if (!instance) {
      this.patch(entry, { status: 'no instance available' });
      setTimeout(() => this.connect(entry, this.pick()), 1500);
      return;
    }
    const { id, name } = entry.state;
    const ws = new WebSocket(`${toWs(instance.url)}/ws/client?clientId=${id}&name=${encodeURIComponent(name)}`);
    entry.ws = ws;
    this.patch(entry, { status: 'connecting', instanceId: instance.id });

    ws.onmessage = (ev) => {
      const msg = JSON.parse(ev.data);
      if (msg.type === 'welcome') this.patch(entry, { status: 'online', instanceId: msg.instance });
      if (msg.type === 'notification') {
        this.patch(entry, (s) => ({ inbox: [{ ...msg, receivedAt: Date.now() }, ...s.inbox].slice(0, 15) }));
      }
    };
    ws.onclose = () => {
      if (entry.ws !== ws || entry.removed) return;
      this.patch(entry, (s) => ({ status: 'reconnecting', reconnects: s.reconnects + 1 }));
      setTimeout(() => this.connect(entry, this.pick(instance.id)), 1000);
    };
  }

  remove(id) {
    const entry = this.entries.get(id);
    if (!entry) return;
    entry.removed = true;
    entry.ws?.close();
    this.entries.delete(id);
    this.changed();
  }

  removeAll() {
    [...this.entries.keys()].forEach((id) => this.remove(id));
  }

  clearInboxes() {
    this.entries.forEach((e) => (e.state = { ...e.state, inbox: [] }));
    this.changed();
  }
}

export function useVirtualClients() {
  const { pick } = useCluster();
  const pool = useMemo(() => new ClientPool(), []);
  pool.pick = pick;
  useEffect(() => () => pool.removeAll(), [pool]);
  const clients = useSyncExternalStore(pool.subscribe, pool.getSnapshot);
  return { clients, pool };
}
