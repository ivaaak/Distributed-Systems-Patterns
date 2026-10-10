import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState } from 'react';
import { SEED_URLS, call, toWs } from './api.js';

const ClusterContext = createContext(null);
const EventsContext = createContext(null); // stable: { subscribe }
const LogContext = createContext(null); // changes on every event: { log, clear }

const MAX_LOG = 400;

/**
 * Polls cluster membership from whichever instance answers and keeps one monitor
 * WebSocket open to a live instance. Because every instance republishes the shared
 * `system:events` Redis channel, one connection is enough to observe the whole cluster.
 */
export function ClusterProvider({ children }) {
  const [snapshot, setSnapshot] = useState({ instances: [], leader: null, servedBy: null, ts: 0 });
  const [reachable, setReachable] = useState(false);
  const [log, setLog] = useState([]);
  const [monitorOn, setMonitorOn] = useState(null);
  const listeners = useRef(new Set());
  const snapRef = useRef(snapshot);
  snapRef.current = snapshot;
  const rr = useRef(0);

  const knownUrls = useCallback(() => {
    const urls = new Set(SEED_URLS);
    snapRef.current.instances.forEach((i) => urls.add(i.url));
    return [...urls];
  }, []);

  // Membership polling.
  useEffect(() => {
    let stopped = false;
    let preferred = 0;
    const poll = async () => {
      const urls = knownUrls();
      for (let k = 0; k < urls.length; k++) {
        const idx = (preferred + k) % urls.length;
        try {
          const { ok, data } = await call(urls[idx], '/api/cluster', undefined, { timeout: 1500 });
          if (ok && !stopped) {
            preferred = idx;
            setSnapshot(data);
            setReachable(true);
            return;
          }
        } catch {
          /* try next */
        }
      }
      if (!stopped) setReachable(false);
    };
    poll();
    const t = setInterval(poll, 700);
    return () => {
      stopped = true;
      clearInterval(t);
    };
  }, [knownUrls]);

  // Monitor socket with failover.
  useEffect(() => {
    let ws;
    let stopped = false;
    let attempt = 0;
    const connect = () => {
      if (stopped) return;
      const urls = knownUrls();
      const url = urls[attempt++ % urls.length];
      ws = new WebSocket(`${toWs(url)}/ws/monitor`);
      ws.onmessage = (ev) => {
        const msg = JSON.parse(ev.data);
        if (msg.type === 'hello') setMonitorOn(msg.instance);
        if (msg.type !== 'event') return;
        const e = msg.event;
        listeners.current.forEach((fn) => fn(e));
        setLog((l) => (l.length >= MAX_LOG ? [e, ...l.slice(0, MAX_LOG - 1)] : [e, ...l]));
      };
      ws.onclose = () => {
        setMonitorOn(null);
        if (!stopped) setTimeout(connect, 1000);
      };
    };
    connect();
    return () => {
      stopped = true;
      ws?.close();
    };
  }, [knownUrls]);

  const alive = useMemo(() => snapshot.instances.filter((i) => i.alive), [snapshot]);

  /** Round-robin "load balancer" over live instances. */
  const pick = useCallback((exclude) => {
    const live = snapRef.current.instances.filter((i) => i.alive);
    const pool = live.filter((i) => i.id !== exclude);
    const list = pool.length ? pool : live;
    if (!list.length) return null;
    return list[rr.current++ % list.length];
  }, []);

  const byId = useCallback((id) => snapRef.current.instances.find((i) => i.id === id), []);

  /** Call any live instance (for reads / pattern-agnostic endpoints). */
  const callAny = useCallback(
    async (path, body) => {
      const inst = pick();
      const urls = inst ? [inst.url, ...knownUrls()] : knownUrls();
      for (const url of urls) {
        try {
          return await call(url, path, body);
        } catch {
          /* next */
        }
      }
      throw new Error('no instance reachable');
    },
    [pick, knownUrls]
  );

  const crash = useCallback((id) => call(byId(id).url, '/api/chaos/crash', {}), [byId]);
  const recover = useCallback((id) => call(byId(id).url, '/api/chaos/recover', {}), [byId]);

  const cluster = useMemo(
    () => ({ ...snapshot, alive, reachable, monitorOn, pick, byId, callAny, crash, recover }),
    [snapshot, alive, reachable, monitorOn, pick, byId, callAny, crash, recover]
  );

  const events = useMemo(
    () => ({
      subscribe: (fn) => {
        listeners.current.add(fn);
        return () => listeners.current.delete(fn);
      },
    }),
    []
  );
  const logValue = useMemo(() => ({ log, clear: () => setLog([]) }), [log]);

  return (
    <ClusterContext.Provider value={cluster}>
      <EventsContext.Provider value={events}>
        <LogContext.Provider value={logValue}>{children}</LogContext.Provider>
      </EventsContext.Provider>
    </ClusterContext.Provider>
  );
}

export const useCluster = () => useContext(ClusterContext);
export const useEventLog = () => useContext(LogContext);

/** Subscribe to live events whose type starts with one of `prefixes`. */
export function useEvents(prefixes, handler) {
  const { subscribe } = useContext(EventsContext);
  const handlerRef = useRef(handler);
  handlerRef.current = handler;
  const key = prefixes.join('|');
  useEffect(() => {
    const list = key.split('|');
    return subscribe((e) => {
      if (list.some((p) => e.type.startsWith(p))) handlerRef.current(e);
    });
  }, [key, subscribe]);
}
