import { useRef, useState } from 'react';
import Explainer from '../components/Explainer.jsx';
import { useCluster } from '../lib/cluster.jsx';
import { call, instanceColor } from '../lib/api.js';
import { useLatest, useNow } from '../hooks/usePackets.jsx';
import { usePolling } from '../lib/page.js';

const VIEW_MS = 15_000;
const STATS_MS = 5_000;
const ENDPOINTS = {
  inventory: { label: 'GET /inventory', note: 'fast, healthy dependency', color: '#38bdf8' },
  recommendations: { label: 'GET /recommendations', note: 'dependency you slow down', color: '#c084fc' },
};
const MODES = {
  shared: { label: 'One shared pool', color: '#f87171' },
  bulkhead: { label: 'Bulkheads (pool per dependency)', color: '#34d399' },
};
const TRAFFIC = { fastRps: 4, slowRps: 5 };

const pct = (xs, p) => {
  if (!xs.length) return 0;
  const s = [...xs].sort((a, b) => a - b);
  return s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))];
};

function PoolView({ pool }) {
  const cells = [];
  for (const [tag, n] of Object.entries(pool.holders)) for (let i = 0; i < n; i++) cells.push(tag);
  while (cells.length < pool.size) cells.push(null);
  return (
    <div className="bh-pool">
      <span className="muted small mono">
        {pool.name} {pool.inUse}/{pool.size}
      </span>
      <div className="bh-slots">
        {cells.map((tag, i) => (
          <span key={i} className="bh-slot" style={tag ? { '--c': ENDPOINTS[tag].color } : undefined} data-busy={Boolean(tag)} />
        ))}
      </div>
      <span className={`small ${pool.queued ? 'bad-text' : 'muted'}`}>{pool.queued} queued</span>
    </div>
  );
}

export default function Bulkhead() {
  const { alive, callAny } = useCluster();
  const [states, setStates] = useState({});
  const [cfg, setCfg] = useState({ mode: 'shared', recsLatencyMs: 80, fastRps: 0, slowRps: 0 });
  const synced = useRef(false);
  const aliveRef = useLatest(alive);
  const now = useNow(200);

  usePolling(async () => {
    const views = await Promise.all(aliveRef.current.map((i) => call(i.url, '/api/bulkhead/state').then((r) => r.data).catch(() => null)));
    const valid = views.filter((v) => v?.pools);
    if (!synced.current && valid[0]) {
      synced.current = true;
      setCfg(valid[0].config);
    }
    setStates(Object.fromEntries(valid.map((v) => [v.instance, v])));
  }, 500);

  const update = (patch) => {
    setCfg((c) => ({ ...c, ...patch }));
    callAny('/api/bulkhead/config', patch).catch(() => {});
  };

  const trafficOn = cfg.fastRps > 0 || cfg.slowRps > 0;
  const views = Object.values(states);
  const history = views.flatMap((v) => v.history.map((h) => ({ ...h, instance: v.instance })));
  const limits = views[0]?.limits ?? { sharedSize: 8, bulkheadSizes: { inventory: 5, recommendations: 3 }, queueWaitMs: 500 };

  const summary = (endpoint) => {
    const recent = history.filter((h) => h.endpoint === endpoint && h.at > now - STATS_MS);
    const ok = recent.filter((h) => h.result === 'ok');
    return {
      total: recent.length,
      okPct: recent.length ? Math.round((ok.length / recent.length) * 100) : null,
      p95: pct(recent.map((h) => h.ms), 95),
    };
  };
  const x = (t) => 100 - ((now - t) / VIEW_MS) * 100;

  return (
    <div className="pattern">
      <Explainer
        title="Bulkheads"
        subtitle={`bounded worker pools · shared pool of ${limits.sharedSize} vs ${limits.bulkheadSizes.inventory} + ${limits.bulkheadSizes.recommendations} · wait ≤ ${limits.queueWaitMs}ms for a worker, else 503`}
        tryThis={[
          'Start traffic with both dependencies healthy: every request succeeds on both endpoints.',
          'Keep "One shared pool" and drag the recommendations latency to ~3s. Recommendation calls pile up and hold every worker; inventory requests queue behind them and get rejected, although the inventory dependency is perfectly fine.',
          'Switch to "Bulkheads": recommendations can only use their own 3 workers. They still degrade (rejected fast), but inventory is back to 100% with low latency.',
          'Crash an instance while traffic runs: its share of the load simply disappears (the generator runs inside each instance).',
        ]}
      >
        <p>
          Every service has finite concurrency: threads, event-loop slots, connection pools, memory. If all outbound calls draw
          from the <strong>same</strong> pool, one slow dependency ties up every worker waiting on it, and requests that
          don&apos;t even touch that dependency start failing. A slow dependency ends up taking down unrelated features.
        </p>
        <p>
          A <strong>bulkhead</strong> (named after the watertight compartments in a ship&apos;s hull) partitions the resources:
          each dependency, tenant or request class gets its own bounded pool. A failure floods one compartment and the rest
          keep working. Bulkheads work well with timeouts and circuit breakers, which stop the flooded compartment from
          staying full.
        </p>
        <p className="muted small">
          Load is generated inside each instance ({TRAFFIC.fastRps} inventory + {TRAFFIC.slowRps} recommendation requests/s per
          instance): browsers allow only ~6 connections per host, which would queue the requests client-side and hide the
          effect.
        </p>
      </Explainer>

      <div className="controls card">
        <div className="control-group">
          <label>Isolation</label>
          <div className="segmented">
            {Object.entries(MODES).map(([k, m]) => (
              <button key={k} className={cfg.mode === k ? 'active' : ''} style={{ '--c': m.color }} onClick={() => update({ mode: k })}>
                {m.label}
              </button>
            ))}
          </div>
        </div>
        <div className="control-group">
          <label>Dependency</label>
          <span className="muted small">recommendations latency</span>
          <input type="range" min={50} max={4000} step={50} value={cfg.recsLatencyMs} onChange={(e) => update({ recsLatencyMs: Number(e.target.value) })} />
          <strong className={cfg.recsLatencyMs > 1000 ? 'bad-text' : ''}>{cfg.recsLatencyMs}ms</strong>
          <button className="btn tiny ghost" onClick={() => update({ recsLatencyMs: 80 })}>
            healthy
          </button>
          <button className="btn tiny ghost" onClick={() => update({ recsLatencyMs: 3000 })}>
            degraded (3s)
          </button>
        </div>
        <div className="control-group">
          <label>Traffic</label>
          <label className="toggle">
            <input type="checkbox" checked={trafficOn} onChange={(e) => update(e.target.checked ? TRAFFIC : { fastRps: 0, slowRps: 0 })} /> generate load
            on every instance
          </label>
        </div>
      </div>

      <div className="stats">
        {Object.entries(ENDPOINTS).map(([ep, meta]) => {
          const s = summary(ep);
          return [
            <div key={`${ep}-ok`} className={`stat card ${s.okPct == null ? '' : s.okPct >= 95 ? 'good' : s.okPct < 70 ? 'bad' : ''}`}>
              <span>{ep} success (5s)</span>
              <strong>{s.okPct == null ? '—' : `${s.okPct}%`}</strong>
              <small>{s.total} requests</small>
            </div>,
            <div key={`${ep}-p95`} className="stat card">
              <span>{ep} p95</span>
              <strong>{s.total ? `${s.p95}ms` : '—'}</strong>
              <small>{meta.note}</small>
            </div>,
          ];
        })}
      </div>

      <div className="breakers">
        {alive.map((i) =>
          states[i.id] ? (
            <div key={i.id} className="breaker card" style={{ '--c': instanceColor(i.id) }}>
              <header>
                <strong>{i.id}</strong>
                <span className="muted small">{states[i.id].config.mode === 'shared' ? 'one pool for everything' : 'one pool per dependency'}</span>
              </header>
              {states[i.id].pools.map((p) => (
                <PoolView key={p.name} pool={p} />
              ))}
            </div>
          ) : null
        )}
      </div>

      <div className="card">
        <h3>Requests — last {VIEW_MS / 1000}s, all instances</h3>
        {Object.entries(ENDPOINTS).map(([ep, meta]) => (
          <div key={ep} className="lane">
            <div className="lane-label bh-lane-label" style={{ '--c': meta.color }}>
              {meta.label}
            </div>
            <div className="lane-track dots">
              {history
                .filter((h) => h.endpoint === ep && h.at > now - VIEW_MS)
                .map((h, k) => (
                  <span key={`${h.instance}-${h.at}-${k}`} className={`req ${h.result === 'ok' ? 'ok' : 'no'}`} style={{ left: `${x(h.at)}%` }} title={`${h.instance} · ${h.result} · ${h.ms}ms`} />
                ))}
            </div>
          </div>
        ))}
        <div className="strip-legend muted small">
          <span className="req ok inline" /> served · <span className="req no inline" /> rejected: no free worker within {limits.queueWaitMs}ms · slots:{' '}
          {Object.entries(ENDPOINTS).map(([ep, m]) => (
            <span key={ep}>
              <span className="bh-slot inline" data-busy="true" style={{ '--c': m.color }} /> {ep}{' '}
            </span>
          ))}
        </div>
      </div>
    </div>
  );
}
