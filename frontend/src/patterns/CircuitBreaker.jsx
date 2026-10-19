import { useEffect, useRef, useState } from 'react';
import Explainer from '../components/Explainer.jsx';
import { useCluster } from '../lib/cluster.jsx';
import { call, instanceColor } from '../lib/api.js';
import { useLatest, useNow } from '../hooks/usePackets.jsx';
import { usePolling } from '../lib/page.js';

const VIEW_MS = 20_000;
const STATES = ['closed', 'half_open', 'open'];
const LABEL = { closed: 'CLOSED', half_open: 'HALF-OPEN', open: 'OPEN' };

const MODES = {
  none: { label: 'No breaker', color: '#f87171' },
  local: { label: 'Per-instance breaker', color: '#fbbf24' },
  shared: { label: 'Shared breaker (Redis)', color: '#34d399' },
};

function Breaker({ title, color, s, cfg }) {
  const remaining = s.state === 'open' ? Math.max(0, cfg.openMs - (s.now - s.openedAt)) : 0;
  return (
    <div className="breaker card" style={{ '--c': color }}>
      <header>
        <strong>{title}</strong>
        <span className="muted small">
          failures {s.failures}/{cfg.threshold}
          {s.state === 'open' && ` · half-open in ${(remaining / 1000).toFixed(1)}s`}
        </span>
      </header>
      <div className="fsm">
        {STATES.map((st) => (
          <span key={st} className={`fsm-state ${st} ${s.state === st ? 'active' : ''}`}>
            {LABEL[st]}
          </span>
        ))}
      </div>
    </div>
  );
}

export default function CircuitBreaker() {
  const { instances, alive, pick, callAny } = useCluster();
  const [mode, setMode] = useState('none');
  const [failRate, setFailRate] = useState(0);
  const [latencyMs, setLatencyMs] = useState(60);
  const [traffic, setTraffic] = useState(false);
  const [reqs, setReqs] = useState([]);
  const [states, setStates] = useState({});
  const [load, setLoad] = useState([]);
  const lastCalls = useRef(null);
  const synced = useRef(false);
  const aliveRef = useLatest(alive);
  const now = useNow(100);

  // Poll every instance's breaker (local state lives in each process).
  usePolling(async () => {
    const views = await Promise.all(aliveRef.current.map((i) => call(i.url, '/api/cb/state').then((r) => r.data).catch(() => null)));
    const valid = views.filter(Boolean);
    if (!synced.current && valid[0]) {
      // Pick up the dependency settings already stored in Redis (e.g. after a page reload).
      synced.current = true;
      setFailRate(valid[0].downstream.failRate);
      setLatencyMs(valid[0].downstream.latencyMs);
    }
    setStates(Object.fromEntries(valid.map((v) => [v.instance, v])));
    const calls = valid[0]?.downstreamCalls;
    if (calls != null) {
      if (lastCalls.current != null) {
        const rate = Math.max(0, calls - lastCalls.current) / 0.5;
        setLoad((l) => [...l.slice(-39), rate]);
      }
      lastCalls.current = calls;
    }
  }, 500);

  const fire = async () => {
    const inst = pick();
    if (!inst) return;
    try {
      const { data } = await call(inst.url, '/api/cb/call', { mode });
      setReqs((r) => [...r.filter((x) => x.at > Date.now() - VIEW_MS - 1000), { ...data, at: Date.now(), id: Math.random() }]);
    } catch {
      /* ignore */
    }
  };
  const fireRef = useLatest(fire);

  useEffect(() => {
    if (!traffic) return;
    const t = setInterval(() => fireRef.current(), 150);
    return () => clearInterval(t);
  }, [traffic, fireRef]);

  const applyDownstream = (patch) => {
    const next = { failRate, latencyMs, ...patch };
    setFailRate(next.failRate);
    setLatencyMs(next.latencyMs);
    callAny('/api/cb/downstream', next).catch(() => {});
  };

  const switchMode = async (m) => {
    setMode(m);
    await callAny('/api/cb/reset', {});
    setReqs([]);
    lastCalls.current = null;
  };

  const any = Object.values(states)[0];
  const cfg = any?.config ?? { threshold: 5, openMs: 5000, timeoutMs: 700 };
  const recent = reqs.filter((r) => r.at > now - 5000);
  const avgMs = recent.length ? Math.round(recent.reduce((s, r) => s + r.ms, 0) / recent.length) : 0;
  const shortPct = recent.length ? Math.round((recent.filter((r) => r.result === 'short_circuit').length / recent.length) * 100) : 0;
  const errPct = recent.length ? Math.round((recent.filter((r) => r.result === 'error').length / recent.length) * 100) : 0;
  const maxLoad = Math.max(8, ...load);
  const lanes = [...new Set([...instances.map((i) => i.id), ...reqs.map((r) => r.instance)])].sort();
  const x = (t) => 100 - ((now - t) / VIEW_MS) * 100;

  return (
    <div className="pattern">
      <Explainer
        title="Circuit breaker"
        subtitle={`closed → open after ${cfg.threshold} consecutive failures → half-open after ${cfg.openMs / 1000}s → one trial call`}
        tryThis={[
          'Start traffic with a healthy dependency (0% failures): everything is green and fast.',
          'Push failure rate to 100% with "No breaker": every request waits for the 700ms timeout and the dependency keeps getting hammered (see the load chart).',
          'Switch to "Per-instance": each instance trips on its own after 5 failures — N instances pay N× the discovery cost.',
          'Switch to "Shared": the first failures trip the breaker for the whole cluster; requests fail fast (grey) and load on the dependency drops to a trickle of half-open probes.',
          'Lower the failure rate back to 0% and watch the half-open trial close the breaker.',
        ]}
      >
        <p>
          When a dependency is struggling, continuing to call it makes things worse: callers tie up threads/sockets waiting
          for timeouts, latency spreads upstream, and the dependency never gets room to recover. A circuit breaker notices
          repeated failures and <strong>fails fast</strong> for a cool-down period, then lets a single trial request through
          to probe for recovery.
        </p>
        <p>
          With many instances, each keeping its own breaker, the cluster needs N × threshold failures before everyone stops.
          Keeping the breaker in Redis (state transitions in Lua, timed with Redis&apos; clock) lets the cluster react as one.
        </p>
      </Explainer>

      <div className="controls card">
        <div className="control-group">
          <label>Breaker</label>
          <div className="segmented">
            {Object.entries(MODES).map(([k, m]) => (
              <button key={k} className={mode === k ? 'active' : ''} style={{ '--c': m.color }} onClick={() => switchMode(k)}>
                {m.label}
              </button>
            ))}
          </div>
        </div>
        <div className="control-group">
          <label>Dependency</label>
          <span className="muted small">failure rate</span>
          <input type="range" min={0} max={100} step={10} value={failRate * 100} onChange={(e) => applyDownstream({ failRate: Number(e.target.value) / 100 })} />
          <strong className={failRate > 0.4 ? 'bad-text' : ''}>{Math.round(failRate * 100)}%</strong>
          <span className="muted small">latency</span>
          <input type="range" min={10} max={400} step={10} value={latencyMs} onChange={(e) => applyDownstream({ latencyMs: Number(e.target.value) })} />
          <strong>{latencyMs}ms</strong>
        </div>
        <div className="control-group">
          <label>Traffic</label>
          <label className="toggle">
            <input type="checkbox" checked={traffic} onChange={(e) => setTraffic(e.target.checked)} /> ~7 req/s via LB
          </label>
          <button className="btn" onClick={fire}>
            1 request
          </button>
          <button className="btn ghost" onClick={() => switchMode(mode)}>
            Reset breakers
          </button>
        </div>
      </div>

      <div className="stats">
        <div className="stat card">
          <span>avg latency (5s)</span>
          <strong>{avgMs}ms</strong>
          <small>as seen by callers</small>
        </div>
        <div className={`stat card ${errPct > 30 ? 'bad' : ''}`}>
          <span>errors (5s)</span>
          <strong>{errPct}%</strong>
          <small>waited for timeout</small>
        </div>
        <div className={`stat card ${shortPct > 0 ? 'good' : ''}`}>
          <span>failed fast (5s)</span>
          <strong>{shortPct}%</strong>
          <small>short-circuited</small>
        </div>
        <div className="stat card">
          <span>load on dependency</span>
          <strong>{(load.at(-1) ?? 0).toFixed(0)}/s</strong>
          <div className="sparkline">
            {load.map((v, i) => (
              <span key={i} style={{ height: `${(v / maxLoad) * 100}%` }} />
            ))}
          </div>
        </div>
      </div>

      <div className="breakers">
        {mode === 'none' && <div className="card muted">No breaker: every request goes straight to the dependency.</div>}
        {mode === 'shared' && any && <Breaker title="cb:shared (Redis) — used by every instance" color="#34d399" s={{ ...any.shared, now: any.now }} cfg={cfg} />}
        {mode === 'local' &&
          instances.map((i) =>
            states[i.id] ? (
              <Breaker key={i.id} title={`${i.id} (in-memory)`} color={instanceColor(i.id)} s={{ ...states[i.id].local, now: states[i.id].now }} cfg={cfg} />
            ) : null
          )}
      </div>

      <div className="card">
        <h3>Requests — last {VIEW_MS / 1000}s</h3>
        {lanes.map((id) => (
          <div key={id} className="lane">
            <div className="lane-label" style={{ '--c': instanceColor(id) }}>
              {id}
            </div>
            <div className="lane-track dots">
              {reqs
                .filter((r) => r.instance === id && r.at > now - VIEW_MS)
                .map((r) => (
                  <span
                    key={r.id}
                    className={`req ${r.result === 'ok' ? 'ok' : r.result === 'error' ? 'no' : 'short'} ${r.trial ? 'trial' : ''}`}
                    style={{ left: `${x(r.at)}%` }}
                    title={`${r.result}${r.trial ? ' (half-open trial)' : ''} · ${r.ms}ms`}
                  />
                ))}
            </div>
          </div>
        ))}
        <div className="strip-legend muted small">
          <span className="req ok inline" /> ok · <span className="req no inline" /> error (timeout) · <span className="req short inline" /> short-circuited ·{' '}
          <span className="req ok trial inline" /> half-open trial
        </div>
      </div>
    </div>
  );
}
