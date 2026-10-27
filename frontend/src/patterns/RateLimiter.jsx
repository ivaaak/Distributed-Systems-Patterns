import { useEffect, useRef, useState } from 'react';
import Explainer from '../components/Explainer.jsx';
import { useCluster } from '../lib/cluster.jsx';
import { call, instanceColor, sleep } from '../lib/api.js';
import { useNow } from '../hooks/usePackets.jsx';

const VIEW_MS = 20_000;

const MODES = {
  local: { label: 'Per-instance (in-memory)', color: '#fbbf24' },
  global: { label: 'Global (Redis sliding window)', color: '#34d399' },
};

export default function RateLimiter() {
  const { instances, pick, callAny } = useCluster();
  const [mode, setMode] = useState('local');
  const [cfg, setCfg] = useState({ limit: 10, windowMs: 10_000 });
  const [reqs, setReqs] = useState([]);
  const [auto, setAuto] = useState(false);
  const seq = useRef(0);
  const now = useNow(100);

  useEffect(() => {
    callAny('/api/ratelimit/config').then(({ data }) => data?.limit && setCfg(data)).catch(() => {});
  }, [callAny]);

  const hit = async () => {
    const inst = pick();
    if (!inst) return;
    const reqId = ++seq.current;
    try {
      const { data } = await call(inst.url, '/api/ratelimit/hit', { mode, reqId });
      setReqs((r) => [...r.filter((x) => x.at > Date.now() - VIEW_MS - 2000), { ...data, at: Date.now() }]);
    } catch {
      /* instance died mid-request */
    }
  };

  const burst = async (n) => {
    for (let i = 0; i < n; i++) {
      hit();
      await sleep(60);
    }
  };

  const hitRef = useRef(hit);
  hitRef.current = hit;
  useEffect(() => {
    if (!auto) return;
    const t = setInterval(() => hitRef.current(), 400);
    return () => clearInterval(t);
  }, [auto]);

  const reset = async () => {
    await callAny('/api/ratelimit/reset', {});
    setReqs([]);
  };

  const switchMode = async (m) => {
    setMode(m);
    await reset();
  };

  const inWindow = reqs.filter((r) => r.at > now - cfg.windowMs);
  const allowedInWindow = inWindow.filter((r) => r.allowed).length;
  const lanes = [...new Set([...instances.map((i) => i.id), ...reqs.map((r) => r.instance)])].sort();
  const x = (t) => 100 - ((now - t) / VIEW_MS) * 100;
  const over = allowedInWindow > cfg.limit;

  return (
    <div className="pattern">
      <Explainer
        title="Distributed rate limiting"
        subtitle={`limit: ${cfg.limit} requests / ${cfg.windowMs / 1000}s per user · sliding window`}
        tryThis={[
          'In "Per-instance" mode, fire a burst of 40. Each instance counts only what it sees, so the user gets ~3× the limit through.',
          'Switch to "Global": every instance runs the same Lua script against one sorted set in Redis — exactly the limit gets through, regardless of which instance serves the request.',
          'Turn on steady traffic (2.5 req/s) and watch the window slide: old requests fall out and new ones are admitted.',
          'Crash an instance: in Global mode the remaining instances keep enforcing the same shared budget.',
        ]}
      >
        <p>
          In-memory limiters break once you scale out: with N instances behind a round-robin balancer, each one sees
          ~1/N of a user&apos;s traffic, so the effective limit becomes N × limit — and it changes every time you
          autoscale.
        </p>
        <p>
          The global limiter keeps one <code>ZSET</code> per user in Redis (score = timestamp). A Lua script atomically
          trims entries older than the window, counts, and adds the new request only if under the limit. It uses{' '}
          <code>redis.call(&apos;TIME&apos;)</code> so skewed instance clocks can&apos;t disagree about the window.
        </p>
      </Explainer>

      <div className="controls card">
        <div className="control-group">
          <label>Limiter</label>
          <div className="segmented">
            {Object.entries(MODES).map(([k, m]) => (
              <button key={k} className={mode === k ? 'active' : ''} style={{ '--c': m.color }} onClick={() => switchMode(k)}>
                {m.label}
              </button>
            ))}
          </div>
        </div>
        <div className="control-group">
          <label>Traffic</label>
          <button className="btn" onClick={hit}>
            1 request
          </button>
          <button className="btn" onClick={() => burst(15)}>
            Burst 15
          </button>
          <button className="btn primary" onClick={() => burst(40)}>
            Burst 40
          </button>
          <label className="toggle">
            <input type="checkbox" checked={auto} onChange={(e) => setAuto(e.target.checked)} /> steady 2.5 req/s
          </label>
          <button className="btn ghost" onClick={reset}>
            Reset
          </button>
        </div>
      </div>

      <div className="stats">
        <div className={`stat card ${over ? 'bad' : allowedInWindow ? 'good' : ''}`}>
          <span>allowed in current window</span>
          <strong>
            {allowedInWindow} <small>/ {cfg.limit}</small>
          </strong>
          <small>{over ? `limit exceeded ${(allowedInWindow / cfg.limit).toFixed(1)}×!` : 'within limit'}</small>
        </div>
        <div className="stat card">
          <span>rejected (429) in window</span>
          <strong>{inWindow.length - allowedInWindow}</strong>
          <small>&nbsp;</small>
        </div>
        {lanes.map((id) => (
          <div key={id} className="stat card" style={{ '--c': instanceColor(id) }}>
            <span className="swatch-label">{id} allowed</span>
            <strong>{inWindow.filter((r) => r.instance === id && r.allowed).length}</strong>
            <small>of {inWindow.filter((r) => r.instance === id).length} it received</small>
          </div>
        ))}
      </div>

      <div className="card">
        <h3>Requests over time — last {VIEW_MS / 1000}s</h3>
        <div className="rl-chart">
          <div className="rl-window" style={{ left: `${x(now - cfg.windowMs)}%`, width: `${(cfg.windowMs / VIEW_MS) * 100}%` }}>
            <span>current {cfg.windowMs / 1000}s window</span>
          </div>
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
                      key={r.reqId}
                      className={`req ${r.allowed ? 'ok' : 'no'}`}
                      style={{ left: `${x(r.at)}%` }}
                      title={`#${r.reqId} ${r.allowed ? 'allowed' : '429'} (${r.mode}, count ${r.count})`}
                    />
                  ))}
              </div>
            </div>
          ))}
        </div>
        <div className="strip-legend muted small">
          <span className="req ok inline" /> allowed · <span className="req no inline" /> rejected with 429
        </div>
      </div>
    </div>
  );
}
