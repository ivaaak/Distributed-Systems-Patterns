import { useState } from 'react';
import { usePolling } from '../lib/page.js';
import Explainer from '../components/Explainer.jsx';
import { useCluster } from '../lib/cluster.jsx';
import { call, instanceColor } from '../lib/api.js';

const SIZE = 460;
const C = SIZE / 2;
const R = 175;
const MAX = 2 ** 32;
const angle = (h) => (h / MAX) * Math.PI * 2 - Math.PI / 2;
const at = (h, r) => ({ x: C + r * Math.cos(angle(h)), y: C + r * Math.sin(angle(h)) });
const pct = (x) => (x == null ? '—' : `${Math.round(x * 100)}%`);

function arc(h1, h2, r) {
  const a = at(h1, r);
  const b = at(h2, r);
  let span = h2 - h1;
  if (span < 0) span += MAX;
  return `M ${a.x} ${a.y} A ${r} ${r} 0 ${span > MAX / 2 ? 1 : 0} 1 ${b.x} ${b.y}`;
}

export default function Sharding() {
  const { instances, pick, byId, callAny } = useCluster();
  const [state, setState] = useState(null);
  const [key, setKey] = useState('user:42');
  const [value, setValue] = useState('Grace Hopper');
  const [via, setVia] = useState('lb');
  const [lookup, setLookup] = useState(null);
  const [rebalance, setRebalance] = useState(null);
  const [busy, setBusy] = useState(false);

  const refresh = () =>
    callAny('/api/shard/state')
      .then(({ data }) => setState(data))
      .catch(() => {});

  usePolling(refresh, 1000);

  const act = async (fn) => {
    setBusy(true);
    try {
      await fn();
      await refresh();
    } finally {
      setBusy(false);
    }
  };

  const setStrategy = (strategy) =>
    act(async () => {
      await callAny('/api/shard/strategy', { strategy });
      await callAny('/api/shard/seed', { count: 90 });
      setRebalance(null);
    });

  const toggleMember = (id, include) =>
    act(async () => {
      await callAny('/api/shard/members', { instance: id, include });
      setRebalance(null);
    });

  const request = async (method) => {
    const inst = via === 'lb' ? pick() : byId(via);
    if (!inst) return;
    const path = `/api/shard/kv/${encodeURIComponent(key)}`;
    try {
      const { data, status } = await call(inst.url, path, method === 'PUT' ? { value } : undefined, { method });
      setLookup({ method, status, entry: inst.id, ...data });
      refresh();
    } catch (err) {
      setLookup({ method, entry: inst.id, error: err.message });
    }
  };

  const members = state?.members ?? [];
  const ring = state?.ring ?? [];
  const keys = state?.keys ?? [];
  const misplaced = keys.filter((k) => k.storedOn !== k.owner);
  const counts = {};
  keys.forEach((k) => (counts[k.storedOn] = (counts[k.storedOn] ?? 0) + 1));
  const lookedUp = lookup && keys.find((k) => k.key === lookup.key);

  return (
    <div className="pattern">
      <Explainer
        title="Sharding with consistent hashing"
        subtitle="partitioned key-value store · request forwarding · hash ring with virtual nodes vs hash % N"
        tryThis={[
          'GET a key via the load balancer: if the receiving instance isn\'t the owner it forwards the request (see the hop path).',
          'Remove a node from the ring. Hollow red keys are now stored on the wrong node; press Rebalance to migrate them.',
          'Compare the "what-if" numbers: removing a node moves ~1/N of keys on the ring but most keys with modulo.',
          'Switch to modulo (reseeds) and repeat — watch nearly every key turn red.',
          'Crash the owner of a key and read it: the request is routed correctly but the owner is down → why real systems replicate each shard.',
        ]}
      >
        <p>
          One Redis can&apos;t hold everything forever; at some point data is <strong>partitioned</strong> across nodes. The
          question is which node owns which key, and how much data must move when nodes join or leave.
        </p>
        <p>
          With <code>hash(key) % N</code>, changing N changes almost every key&apos;s owner. With a <strong>hash ring</strong>
          each node owns {state?.vnodes ?? 32} virtual points on a 2³² circle and a key belongs to the next point clockwise, so
          adding/removing a node only moves the keys in its arcs (≈1/N). Each instance here stores its partition in{' '}
          <code>shard:data:&lt;instance&gt;</code> and forwards requests for keys it doesn&apos;t own.
        </p>
      </Explainer>

      <div className="controls card">
        <div className="control-group">
          <label>Strategy</label>
          <div className="segmented">
            {['ring', 'modulo'].map((s) => (
              <button key={s} className={state?.strategy === s ? 'active' : ''} disabled={busy} onClick={() => setStrategy(s)}>
                {s === 'ring' ? 'Consistent hash ring' : 'hash % N'}
              </button>
            ))}
          </div>
          <button className="btn" disabled={busy} onClick={() => act(() => callAny('/api/shard/seed', { count: 90 }))}>
            Reseed 90 keys
          </button>
          <span className="muted small">switching strategy reseeds so every key starts on its correct node</span>
        </div>
        <div className="control-group">
          <label>Ring members</label>
          {(state?.registered ?? []).map((id) => {
            const inRing = members.includes(id);
            const w = state.whatIf[id];
            return (
              <div key={id} className={`member-chip ${inRing ? 'on' : ''}`} style={{ '--c': instanceColor(id) }}>
                <button className="btn tiny" disabled={busy} onClick={() => toggleMember(id, !inRing)}>
                  {inRing ? `Remove ${id}` : `Add ${id}`}
                </button>
                <small title="Fraction of keys that would change owner">
                  moves ring {pct(w?.ring)} · modulo {pct(w?.modulo)}
                </small>
              </div>
            );
          })}
        </div>
        <div className="control-group">
          <label>Migration</label>
          <button className="btn primary" disabled={busy || !misplaced.length} onClick={() => act(async () => setRebalance((await callAny('/api/shard/rebalance', {})).data))}>
            Rebalance {misplaced.length ? `(${misplaced.length} misplaced)` : ''}
          </button>
          {rebalance && <span className="muted small">moved {rebalance.moved} keys</span>}
        </div>
      </div>

      <div className="grid-2">
        <div className="card viz">
          <svg viewBox={`0 0 ${SIZE} ${SIZE}`} className="diagram ring">
            <circle cx={C} cy={C} r={R} className="orbit" />
            {ring.map((p, i) => {
              const prev = ring[(i - 1 + ring.length) % ring.length];
              return (
                <path
                  key={`arc-${i}`}
                  d={arc(prev.point, p.point, R)}
                  className="ring-arc"
                  style={{ stroke: instanceColor(p.instance) }}
                />
              );
            })}
            {ring.map((p) => {
              const a = at(p.point, R - 7);
              const b = at(p.point, R + 7);
              return <line key={`${p.instance}-${p.vnode}`} x1={a.x} y1={a.y} x2={b.x} y2={b.y} className="vnode" style={{ stroke: instanceColor(p.instance) }} />;
            })}
            {keys.map((k) => {
              const p = at(k.hash, R - 26);
              const bad = k.storedOn !== k.owner;
              const hit = lookedUp?.key === k.key;
              return (
                <circle
                  key={k.key}
                  cx={p.x}
                  cy={p.y}
                  r={hit ? 7 : 4}
                  className={`key-dot ${bad ? 'misplaced' : ''} ${hit ? 'hit' : ''}`}
                  style={{ '--c': instanceColor(k.storedOn) }}
                >
                  <title>{`${k.key} → stored on ${k.storedOn}, owner ${k.owner}`}</title>
                </circle>
              );
            })}
            <text x={C} y={C - 8} textAnchor="middle" className="box-title">
              {state?.strategy === 'modulo' ? 'hash % N' : 'hash ring'}
            </text>
            <text x={C} y={C + 12} textAnchor="middle" className="box-sub">
              {keys.length} keys · {members.length} nodes
            </text>
            <text x={C} y={C + 30} textAnchor="middle" className="box-sub faint">
              {state?.strategy === 'modulo' ? 'position = hash, color = node' : 'arcs/ticks = virtual nodes'}
            </text>
          </svg>
          <div className="strip-legend muted small">
            dot = key at its hash position, colored by the node storing it · hollow red = stored on a node that no longer owns it
          </div>
        </div>

        <div className="card">
          <h3>Keys per node</h3>
          {(state?.registered ?? []).map((id) => (
            <div key={id} className="dist-row">
              <span className="tag" style={{ '--c': instanceColor(id) }}>
                {id}
              </span>
              <div className="dist-bar">
                <div style={{ width: `${((counts[id] ?? 0) / Math.max(keys.length, 1)) * 100}%`, '--c': instanceColor(id) }} />
              </div>
              <span className="mono small">
                {counts[id] ?? 0}
                {!members.includes(id) && counts[id] ? ' (draining)' : ''}
              </span>
            </div>
          ))}

          <h3 style={{ marginTop: 18 }}>Routed request</h3>
          <div className="control-group">
            <input className="input" value={key} onChange={(e) => setKey(e.target.value)} style={{ width: 110 }} />
            <input className="input grow" value={value} onChange={(e) => setValue(e.target.value)} />
          </div>
          <div className="control-group" style={{ marginTop: 8 }}>
            <select className="input" value={via} onChange={(e) => setVia(e.target.value)}>
              <option value="lb">via LB</option>
              {instances.map((i) => (
                <option key={i.id} value={i.id} disabled={!i.alive}>
                  via {i.id}
                </option>
              ))}
            </select>
            <button className="btn" onClick={() => request('GET')}>
              GET
            </button>
            <button className="btn" onClick={() => request('PUT')}>
              PUT
            </button>
          </div>
          {lookup && (
            <div className={`hops ${lookup.error || lookup.status >= 400 ? 'bad' : ''}`}>
              <div className="hop-path">
                <span className="hop client">client</span>→
                <span className="hop" style={{ '--c': instanceColor(lookup.receivedBy ?? lookup.entry) }}>
                  {lookup.receivedBy ?? lookup.entry}
                </span>
                {lookup.receivedBy && lookup.servedBy && (
                  <>
                    → <span className="hop" style={{ '--c': instanceColor(lookup.servedBy) }}>{lookup.servedBy}</span>
                  </>
                )}
                {lookup.receivedBy && lookup.error && (
                  <>
                    → <span className="hop dead">{lookup.owner} ✖</span>
                  </>
                )}
              </div>
              <div className="mono small">
                {lookup.error
                  ? lookup.error
                  : `${lookup.method} ${lookup.key}: ${lookup.found ? JSON.stringify(lookup.value) : 'NOT FOUND on owner (not migrated yet?)'} · owner ${lookup.owner}${lookup.receivedBy ? ' (forwarded)' : ' (served locally)'}`}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
