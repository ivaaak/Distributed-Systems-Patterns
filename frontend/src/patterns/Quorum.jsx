import { useRef, useState } from 'react';
import { usePolling } from '../lib/page.js';
import Explainer from '../components/Explainer.jsx';
import { useCluster } from '../lib/cluster.jsx';
import { call, instanceColor } from '../lib/api.js';

const KEYS = ['x', 'y', 'z'];

const newer = (a, b) => Boolean(a) && (!b || a.ts > b.ts || (a.ts === b.ts && a.node > b.node));
const fmtVersion = (v) => (v ? `${v.value}` : '—');

function ReplicaChips({ replicas, want }) {
  return (
    <span className="q-chips">
      {replicas.map((r) => {
        const kind = r.pending ? 'pending' : r.ok ? 'ok' : 'fail';
        return (
          <span key={r.replica} className={`q-chip ${kind}`} style={{ '--c': instanceColor(r.replica) }} title={r.error ?? ''}>
            {r.replica}
            {kind === 'ok' && (want === 'read' ? ` ${fmtVersion(r.version)}` : ' ✓')}
            {kind === 'fail' && ' ✗'}
            {kind === 'pending' && ' …'}
            {!r.pending && <small>{r.ms}ms</small>}
          </span>
        );
      })}
    </span>
  );
}

export default function Quorum() {
  const { pick, callAny } = useCluster();
  const [state, setState] = useState({ config: { w: 2, r: 2, readRepair: true, slow: null }, n: 0, replicas: [] });
  const [key, setKey] = useState('x');
  const [ops, setOps] = useState([]);
  const [stats, setStats] = useState({ reads: 0, stale: 0 });
  const lastWrite = useRef({}); // key -> newest acknowledged version (what a client expects to read)
  const busy = useRef(false);

  usePolling(() => callAny('/api/quorum/state').then(({ data }) => data.replicas && setState(data)), 700);

  const { config, n } = state;
  const logOp = (op) => setOps((o) => [{ ...op, id: Math.random() }, ...o].slice(0, 14));

  const maxOnReplicas = (k) =>
    state.replicas.reduce((m, r) => Math.max(m, Number(r.data[k]?.value ?? 0)), Number(lastWrite.current[k]?.value ?? 0));

  const write = async () => {
    const inst = pick();
    if (!inst) return;
    const value = String(maxOnReplicas(key) + 1);
    try {
      const { ok, data } = await call(inst.url, `/api/quorum/kv/${key}`, { value }, { method: 'PUT', timeout: 5000 });
      if (ok && newer(data.version, lastWrite.current[key])) lastWrite.current[key] = data.version;
      logOp({ type: 'write', key, value, ok, coordinator: inst.id, ms: data.ms, replicas: data.replicas ?? [], note: ok ? `acked by ${data.acks}/${data.w}` : data.error });
    } catch {
      logOp({ type: 'write', key, value, ok: false, coordinator: inst.id, replicas: [], note: 'coordinator unreachable' });
    }
  };

  const read = async () => {
    const inst = pick();
    if (!inst) return;
    try {
      const { ok, data } = await call(inst.url, `/api/quorum/kv/${key}`, undefined, { timeout: 5000 });
      const expected = lastWrite.current[key];
      const stale = ok && newer(expected, data.version);
      if (ok) setStats((s) => ({ reads: s.reads + 1, stale: s.stale + (stale ? 1 : 0) }));
      logOp({
        type: 'read',
        key,
        value: ok ? fmtVersion(data.version) : null,
        ok,
        stale,
        coordinator: inst.id,
        ms: data.ms,
        replicas: data.replicas ?? [],
        note: !ok ? data.error : stale ? `STALE — latest acked write is ${expected.value}` : `newest of first ${data.r}`,
      });
    } catch {
      logOp({ type: 'read', key, ok: false, coordinator: inst.id, replicas: [], note: 'coordinator unreachable' });
    }
  };

  const readMany = async () => {
    if (busy.current) return;
    busy.current = true;
    for (let i = 0; i < 10; i++) await read();
    busy.current = false;
  };

  const setConfig = (patch) => {
    setState((s) => ({ ...s, config: { ...s.config, ...patch } }));
    callAny('/api/quorum/config', patch).catch(() => {});
  };

  const antiEntropy = async () => {
    const { data } = await callAny('/api/quorum/anti-entropy', {});
    logOp({ type: 'repair', key: '*', ok: true, replicas: [], note: `anti-entropy repaired ${data.repaired?.length ?? 0} replica entries` });
  };

  const reset = async () => {
    await callAny('/api/quorum/reset', {});
    lastWrite.current = {};
    setOps([]);
    setStats({ reads: 0, stale: 0 });
  };

  const overlap = config.r + config.w > n;
  const allKeys = [...new Set([...KEYS, ...state.replicas.flatMap((r) => Object.keys(r.data))])].sort();
  const counts = Array.from({ length: Math.max(n, 1) }, (_, i) => i + 1);

  return (
    <div className="pattern">
      <Explainer
        title="Quorum replication"
        subtitle="N replicas · write to W · read from R · R + W > N ⇒ read and write quorums overlap · read repair & anti-entropy"
        tryThis={[
          'With W=2, R=2 (N=3), crash api-3 from the bar above and write x a few times: writes still succeed with 2 acks. Recover api-3: its copy of x is stale (red).',
          'Read x ×10: every read returns the latest value, because any 2 replicas include at least one that has it. Read repair quietly fixes api-3.',
          'Now set W=1, R=1 (R+W ≤ N) and turn read repair off. Crash api-3, write x, recover it and Read ×10: reads that reach the stale replica first return an old value.',
          'Make one replica slow and compare write latency for W=1 vs W=3 — waiting for all replicas means waiting for the slowest one.',
          'With stale copies still around, press Anti-entropy: it compares whole replicas and fixes every key at once.',
        ]}
      >
        <p>
          Leaderless stores (Dynamo, Cassandra, Riak) keep N copies of each key and let any node coordinate a request. A write
          is sent to all N replicas and succeeds once <strong>W</strong> acknowledge; a read asks all N and returns the newest
          version among the first <strong>R</strong> answers. Versions here are last-write-wins timestamps.
        </p>
        <p>
          The knobs trade latency and availability for consistency. With <code>R + W &gt; N</code> the replicas you read from
          always include one that saw the latest acknowledged write. With smaller quorums you get faster, more available
          operations and <strong>eventual</strong> consistency: replicas that missed a write are stale until{' '}
          <em>read repair</em> or <em>anti-entropy</em> catches them up.
        </p>
        <p className="muted small">
          Simplifications: replicas live in each instance&apos;s memory; there are no hinted handoffs or sloppy quorums; and a
          write that fails to reach W is not rolled back on the replicas that did take it. Real systems also have to handle
          concurrent writes (vector clocks or CRDTs) instead of relying on synchronized clocks.
        </p>
      </Explainer>

      <div className="controls card">
        <div className="control-group">
          <label>Quorums</label>
          <span className="muted small">W</span>
          <div className="segmented">
            {counts.map((k) => (
              <button key={k} className={config.w === k ? 'active' : ''} onClick={() => setConfig({ w: k })}>
                {k}
              </button>
            ))}
          </div>
          <span className="muted small">R</span>
          <div className="segmented">
            {counts.map((k) => (
              <button key={k} className={config.r === k ? 'active' : ''} onClick={() => setConfig({ r: k })}>
                {k}
              </button>
            ))}
          </div>
          <span className={`q-verdict ${overlap ? 'good' : 'bad'}`}>
            R + W = {config.r + config.w} {overlap ? '>' : '≤'} N = {n} → {overlap ? 'quorums overlap (latest write always seen)' : 'stale reads possible'}
          </span>
        </div>
        <div className="control-group">
          <label>Replicas</label>
          <label className="toggle">
            <input type="checkbox" checked={config.readRepair} onChange={(e) => setConfig({ readRepair: e.target.checked })} /> read repair
          </label>
          <span className="muted small">slow replica (+1.2s)</span>
          <div className="segmented">
            {[null, ...state.replicas.map((r) => r.id)].map((id) => (
              <button key={id ?? 'none'} className={config.slow === id ? 'active' : ''} style={id ? { '--c': instanceColor(id) } : undefined} onClick={() => setConfig({ slow: id })}>
                {id ?? 'none'}
              </button>
            ))}
          </div>
          <button className="btn" onClick={antiEntropy}>
            Anti-entropy
          </button>
        </div>
        <div className="control-group">
          <label>Client</label>
          <div className="segmented">
            {KEYS.map((k) => (
              <button key={k} className={key === k ? 'active' : ''} onClick={() => setKey(k)}>
                {k}
              </button>
            ))}
          </div>
          <button className="btn primary" onClick={write}>
            Write {key} = {maxOnReplicas(key) + 1}
          </button>
          <button className="btn" onClick={read}>
            Read {key}
          </button>
          <button className="btn" onClick={readMany}>
            Read {key} ×10
          </button>
          <button className="btn ghost" onClick={reset}>
            Reset
          </button>
        </div>
      </div>

      <div className="stats">
        <div className="stat card">
          <span>replicas (N)</span>
          <strong>{n}</strong>
          <small>{state.replicas.filter((r) => r.reachable && !r.crashed).length} up</small>
        </div>
        <div className="stat card">
          <span>reads</span>
          <strong>{stats.reads}</strong>
          <small>since reset</small>
        </div>
        <div className={`stat card ${stats.stale ? 'bad' : stats.reads ? 'good' : ''}`}>
          <span>stale reads</span>
          <strong>{stats.stale}</strong>
          <small>older than the last acknowledged write</small>
        </div>
      </div>

      <div className="grid-2">
        <div className="card">
          <h3>Replica contents</h3>
          <div className="table-scroll">
            <table className="table">
              <thead>
                <tr>
                  <th>key</th>
                  {state.replicas.map((r) => (
                    <th key={r.id} style={{ color: instanceColor(r.id) }}>
                      {r.id}
                      {r.crashed ? ' 💥' : !r.reachable ? ' ⛔' : ''}
                      {config.slow === r.id ? ' 🐢' : ''}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {allKeys.map((k) => {
                  const latest = state.replicas.reduce((best, r) => (newer(r.data[k], best) ? r.data[k] : best), null);
                  return (
                    <tr key={k} className={k === key ? 'q-selected' : ''}>
                      <td className="mono">{k}</td>
                      {state.replicas.map((r) => {
                        const v = r.data[k];
                        const stale = latest && newer(latest, v);
                        return (
                          <td key={r.id} className={`cache-cell ${!r.reachable ? '' : stale ? 'stale' : v ? 'fresh' : ''}`} title={v ? `ts ${v.ts} by ${v.node}` : 'no value'}>
                            {fmtVersion(v)} {stale && <b>STALE</b>}
                          </td>
                        );
                      })}
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
          <p className="muted small">
            💥 crashed (still shown so you can see what it missed) · 🐢 slow · <span className="bad-text">STALE</span> = older than
            the newest copy on any replica
          </p>
        </div>

        <div className="card">
          <h3>Operations</h3>
          <div className="q-ops">
            {ops.map((op) => (
              <div key={op.id} className={`q-op ${!op.ok ? 'bad' : op.stale ? 'bad' : 'good'}`}>
                <header>
                  <strong>{op.type === 'write' ? `write ${op.key}=${op.value}` : op.type === 'read' ? `read ${op.key} → ${op.value ?? '✗'}` : 'anti-entropy'}</strong>
                  {op.coordinator && (
                    <span className="muted small">
                      via <span style={{ color: instanceColor(op.coordinator) }}>{op.coordinator}</span>
                    </span>
                  )}
                  {op.ms != null && <span className="muted small mono">{op.ms}ms</span>}
                </header>
                {op.replicas.length > 0 && <ReplicaChips replicas={op.replicas} want={op.type} />}
                <div className={`small ${op.ok && !op.stale ? 'muted' : 'bad-text'}`}>{op.note}</div>
              </div>
            ))}
            {ops.length === 0 && <div className="viz-empty static">Write and read a key to see the quorums at work.</div>}
          </div>
          <p className="muted small">Replica chips show who answered before the coordinator replied; … = still in flight.</p>
        </div>
      </div>
    </div>
  );
}
