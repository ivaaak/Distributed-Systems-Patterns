import { useMemo, useRef, useState } from 'react';
import Explainer from '../components/Explainer.jsx';
import { useCluster, useEvents } from '../lib/cluster.jsx';
import { call, instanceColor } from '../lib/api.js';

const MODES = {
  none: { label: 'No coordination', desc: 'GET → work → SET', color: '#f87171' },
  lock: { label: 'Redis lock', desc: 'SET lock NX PX → GET → work → SET → release', color: '#34d399' },
  atomic: { label: 'Atomic INCR', desc: 'single atomic command', color: '#38bdf8' },
};

/** Greedy interval packing so overlapping ops on the same instance get their own sub-row. */
function packRows(ops) {
  const rowsEnd = [];
  return ops
    .slice()
    .sort((a, b) => a.start - b.start)
    .map((op) => {
      let row = rowsEnd.findIndex((end) => end <= op.start);
      if (row === -1) row = rowsEnd.push(0) - 1;
      rowsEnd[row] = op.end;
      return { ...op, row };
    });
}

export default function DistributedLock() {
  const { instances, pick, callAny } = useCluster();
  const [mode, setMode] = useState('none');
  const [opsCount, setOpsCount] = useState(20);
  const [run, setRun] = useState(null);
  const [holder, setHolder] = useState(null);
  const runSeq = useRef(0);

  useEvents(['lock.'], (e) => {
    if (e.type === 'lock.acquired') setHolder({ instance: e.instance, opId: e.data.opId });
    else setHolder((h) => (h?.opId === e.data.opId ? null : h));
  });

  const start = async () => {
    const id = ++runSeq.current;
    const update = (fn) => setRun((r) => (r?.id === id ? fn(r) : r));
    await callAny('/api/lock/reset', {});
    setRun({ id, mode, total: opsCount, results: [], errors: 0, final: null, running: true });
    await Promise.all(
      Array.from({ length: opsCount }, async (_, i) => {
        const inst = pick();
        try {
          const { ok, data } = await call(inst.url, '/api/lock/op', { mode, opId: `op${i + 1}` }, { timeout: 20000 });
          update((r) => (ok ? { ...r, results: [...r.results, data] } : { ...r, errors: r.errors + 1 }));
        } catch {
          update((r) => ({ ...r, errors: r.errors + 1 }));
        }
      })
    );
    const { data } = await callAny('/api/lock/counter');
    update((r) => ({ ...r, final: data.counter, running: false }));
  };

  const gantt = useMemo(() => {
    if (!run?.results.length) return null;
    const t0 = Math.min(...run.results.map((r) => r.start));
    const t1 = Math.max(...run.results.map((r) => r.end));
    const span = Math.max(t1 - t0, 1);
    const wroteCount = {};
    run.results.forEach((r) => (wroteCount[r.wrote] = (wroteCount[r.wrote] ?? 0) + 1));
    const lanes = [...new Set([...instances.map((i) => i.id), ...run.results.map((r) => r.instance)])].sort();
    return {
      span,
      pct: (t) => ((t - t0) / span) * 100,
      lanes: lanes.map((id) => ({ id, ops: packRows(run.results.filter((r) => r.instance === id)) })),
      collided: (r) => wroteCount[r.wrote] > 1,
    };
  }, [run, instances]);

  const completed = run?.results.length ?? 0;
  const lost = run?.final != null ? completed - run.final : null;

  return (
    <div className="pattern">
      <Explainer
        title="Distributed locks & lost updates"
        subtitle="read-modify-write races across instances · SET NX PX with a unique token · compare-and-delete release"
        tryThis={[
          'Run 20 ops with "No coordination": ops overlap across instances, read the same value and overwrite each other. Red bars wrote a value someone else also wrote.',
          'Run with "Redis lock": ops serialize (hatched = waiting for the lock), the counter is exact — but look how much longer it takes.',
          'Run with "Atomic INCR": correct and fast. Prefer atomic operations / Lua scripts over locks whenever the critical section fits in Redis.',
        ]}
      >
        <p>
          A local mutex protects one process. With several instances, two requests handled by different processes can
          interleave: both <code>GET counter = 7</code>, both <code>SET counter = 8</code> — one increment is lost.
        </p>
        <p>
          A Redis lock is <code>SET lock:counter &lt;random-token&gt; NX PX 3000</code>. <code>NX</code> gives mutual
          exclusion, <code>PX</code> guarantees the lock is freed if the holder dies, and the random token lets the
          holder release it with a Lua compare-and-delete so it never deletes a lock that already expired and was
          taken by someone else. (For correctness-critical locks, combine with fencing tokens — see Leader Election.)
        </p>
      </Explainer>

      <div className="controls card">
        <div className="control-group">
          <label>Strategy</label>
          <div className="segmented">
            {Object.entries(MODES).map(([k, m]) => (
              <button key={k} className={mode === k ? 'active' : ''} style={{ '--c': m.color }} onClick={() => setMode(k)}>
                {m.label}
              </button>
            ))}
          </div>
          <span className="muted small">
            <code>{MODES[mode].desc}</code>
          </span>
        </div>
        <div className="control-group">
          <label>Concurrent ops</label>
          <input type="range" min={5} max={50} value={opsCount} onChange={(e) => setOpsCount(Number(e.target.value))} />
          <strong>{opsCount}</strong>
          <span className="muted small">spread round-robin over live instances</span>
          <button className="btn primary" onClick={start} disabled={run?.running}>
            {run?.running ? 'Running…' : `Run ${opsCount} increments`}
          </button>
        </div>
      </div>

      <div className="stats">
        <div className="stat card">
          <span>expected</span>
          <strong>{run ? completed : '—'}</strong>
          <small>successful increments</small>
        </div>
        <div className="stat card">
          <span>actual counter</span>
          <strong>{run?.final ?? (run?.running ? '…' : '—')}</strong>
          <small>value in Redis</small>
        </div>
        <div className={`stat card ${lost > 0 ? 'bad' : lost === 0 ? 'good' : ''}`}>
          <span>lost updates</span>
          <strong>{lost ?? '—'}</strong>
          <small>{lost > 0 ? 'race condition!' : lost === 0 ? 'consistent ✔' : ''}</small>
        </div>
        <div className="stat card">
          <span>duration</span>
          <strong>{gantt ? `${gantt.span}ms` : '—'}</strong>
          <small>first start → last end</small>
        </div>
        <div className="stat card">
          <span>lock:counter holder</span>
          <strong style={{ color: holder ? instanceColor(holder.instance) : undefined }}>{holder ? holder.instance : 'free'}</strong>
          <small>{holder?.opId ?? ' '}</small>
        </div>
      </div>

      <div className="card">
        <h3>Execution timeline {run && <span className="muted small">({MODES[run.mode].label})</span>}</h3>
        {!gantt && <div className="viz-empty static">Run a batch to see how operations interleave across instances.</div>}
        {gantt &&
          gantt.lanes.map((lane) => {
            const rows = Math.max(1, ...lane.ops.map((o) => o.row + 1));
            return (
              <div key={lane.id} className="lane">
                <div className="lane-label" style={{ '--c': instanceColor(lane.id) }}>
                  {lane.id}
                </div>
                <div className="lane-track" style={{ height: rows * 24 + 6 }}>
                  {lane.ops.map((op) => {
                    const bad = gantt.collided(op);
                    const critStart = op.lockedAt ?? op.start;
                    return (
                      <div key={op.opId}>
                        {op.lockedAt && (
                          <div
                            className="bar waiting"
                            style={{ left: `${gantt.pct(op.start)}%`, width: `${gantt.pct(op.lockedAt) - gantt.pct(op.start)}%`, top: op.row * 24 + 3 }}
                            title={`${op.opId} waited ${op.lockedAt - op.start}ms for the lock`}
                          />
                        )}
                        <div
                          className={`bar ${bad ? 'bad' : ''}`}
                          style={{
                            left: `${gantt.pct(critStart)}%`,
                            width: `${Math.max(0.4, gantt.pct(op.end) - gantt.pct(critStart))}%`,
                            top: op.row * 24 + 3,
                            '--c': instanceColor(lane.id),
                          }}
                          title={`${op.opId}: read ${op.read} → wrote ${op.wrote}`}
                        >
                          {op.read}→{op.wrote}
                        </div>
                      </div>
                    );
                  })}
                </div>
              </div>
            );
          })}
        {gantt && (
          <div className="strip-legend muted small">
            bar = read→write critical section · hatched = waiting for lock · <span className="bad-text">red</span> = wrote a
            value another op also wrote (lost update)
          </div>
        )}
      </div>
    </div>
  );
}
