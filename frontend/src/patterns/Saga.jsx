import { useState } from 'react';
import { usePolling } from '../lib/page.js';
import Explainer from '../components/Explainer.jsx';
import { useCluster } from '../lib/cluster.jsx';
import { call, instanceColor } from '../lib/api.js';

const STOCK0 = 100;
const PRICE = 25;
const STATUS_CLASS = { running: 'running', compensating: 'warn', completed: 'good', compensated: 'comp' };

function phaseOf(log, name, kind) {
  const [started, done, failed] = kind === 'forward' ? ['step_started', 'step_done', 'step_failed'] : ['comp_started', 'comp_done', null];
  let state = { status: 'pending' };
  for (const e of log) {
    if (e.step !== name) continue;
    if (e.phase === started) state = { status: 'running', instance: e.instance, since: e.ts };
    if (e.phase === done) state = { status: 'done', instance: e.instance };
    if (failed && e.phase === failed) state = { status: 'failed', instance: e.instance };
  }
  return state;
}

function StepBox({ label, phase, aliveIds }) {
  const orphan = phase.status === 'running' && !aliveIds.has(phase.instance);
  return (
    <div className={`step ${phase.status} ${orphan ? 'orphan' : ''}`} style={{ '--c': instanceColor(phase.instance) }}>
      <span className="step-name">{label}</span>
      <span className="step-meta">
        {phase.status === 'pending' && 'pending'}
        {phase.status === 'running' && (orphan ? `${phase.instance} died here` : `running on ${phase.instance}`)}
        {phase.status === 'done' && `✓ ${phase.instance}`}
        {phase.status === 'failed' && `✗ failed on ${phase.instance}`}
      </span>
    </div>
  );
}

export default function Saga() {
  const { alive, byId, crash, callAny, pick } = useCluster();
  const [state, setState] = useState(null);
  const [failAt, setFailAt] = useState('none');
  const [stepMs, setStepMs] = useState(1500);

  usePolling(() => callAny('/api/saga/state').then(({ data }) => setState(data)), 500);

  const start = async (n = 1) => {
    for (let i = 0; i < n; i++) {
      const inst = pick();
      if (inst) await call(inst.url, '/api/saga', { failAt, stepMs, amount: PRICE });
    }
  };

  const steps = state?.steps ?? [];
  const sagas = state?.sagas ?? [];
  const services = state?.services;
  const aliveIds = new Set(alive.map((i) => i.id));
  const completed = state?.totals.completed ?? 0;
  const inFlight = (state?.totals.active ?? 0) > 0;
  const expected = { stock: STOCK0 - completed, revenue: completed * PRICE, shipments: completed };
  const consistent = services && services.stock === expected.stock && services.revenue === expected.revenue && services.shipments === expected.shipments;

  return (
    <div className="pattern">
      <Explainer
        title="Saga orchestration with compensations"
        subtitle="durable saga state in Redis · per-saga lease · takeover on crash · idempotent 'apply once' side effects"
        tryThis={[
          'Start an order saga that succeeds: inventory, payment and shipping services update one step at a time.',
          'Set "fail at createShipment": after shipping fails, the orchestrator refunds the payment and releases the inventory (compensations run in reverse).',
          'Start a slow saga and crash its orchestrator mid-step. Within ~3s its lease expires and another instance resumes the saga from the last persisted step.',
          'When nothing is in flight, the services must satisfy the invariant — no leaked inventory or money, even across crashes.',
        ]}
      >
        <p>
          An order touches several services (inventory, payments, shipping) that can&apos;t share one database transaction,
          and two-phase commit would couple their availability. A <strong>saga</strong> executes local steps one at a time;
          if one fails, it runs <strong>compensating actions</strong> for the steps already done. The system is temporarily
          inconsistent but converges.
        </p>
        <p>
          The orchestrator itself can crash, so its progress is persisted in <code>saga:&lt;id&gt;</code> and guarded by a
          renewable lease. A survivor that finds an expired lease takes over. Because a step may be re-run after a crash, every
          effect is applied through a Lua &quot;apply once&quot; script keyed by saga + step.
        </p>
      </Explainer>

      <div className="controls card">
        <div className="control-group">
          <label>New order</label>
          <select className="input" value={failAt} onChange={(e) => setFailAt(e.target.value)}>
            <option value="none">all steps succeed</option>
            {steps.slice(1).map((s) => (
              <option key={s.name} value={s.name}>
                fail at {s.name}
              </option>
            ))}
          </select>
          <span className="muted small">step duration</span>
          <input type="range" min={300} max={4000} step={100} value={stepMs} onChange={(e) => setStepMs(Number(e.target.value))} />
          <strong>{(stepMs / 1000).toFixed(1)}s</strong>
          <button className="btn primary" onClick={() => start(1)}>
            Start saga
          </button>
          <button className="btn" onClick={() => start(3)}>
            Start 3
          </button>
          <button className="btn ghost" onClick={() => callAny('/api/saga/reset', {})}>
            Reset
          </button>
        </div>
      </div>

      <div className="stats">
        <div className="stat card">
          <span>📦 inventory stock</span>
          <strong>{services?.stock ?? '—'}</strong>
          <small>expected {expected.stock}</small>
        </div>
        <div className="stat card">
          <span>💳 revenue</span>
          <strong>${services?.revenue ?? '—'}</strong>
          <small>expected ${expected.revenue}</small>
        </div>
        <div className="stat card">
          <span>🚚 shipments</span>
          <strong>{services?.shipments ?? '—'}</strong>
          <small>expected {expected.shipments}</small>
        </div>
        <div className={`stat card ${inFlight ? '' : consistent ? 'good' : 'bad'}`}>
          <span>invariant</span>
          <strong>{inFlight ? '…' : consistent ? '✓' : '✗'}</strong>
          <small>{inFlight ? 'sagas in flight — temporarily inconsistent is OK' : consistent ? 'services agree' : 'services disagree'}</small>
        </div>
      </div>

      <div className="sagas">
        {sagas.length === 0 && <div className="card viz-empty static">No sagas yet — start one above.</div>}
        {sagas.map((s) => {
          const log = s.log ?? [];
          const failed = log.some((e) => e.phase === 'step_failed');
          const takeovers = log.filter((e) => e.phase === 'resumed');
          const running = s.status === 'running' || s.status === 'compensating';
          const ownerAlive = aliveIds.has(s.owner);
          return (
            <div key={s.id} className={`card saga ${STATUS_CLASS[s.status]}`}>
              <header>
                <strong>{s.id}</strong>
                <span className={`badge status ${STATUS_CLASS[s.status]}`}>{s.status}</span>
                <span className="muted small">orchestrator</span>
                <span className="tag" style={{ '--c': instanceColor(s.owner) }}>
                  {s.owner}
                  {!ownerAlive && running ? ' (down)' : ''}
                </span>
                {takeovers.map((t, i) => (
                  <span key={i} className="badge warn">
                    resumed by {t.instance} (from {t.from})
                  </span>
                ))}
                {running && ownerAlive && byId(s.owner) && (
                  <button className="btn tiny danger" onClick={() => crash(s.owner)}>
                    Crash {s.owner}
                  </button>
                )}
              </header>
              <div className="pipeline">
                {steps.map((st, i) => (
                  <div key={st.name} className="pipe-col">
                    <StepBox label={st.name} phase={phaseOf(log, st.name, 'forward')} aliveIds={aliveIds} />
                    {failed && phaseOf(log, st.name, 'forward').status === 'done' && (
                      <>
                        <div className="comp-arrow">↓ compensate</div>
                        <StepBox label={st.compensation} phase={phaseOf(log, st.compensation, 'comp')} aliveIds={aliveIds} />
                      </>
                    )}
                    {i < steps.length - 1 && <span className="pipe-arrow">→</span>}
                  </div>
                ))}
              </div>
              <ol className="saga-log">
                {log.slice(-5).map((e, i) => (
                  <li key={i}>
                    <span className="tag" style={{ '--c': instanceColor(e.instance) }}>
                      {e.instance}
                    </span>{' '}
                    {e.phase.replace('_', ' ')} {e.step ?? ''}
                  </li>
                ))}
              </ol>
            </div>
          );
        })}
      </div>
    </div>
  );
}
