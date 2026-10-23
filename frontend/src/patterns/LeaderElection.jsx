import { useState } from 'react';
import { usePolling } from '../lib/page.js';
import Explainer from '../components/Explainer.jsx';
import { useCluster, useEvents } from '../lib/cluster.jsx';
import { call, instanceColor } from '../lib/api.js';
import { useNow } from '../hooks/usePackets.jsx';

const SIZE = 420;
const C = SIZE / 2;
const R = 150;
const STRIP_WINDOW_MS = 60_000;

const LABELS = {
  'leader.elected': (d) => `won election → epoch ${d.epoch}`,
  'leader.lost': (d) => `lost leadership (epoch ${d.epoch}): ${d.reason}`,
  'leader.resigned': (d) => `resigned gracefully (epoch ${d.epoch})`,
  'leader.job': (d) => `ran singleton job with fencing token ${d.epoch} ✔`,
  'leader.fenced': (d) => `write REJECTED: token ${d.epoch} < ${d.currentEpoch} (stale leader)`,
  'leader.stalled': (d) => `frozen for ${d.ms / 1000}s (GC pause)${d.wasLeader ? ' while leader' : ''}`,
  'leader.resumed': (d) => `woke up — still thinks it's leader: ${d.stillThinksLeader}`,
  'instance.crashed': () => 'crashed',
  'instance.recovered': () => 'recovered',
};

export default function LeaderElection() {
  const { instances, byId, crash, recover, callAny } = useCluster();
  const [state, setState] = useState(null);
  const [fetchedAt, setFetchedAt] = useState(0);
  const [history, setHistory] = useState([]);
  const now = useNow(100);

  usePolling(async () => {
    const { data } = await callAny('/api/leader/state');
    setState(data);
    setFetchedAt(Date.now());
  }, 400);

  useEvents(['leader.', 'instance.crashed', 'instance.recovered'], (e) => {
    setHistory((h) => [e, ...h].slice(0, 200));
  });

  const leaseLeft = state?.leader ? Math.max(0, state.ttlMs - (now - fetchedAt)) : 0;
  const leaseFrac = state?.leaseMs ? leaseLeft / state.leaseMs : 0;

  const positions = {};
  instances.forEach((inst, i) => {
    const a = (i / Math.max(instances.length, 1)) * Math.PI * 2 - Math.PI / 2;
    positions[inst.id] = { x: C + R * Math.cos(a), y: C + R * Math.sin(a) };
  });

  // Term strip: who led when, over the last minute.
  const elections = history.filter((e) => e.type === 'leader.elected').reverse();
  const stripStart = now - STRIP_WINDOW_MS;
  const xOf = (ts) => ((ts - stripStart) / STRIP_WINDOW_MS) * 100;
  const terms = elections.map((e, i) => {
    // A term ends when its leader notices it lost/resigned, or when someone else is elected.
    const stepDown = history
      .filter((h) => h.instance === e.instance && (h.type === 'leader.lost' || h.type === 'leader.resigned') && h.ts > e.ts)
      .reduce((min, h) => Math.min(min, h.ts), Infinity);
    return { ...e, end: Math.min(stepDown, elections[i + 1]?.ts ?? now, now) };
  });
  const jobs = history.filter((e) => e.type === 'leader.job' || e.type === 'leader.fenced');

  return (
    <div className="pattern">
      <Explainer
        title="Leader election with leases & fencing tokens"
        subtitle="SET leader:lock <id> NX PX 4000 · renew every 1s · INCR leader:epoch"
        tryThis={[
          'Watch the leader renew its lease (the ring refills every second).',
          'Crash the leader 👑 — nobody renews, the key expires after ≤4s and another instance wins with a higher epoch.',
          'Resign: the leader deletes its own key, so failover is immediate instead of waiting for the TTL.',
          '"GC pause" the leader for 6s: it wakes up still believing it leads and tries to write with its old epoch — the fenced resource rejects it.',
        ]}
      >
        <p>
          Some work must run on exactly one instance: schedulers, cron jobs, stream compaction. Instances race to{' '}
          <code>SET leader:lock &lt;id&gt; NX PX 4000</code>; the winner is leader for the length of its lease and must
          keep renewing it (with a compare-and-<code>PEXPIRE</code> Lua script so it only renews its <em>own</em> lease).
        </p>
        <p>
          Leases alone aren&apos;t safe: a leader can freeze (GC, VM migration, network) past its lease while still
          believing it leads. Each election therefore <code>INCR</code>s <code>leader:epoch</code>, and the protected
          resource rejects writes carrying an epoch lower than the highest it has already seen. That number is a{' '}
          <strong>fencing token</strong>.
        </p>
      </Explainer>

      <div className="grid-2">
        <div className="card viz">
          <svg viewBox={`0 0 ${SIZE} ${SIZE}`} className="diagram ring">
            <circle cx={C} cy={C} r={R} className="orbit" />
            {instances.map((inst) => {
              const p = positions[inst.id];
              const leader = state?.leader === inst.id;
              return (
                <line
                  key={`l-${inst.id}`}
                  x1={C}
                  y1={C}
                  x2={p.x}
                  y2={p.y}
                  className={`link ${leader ? 'leader-link' : ''} ${inst.alive ? '' : 'dead'}`}
                  style={{ '--c': instanceColor(inst.id) }}
                />
              );
            })}
            <g transform={`translate(${C},${C})`}>
              <rect x={-78} y={-40} width={156} height={80} rx={12} className="box redis" />
              <text textAnchor="middle" y={-16} className="box-title">leader:lock</text>
              <text textAnchor="middle" y={4} className="box-sub">{state?.leader ?? '(empty)'}</text>
              <text textAnchor="middle" y={24} className="box-sub faint">
                {state?.leader ? `PTTL ${Math.round(leaseLeft)}ms · epoch ${state.epoch}` : `epoch ${state?.epoch ?? 0}`}
              </text>
            </g>
            {instances.map((inst) => {
              const p = positions[inst.id];
              const leader = state?.leader === inst.id;
              const circ = 2 * Math.PI * 44;
              return (
                <g key={inst.id} transform={`translate(${p.x},${p.y})`} className={`instance ${inst.alive ? '' : 'down'}`}>
                  {leader && (
                    <circle
                      r={44}
                      className="lease-ring"
                      style={{ stroke: instanceColor(inst.id) }}
                      strokeDasharray={`${circ * leaseFrac} ${circ}`}
                      transform="rotate(-90)"
                    />
                  )}
                  <circle r={36} className="box inst" style={{ '--c': instanceColor(inst.id) }} />
                  {leader && (
                    <text y={-50} textAnchor="middle" fontSize="22">
                      👑
                    </text>
                  )}
                  <text textAnchor="middle" y={-2} className="box-title">
                    {inst.id}
                  </text>
                  <text textAnchor="middle" y={14} className="box-sub faint">
                    {inst.alive ? (leader ? 'leader' : 'follower') : 'CRASHED'}
                  </text>
                </g>
              );
            })}
          </svg>
        </div>

        <div className="card">
          <h3>Controls</h3>
          <table className="table">
            <tbody>
              {instances.map((inst) => {
                const leader = state?.leader === inst.id;
                return (
                  <tr key={inst.id}>
                    <td>
                      <span className="swatch" style={{ '--c': instanceColor(inst.id) }} /> {inst.id} {leader && '👑'}
                    </td>
                    <td className="actions">
                      {inst.alive ? (
                        <button className="btn tiny danger" onClick={() => crash(inst.id)}>
                          Crash
                        </button>
                      ) : (
                        <button className="btn tiny" onClick={() => recover(inst.id)}>
                          Recover
                        </button>
                      )}
                      <button
                        className="btn tiny"
                        disabled={!inst.alive}
                        onClick={() => call(byId(inst.id).url, '/api/leader/stall', { ms: 6000 })}
                        title="Freeze the leader loop for 6s without it noticing"
                      >
                        GC pause 6s
                      </button>
                      <button
                        className="btn tiny ghost"
                        disabled={!leader || !inst.alive}
                        onClick={() => call(byId(inst.id).url, '/api/leader/resign', {})}
                      >
                        Resign
                      </button>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>

          <h3>Fenced resource</h3>
          <p className="muted small">
            <code>leader:resource</code> — written by the leader&apos;s singleton job every 2s, only if its epoch ≥ stored epoch.
          </p>
          <div className="kv">
            <div>
              <span>last writer</span>
              <strong style={{ color: instanceColor(state?.resource?.writer) }}>{state?.resource?.writer ?? '—'}</strong>
            </div>
            <div>
              <span>highest epoch</span>
              <strong>{state?.resource?.epoch ?? '—'}</strong>
            </div>
            <div>
              <span>total runs</span>
              <strong>{state?.resource?.runs ?? 0}</strong>
            </div>
          </div>
        </div>
      </div>

      <div className="card">
        <h3>Leadership terms — last 60s</h3>
        <div className="term-strip">
          {terms
            .filter((t) => t.end > stripStart)
            .map((t) => (
              <div
                key={t.id}
                className="term"
                style={{
                  left: `${Math.max(0, xOf(t.ts))}%`,
                  width: `${Math.max(0.5, xOf(t.end) - Math.max(0, xOf(t.ts)))}%`,
                  '--c': instanceColor(t.instance),
                }}
                title={`${t.instance} epoch ${t.data.epoch}`}
              >
                {t.instance} · e{t.data.epoch}
              </div>
            ))}
          {jobs
            .filter((j) => j.ts > stripStart)
            .map((j) => (
              <span
                key={j.id}
                className={`tick ${j.type === 'leader.fenced' ? 'bad' : ''}`}
                style={{ left: `${xOf(j.ts)}%`, '--c': instanceColor(j.instance) }}
                title={LABELS[j.type](j.data)}
              />
            ))}
        </div>
        <div className="strip-legend muted small">
          bars = terms (who held the lease) · dots = singleton job runs · <span className="bad-text">red</span> = rejected by
          fencing token
        </div>

        <ol className="timeline">
          {history
            .filter((e) => e.type !== 'leader.job')
            .slice(0, 12)
            .map((e) => (
              <li key={e.id} className={e.type.replace('.', '-')}>
                <span className="ts">{new Date(e.ts).toLocaleTimeString([], { hour12: false })}</span>
                <span className="tag" style={{ '--c': instanceColor(e.instance) }}>
                  {e.instance}
                </span>
                {LABELS[e.type]?.(e.data) ?? e.type}
              </li>
            ))}
        </ol>
      </div>
    </div>
  );
}
