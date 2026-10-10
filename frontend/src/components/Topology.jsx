import { useCluster } from '../lib/cluster.jsx';
import { instanceColor } from '../lib/api.js';

export default function Topology() {
  const { instances, reachable, monitorOn, crash, recover, callAny } = useCluster();
  const hasDead = instances.some((i) => !i.alive);

  return (
    <div className="topology">
      <div className={`node-chip redis ${reachable ? '' : 'down'}`}>
        <span className="dot" />
        <div>
          <strong>Redis</strong>
          <small>{reachable ? 'shared state · pub/sub · streams' : 'cluster unreachable'}</small>
        </div>
      </div>
      <div className="topology-instances">
        {instances.length === 0 && (
          <div className="muted">
            {reachable ? 'No instances registered yet…' : 'Waiting for backend instances on the seed URLs…'}
          </div>
        )}
        {instances.map((inst) => (
          <div
            key={inst.id}
            className={`node-chip instance ${inst.alive ? '' : 'down'}`}
            style={{ '--c': instanceColor(inst.id) }}
          >
            <span className="dot" />
            <div>
              <strong>
                {inst.id} {inst.isLeader && <span title="leader">👑</span>}
              </strong>
              <small>
                {inst.alive
                  ? `${inst.stats?.clients ?? 0} ws clients${inst.stats?.job ? ` · ⚙ ${inst.stats.job}` : ''}`
                  : 'no heartbeat'}
              </small>
            </div>
            {inst.alive ? (
              <button className="btn tiny danger" onClick={() => crash(inst.id)} title="Simulate a crash">
                Crash
              </button>
            ) : (
              <button className="btn tiny" onClick={() => recover(inst.id).catch(() => {})} title="Recover">
                Recover
              </button>
            )}
          </div>
        ))}
        {hasDead && (
          <button
            className="btn tiny ghost"
            title="Remove instances that stopped heartbeating (e.g. killed containers)"
            onClick={() => callAny('/api/cluster/forget-dead', {})}
          >
            Forget dead
          </button>
        )}
      </div>
      <div className="monitor-status" title="The event stream below comes from this instance, but includes events from every instance via Redis pub/sub">
        <span className={`dot ${monitorOn ? 'ok' : ''}`} /> events via {monitorOn ?? '—'}
      </div>
    </div>
  );
}
