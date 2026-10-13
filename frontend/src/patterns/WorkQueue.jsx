import { useState } from 'react';
import { usePolling } from '../lib/page.js';
import Explainer from '../components/Explainer.jsx';
import { useCluster, useEvents } from '../lib/cluster.jsx';
import { call, instanceColor } from '../lib/api.js';
import { useNow } from '../hooks/usePackets.jsx';

function JobChip({ job, children }) {
  return (
    <div className={`job-chip ${job.deliveries > 1 ? 'redelivered' : ''}`} style={{ '--c': instanceColor(job.instance) }}>
      <strong>{job.jobId}</strong>
      {children}
    </div>
  );
}

export default function WorkQueue() {
  const { instances, pick, callAny, crash, recover } = useCluster();
  const [jobs, setJobs] = useState({});
  const [stats, setStats] = useState(null);
  const now = useNow(100);

  const patch = (jobId, p) => setJobs((js) => ({ ...js, [jobId]: { ...js[jobId], jobId, ...p } }));

  useEvents(['job.'], (e) => {
    const d = e.data;
    switch (e.type) {
      case 'job.enqueued':
        return patch(d.jobId, { status: 'queued', durationMs: d.durationMs, enqueuedAt: e.ts, deliveries: 0 });
      case 'job.started':
        return patch(d.jobId, {
          status: 'running',
          instance: e.instance,
          startedAt: e.ts,
          durationMs: d.durationMs,
          deliveries: d.deliveries ?? 1,
          reclaimedFrom: d.reclaimedFrom,
        });
      case 'job.reclaimed':
        return patch(d.jobId, { status: 'claimed', instance: e.instance, reclaimedFrom: d.from, deliveries: d.deliveries });
      case 'job.completed':
        return patch(d.jobId, { status: 'done', instance: e.instance, doneAt: e.ts });
      default:
    }
  });

  usePolling(() => callAny('/api/jobs/stats').then(({ data }) => setStats(data)), 1000);

  const enqueue = async (count) => {
    const inst = pick();
    if (inst) await call(inst.url, '/api/jobs', { count });
  };

  const list = Object.values(jobs);
  const queued = list.filter((j) => j.status === 'queued');
  const done = list.filter((j) => j.status === 'done').sort((a, b) => b.doneAt - a.doneAt);
  const aliveIds = new Set(instances.filter((i) => i.alive).map((i) => i.id));
  const redelivered = list.filter((j) => j.deliveries > 1).length;

  return (
    <div className="pattern">
      <Explainer
        title="Work queue with Redis Streams consumer groups"
        subtitle="XADD · XREADGROUP (competing consumers) · XACK · XPENDING + XCLAIM for crash recovery"
        tryThis={[
          'Enqueue 10 jobs: each is delivered to exactly one instance; idle instances pull the next job (natural load balancing).',
          'Crash an instance while it is working: its job stays in the Pending Entries List (PEL) because it was never ACKed.',
          'After 5s of idleness a surviving instance XCLAIMs the orphan and re-runs it (delivery #2) — at-least-once delivery.',
          'Recover the instance: it rejoins the group and starts pulling new jobs.',
        ]}
      >
        <p>
          Pub/Sub is fire-and-forget: a message published while a subscriber is down is lost, and every subscriber gets
          every message. Work distribution needs the opposite: <strong>durable</strong> messages, each processed by{' '}
          <strong>one</strong> worker, and recovery when a worker dies mid-task.
        </p>
        <p>
          A Redis Stream with a consumer group gives exactly that. <code>XREADGROUP … &gt;</code> hands new entries to one
          consumer and records them as pending; the worker <code>XACK</code>s after finishing. Because a job may run twice
          (crash after work but before ACK), handlers should be <strong>idempotent</strong>.
        </p>
      </Explainer>

      <div className="controls card">
        <div className="control-group">
          <label>Producer</label>
          <button className="btn" onClick={() => enqueue(1)}>
            Enqueue 1 job
          </button>
          <button className="btn primary" onClick={() => enqueue(10)}>
            Enqueue 10 jobs
          </button>
          <button className="btn ghost" onClick={() => setJobs({})}>
            Clear view
          </button>
          {stats && (
            <span className="muted small">
              stream length {stats.length} · pending (PEL) {stats.pending}
              {Object.keys(stats.consumers).length > 0 &&
                ` · ${Object.entries(stats.consumers)
                  .map(([c, n]) => `${c}: ${n}`)
                  .join(', ')}`}
            </span>
          )}
        </div>
      </div>

      <div className="queue-board">
        <div className="card column">
          <h3>
            jobs:stream <span className="muted small">undelivered ({queued.length})</span>
          </h3>
          <div className="chips">
            {queued.map((j) => (
              <JobChip key={j.jobId} job={j}>
                <small>{(j.durationMs / 1000).toFixed(1)}s</small>
              </JobChip>
            ))}
            {!queued.length && <div className="muted small">empty</div>}
          </div>
        </div>

        <div className="consumers">
          {instances.map((inst) => {
            const running = list.filter((j) => j.instance === inst.id && (j.status === 'running' || j.status === 'claimed'));
            const count = done.filter((j) => j.instance === inst.id).length;
            return (
              <div key={inst.id} className={`card consumer ${inst.alive ? '' : 'down'}`} style={{ '--c': instanceColor(inst.id) }}>
                <header>
                  <strong>{inst.id}</strong>
                  <span className="muted small">consumer · {count} done</span>
                  {inst.alive ? (
                    <button className="btn tiny danger" onClick={() => crash(inst.id)}>
                      Crash
                    </button>
                  ) : (
                    <button className="btn tiny" onClick={() => recover(inst.id)}>
                      Recover
                    </button>
                  )}
                </header>
                {running.length === 0 && <div className="muted small idle">{inst.alive ? 'idle — blocked on XREADGROUP' : 'crashed'}</div>}
                {running.map((j) => {
                  const orphan = !aliveIds.has(inst.id);
                  const progress = j.status === 'claimed' ? 0 : Math.min(1, (now - j.startedAt) / j.durationMs);
                  return (
                    <div key={j.jobId} className={`running ${orphan ? 'orphan' : ''}`}>
                      <div className="running-head">
                        <strong>{j.jobId}</strong>
                        {j.deliveries > 1 && <span className="badge warn">delivery #{j.deliveries} (from {j.reclaimedFrom})</span>}
                      </div>
                      <div className="progress">
                        <div style={{ width: `${progress * 100}%` }} />
                      </div>
                      {orphan && (
                        <div className="small bad-text">
                          ⚠ un-ACKed in PEL — idle {((now - j.startedAt) / 1000).toFixed(1)}s, reclaimable after 5s
                        </div>
                      )}
                    </div>
                  );
                })}
              </div>
            );
          })}
        </div>

        <div className="card column">
          <h3>
            XACKed <span className="muted small">({done.length}{redelivered ? ` · ${redelivered} redelivered` : ''})</span>
          </h3>
          <div className="chips">
            {done.slice(0, 40).map((j) => (
              <JobChip key={j.jobId} job={j}>
                <small>
                  {j.instance}
                  {j.deliveries > 1 ? ' ↻' : ''}
                </small>
              </JobChip>
            ))}
            {!done.length && <div className="muted small">nothing yet</div>}
          </div>
        </div>
      </div>
    </div>
  );
}
