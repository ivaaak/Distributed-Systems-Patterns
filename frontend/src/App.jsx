import { useEffect, useState } from 'react';
import Topology from './components/Topology.jsx';
import { PageActiveContext } from './lib/page.js';
import EventLog from './components/EventLog.jsx';
import Notifications from './patterns/Notifications.jsx';
import LeaderElection from './patterns/LeaderElection.jsx';
import DistributedLock from './patterns/DistributedLock.jsx';
import RateLimiter from './patterns/RateLimiter.jsx';
import WorkQueue from './patterns/WorkQueue.jsx';
import Sharding from './patterns/Sharding.jsx';
import Caching from './patterns/Caching.jsx';
import Idempotency from './patterns/Idempotency.jsx';
import CircuitBreaker from './patterns/CircuitBreaker.jsx';
import Saga from './patterns/Saga.jsx';
import Outbox from './patterns/Outbox.jsx';
import Quorum from './patterns/Quorum.jsx';
import Bulkhead from './patterns/Bulkhead.jsx';

const PAGES = [
  { group: 'Messaging', id: 'notifications', title: 'WebSocket Fan-out', icon: '📡', events: ['notify.', 'client.'], Component: Notifications },
  { group: 'Messaging', id: 'queue', title: 'Work Queue', icon: '📦', events: ['job.', 'instance.'], Component: WorkQueue },
  { group: 'Messaging', id: 'outbox', title: 'Outbox', icon: '📮', events: ['outbox.', 'instance.'], Component: Outbox },
  { group: 'Coordination', id: 'leader', title: 'Leader Election', icon: '👑', events: ['leader.', 'instance.'], Component: LeaderElection },
  { group: 'Coordination', id: 'lock', title: 'Distributed Lock', icon: '🔒', events: ['lock.', 'counter.'], Component: DistributedLock },
  { group: 'Coordination', id: 'saga', title: 'Saga', icon: '🧾', events: ['saga.', 'instance.'], Component: Saga },
  { group: 'Data', id: 'sharding', title: 'Sharding', icon: '🧩', events: ['shard.'], Component: Sharding },
  { group: 'Data', id: 'quorum', title: 'Quorum Replication', icon: '🗳️', events: ['quorum.', 'instance.'], Component: Quorum },
  { group: 'Data', id: 'caching', title: 'Caching', icon: '⚡', events: ['cache.'], Component: Caching },
  { group: 'Data', id: 'idempotency', title: 'Idempotency', icon: '🔁', events: ['idem.'], Component: Idempotency },
  { group: 'Resilience', id: 'ratelimit', title: 'Rate Limiting', icon: '🚦', events: ['ratelimit.'], Component: RateLimiter },
  { group: 'Resilience', id: 'breaker', title: 'Circuit Breaker', icon: '🔌', events: ['cb.'], Component: CircuitBreaker },
  { group: 'Resilience', id: 'bulkhead', title: 'Bulkhead', icon: '🚢', events: ['bulkhead.', 'instance.'], Component: Bulkhead },
];

const GROUPS = [...new Set(PAGES.map((p) => p.group))];

const fromHash = () => PAGES.find((p) => `#${p.id}` === window.location.hash)?.id ?? PAGES[0].id;

export default function App() {
  const [page, setPage] = useState(fromHash);

  useEffect(() => {
    const onHash = () => setPage(fromHash());
    window.addEventListener('hashchange', onHash);
    return () => window.removeEventListener('hashchange', onHash);
  }, []);

  const current = PAGES.find((p) => p.id === page);

  return (
    <div className="app">
      <header className="app-header">
        <div className="brand">
          <span className="logo">🛰️</span>
          <div>
            <h1>Distributed Systems Lab</h1>
            <small>React · Express · WebSockets · Redis — one cluster, many instances</small>
          </div>
        </div>
        <nav className="tabs">
          {GROUPS.map((g) => (
            <div key={g} className="tab-group">
              <span className="tab-group-label">{g}</span>
              <div className="tab-group-items">
                {PAGES.filter((p) => p.group === g).map((p) => (
                  <a key={p.id} href={`#${p.id}`} className={p.id === page ? 'active' : ''}>
                    <span>{p.icon}</span> {p.title}
                  </a>
                ))}
              </div>
            </div>
          ))}
        </nav>
      </header>

      <Topology />

      <main className="layout">
        <div className="pages">
          {/* Keep every page mounted so sockets, runs and histories survive tab switches. */}
          {PAGES.map(({ id, Component }) => (
            <div key={id} hidden={id !== page}>
              <PageActiveContext.Provider value={id === page}>
                <Component />
              </PageActiveContext.Provider>
            </div>
          ))}
        </div>
        <EventLog prefixes={current.events} />
      </main>
    </div>
  );
}
