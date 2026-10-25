import { useEffect, useState } from 'react';
import { usePolling } from '../lib/page.js';
import Explainer from '../components/Explainer.jsx';
import { useCluster } from '../lib/cluster.jsx';
import { call, instanceColor } from '../lib/api.js';
import { useLatest, useNow } from '../hooks/usePackets.jsx';

const MODES = {
  dual: { label: 'Dual write (DB, then broker)', color: '#f87171' },
  outbox: { label: 'Transactional outbox', color: '#34d399' },
};
const PUBLISH_GRACE_MS = 1500; // a dual write publishes right after the commit

const EMPTY = { orders: [], outbox: [], broker: [], faults: {}, relayHolder: null };

export default function Outbox() {
  const { pick, callAny } = useCluster();
  const [mode, setMode] = useState('dual');
  const [traffic, setTraffic] = useState(false);
  const [state, setState] = useState(EMPTY);
  const [responses, setResponses] = useState([]);
  const now = useNow(250);

  usePolling(() => callAny('/api/outbox/state').then(({ data }) => data.orders && setState(data)), 600);

  const placeOrder = async () => {
    const inst = pick();
    if (!inst) return;
    let r;
    try {
      const { status, data } = await call(inst.url, '/api/outbox/orders', { mode }, { timeout: 3000 });
      r = status === 201 ? { kind: 'ok', text: `201 ${data.orderId}` } : { kind: 'bad', text: `${status} ${data.orderId ?? ''} ${data.error ?? ''}` };
    } catch {
      r = { kind: 'bad', text: 'no response — instance died mid-request' };
    }
    setResponses((rs) => [{ ...r, instance: inst.id, id: Math.random() }, ...rs].slice(0, 12));
  };
  const placeRef = useLatest(placeOrder);

  useEffect(() => {
    if (!traffic) return;
    const t = setInterval(() => placeRef.current(), 600);
    return () => clearInterval(t);
  }, [traffic, placeRef]);

  const setFault = (patch) => callAny('/api/outbox/faults', patch).catch(() => {});
  const reset = async () => {
    setTraffic(false);
    await callAny('/api/outbox/reset', {});
    setState(EMPTY);
    setResponses([]);
  };

  // Join the three stores: what does the rest of the system know about each order?
  const eventsByOrder = {};
  for (const e of state.broker) (eventsByOrder[e.orderId] ??= []).push(e);
  const inOutbox = new Set(state.outbox.map((e) => e.orderId));
  const rows = state.orders.map((o) => {
    const published = eventsByOrder[o.orderId]?.length ?? 0;
    let status;
    if (published > 1) status = { kind: 'warn', text: `published ${published}× — consumer must dedupe` };
    else if (published === 1) status = { kind: 'good', text: 'published ✓' };
    else if (inOutbox.has(o.orderId)) status = { kind: 'pending', text: 'waiting in outbox ⏳' };
    else if (now - o.ts < PUBLISH_GRACE_MS) status = { kind: 'pending', text: 'publishing…' };
    else status = { kind: 'bad', text: 'LOST — nobody downstream knows' };
    return { ...o, status };
  });
  const lost = rows.filter((r) => r.status.kind === 'bad').length;
  const uniqueEvents = new Set(state.broker.map((e) => e.eventId)).size;
  const dupes = state.broker.length - uniqueEvents;
  const shipped = new Set(state.broker.map((e) => e.orderId)).size;
  const f = state.faults;

  return (
    <div className="pattern">
      <Explainer
        title="Transactional outbox"
        subtitle="dual-write problem · order + event in one MULTI/EXEC · relay → broker · at-least-once, dedupe by event id"
        tryThis={[
          'Dual write mode: place a few orders — each gets exactly one event. Now "Arm: crash after DB commit" and place an order: the order is saved, the instance dies before publishing, the event is LOST forever.',
          'Still in dual mode, toggle "Broker down" and place orders: they are saved but the publish fails — the client gets a 502 for an order that actually exists.',
          'Switch to Transactional outbox and repeat both: events wait in the outbox while the broker is down (or the instance is dead), and the relay publishes them once it can. Nothing is lost.',
          'Arm "relay crash after publish": the relay publishes an event, dies before deleting the outbox row, and another instance publishes it again → a duplicate. The consumer dedupes by eventId.',
        ]}
      >
        <p>
          A service often needs to update its database <em>and</em> tell other services about it. The DB and the message
          broker are separate systems with no shared transaction, so &quot;write, then publish&quot; can stop halfway: a crash,
          deploy or broker outage between the two steps leaves the database and the rest of the system disagreeing, and
          nothing will ever notice. Publishing first has the mirror-image problem (events for orders that never saved).
        </p>
        <p>
          The outbox pattern makes the event part of the <strong>same local transaction</strong> as the business data: the
          order and an <code>OrderPlaced</code> row are committed together to <code>ob:orders</code> and{' '}
          <code>ob:outbox</code>. A relay (one instance at a time, via a short lock) copies outbox rows to the broker and
          deletes them. It can crash after publishing and before deleting, so delivery is <strong>at-least-once</strong> and
          consumers must be idempotent.
        </p>
        <p className="muted small">
          In production the relay is often change-data-capture (e.g. Debezium tailing the DB log) instead of polling.
        </p>
      </Explainer>

      <div className="controls card">
        <div className="control-group">
          <label>Write path</label>
          <div className="segmented">
            {Object.entries(MODES).map(([k, m]) => (
              <button key={k} className={mode === k ? 'active' : ''} style={{ '--c': m.color }} onClick={() => setMode(k)}>
                {m.label}
              </button>
            ))}
          </div>
        </div>
        <div className="control-group">
          <label>Orders</label>
          <button className="btn primary" onClick={placeOrder}>
            Place order
          </button>
          <label className="toggle">
            <input type="checkbox" checked={traffic} onChange={(e) => setTraffic(e.target.checked)} /> ~2 orders/s via LB
          </label>
          <button className="btn ghost" onClick={reset}>
            Reset
          </button>
        </div>
        <div className="control-group">
          <label>Faults</label>
          <label className="toggle">
            <input type="checkbox" checked={Boolean(f.brokerDown)} onChange={(e) => setFault({ brokerDown: e.target.checked })} /> broker down
          </label>
          <button className={`btn ${f.crashAfterCommit ? 'danger' : ''}`} onClick={() => setFault({ crashAfterCommit: !f.crashAfterCommit })}>
            {f.crashAfterCommit ? '💣 armed: ' : 'Arm: '}crash after DB commit
          </button>
          <button className={`btn ${f.crashRelay ? 'danger' : ''}`} onClick={() => setFault({ crashRelay: !f.crashRelay })}>
            {f.crashRelay ? '💣 armed: ' : 'Arm: '}relay crash after publish
          </button>
          <span className="muted small">one-shot; recover crashed instances from the bar above</span>
        </div>
      </div>

      <div className="stats">
        <div className="stat card">
          <span>orders in DB</span>
          <strong>{state.orders.length}</strong>
          <small>source of truth</small>
        </div>
        <div className={`stat card ${lost ? 'bad' : ''}`}>
          <span>lost events</span>
          <strong>{lost}</strong>
          <small>order saved, never published</small>
        </div>
        <div className={`stat card ${state.outbox.length ? '' : 'good'}`}>
          <span>outbox depth</span>
          <strong>{state.outbox.length}</strong>
          <small>{state.relayHolder ? `relay: ${state.relayHolder}` : 'relay idle'}</small>
        </div>
        <div className="stat card">
          <span>duplicate deliveries</span>
          <strong>{dupes}</strong>
          <small>
            shipping handled {shipped} order{shipped === 1 ? '' : 's'} (deduped)
          </small>
        </div>
      </div>

      <div className="outbox-flow">
        <div className="card">
          <h3>
            Service DB <span className="muted small">ob:orders</span>
          </h3>
          <table className="table">
            <tbody>
              {rows
                .slice()
                .reverse()
                .slice(0, 14)
                .map((o) => (
                  <tr key={o.orderId} className={o.status.kind === 'bad' ? 'dup' : ''}>
                    <td className="mono small">{o.orderId}</td>
                    <td>
                      <span className="tag" style={{ '--c': instanceColor(o.instance) }}>
                        {o.instance}
                      </span>
                    </td>
                    <td className="muted small">{o.mode === 'outbox' ? 'outbox' : 'dual'}</td>
                    <td className={`ob-status ${o.status.kind}`}>{o.status.text}</td>
                  </tr>
                ))}
              {rows.length === 0 && (
                <tr>
                  <td className="muted">No orders yet.</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>

        <div className="card">
          <h3>
            Outbox <span className="muted small">ob:outbox · same DB</span>
          </h3>
          <div className="ob-queue">
            {state.outbox.map((e) => (
              <span key={e.eventId} className="ob-chip pending">
                {e.eventId}
                <small>{e.orderId}</small>
              </span>
            ))}
            {state.outbox.length === 0 && <span className="muted small">empty</span>}
          </div>
          <p className="muted small">
            {f.brokerDown ? '⛔ broker down — relay keeps rows and retries' : 'relay polls every 400ms, one instance at a time'}
          </p>
        </div>

        <div className="card">
          <h3>
            Broker <span className="muted small">ob:broker stream → shipping</span>
          </h3>
          <div className="ob-queue">
            {state.broker
              .slice(-24)
              .reverse()
              .map((e) => {
                const dup = eventsByOrder[e.orderId].length > 1;
                return (
                  <span key={e.streamId} className={`ob-chip ${dup ? 'dup' : ''}`} title={`${e.streamId} · published by ${e.publishedBy}`}>
                    {e.eventId}
                    <small>
                      {e.orderId} · <span style={{ color: instanceColor(e.publishedBy) }}>{e.publishedBy}</span>
                    </small>
                  </span>
                );
              })}
            {state.broker.length === 0 && <span className="muted small">no events</span>}
          </div>
        </div>
      </div>

      <div className="card">
        <h3>Client responses</h3>
        <div className="flow">
          {responses.map((r) => (
            <span key={r.id} className={`attempt ${r.kind}`}>
              <span className="tag" style={{ '--c': instanceColor(r.instance) }}>
                {r.instance}
              </span>
              {r.text}
            </span>
          ))}
          {responses.length === 0 && <span className="muted small">Place an order to see what the client is told.</span>}
        </div>
      </div>
    </div>
  );
}
