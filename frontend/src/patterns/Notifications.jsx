import { useMemo, useRef, useState } from 'react';
import Explainer from '../components/Explainer.jsx';
import { useCluster, useEvents } from '../lib/cluster.jsx';
import { call, instanceColor } from '../lib/api.js';
import { usePackets, PacketLayer, useLatest } from '../hooks/usePackets.jsx';
import { useVirtualClients } from '../hooks/useVirtualClients.js';

const STEP = 450;
const W = 1000;
const INSTANCE_Y = 230;
const CLIENT_Y0 = 370;
const ROW_H = 74;
const REDIS = { x: 560, y: 64 };
const CALLER = { x: 110, y: 64 };

const MODES = {
  broadcast: { label: 'Broadcast (all clients)', color: '#38bdf8' },
  direct: { label: 'Direct (one client)', color: '#f472b6' },
  local: { label: 'Naive local-only (broken)', color: '#fbbf24' },
};

function useLayout(instances, clients) {
  return useMemo(() => {
    const n = Math.max(instances.length, 1);
    const colW = W / n;
    const perRow = Math.max(1, Math.floor((colW - 20) / 104));
    const inst = {};
    instances.forEach((i, idx) => (inst[i.id] = { x: colW * (idx + 0.5), y: INSTANCE_Y }));

    const cl = {};
    const counts = {};
    let maxRows = 1;
    clients.forEach((c) => {
      const home = inst[c.instanceId];
      if (!home) return;
      const k = (counts[c.instanceId] = (counts[c.instanceId] ?? 0) + 1) - 1;
      const row = Math.floor(k / perRow);
      maxRows = Math.max(maxRows, row + 1);
      cl[c.id] = { row, k, home };
    });
    // Center each row under its instance.
    Object.entries(cl).forEach(([id, p]) => {
      const c = clients.find((x) => x.id === id);
      const total = counts[c.instanceId];
      const inRow = Math.min(perRow, total - p.row * perRow);
      const col = p.k % perRow;
      cl[id] = { x: p.home.x + (col - (inRow - 1) / 2) * 104, y: CLIENT_Y0 + p.row * ROW_H };
    });
    return { inst, clients: cl, height: CLIENT_Y0 + maxRows * ROW_H + 10 };
  }, [instances, clients]);
}

export default function Notifications() {
  const cluster = useCluster();
  const { instances, pick, byId } = cluster;
  const { clients, pool } = useVirtualClients();
  const layout = useLayout(instances, clients);
  const layoutRef = useLatest(layout);
  const { packets, now, launch } = usePackets();

  const [mode, setMode] = useState('broadcast');
  const [via, setVia] = useState('lb');
  const [target, setTarget] = useState('');
  const [message, setMessage] = useState('Deploy finished ✅');
  const [result, setResult] = useState(null);
  const [redisNote, setRedisNote] = useState(null);

  const starts = useRef({});
  const flashes = useRef({});

  useEvents(['notify.'], (e) => {
    const L = layoutRef.current;
    const d = e.data;
    const origin = L.inst[e.instance];
    if (e.type === 'notify.received') {
      starts.current[d.nid] = performance.now();
      if (origin) launch({ from: CALLER, to: origin, color: MODES[d.mode]?.color, label: 'POST /notify' });
      return;
    }
    const t0 = starts.current[d.nid] ?? performance.now();
    const at = (steps) => t0 + steps * STEP;

    if (e.type === 'notify.lookup' && origin) {
      launch(
        { from: origin, to: REDIS, start: at(1), color: '#fde047', label: `GET presence:${d.targetClientId}`, r: 5 },
        { from: REDIS, to: origin, start: at(2), color: '#fde047', label: d.owner ?? 'nil', r: 5 }
      );
    }
    if (e.type === 'notify.published' && origin) {
      const direct = Boolean(d.owner);
      launch({ from: origin, to: REDIS, start: at(direct ? 3 : 1), color: '#ef4444', label: 'PUBLISH' });
      setRedisNote({ channel: d.channel, until: Date.now() + 2500 });
    }
    if (e.type === 'notify.delivered' || e.type === 'notify.missed') {
      const inst = L.inst[e.instance];
      if (!inst) return;
      const base = d.via === 'local' ? 1 : d.via === 'direct' ? 4 : 2;
      if (d.via !== 'local') launch({ from: REDIS, to: inst, start: at(base), color: '#ef4444' });
      const clientStart = d.via === 'local' ? at(base) : at(base + 1);
      d.clientIds.forEach((cid) => {
        const cp = L.clients[cid];
        if (!cp) return;
        flashes.current[cid] = clientStart + 400;
        launch({ from: inst, to: cp, start: clientStart, duration: 400, color: instanceColor(e.instance), linger: 900 });
      });
    }
  });

  const send = async () => {
    const inst = via === 'lb' ? pick() : byId(via);
    if (!inst) return setResult({ error: 'no instance available' });
    try {
      const { data, status } = await call(inst.url, '/api/notify', { mode, message, targetClientId: target || undefined });
      setResult({ via: inst.id, status, ...data });
    } catch (err) {
      setResult({ via: inst.id, error: err.message });
    }
  };

  const onlineClients = clients.filter((c) => c.status === 'online');

  return (
    <div className="pattern">
      <Explainer
        title="Multi-instance WebSocket notifications"
        subtitle="Redis Pub/Sub as a backplane · presence registry for targeted delivery"
        tryThis={[
          'Add ~6 clients — the load balancer spreads their sockets across instances.',
          'Send in "Naive local-only" mode: only clients on the instance that got the HTTP request receive it.',
          'Switch to Broadcast: the instance PUBLISHes to Redis and every instance fans out to its own sockets.',
          'Click a client, use Direct: the receiving instance looks up presence:<client> in Redis and publishes to the owning instance\'s private channel only.',
          'Crash an instance: its clients reconnect elsewhere and keep receiving.',
        ]}
      >
        <p>
          A WebSocket is a long-lived connection to <em>one</em> process. With several API instances behind a load
          balancer, the HTTP request that triggers a notification usually lands on a different instance than the
          socket it should reach. Each instance only knows its own sockets.
        </p>
        <p>
          <strong>Fix:</strong> every instance subscribes to <code>notif:broadcast</code> and to its own{' '}
          <code>notif:instance:&lt;id&gt;</code> channel. On connect, a client&apos;s owner is recorded as{' '}
          <code>presence:&lt;clientId&gt; = &lt;instanceId&gt;</code> (with a TTL, refreshed while connected).
        </p>
      </Explainer>

      <div className="controls card">
        <div className="control-group">
          <label>Clients</label>
          <button className="btn" onClick={() => pool.add()}>
            + Client (via LB)
          </button>
          <button className="btn" onClick={() => [...Array(5)].forEach(() => pool.add())}>
            + 5 clients
          </button>
          {instances
            .filter((i) => i.alive)
            .map((i) => (
              <button key={i.id} className="btn ghost" style={{ '--c': instanceColor(i.id) }} onClick={() => pool.add(i)}>
                + on {i.id}
              </button>
            ))}
          <button className="btn ghost" onClick={() => pool.removeAll()} disabled={!clients.length}>
            Disconnect all
          </button>
        </div>

        <div className="control-group">
          <label>Mode</label>
          <div className="segmented">
            {Object.entries(MODES).map(([k, m]) => (
              <button key={k} className={mode === k ? 'active' : ''} style={{ '--c': m.color }} onClick={() => setMode(k)}>
                {m.label}
              </button>
            ))}
          </div>
        </div>

        <div className="control-group">
          <label>Send</label>
          <input className="input grow" value={message} onChange={(e) => setMessage(e.target.value)} />
          {mode === 'direct' && (
            <select className="input" value={target} onChange={(e) => setTarget(e.target.value)}>
              <option value="">— pick target —</option>
              {clients.map((c) => (
                <option key={c.id} value={c.id}>
                  {c.name} ({c.id})
                </option>
              ))}
            </select>
          )}
          <select className="input" value={via} onChange={(e) => setVia(e.target.value)} title="Which instance receives the HTTP request">
            <option value="lb">request via LB (round-robin)</option>
            {instances.map((i) => (
              <option key={i.id} value={i.id} disabled={!i.alive}>
                request via {i.id}
              </option>
            ))}
          </select>
          <button className="btn primary" onClick={send} disabled={mode === 'direct' && !target}>
            Send notification
          </button>
        </div>
        {result && (
          <div className={`result ${result.error ? 'bad' : ''}`}>
            {result.error
              ? `✖ ${result.via ?? ''} ${result.error}`
              : mode === 'local' || result.mode === 'local'
                ? `${result.via} delivered to ${result.delivered} local socket(s) — ${onlineClients.length - result.delivered} client(s) on other instances missed it`
                : result.owner
                  ? `${result.via} looked up presence → ${result.owner}, published to ${result.channel}`
                  : `${result.via} published to ${result.channel} (${result.subscribers} subscribed instance connections)`}
          </div>
        )}
      </div>

      <div className="card viz">
        <svg viewBox={`0 0 ${W} ${layout.height}`} className="diagram">
          <defs>
            <filter id="glow" x="-50%" y="-50%" width="200%" height="200%">
              <feGaussianBlur stdDeviation="4" result="b" />
              <feMerge>
                <feMergeNode in="b" />
                <feMergeNode in="SourceGraphic" />
              </feMerge>
            </filter>
          </defs>

          {/* Caller */}
          <g transform={`translate(${CALLER.x},${CALLER.y})`}>
            <rect x={-80} y={-26} width={160} height={52} rx={10} className="box caller" />
            <text textAnchor="middle" y={-3} className="box-title">HTTP caller</text>
            <text textAnchor="middle" y={14} className="box-sub">POST /api/notify</text>
          </g>

          {/* Redis */}
          <g transform={`translate(${REDIS.x},${REDIS.y})`}>
            <rect x={-190} y={-40} width={380} height={80} rx={12} className="box redis" />
            <text textAnchor="middle" y={-14} className="box-title">Redis Pub/Sub</text>
            <text textAnchor="middle" y={6} className="box-sub">
              notif:broadcast · notif:instance:&lt;id&gt; · presence:*
            </text>
            {redisNote && redisNote.until > Date.now() && (
              <text textAnchor="middle" y={26} className="box-note">
                → {redisNote.channel}
              </text>
            )}
          </g>

          {/* Instance <-> redis subscription lines */}
          {instances.map((i) => {
            const p = layout.inst[i.id];
            return (
              <line
                key={`sub-${i.id}`}
                x1={p.x}
                y1={p.y - 34}
                x2={REDIS.x + (p.x - REDIS.x) * 0.3}
                y2={REDIS.y + 40}
                className={`link ${i.alive ? '' : 'dead'}`}
              />
            );
          })}

          {/* Client links */}
          {clients.map((c) => {
            const p = layout.clients[c.id];
            const home = layout.inst[c.instanceId];
            if (!p || !home) return null;
            return (
              <line
                key={`l-${c.id}`}
                x1={home.x}
                y1={home.y + 34}
                x2={p.x}
                y2={p.y - 22}
                className={`link ws ${c.status === 'online' ? '' : 'dead'}`}
                style={{ '--c': instanceColor(c.instanceId) }}
              />
            );
          })}

          {/* Instances */}
          {instances.map((i) => {
            const p = layout.inst[i.id];
            return (
              <g key={i.id} transform={`translate(${p.x},${p.y})`} className={`instance ${i.alive ? '' : 'down'}`}>
                <rect x={-85} y={-34} width={170} height={68} rx={12} className="box inst" style={{ '--c': instanceColor(i.id) }} />
                <text textAnchor="middle" y={-8} className="box-title">
                  {i.id} {i.isLeader ? '👑' : ''}
                </text>
                <text textAnchor="middle" y={12} className="box-sub">
                  {i.alive ? `${clients.filter((c) => c.instanceId === i.id && c.status === 'online').length} sockets` : 'CRASHED'}
                </text>
                <text textAnchor="middle" y={27} className="box-sub faint">
                  {i.url.replace('http://', '')}
                </text>
              </g>
            );
          })}

          {/* Clients */}
          {clients.map((c) => {
            const p = layout.clients[c.id];
            if (!p) return null;
            const fa = flashes.current[c.id];
            const flashing = fa && now >= fa && now < fa + 900;
            return (
              <g
                key={c.id}
                transform={`translate(${p.x},${p.y})`}
                className={`client ${c.status} ${flashing ? 'flash' : ''} ${target === c.id ? 'targeted' : ''}`}
                onClick={() => {
                  setTarget(c.id);
                  setMode('direct');
                }}
              >
                <rect x={-48} y={-22} width={96} height={44} rx={9} className="box client-box" style={{ '--c': instanceColor(c.instanceId) }} />
                <text textAnchor="middle" y={-4} className="client-name">
                  {c.name}
                </text>
                <text textAnchor="middle" y={12} className="box-sub faint">
                  {c.status === 'online' ? `📬 ${c.inbox.length}` : c.status}
                </text>
              </g>
            );
          })}

          <PacketLayer packets={packets} now={now} />
        </svg>
        {clients.length === 0 && <div className="viz-empty">Add some clients to get started ↑</div>}
      </div>

      {clients.length > 0 && (
        <div className="inboxes">
          {clients.map((c) => (
            <div key={c.id} className={`inbox card ${target === c.id ? 'targeted' : ''}`} style={{ '--c': instanceColor(c.instanceId) }}>
              <header>
                <strong>{c.name}</strong>
                <span className="muted small">
                  {c.id} · on {c.instanceId ?? '—'}
                  {c.reconnects ? ` · ${c.reconnects} reconnect(s)` : ''}
                </span>
                <button className="btn tiny ghost" onClick={() => pool.remove(c.id)} title="Disconnect">
                  ✕
                </button>
              </header>
              <ul>
                {c.inbox.slice(0, 4).map((m) => (
                  <li key={m.id}>
                    <span className="badge" style={{ '--c': MODES[m.mode]?.color }}>
                      {m.mode}
                    </span>{' '}
                    {m.message}
                    <span className="muted small">
                      {' '}
                      · via {m.origin}→{m.deliveredBy}
                    </span>
                  </li>
                ))}
                {c.inbox.length === 0 && <li className="muted small">no notifications yet</li>}
              </ul>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
