import { useState } from 'react';
import { useEventLog } from '../lib/cluster.jsx';
import { instanceColor } from '../lib/api.js';

const fmt = (ts) => new Date(ts).toLocaleTimeString([], { hour12: false }) + '.' + String(ts % 1000).padStart(3, '0');

function summarize(data) {
  return Object.entries(data ?? {})
    .filter(([, v]) => v !== undefined && v !== null)
    .map(([k, v]) => `${k}=${Array.isArray(v) ? `[${v.join(',')}]` : typeof v === 'object' ? JSON.stringify(v) : v}`)
    .join(' ');
}

export default function EventLog({ prefixes }) {
  const { log, clear } = useEventLog();
  const [onlyPage, setOnlyPage] = useState(true);
  const rows = (onlyPage && prefixes ? log.filter((e) => prefixes.some((p) => e.type.startsWith(p))) : log).slice(0, 150);

  return (
    <aside className="event-log">
      <div className="event-log-head">
        <h3>Cluster event stream</h3>
        <label className="toggle">
          <input type="checkbox" checked={onlyPage} onChange={(e) => setOnlyPage(e.target.checked)} /> this pattern only
        </label>
        <button className="btn tiny ghost" onClick={clear}>
          Clear
        </button>
      </div>
      <p className="muted small">
        Every instance publishes to the <code>system:events</code> Redis channel; this panel subscribes through one instance.
      </p>
      <ol>
        {rows.map((e) => (
          <li key={e.id}>
            <span className="ts">{fmt(e.ts)}</span>
            <span className="tag" style={{ '--c': instanceColor(e.instance) }}>
              {e.instance}
            </span>
            <span className="type">{e.type}</span>
            <span className="data">{summarize(e.data)}</span>
          </li>
        ))}
        {rows.length === 0 && <li className="muted">No events yet.</li>}
      </ol>
    </aside>
  );
}
