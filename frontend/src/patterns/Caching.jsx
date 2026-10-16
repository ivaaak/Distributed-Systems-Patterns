import { useState } from 'react';
import { usePolling } from '../lib/page.js';
import Explainer from '../components/Explainer.jsx';
import { useCluster } from '../lib/cluster.jsx';
import { call, instanceColor } from '../lib/api.js';
import { useLatest } from '../hooks/usePackets.jsx';

const SOURCE_CLASS = { L1: 'l1', L2: 'l2', 'L2 (waited)': 'l2w', DB: 'db' };

export default function Caching() {
  const { instances, alive, pick, callAny } = useCluster();
  const [db, setDb] = useState(null);
  const [l1s, setL1s] = useState({});
  const [invalidation, setInvalidation] = useState('none');
  const [product, setProduct] = useState('p1');
  const [reads, setReads] = useState([]);
  const [protect, setProtect] = useState(false);
  const [concurrency, setConcurrency] = useState(30);
  const [stampede, setStampede] = useState(null);
  const aliveRef = useLatest(alive);

  usePolling(async () => {
    const { data } = await callAny('/api/cache/db');
    setDb(data);
    const views = await Promise.all(aliveRef.current.map((i) => call(i.url, '/api/cache/l1').then((r) => r.data).catch(() => null)));
    setL1s(Object.fromEntries(views.filter(Boolean).map((v) => [v.instance, v.entries])));
  }, 700);

  const read = async (inst, id = product, prot = false) => {
    const { data } = await call(inst.url, `/api/cache/products/${id}${prot ? '?protect=1' : ''}`);
    return { ...data, id, at: Date.now() };
  };

  const readEverywhere = async () => {
    const results = await Promise.all(alive.map((i) => read(i).catch(() => null)));
    setReads((r) => [...results.filter(Boolean), ...r].slice(0, 14));
  };

  const update = async () => {
    const inst = pick();
    const current = db?.products[product]?.price ?? 0;
    await call(inst.url, `/api/cache/products/${product}`, { price: current + 10, invalidation }, { method: 'PUT' });
  };

  const runStampede = async () => {
    setStampede({ running: true });
    await callAny(`/api/cache/expire/${'p2'}`, {});
    const before = (await callAny('/api/cache/db')).data.queries;
    const t0 = Date.now();
    const results = await Promise.all(
      Array.from({ length: concurrency }, () => {
        const inst = pick();
        return read(inst, 'p2', protect).catch(() => null);
      })
    );
    const after = (await callAny('/api/cache/db')).data.queries;
    setStampede({ running: false, protect, queries: after - before, results: results.filter(Boolean), ms: Date.now() - t0 });
  };

  const products = db ? Object.values(db.products).sort((a, b) => a.id.localeCompare(b.id)) : [];

  const cell = (entry, truth) => {
    if (!entry) return <span className="muted">—</span>;
    const stale = entry.version < truth.version;
    return (
      <span className={`cache-cell ${stale ? 'stale' : 'fresh'}`}>
        ${entry.price} <small>v{entry.version}</small>
        {stale && <b> STALE</b>}
        {entry.ageMs != null && <small className="muted"> · {Math.round(entry.expiresInMs / 1000)}s left</small>}
      </span>
    );
  };

  return (
    <div className="pattern">
      <Explainer
        title="Distributed caching & invalidation"
        subtitle="cache-aside · L1 in-process + L2 Redis · pub/sub invalidation · stampede protection with a fill lock"
        tryThis={[
          'Read p1 from every instance to warm all L1 caches.',
          'With invalidation "None", bump the price: the writer\'s cache is fresh but the other instances keep serving the old price (STALE) for up to 60s.',
          'Switch to "Pub/Sub broadcast" and bump again: every instance drops its L1 copy and the next read is fresh.',
          'Stampede: expire a hot key and fire 30 concurrent reads. Unprotected, almost every request queries the DB; with the fill lock exactly one does.',
        ]}
      >
        <p>
          Per-instance memory caches are the fastest thing you have, but with N instances there are N copies of every hot
          value. Deleting the entry on the instance that handled the write doesn&apos;t touch the others.{' '}
          <strong>Invalidation has to be broadcast</strong> — here via Redis Pub/Sub on <code>cache:invalidate</code>.
        </p>
        <p>
          When a popular entry expires, every concurrent request misses at once and hammers the database (a{' '}
          <strong>cache stampede</strong> / thundering herd). A short-lived <code>SET cache:fill:&lt;id&gt; NX PX</code> lock
          lets one request recompute the value while the others wait for L2 to be filled.
        </p>
      </Explainer>

      <div className="controls card">
        <div className="control-group">
          <label>Invalidation</label>
          <div className="segmented">
            <button className={invalidation === 'none' ? 'active' : ''} style={{ '--c': '#f87171' }} onClick={() => setInvalidation('none')}>
              None (local delete only)
            </button>
            <button className={invalidation === 'pubsub' ? 'active' : ''} style={{ '--c': '#34d399' }} onClick={() => setInvalidation('pubsub')}>
              Pub/Sub broadcast
            </button>
          </div>
        </div>
        <div className="control-group">
          <label>Product</label>
          <select className="input" value={product} onChange={(e) => setProduct(e.target.value)}>
            {products.map((p) => (
              <option key={p.id} value={p.id}>
                {p.id} · {p.name}
              </option>
            ))}
          </select>
          <button className="btn" onClick={readEverywhere}>
            Read from every instance
          </button>
          <button className="btn primary" onClick={update}>
            Update price +$10 (via LB)
          </button>
          <button className="btn ghost" onClick={() => callAny('/api/cache/reset', {}).then(() => setReads([]))}>
            Reset
          </button>
        </div>
      </div>

      <div className="card">
        <h3>
          Who sees what <span className="muted small">— DB queries so far: {db?.queries ?? '—'}</span>
        </h3>
        <div className="table-scroll">
          <table className="table cache-table">
            <thead>
              <tr>
                <th>product</th>
                <th>DB (truth)</th>
                <th>L2 · Redis</th>
                {instances.map((i) => (
                  <th key={i.id} style={{ color: instanceColor(i.id) }}>
                    L1 · {i.id}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {products.map((p) => (
                <tr key={p.id} className={p.id === product ? 'selected' : ''}>
                  <td>
                    <strong>{p.id}</strong> <span className="muted small">{p.name}</span>
                  </td>
                  <td>
                    <span className="cache-cell truth">
                      ${p.price} <small>v{p.version}</small>
                    </span>
                  </td>
                  <td>{cell(db.l2[p.id], p)}</td>
                  {instances.map((i) => (
                    <td key={i.id}>{i.alive ? cell(l1s[i.id]?.[p.id], p) : <span className="muted">down</span>}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        {reads.length > 0 && (
          <div className="read-log">
            {reads.map((r, i) => (
              <span key={`${r.at}-${i}`} className={`read-chip ${SOURCE_CLASS[r.source]}`} title={`${r.ms}ms`}>
                <b style={{ color: instanceColor(r.instance) }}>{r.instance}</b> {r.id} ← {r.source} · ${r.value?.price} ({r.ms}ms)
              </span>
            ))}
          </div>
        )}
      </div>

      <div className="card">
        <h3>Cache stampede on p2</h3>
        <div className="control-group">
          <label className="toggle">
            <input type="checkbox" checked={protect} onChange={(e) => setProtect(e.target.checked)} /> protect with fill lock
          </label>
          <input type="range" min={5} max={60} value={concurrency} onChange={(e) => setConcurrency(Number(e.target.value))} />
          <strong>{concurrency}</strong>
          <span className="muted small">concurrent readers</span>
          <button className="btn primary" disabled={stampede?.running} onClick={runStampede}>
            {stampede?.running ? 'Running…' : 'Expire p2 & fire reads'}
          </button>
        </div>
        {stampede?.results && (
          <>
            <div className="stats" style={{ marginTop: 12 }}>
              <div className={`stat card ${stampede.queries > 1 ? 'bad' : 'good'}`}>
                <span>DB queries</span>
                <strong>{stampede.queries}</strong>
                <small>for {stampede.results.length} reads · {stampede.protect ? 'protected' : 'unprotected'}</small>
              </div>
              <div className="stat card">
                <span>slowest read</span>
                <strong>{Math.max(...stampede.results.map((r) => r.ms))}ms</strong>
                <small>DB latency is 300ms</small>
              </div>
            </div>
            <div className="stampede-grid">
              {stampede.results.map((r, i) => (
                <span key={i} className={`sq ${SOURCE_CLASS[r.source]}`} title={`${r.instance} · ${r.source} · ${r.ms}ms`} />
              ))}
            </div>
            <div className="strip-legend muted small">
              <span className="sq db inline" /> hit the DB · <span className="sq l2w inline" /> waited for the lock holder to fill L2 ·{' '}
              <span className="sq l1 inline" /> L1/<span className="sq l2 inline" />L2 hit
            </div>
          </>
        )}
      </div>
    </div>
  );
}
