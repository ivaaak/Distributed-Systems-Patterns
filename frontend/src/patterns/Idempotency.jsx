import { useRef, useState } from 'react';
import { usePolling } from '../lib/page.js';
import Explainer from '../components/Explainer.jsx';
import { useCluster } from '../lib/cluster.jsx';
import { instanceColor, sleep } from '../lib/api.js';

const AMOUNT = 42;
const MAX_ATTEMPTS = 4;
const CLIENT_TIMEOUT_MS = 1500;

export default function Idempotency() {
  const { pick, callAny } = useCluster();
  const [useKey, setUseKey] = useState(false);
  const [flaky, setFlaky] = useState(true);
  const [doubleClick, setDoubleClick] = useState(false);
  const [checkouts, setCheckouts] = useState([]);
  const [ledger, setLedger] = useState([]);
  const seq = useRef(0);

  usePolling(() => callAny('/api/payments/ledger').then(({ data }) => setLedger(data.charges)), 800);

  const addAttempt = (orderId, flow, attempt) =>
    setCheckouts((cs) =>
      cs.map((c) => (c.orderId !== orderId ? c : { ...c, flows: c.flows.map((f, i) => (i === flow ? [...f, attempt] : f)) }))
    );

  /** A client that retries on network errors and 409s, like any well-behaved SDK. */
  const submit = async (orderId, key, flow, dropFirst) => {
    for (let n = 1; n <= MAX_ATTEMPTS; n++) {
      const inst = pick();
      if (!inst) return;
      try {
        const res = await fetch(`${inst.url}/api/payments`, {
          method: 'POST',
          headers: { 'content-type': 'application/json', ...(key ? { 'Idempotency-Key': key } : {}) },
          body: JSON.stringify({ amount: AMOUNT, orderId, dropResponse: dropFirst && n === 1, attempt: n }),
          signal: AbortSignal.timeout(CLIENT_TIMEOUT_MS),
          cache: 'no-store',
        });
        const data = await res.json();
        if (res.status === 409) {
          addAttempt(orderId, flow, { n, instance: inst.id, kind: 'warn', text: '409 · original still in flight' });
          await sleep(350 * n);
          continue;
        }
        addAttempt(orderId, flow, {
          n,
          instance: inst.id,
          kind: data.replayed ? 'replay' : 'ok',
          text: data.replayed ? `replayed ${data.chargeId}` : `charged ${data.chargeId}`,
        });
        return;
      } catch {
        addAttempt(orderId, flow, { n, instance: inst.id, kind: 'bad', text: `no response after ${CLIENT_TIMEOUT_MS / 1000}s → retry` });
        await sleep(300 * n);
      }
    }
  };

  const pay = () => {
    const orderId = `order-${++seq.current}`;
    const key = useKey ? `idem-${crypto.randomUUID().slice(0, 8)}` : null;
    const flows = doubleClick ? 2 : 1;
    setCheckouts((cs) => [{ orderId, key, flaky, flows: Array.from({ length: flows }, () => []) }, ...cs].slice(0, 6));
    for (let f = 0; f < flows; f++) submit(orderId, key, f, flaky && f === 0);
  };

  const reset = async () => {
    await callAny('/api/payments/reset', {});
    setCheckouts([]);
    setLedger([]);
  };

  const chargesFor = (orderId) => ledger.filter((c) => c.orderId === orderId);
  const doubleCharged = new Set(
    Object.entries(ledger.reduce((m, c) => ({ ...m, [c.orderId]: (m[c.orderId] ?? 0) + 1 }), {}))
      .filter(([, n]) => n > 1)
      .map(([o]) => o)
  );

  return (
    <div className="pattern">
      <Explainer
        title="Idempotency keys & safe retries"
        subtitle="Idempotency-Key header · SET idem:<key> NX · store & replay the response · 409 for concurrent duplicates"
        tryThis={[
          'Keep "flaky network" on and the key off, then Pay: the first response is lost, the client retries on another instance → charged twice.',
          'Turn on "Send Idempotency-Key" and Pay again: the retry lands on a different instance but gets the stored response replayed. One charge.',
          'Try "double-click": two concurrent submissions with the same key. One wins; the other gets 409, backs off and then receives the replay.',
        ]}
      >
        <p>
          Retries are the only way to survive a flaky network, but a timeout doesn&apos;t tell you whether the server did
          the work. For non-idempotent operations (charging a card, sending an email, creating an order) a blind retry can
          execute twice. In a multi-instance deployment the retry often reaches a <em>different</em> instance, so an
          in-memory dedupe table doesn&apos;t help.
        </p>
        <p>
          The client generates a unique key per logical operation and sends it on every attempt. The server claims{' '}
          <code>idem:&lt;key&gt;</code> in Redis with <code>SET NX</code>, performs the work once, stores the response, and
          replays it for any later attempt with the same key.
        </p>
        <p className="muted small">
          Retries don&apos;t only come from your code: browsers re-send a request on their own when a kept-alive connection
          resets, and proxies, SDKs and message brokers all retry. Assume every non-idempotent request can arrive twice.
        </p>
      </Explainer>

      <div className="controls card">
        <div className="control-group">
          <label>Client</label>
          <label className="toggle">
            <input type="checkbox" checked={useKey} onChange={(e) => setUseKey(e.target.checked)} /> send Idempotency-Key
          </label>
          <label className="toggle">
            <input type="checkbox" checked={flaky} onChange={(e) => setFlaky(e.target.checked)} /> flaky network (1st response lost)
          </label>
          <label className="toggle">
            <input type="checkbox" checked={doubleClick} onChange={(e) => setDoubleClick(e.target.checked)} /> double-click (2 concurrent submits)
          </label>
        </div>
        <div className="control-group">
          <label>Checkout</label>
          <button className="btn primary" onClick={pay}>
            Pay ${AMOUNT}
          </button>
          <button className="btn ghost" onClick={reset}>
            Reset
          </button>
          <span className="muted small">retries up to {MAX_ATTEMPTS}× with backoff, each attempt via the round-robin LB</span>
        </div>
      </div>

      <div className="grid-2">
        <div className="card">
          <h3>Checkouts</h3>
          {checkouts.length === 0 && <div className="viz-empty static">Press Pay to start a checkout.</div>}
          {checkouts.map((c) => {
            const charges = chargesFor(c.orderId);
            return (
              <div key={c.orderId} className={`checkout ${charges.length > 1 ? 'bad' : charges.length === 1 ? 'good' : ''}`}>
                <header>
                  <strong>{c.orderId}</strong>
                  <span className="muted small mono">{c.key ? `key ${c.key}` : 'no key'}</span>
                  <span className={`verdict ${charges.length > 1 ? 'bad' : 'good'}`}>
                    {charges.length === 0 ? '…' : charges.length === 1 ? `charged once ✓` : `charged ${charges.length}× — $${charges.length * AMOUNT}!`}
                  </span>
                </header>
                {c.flows.map((attempts, f) => (
                  <div key={f} className="flow">
                    {c.flows.length > 1 && <span className="muted small">click {f + 1}</span>}
                    {attempts.map((a) => (
                      <span key={a.n} className={`attempt ${a.kind}`}>
                        <b>#{a.n}</b>
                        <span className="tag" style={{ '--c': instanceColor(a.instance) }}>
                          {a.instance}
                        </span>
                        {a.text}
                      </span>
                    ))}
                  </div>
                ))}
              </div>
            );
          })}
        </div>

        <div className="card">
          <h3>
            Payment ledger <span className="muted small">(what the card network actually charged)</span>
          </h3>
          <table className="table">
            <tbody>
              {ledger
                .slice()
                .reverse()
                .slice(0, 16)
                .map((c) => (
                  <tr key={c.chargeId} className={doubleCharged.has(c.orderId) ? 'dup' : ''}>
                    <td className="mono small">{c.chargeId}</td>
                    <td>{c.orderId}</td>
                    <td className="mono small">${c.amount}</td>
                    <td>
                      <span className="tag" style={{ '--c': instanceColor(c.instance) }}>
                        {c.instance}
                      </span>
                    </td>
                    <td className="muted small">{c.key ? 'keyed' : 'no key'}</td>
                  </tr>
                ))}
              {ledger.length === 0 && (
                <tr>
                  <td className="muted">No charges yet.</td>
                </tr>
              )}
            </tbody>
          </table>
          <div className="strip-legend muted small">
            <span className="bad-text">red rows</span> = orders charged more than once
          </div>
        </div>
      </div>
    </div>
  );
}
