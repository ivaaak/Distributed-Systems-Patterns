import { useCallback, useEffect, useRef, useState } from 'react';

let nextId = 0;

/**
 * Tiny animation engine for "messages in flight". A packet travels from `from` to `to`
 * starting at `start` (performance.now() based) over `duration` ms. The rAF loop only runs
 * while something is scheduled.
 */
export function usePackets() {
  const [packets, setPackets] = useState([]);
  const [now, setNow] = useState(() => performance.now());
  const active = packets.length > 0;

  useEffect(() => {
    if (!active) return;
    let raf;
    const loop = () => {
      const t = performance.now();
      setNow(t);
      setPackets((ps) => {
        const keep = ps.filter((p) => t < p.start + p.duration + (p.linger ?? 0));
        return keep.length === ps.length ? ps : keep;
      });
      raf = requestAnimationFrame(loop);
    };
    raf = requestAnimationFrame(loop);
    return () => cancelAnimationFrame(raf);
  }, [active]);

  const launch = useCallback((...specs) => {
    const t = performance.now();
    setPackets((ps) => [
      ...ps,
      ...specs.flat().map((s) => ({ duration: 450, color: '#e2e8f0', ...s, start: s.start ?? t, id: nextId++ })),
    ]);
  }, []);

  return { packets, now, launch };
}

const ease = (x) => (x < 0.5 ? 2 * x * x : 1 - (-2 * x + 2) ** 2 / 2);

export function PacketLayer({ packets, now }) {
  return (
    <g className="packets">
      {packets.map((p) => {
        if (now < p.start) return null;
        const k = ease(Math.min(1, (now - p.start) / p.duration));
        if (k >= 1 && !p.showAtEnd) return null;
        const x = p.from.x + (p.to.x - p.from.x) * k;
        const y = p.from.y + (p.to.y - p.from.y) * k;
        return (
          <g key={p.id} transform={`translate(${x},${y})`}>
            <circle r={p.r ?? 7} fill={p.color} className="packet-dot" style={{ color: p.color }} />
            {p.label && (
              <text y={-12} className="packet-label" textAnchor="middle">
                {p.label}
              </text>
            )}
          </g>
        );
      })}
    </g>
  );
}

/** Keep a component re-rendering on an interval (for countdowns etc.). */
export function useNow(every = 100) {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const t = setInterval(() => setNow(Date.now()), every);
    return () => clearInterval(t);
  }, [every]);
  return now;
}

export function useLatest(value) {
  const ref = useRef(value);
  ref.current = value;
  return ref;
}
