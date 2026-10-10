import { useState } from 'react';

export default function Explainer({ title, subtitle, children, tryThis }) {
  const [open, setOpen] = useState(true);
  return (
    <section className="explainer">
      <header onClick={() => setOpen((o) => !o)}>
        <div>
          <h2>{title}</h2>
          {subtitle && <p className="subtitle">{subtitle}</p>}
        </div>
        <span className="chevron">{open ? '▾' : '▸'}</span>
      </header>
      {open && (
        <div className="explainer-body">
          <div className="explainer-text">{children}</div>
          {tryThis && (
            <div className="try-this">
              <h4>Try this</h4>
              <ol>
                {tryThis.map((t, i) => (
                  <li key={i}>{t}</li>
                ))}
              </ol>
            </div>
          )}
        </div>
      )}
    </section>
  );
}
