import { createContext, useContext, useEffect, useRef } from 'react';

/** True for the currently visible pattern page. Pages stay mounted when hidden. */
export const PageActiveContext = createContext(true);

export const usePageActive = () => useContext(PageActiveContext);

/** Run `fn` immediately and then every `ms`, but only while the page is visible. */
export function usePolling(fn, ms) {
  const active = usePageActive();
  const fnRef = useRef(fn);
  fnRef.current = fn;
  useEffect(() => {
    if (!active) return;
    const tick = () => Promise.resolve(fnRef.current()).catch(() => {});
    tick();
    const t = setInterval(tick, ms);
    return () => clearInterval(t);
  }, [active, ms]);
}
