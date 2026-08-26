import { useCallback, useEffect, useRef, useState } from "react";

export function usePolling<T>(load: (signal: AbortSignal) => Promise<T>, intervalMs: number, enabled: boolean): { data?: T; loading: boolean; error?: Error; refresh(): void } {
  const loadRef = useRef(load);
  const [data, setData] = useState<T>();
  const [error, setError] = useState<Error>();
  const [loading, setLoading] = useState(enabled);
  const [refreshToken, setRefreshToken] = useState(0);
  loadRef.current = load;
  const refresh = useCallback(() => setRefreshToken((value) => value + 1), []);
  useEffect(() => {
    if (!enabled) { setLoading(false); return; }
    let disposed = false;
    let timer: number | undefined;
    const controller = new AbortController();
    const run = async () => {
      setLoading(true);
      try { setData(await loadRef.current(controller.signal)); setError(undefined); }
      catch (caught) { if (!disposed && !(caught instanceof DOMException && caught.name === "AbortError")) setError(caught instanceof Error ? caught : new Error("request failed")); }
      finally { if (!disposed) { setLoading(false); timer = window.setTimeout(run, intervalMs); } }
    };
    void run();
    return () => { disposed = true; controller.abort(); if (timer !== undefined) window.clearTimeout(timer); };
  }, [enabled, intervalMs, refreshToken]);
  return { data, loading, error, refresh };
}
