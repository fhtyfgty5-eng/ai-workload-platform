import { useEffect, useState, type ReactElement } from "react";
import type { ApiClient } from "../api/client";
import { AsyncState } from "../components/AsyncState";

export function ObservabilityPage({ client }: { client: ApiClient }): ReactElement {
  const [metrics, setMetrics] = useState<string>();
  const [error, setError] = useState<Error>();
  useEffect(() => { const controller = new AbortController(); void client.getText("/metrics", controller.signal).then(setMetrics).catch((caught) => { if (!(caught instanceof DOMException && caught.name === "AbortError")) setError(caught instanceof Error ? caught : new Error("加载 Metrics 失败")); }); return () => controller.abort(); }, [client]);
  return <section><header className="page-header"><p className="eyebrow">Observability</p><h1>观测</h1><p className="muted">这里只展示低基数 Metrics 和运行事件；详细日志与 Trace 仍由服务端输出。</p></header><section className="content-panel"><AsyncState loading={!metrics && !error} error={error}><pre className="metrics-preview">{metrics}</pre></AsyncState></section></section>;
}
