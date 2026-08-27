import { useEffect, useState, type ReactElement } from "react";
import type { ApiClient } from "../api/client";
import type { RunPage, WorkerPage } from "../api/types";
import { AsyncState } from "../components/AsyncState";
import { StatusBadge } from "../components/StatusBadge";

export function OverviewPage({ client }: { client: ApiClient }): ReactElement {
  const [runs, setRuns] = useState<RunPage>();
  const [workers, setWorkers] = useState<WorkerPage>();
  const [metrics, setMetrics] = useState("");
  const [error, setError] = useState<Error>();
  useEffect(() => { const controller = new AbortController(); void Promise.all([client.get<RunPage>("/api/v1/runs?limit=10", controller.signal), client.get<WorkerPage>("/api/v1/workers?limit=50", controller.signal), client.getText("/metrics", controller.signal)]).then(([runPage, workerPage, text]) => { setRuns(runPage); setWorkers(workerPage); setMetrics(text); }).catch((caught) => { if (!(caught instanceof DOMException && caught.name === "AbortError")) setError(caught instanceof Error ? caught : new Error("加载概览失败")); }); return () => controller.abort(); }, [client]);
  const counts = runs?.items.reduce<Record<string, number>>((all, run) => ({ ...all, [run.status]: (all[run.status] ?? 0) + 1 }), {}) ?? {};
  const runStatusLabels: Record<string, string> = { pending: "等待中的 Run", running: "执行中的 Run", succeeded: "成功的 Run", failed: "失败的 Run" };
  return <section><header className="page-header"><p className="eyebrow">Overview</p><h1>平台概览</h1><p className="muted">从控制面返回的运行和执行节点摘要；状态卡片只统计最近 10 条 Run。</p></header><AsyncState loading={!runs && !workers} error={error}><div className="stat-grid">{["pending", "running", "succeeded", "failed"].map((status) => <div className="stat-panel" key={status}><span className="muted">{runStatusLabels[status]}</span><strong>{counts[status] ?? 0}</strong></div>)}<div className="stat-panel"><span className="muted">在线 Worker（最多 50 个）</span><strong>{workers?.items.filter((worker) => worker.status === "active").length ?? 0}</strong></div></div><section className="content-panel"><h2>最近运行</h2>{runs?.items.length ? <div className="table-wrap"><table><thead><tr><th>Run</th><th>Workflow</th><th>状态</th><th>创建时间</th></tr></thead><tbody>{runs.items.map((run) => <tr key={run.run_id}><td><code>{run.run_id}</code></td><td>{run.workflow_id}</td><td><StatusBadge value={run.status} /></td><td>{new Date(run.created_at).toLocaleString()}</td></tr>)}</tbody></table></div> : <p className="muted">暂无运行记录</p>}</section><section className="content-panel"><h2>观测摘要</h2><pre className="metrics-preview">{metrics || "暂无 Metrics"}</pre></section></AsyncState></section>;
}
