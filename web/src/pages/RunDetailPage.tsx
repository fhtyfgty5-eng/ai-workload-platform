import { useEffect, useRef, useState, type ReactElement } from "react";
import type { ApiClient } from "../api/client";
import type { EventPage, RunSummary, TaskPage } from "../api/types";
import { StatusBadge } from "../components/StatusBadge";
import { AsyncState } from "../components/AsyncState";
import { usePolling } from "../hooks/usePolling";

export function RunDetailPage({ client, runId }: { client: ApiClient; runId: string }): ReactElement {
  const [tasks, setTasks] = useState<TaskPage>();
  const [events, setEvents] = useState<EventPage>();
  const [error, setError] = useState<Error>();
  const [canceling, setCanceling] = useState(false);
  const [cancelMessage, setCancelMessage] = useState("");
  const [pollingEnabled, setPollingEnabled] = useState(true);
  const controller = useRef<AbortController | null>(null);
  const runResult = usePolling<RunSummary>(() => client.get(`/api/v1/runs/${encodeURIComponent(runId)}`), 2000, pollingEnabled);
  const run = runResult.data;
  useEffect(() => { if (run && ["succeeded", "failed", "canceled"].includes(run.status)) setPollingEnabled(false); }, [run]);
  useEffect(() => {
    controller.current?.abort(); const next = new AbortController(); controller.current = next;
    void Promise.all([
      client.get<TaskPage>(`/api/v1/runs/${encodeURIComponent(runId)}/tasks`, next.signal),
      client.get<EventPage>(`/api/v1/runs/${encodeURIComponent(runId)}/events`, next.signal),
    ]).then(([taskPage, eventPage]) => { setTasks(taskPage); setEvents(eventPage); setError(undefined); }).catch((caught) => { if (!(caught instanceof DOMException && caught.name === "AbortError")) setError(caught instanceof Error ? caught : new Error("加载运行详情失败")); });
    return () => next.abort();
  }, [client, runId, run?.revision]);
  const cancel = async () => {
    setCanceling(true); setCancelMessage("");
    try { await client.post(`/api/v1/runs/${encodeURIComponent(runId)}/cancel`, {}, `cancel-${runId}-${crypto.randomUUID()}`); setCancelMessage("已提交取消请求"); runResult.refresh(); } catch (caught) { setCancelMessage(caught instanceof Error ? caught.message : "取消失败"); } finally { setCanceling(false); }
  };
  return <section><header className="page-header"><p className="eyebrow">Run Detail</p><h1>运行详情</h1><p className="muted">Run ID：<code>{runId}</code></p></header><AsyncState loading={runResult.loading && !run} error={runResult.error}><div className="content-panel run-header"><div><span className="muted">状态</span><div>{run && <StatusBadge value={run.status} />}</div></div><div><span className="muted">Workflow</span><strong>{run?.workflow_id ?? "-"}</strong></div><div><span className="muted">任务数</span><strong>{run?.task_count ?? "-"}</strong></div><button onClick={cancel} disabled={canceling || run?.status === "succeeded" || run?.status === "failed" || run?.status === "canceled"}>{canceling ? "提交中..." : "取消 Run"}</button></div></AsyncState>{cancelMessage && <p className="muted" role="status">{cancelMessage}</p>}<section className="content-panel"><h2>任务</h2><AsyncState loading={!tasks && !error} error={error} empty={tasks?.items.length === 0}>{tasks && <div className="table-wrap"><table><thead><tr><th>任务</th><th>状态</th><th>完成时间</th></tr></thead><tbody>{tasks.items.map((task) => <tr key={task.key}><td><code>{task.key}</code></td><td><StatusBadge value={task.status} /></td><td>{task.finished_at ? new Date(task.finished_at).toLocaleString() : "-"}</td></tr>)}</tbody></table></div>}</AsyncState></section><section className="content-panel"><h2>事件时间线</h2><AsyncState loading={!events && !error} error={error} empty={events?.items.length === 0}>{events && <ol className="event-list">{events.items.map((event) => <li key={event.sequence}><span className="muted">#{event.sequence} {new Date(event.at).toLocaleTimeString()}</span><strong>{event.from} → {event.to}</strong>{event.reason && <span>{event.reason}</span>}</li>)}</ol>}</AsyncState></section></section>;
}
