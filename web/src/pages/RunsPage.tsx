import { useState, type ReactElement } from "react";
import type { ApiClient } from "../api/client";
import type { RunPage } from "../api/types";
import { AsyncState } from "../components/AsyncState";
import { StatusBadge } from "../components/StatusBadge";
import { usePolling } from "../hooks/usePolling";
import { RunDetailPage } from "./RunDetailPage";

export function RunsPage({ client, initialRunId }: { client: ApiClient; initialRunId?: string }): ReactElement {
  const [selected, setSelected] = useState<string | undefined>(initialRunId);
  const result = usePolling<RunPage>((signal) => client.get("/api/v1/runs?limit=50", signal), 3000, !selected);
  if (selected) return <><button className="back-button" onClick={() => setSelected(undefined)}>返回运行记录</button><RunDetailPage client={client} runId={selected} /></>;
  return <section><header className="page-header"><p className="eyebrow">Runs</p><h1>运行记录</h1><p className="muted">所有运行实例都固定引用一个不可变 Workflow 版本。</p></header><section className="content-panel"><AsyncState loading={result.loading && !result.data} error={result.error} empty={result.data?.items.length === 0}>{result.data && <div className="table-wrap"><table><thead><tr><th>Run</th><th>Workflow</th><th>版本</th><th>状态</th><th>创建时间</th></tr></thead><tbody>{result.data.items.map((run) => <tr key={run.run_id}><td><button className="link-button" onClick={() => setSelected(run.run_id)}>{run.run_id}</button></td><td>{run.workflow_id}</td><td>v{run.workflow_version}</td><td><StatusBadge value={run.status} /></td><td>{new Date(run.created_at).toLocaleString()}</td></tr>)}</tbody></table></div>}</AsyncState></section></section>;
}
