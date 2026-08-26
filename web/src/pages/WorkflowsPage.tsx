import { type ReactElement } from "react";
import type { ApiClient } from "../api/client";
import type { WorkflowPage } from "../api/types";
import { AsyncState } from "../components/AsyncState";
import { usePolling } from "../hooks/usePolling";

export function WorkflowsPage({ client }: { client: ApiClient }): ReactElement {
  const result = usePolling<WorkflowPage>((signal) => client.get("/api/v1/workflows?limit=50", signal), 5000, true);
  return <section><header className="page-header"><p className="eyebrow">Workflows</p><h1>工作流</h1><p className="muted">工作流版本不可变，运行实例会固定引用创建时的版本。</p></header><section className="content-panel"><AsyncState loading={result.loading && !result.data} error={result.error} empty={result.data?.items.length === 0}>{result.data && <div className="table-wrap"><table><thead><tr><th>Workflow</th><th>最新版本</th><th>创建者</th><th>创建时间</th></tr></thead><tbody>{result.data.items.map((workflow) => <tr key={workflow.workflow_id}><td><code>{workflow.workflow_id}</code></td><td>v{workflow.latest_version}</td><td>{workflow.created_by}</td><td>{new Date(workflow.created_at).toLocaleString()}</td></tr>)}</tbody></table></div>}</AsyncState></section></section>;
}
