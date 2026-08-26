import { type ReactElement } from "react";
import type { ApiClient } from "../api/client";
import type { WorkerPage } from "../api/types";
import { AsyncState } from "../components/AsyncState";
import { StatusBadge } from "../components/StatusBadge";
import { usePolling } from "../hooks/usePolling";

export function WorkersPage({ client }: { client: ApiClient }): ReactElement {
  const result = usePolling<WorkerPage>((signal) => client.get("/api/v1/workers?limit=50", signal), 5000, true);
  return <section><header className="page-header"><p className="eyebrow">Workers</p><h1>执行节点</h1><p className="muted">查看 Worker 会话、能力和当前租约，不直接操作 Worker 协议。</p></header><section className="content-panel"><AsyncState loading={result.loading && !result.data} error={result.error} empty={result.data?.items.length === 0}>{result.data && <div className="table-wrap"><table><thead><tr><th>名称</th><th>状态</th><th>执行器</th><th>并发</th><th>活动租约</th></tr></thead><tbody>{result.data.items.map((worker) => <tr key={worker.worker_id}><td>{worker.display_name}</td><td><StatusBadge value={worker.status} /></td><td>{worker.executor_kinds.join(", ")}</td><td>{worker.max_concurrency}</td><td>{worker.active_leases}</td></tr>)}</tbody></table></div>}</AsyncState></section></section>;
}
