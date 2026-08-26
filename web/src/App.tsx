import { useState, type ReactElement } from "react";
import type { Session } from "./api/types";
import { AuthGate } from "./auth/AuthGate";
import { loadDraft, loadSession } from "./auth/session";
import { ApiClient } from "./api/client";
import { AppLayout, type Page } from "./components/AppLayout";
import { CreatePage } from "./pages/CreatePage";
import { DraftReviewPage } from "./pages/DraftReviewPage";
import { OverviewPage } from "./pages/OverviewPage";
import { WorkflowsPage } from "./pages/WorkflowsPage";
import { WorkersPage } from "./pages/WorkersPage";
import { ObservabilityPage } from "./pages/ObservabilityPage";
import { RunDetailPage } from "./pages/RunDetailPage";
import { RunsPage } from "./pages/RunsPage";
import type { WorkflowDraft } from "./api/types";
import { useMemo } from "react";

const pageTitles: Record<Page, string> = {
  overview: "概览",
  workflows: "工作流",
  create: "创建草稿",
  "draft-review": "审核草稿",
  runs: "运行记录",
  workers: "Worker",
  observability: "观测",
};

export default function App(): ReactElement {
  const [session, setSession] = useState<Session | null>(() => loadSession());
  const [page, setPage] = useState<Page>("overview");
  const [draft, setDraft] = useState<WorkflowDraft | null>(() => loadDraft());
  const [selectedRunId, setSelectedRunId] = useState<string>();
  // 即使认证状态发生变化，也必须在每次渲染中按相同顺序调用 Hook。
  const client = useMemo(() => session ? new ApiClient(session.baseUrl, session.token) : null, [session]);
  if (!session || !client) return <AuthGate onAuthenticated={setSession} />;
  let content: ReactElement;
  if (page === "create") content = <CreatePage client={client} onDraft={(next) => { setDraft(next); setPage("draft-review"); }} />;
  else if (page === "draft-review" && draft) content = <DraftReviewPage client={client} draft={draft} onStarted={(runId) => { setSelectedRunId(runId); setPage("runs"); }} />;
  else if (page === "overview") content = <OverviewPage client={client} />;
  else if (page === "workflows") content = <WorkflowsPage client={client} />;
  else if (page === "runs") content = <RunsPage client={client} initialRunId={selectedRunId} />;
  else if (page === "workers") content = <WorkersPage client={client} />;
  else if (page === "observability") content = <ObservabilityPage client={client} />;
  else content = <><header className="page-header"><p className="eyebrow">AI Workload Platform</p><h1>{pageTitles[page]}</h1><p className="muted">控制面：{session.baseUrl || window.location.origin}</p></header><section className="placeholder-panel"><h2>页面不可用</h2><p className="muted">请从主导航重新选择页面。</p></section></>;
  return <AppLayout page={page} onNavigate={setPage} session={session} onLogout={() => setSession(null)}>{content}</AppLayout>;
}
