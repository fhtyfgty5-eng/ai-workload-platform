import { useRef, useState, type ReactElement } from "react";
import { ApiError, type ApiClient } from "../api/client";
import type { DefinitionRef, StartRunResponse, WorkflowDraft } from "../api/types";
import { clearDraft, saveDraft } from "../auth/session";
import { DraftSummary } from "../components/DraftSummary";
import { TaskDefinitionTable } from "../components/TaskDefinitionTable";

export function DraftReviewPage({ client, draft: initialDraft, onStarted }: { client: ApiClient; draft: WorkflowDraft; onStarted: (runId: string) => void }): ReactElement {
  const [draft, setDraft] = useState(initialDraft);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [confirmed, setConfirmed] = useState(false);
  const controller = useRef<AbortController | null>(null);
  const hasErrors = draft.validation.errors.length > 0;
  const unresolved = draft.questions.some((question) => !question.resolved);
  const validate = async () => {
    controller.current?.abort(); const next = new AbortController(); controller.current = next;
    setLoading(true); setError("");
    try { const updated = await client.post<WorkflowDraft>(`/api/v1/agent/drafts/${encodeURIComponent(draft.draft_id)}/validate`, { draft }, undefined, next.signal); setDraft(updated); saveDraft(updated); } catch (caught) { setError(caught instanceof Error ? caught.message : "校验草稿失败"); } finally { setLoading(false); }
  };
  const confirm = async () => {
    if (draft.status !== "needs_confirmation" || hasErrors || unresolved) return;
    controller.current?.abort(); const next = new AbortController(); controller.current = next;
    setLoading(true); setError("");
    try {
      const definition = await client.post<WorkflowDraft["definition"]>(`/api/v1/agent/drafts/${encodeURIComponent(draft.draft_id)}/confirm`, { draft, content_hash: draft.content_hash }, undefined, next.signal);
      setConfirmed(true);
      let ref: DefinitionRef;
      try {
        ref = await client.post<DefinitionRef>("/api/v1/workflows", definition, `workflow-${definition.id}-${crypto.randomUUID()}`, next.signal);
      } catch (caught) {
        if (!(caught instanceof ApiError) || caught.code !== "workflow_exists") throw caught;
        // 同名 Workflow 已存在时创建不可变的新版本，使公开演示可以重复运行。
        ref = await client.post<DefinitionRef>(`/api/v1/workflows/${encodeURIComponent(definition.id)}/versions`, definition, `workflow-version-${definition.id}-${crypto.randomUUID()}`, next.signal);
      }
      const started = await client.post<StartRunResponse>(`/api/v1/workflows/${encodeURIComponent(ref.workflow_id)}/versions/${ref.version}/runs`, {}, `run-${ref.workflow_id}-${crypto.randomUUID()}`, next.signal);
      clearDraft();
      onStarted(started.run_id);
    } catch (caught) { setError(caught instanceof Error ? caught.message : "确认草稿失败"); } finally { setLoading(false); }
  };
  return <section><header className="page-header"><p className="eyebrow">Draft Review</p><h1>审核工作流草稿</h1><p className="muted">确认前请检查任务、依赖、事实、假设和校验结果。</p></header><DraftSummary draft={draft} /><section className="content-panel"><h2>任务定义</h2><TaskDefinitionTable tasks={draft.definition.tasks} /></section><section className="content-panel validation-panel"><h2>校验结果</h2>{hasErrors ? <ul className="error-list">{draft.validation.errors.map((item) => <li key={`${item.code}-${item.path}`}>{item.message}</li>)}</ul> : <p className="success-text">没有阻塞错误</p>}{draft.validation.warnings.length > 0 && <ul className="warning-list">{draft.validation.warnings.map((item) => <li key={`${item.code}-${item.path}`}>{item.message}</li>)}</ul>}</section>{error && <p className="form-error" role="alert">{error}</p>}{confirmed && <p className="success-text" role="status">草稿已确认，正在创建 Workflow 并启动 Run。</p>}<div className="action-row">{draft.status !== "needs_confirmation" && <button onClick={validate} disabled={loading}>校验草稿</button>}<button onClick={confirm} disabled={loading || draft.status !== "needs_confirmation" || hasErrors || unresolved}>确认并启动运行</button></div></section>;
}
