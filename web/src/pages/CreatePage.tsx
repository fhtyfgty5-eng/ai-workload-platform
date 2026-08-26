import { useEffect, useRef, useState, type ReactElement } from "react";
import type { ApiClient } from "../api/client";
import type { WorkflowDraft } from "../api/types";
import { loadDraft, saveDraft } from "../auth/session";

export function CreatePage({ client, onDraft }: { client: ApiClient; onDraft: (draft: WorkflowDraft) => void }): ReactElement {
  const [goal, setGoal] = useState(() => loadDraft()?.goal ?? "");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const controller = useRef<AbortController | null>(null);
  useEffect(() => () => controller.current?.abort(), []);
  const submit = async (event: React.FormEvent) => {
    event.preventDefault();
    if (!goal.trim()) { setError("请输入自然语言目标"); return; }
    controller.current?.abort();
    const next = new AbortController();
    controller.current = next;
    setLoading(true); setError("");
    try {
      const draft = await client.post<WorkflowDraft>("/api/v1/agent/drafts", { goal: goal.trim() }, undefined, next.signal);
      saveDraft(draft); onDraft(draft);
    } catch (caught) {
      if (!(caught instanceof DOMException && caught.name === "AbortError")) setError(caught instanceof Error ? caught.message : "生成草稿失败");
    } finally { if (!next.signal.aborted) setLoading(false); }
  };
  return <section><header className="page-header"><p className="eyebrow">Agent Runtime</p><h1>创建工作流草稿</h1><p className="muted">用自然语言描述目标，系统会生成可审核的结构化工作流。</p></header><form className="form-panel" onSubmit={submit}><label>自然语言目标<textarea value={goal} onChange={(event) => setGoal(event.target.value)} placeholder="例如：先读取 article.md，再清洗内容，最后生成摘要" rows={5} /></label>{error && <p className="form-error" role="alert">{error}</p>}<button type="submit" disabled={loading}>{loading ? "生成中..." : "生成草稿"}</button></form></section>;
}
