import type { ReactElement } from "react";
import type { WorkflowDraft } from "../api/types";
import { StatusBadge } from "./StatusBadge";

export function DraftSummary({ draft }: { draft: WorkflowDraft }): ReactElement {
  return <div className="draft-summary"><div className="summary-row"><span>草稿状态</span><StatusBadge value={draft.status} /></div><div className="summary-row"><span>内容哈希</span><code>{draft.content_hash}</code></div><h3>用户事实</h3>{draft.facts.length ? <ul>{draft.facts.map((fact) => <li key={`${fact.source}-${fact.statement}`}>{fact.statement} <span className="muted">({fact.source})</span></li>)}</ul> : <p className="muted">暂无用户事实</p>}<h3>Agent 假设</h3>{draft.assumptions.length ? <ul>{draft.assumptions.map((fact) => <li key={`${fact.source}-${fact.statement}`}>{fact.statement} <span className="muted">({fact.source})</span></li>)}</ul> : <p className="muted">暂无 Agent 假设</p>}<h3>待确认问题</h3>{draft.questions.length ? <ul>{draft.questions.map((question) => <li key={question.id}>{question.text} <span className="muted">{question.resolved ? "已解决" : "待解决"}</span></li>)}</ul> : <p className="muted">暂无待确认问题</p>}</div>;
}
