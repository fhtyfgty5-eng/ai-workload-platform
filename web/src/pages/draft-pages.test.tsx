import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { CreatePage } from "./CreatePage";
import { DraftReviewPage } from "./DraftReviewPage";
import type { DefinitionRef, StartRunResponse, WorkflowDraft } from "../api/types";
import type { ApiClient } from "../api/client";
import { ApiError } from "../api/client";

const draft: WorkflowDraft = {
  draft_id: "draft-1",
  goal: "先读取 article.md，再清洗内容，最后生成摘要",
  definition: { id: "agent-document-pipeline", concurrency: 1, tasks: [{ key: "read", action: "read-document", timeout_ms: 30000 }] },
  facts: [{ statement: "用户指定读取 article.md", source: "user" }],
  assumptions: [],
  questions: [],
  validation: { errors: [], warnings: [{ code: "assumption_present", message: "请审核默认参数" }] },
  tool_calls: [{ call_id: "call-1", name: "workflow_catalog_query", result: "allowed" }],
  status: "needs_confirmation",
  content_hash: "a".repeat(64),
  created_at: "2026-08-26T00:00:00Z",
};

afterEach(cleanup);

describe("draft pages", () => {
  it("requires a goal and creates a draft", async () => {
    const client = { post: vi.fn().mockResolvedValue(draft) } as unknown as ApiClient;
    const onDraft = vi.fn();
    render(<CreatePage client={client} onDraft={onDraft} />);
    fireEvent.click(screen.getByRole("button", { name: "生成草稿" }));
    expect(screen.getByText("请输入自然语言目标")).toBeInTheDocument();
    fireEvent.change(screen.getByRole("textbox"), { target: { value: draft.goal } });
    fireEvent.click(screen.getByRole("button", { name: "生成草稿" }));
    await waitFor(() => expect(onDraft).toHaveBeenCalledWith(draft));
    expect(client.post).toHaveBeenCalledWith("/api/v1/agent/drafts", { goal: draft.goal }, undefined, expect.any(AbortSignal));
  });

  it("confirms the reviewed hash, creates one workflow version and starts its run", async () => {
    const ref = { workflow_id: draft.definition.id, version: 1 } as DefinitionRef;
    const client = { post: vi.fn().mockImplementation((path: string) => {
      if (path.includes("confirm")) return Promise.resolve(draft.definition);
      if (path.endsWith("/runs")) return Promise.resolve({ run_id: "run-1", status: "pending" } as StartRunResponse);
      return Promise.resolve(ref);
    }) } as unknown as ApiClient;
    const onStarted = vi.fn();
    render(<DraftReviewPage client={client} draft={draft} onStarted={onStarted} />);
    expect(screen.getByText("请审核默认参数")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "确认并启动运行" }));
    await waitFor(() => expect(onStarted).toHaveBeenCalledWith("run-1"));
    expect(client.post).toHaveBeenCalledWith(expect.stringContaining("/confirm"), { draft, content_hash: draft.content_hash }, undefined, expect.any(AbortSignal));
    expect(client.post).toHaveBeenCalledWith("/api/v1/workflows", draft.definition, expect.stringContaining("workflow-"), expect.any(AbortSignal));
    expect(client.post).toHaveBeenCalledWith(`/api/v1/workflows/${ref.workflow_id}/versions/${ref.version}/runs`, {}, expect.stringContaining("run-"), expect.any(AbortSignal));
  });

  it("creates a new immutable version when the workflow ID already exists", async () => {
    const client = { post: vi.fn().mockImplementation((path: string) => {
      if (path.includes("confirm")) return Promise.resolve(draft.definition);
      if (path === "/api/v1/workflows") return Promise.reject(new ApiError({ status: 409, code: "workflow_exists", message: "workflow already exists" }));
      if (path.endsWith("/versions")) return Promise.resolve({ workflow_id: draft.definition.id, version: 2 } as DefinitionRef);
      return Promise.resolve({ run_id: "run-2", status: "pending" } as StartRunResponse);
    }) } as unknown as ApiClient;
    const onStarted = vi.fn();
    render(<DraftReviewPage client={client} draft={draft} onStarted={onStarted} />);

    fireEvent.click(screen.getByRole("button", { name: "确认并启动运行" }));

    await waitFor(() => expect(onStarted).toHaveBeenCalledWith("run-2"));
    expect(client.post).toHaveBeenCalledWith(`/api/v1/workflows/${draft.definition.id}/versions`, draft.definition, expect.stringContaining("workflow-version-"), expect.any(AbortSignal));
  });
});
