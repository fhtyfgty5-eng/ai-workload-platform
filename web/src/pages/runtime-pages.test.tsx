import { render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { ApiClient } from "../api/client";
import { RunDetailPage } from "./RunDetailPage";
import { WorkersPage } from "./WorkersPage";

function fakeClient(): ApiClient {
  return {
    get: vi.fn().mockImplementation((path: string) => {
      if (path.includes("/tasks")) return Promise.resolve({ items: [{ key: "read", status: "succeeded" }] });
      if (path.includes("/events")) return Promise.resolve({ items: [{ sequence: 1, at: "2026-08-26T00:00:00Z", entity: "workflow", key: "run-1", from: "pending", to: "running" }] });
      if (path.includes("/runs/run-1")) return Promise.resolve({ run_id: "run-1", workflow_id: "demo", workflow_version: 1, status: "succeeded", revision: 2, task_count: 1, created_at: "2026-08-26T00:00:00Z" });
      if (path.includes("/workers")) return Promise.resolve({ items: [{ worker_id: "worker-1", display_name: "worker-a", status: "active", executor_kinds: ["mock"], max_concurrency: 1, active_leases: 0 }] });
      return Promise.resolve({ items: [] });
    }),
    post: vi.fn(),
    getText: vi.fn().mockResolvedValue("workload_http_requests_total 1\n"),
  } as unknown as ApiClient;
}

describe("runtime pages", () => {
  it("shows run status, tasks and events", async () => {
    render(<RunDetailPage client={fakeClient()} runId="run-1" />);
    await waitFor(() => expect(screen.getAllByText("succeeded").length).toBeGreaterThan(0));
    expect(screen.getByText("read")).toBeInTheDocument();
    expect(screen.getByText("pending → running")).toBeInTheDocument();
  });

  it("shows worker capability and active lease count", async () => {
    render(<WorkersPage client={fakeClient()} />);
    await waitFor(() => expect(screen.getByText("worker-a")).toBeInTheDocument());
    expect(screen.getByText("mock")).toBeInTheDocument();
  });
});
