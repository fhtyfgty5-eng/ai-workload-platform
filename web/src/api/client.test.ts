import { describe, expect, it, vi } from "vitest";
import { ApiClient, ApiError } from "./client";

function jsonResponse(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

describe("ApiClient", () => {
  it("sends bearer token and idempotency key", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(jsonResponse({ workflow_id: "demo", version: 1 }));
    const client = new ApiClient("http://127.0.0.1:8080", "operator", fetchImpl);

    await client.post("/api/v1/workflows", { id: "demo" }, "workflow-demo-v1");

    const headers = new Headers(fetchImpl.mock.calls[0]?.[1]?.headers);
    expect(headers.get("Authorization")).toBe("Bearer operator");
    expect(headers.get("Idempotency-Key")).toBe("workflow-demo-v1");
    expect(headers.get("Content-Type")).toBe("application/json");
  });

  it("decodes text responses without treating metrics as JSON", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(new Response("workload_http_requests_total 1\n", { status: 200 }));
    const client = new ApiClient("", "viewer", fetchImpl);

    await expect(client.getText("/metrics")).resolves.toContain("workload_http_requests_total");
  });

  it("converts an error envelope into ApiError", async () => {
    const fetchImpl = vi.fn().mockResolvedValue(jsonResponse({ error: { code: "forbidden", message: "operator role required", request_id: "req-1" } }, 403));
    const client = new ApiClient("", "viewer", fetchImpl);

    await expect(client.get("/api/v1/agent/drafts")).rejects.toMatchObject({
      status: 403,
      code: "forbidden",
      requestId: "req-1",
    });
  });

  it("invokes a browser-like fetch without borrowing the client as this", async () => {
    const fetchImpl = vi.fn(function (this: unknown) {
      if (this !== undefined && this !== globalThis) throw new TypeError("Illegal invocation");
      return Promise.resolve(jsonResponse({ ok: true }));
    });
    const client = new ApiClient("", "viewer", fetchImpl as unknown as typeof fetch);

    await expect(client.get("/health/live")).resolves.toEqual({ ok: true });
  });
});
