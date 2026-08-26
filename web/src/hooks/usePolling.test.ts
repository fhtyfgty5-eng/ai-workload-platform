import { renderHook, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { usePolling } from "./usePolling";

describe("usePolling", () => {
  it("loads once and stops scheduling after disabled", async () => {
    const load = vi.fn().mockResolvedValue({ status: "succeeded" });
    const { result, rerender } = renderHook(({ enabled }) => usePolling(load, 10, enabled), { initialProps: { enabled: true } });
    await waitFor(() => expect(result.current.data).toEqual({ status: "succeeded" }));
    const calls = load.mock.calls.length;
    rerender({ enabled: false });
    await new Promise((resolve) => setTimeout(resolve, 25));
    expect(load).toHaveBeenCalledTimes(calls);
  });
});
