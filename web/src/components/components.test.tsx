import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { AppLayout } from "./AppLayout";
import { AsyncState } from "./AsyncState";
import { StatusBadge } from "./StatusBadge";

describe("console components", () => {
  it("renders navigation and role without hiding the main content", () => {
    render(<AppLayout page="overview" onNavigate={vi.fn()} session={{ baseUrl: "", token: "x", role: "viewer" }} onLogout={vi.fn()}>content</AppLayout>);
    expect(screen.getByRole("navigation", { name: "主导航" })).toBeInTheDocument();
    expect(screen.getByText("viewer")).toBeInTheDocument();
    expect(screen.getByText("content")).toBeInTheDocument();
  });

  it("always renders readable status text", () => {
    render(<StatusBadge value="waiting_retry" />);
    expect(screen.getByText("waiting_retry")).toBeInTheDocument();
  });

  it("renders loading, empty and error states", () => {
    const { rerender } = render(<AsyncState loading>data</AsyncState>);
    expect(screen.getByRole("status")).toHaveTextContent("正在加载");
    rerender(<AsyncState loading={false} empty>data</AsyncState>);
    expect(screen.getByText("暂无数据")).toBeInTheDocument();
    rerender(<AsyncState loading={false} error={new Error("offline")}>data</AsyncState>);
    expect(screen.getByRole("alert")).toHaveTextContent("offline");
  });
});
