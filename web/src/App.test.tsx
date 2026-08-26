import { fireEvent, render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it } from "vitest";
import App from "./App";

describe("App authentication transition", () => {
  beforeEach(() => {
    sessionStorage.clear();
  });

  it("renders the console after signing in from the auth gate", () => {
    render(<App />);
    fireEvent.change(screen.getByLabelText("Bearer Token"), { target: { value: "local-token" } });
    fireEvent.click(screen.getByRole("button", { name: "进入控制台" }));
    expect(screen.getByRole("navigation", { name: "主导航" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "平台概览" })).toBeInTheDocument();
  });
});
