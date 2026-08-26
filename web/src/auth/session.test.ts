import { beforeEach, describe, expect, it } from "vitest";
import { clearSession, loadSession, saveSession } from "./session";

describe("console session", () => {
  beforeEach(() => sessionStorage.clear());

  it("round-trips the current tab session", () => {
    saveSession({ baseUrl: "", token: "operator", role: "operator" });
    expect(loadSession()).toEqual({ baseUrl: "", token: "operator", role: "operator" });
  });

  it("clears credentials when the session ends", () => {
    saveSession({ baseUrl: "", token: "viewer", role: "viewer" });
    clearSession();
    expect(loadSession()).toBeNull();
  });
});
