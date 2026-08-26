import { type FormEvent, useState, type ReactElement } from "react";
import type { Role, Session } from "../api/types";
import { loadSession, saveSession } from "./session";

export function AuthGate({ onAuthenticated }: { onAuthenticated: (session: Session) => void }): ReactElement {
  const existing = loadSession();
  const [baseUrl, setBaseUrl] = useState(existing?.baseUrl ?? "");
  const [token, setToken] = useState(existing?.token ?? "");
  const [role, setRole] = useState<Role>(existing?.role ?? "operator");
  const submit = (event: FormEvent) => {
    event.preventDefault();
    if (!token.trim()) return;
    const session = { baseUrl: baseUrl.trim(), token: token.trim(), role };
    saveSession(session);
    onAuthenticated(session);
  };
  return (
    <main className="auth-shell">
      <form className="auth-panel" onSubmit={submit}>
        <p className="eyebrow">AI Workload Platform</p>
        <h1>连接控制面</h1>
        <p className="muted">输入本机控制面地址和 Token，开始查看或操作可靠工作流。</p>
        <label>控制面地址<input value={baseUrl} onChange={(event) => setBaseUrl(event.target.value)} placeholder="留空使用当前页面地址" /></label>
        <label>角色<select value={role} onChange={(event) => setRole(event.target.value as Role)}><option value="operator">operator（可操作）</option><option value="viewer">viewer（只读）</option></select></label>
        <label>Bearer Token<input type="password" value={token} onChange={(event) => setToken(event.target.value)} autoComplete="off" required /></label>
        <button type="submit">进入控制台</button>
      </form>
    </main>
  );
}
