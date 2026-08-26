import { Activity, Boxes, FilePlus2, Gauge, LayoutDashboard, LogOut, Server, Workflow } from "lucide-react";
import type { ReactElement, ReactNode } from "react";
import type { Session } from "../api/types";
import { clearSession } from "../auth/session";

export type Page = "overview" | "workflows" | "create" | "draft-review" | "runs" | "workers" | "observability";

export function AppLayout({ children, page, onNavigate, session, onLogout }: { children: ReactNode; page: Page; onNavigate: (page: Page) => void; session: Session; onLogout: () => void }): ReactElement {
  const links: { id: Page; label: string; icon: typeof LayoutDashboard }[] = [
    { id: "overview", label: "概览", icon: LayoutDashboard },
    { id: "workflows", label: "工作流", icon: Workflow },
    { id: "create", label: "创建草稿", icon: FilePlus2 },
    { id: "runs", label: "运行记录", icon: Activity },
    { id: "workers", label: "Worker", icon: Server },
    { id: "observability", label: "观测", icon: Gauge },
  ];
  return <div className="console-layout"><aside className="sidebar"><div className="brand"><Boxes size={20} /><span>Workload</span></div><nav aria-label="主导航">{links.map(({ id, label, icon: Icon }) => <button key={id} className={page === id ? "nav-link active" : "nav-link"} onClick={() => onNavigate(id)}><Icon size={17} />{label}</button>)}</nav><div className="sidebar-footer"><span className="role-label">{session.role}</span><button className="nav-link" onClick={() => { clearSession(); onLogout(); }}><LogOut size={17} />退出</button></div></aside><main className="main-content">{children}</main></div>;
}
