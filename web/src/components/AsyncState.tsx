import type { ReactElement, ReactNode } from "react";

export function AsyncState({ loading, error, empty, children }: { loading: boolean; error?: Error; empty?: boolean; children: ReactNode }): ReactElement {
  if (loading) return <p className="async-state" role="status">正在加载...</p>;
  if (error) return <p className="async-state error" role="alert">加载失败：{error.message}</p>;
  if (empty) return <p className="async-state">暂无数据</p>;
  return <>{children}</>;
}
