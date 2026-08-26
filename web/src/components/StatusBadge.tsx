import type { ReactElement } from "react";

export function StatusBadge({ value }: { value: string }): ReactElement {
  const normalized = value.toLowerCase().replace(/[^a-z0-9_]+/g, "-");
  return <span className={`status-badge status-${normalized}`}>{value}</span>;
}
