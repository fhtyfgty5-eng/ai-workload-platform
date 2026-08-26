import type { ButtonHTMLAttributes, ReactElement, ReactNode } from "react";

export function IconButton({ label, children, ...props }: ButtonHTMLAttributes<HTMLButtonElement> & { label: string; children: ReactNode }): ReactElement {
  return <button {...props} className={`icon-button ${props.className ?? ""}`} aria-label={label} title={label}>{children}</button>;
}
