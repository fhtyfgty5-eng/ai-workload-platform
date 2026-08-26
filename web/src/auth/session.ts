import type { Session, WorkflowDraft } from "../api/types";

const sessionKey = "workload.console.session";
const draftKey = "workload.console.draft";

export function loadSession(): Session | null {
  return readJSON<Session>(sessionKey, isSession);
}

export function saveSession(session: Session): void {
  sessionStorage.setItem(sessionKey, JSON.stringify(session));
}

export function clearSession(): void {
  sessionStorage.removeItem(sessionKey);
  sessionStorage.removeItem(draftKey);
}

export function loadDraft(): WorkflowDraft | null {
  return readJSON<WorkflowDraft>(draftKey, (value): value is WorkflowDraft => isObject(value) && typeof value.draft_id === "string" && typeof value.content_hash === "string");
}

export function saveDraft(draft: WorkflowDraft): void {
  sessionStorage.setItem(draftKey, JSON.stringify(draft));
}

export function clearDraft(): void {
  sessionStorage.removeItem(draftKey);
}

function readJSON<T>(key: string, guard: (value: unknown) => value is T): T | null {
  const raw = sessionStorage.getItem(key);
  if (!raw) return null;
  try {
    const value: unknown = JSON.parse(raw);
    return guard(value) ? value : null;
  } catch {
    return null;
  }
}

function isSession(value: unknown): value is Session {
  return isObject(value) && typeof value.baseUrl === "string" && typeof value.token === "string" && (value.role === "viewer" || value.role === "operator");
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}
