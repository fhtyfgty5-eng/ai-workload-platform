export type ApiErrorInit = {
  status: number;
  code: string;
  message: string;
  requestId?: string;
};

export class ApiError extends Error {
  readonly status: number;
  readonly code: string;
  readonly requestId?: string;

  constructor(init: ApiErrorInit) {
    super(init.message);
    this.name = "ApiError";
    this.status = init.status;
    this.code = init.code;
    this.requestId = init.requestId;
  }
}

type FetchLike = typeof fetch;

export class ApiClient {
  private readonly baseUrl: string;
  private readonly token: string;
  private readonly fetchImpl: FetchLike;

  constructor(baseUrl: string, token: string, fetchImpl: FetchLike = fetch) {
    this.baseUrl = baseUrl.replace(/\/$/, "");
    this.token = token;
    // 浏览器原生 fetch 依赖全局对象作为 this，不能作为 ApiClient 方法直接调用。
    this.fetchImpl = fetchImpl.bind(globalThis);
  }

  get<T>(path: string, signal?: AbortSignal): Promise<T> {
    return this.request<T>("GET", path, undefined, undefined, signal);
  }

  getText(path: string, signal?: AbortSignal): Promise<string> {
    return this.requestText("GET", path, undefined, undefined, signal);
  }

  post<T>(path: string, body: unknown, idempotencyKey?: string, signal?: AbortSignal): Promise<T> {
    return this.request<T>("POST", path, body, idempotencyKey, signal);
  }

  private async request<T>(method: string, path: string, body?: unknown, idempotencyKey?: string, signal?: AbortSignal): Promise<T> {
    const response = await this.send(method, path, body, idempotencyKey, signal);
    if (!response.ok) {
      throw await toApiError(response);
    }
    return (await response.json()) as T;
  }

  private async requestText(method: string, path: string, body?: unknown, idempotencyKey?: string, signal?: AbortSignal): Promise<string> {
    const response = await this.send(method, path, body, idempotencyKey, signal);
    if (!response.ok) {
      throw await toApiError(response);
    }
    return response.text();
  }

  private send(method: string, path: string, body?: unknown, idempotencyKey?: string, signal?: AbortSignal): Promise<Response> {
    const headers = new Headers({ Authorization: `Bearer ${this.token}` });
    if (body !== undefined) {
      headers.set("Content-Type", "application/json");
    }
    if (idempotencyKey) {
      headers.set("Idempotency-Key", idempotencyKey);
    }
    return this.fetchImpl(this.baseUrl + path, {
      method,
      headers,
      body: body === undefined ? undefined : JSON.stringify(body),
      signal,
    });
  }
}

async function toApiError(response: Response): Promise<ApiError> {
  try {
    const envelope = (await response.json()) as { error?: { code?: string; message?: string; request_id?: string } };
    return new ApiError({
      status: response.status,
      code: envelope.error?.code ?? "http_error",
      message: envelope.error?.message ?? response.statusText,
      requestId: envelope.error?.request_id,
    });
  } catch {
    return new ApiError({ status: response.status, code: "http_error", message: response.statusText || "request failed" });
  }
}
