import type { WorkflowDef } from "@xflow/core";

export interface WorkflowSummary {
  id: string;
  name: string;
  version?: string;
  status?: string;
  updatedAt?: string;
}

/**
 * Result of registering a workflow (POST /v1/workflows, PUT /v1/workflows/{id}).
 *
 * The server never echoes the full definition back from these endpoints; it
 * returns {@code {workflow_id, warnings}} inside the envelope `data`. The
 * snake_case `workflow_id` is read from the wire and exposed here as
 * `workflowId`; `warnings` is optional on both sides.
 */
export interface RegisterWorkflowResult {
  workflowId: string;
  warnings?: string[];
}

/**
 * Result of executing a workflow (POST /v1/workflows/{id}/execute,
 * POST /v1/workflows/execute). The server returns {@code {execution_id}}
 * inside the envelope `data`; the snake_case field is read from the wire and
 * exposed here as `executionId`.
 */
export interface ExecuteWorkflowResult {
  executionId: string;
}

export interface XFlowApiClientOptions {
  baseUrl: string;
  fetcher?: typeof fetch;
}

export interface XFlowApiClient {
  listWorkflows(): Promise<WorkflowSummary[]>;
  createWorkflow(workflow?: WorkflowDef): Promise<RegisterWorkflowResult>;
  getWorkflow(id: string): Promise<WorkflowDef>;
  saveWorkflow(workflow: WorkflowDef): Promise<RegisterWorkflowResult>;
  runWorkflow(workflowId: string): Promise<ExecuteWorkflowResult>;
}

/**
 * Error thrown when a user-facing endpoint returns a non-2xx envelope.
 *
 * Field sources (spec §5.1/§5.2), all may be absent:
 * - `status`: HTTP status code.
 * - `code`: the envelope `code` field — a stable snake_case business code
 *   (e.g. `workflow_not_found`). Distinct from HTTP status.
 * - `traceId`: the envelope `trace_id` field, server-authoritative
 *   (`envelope.go:94`).
 * - `requestId`: the `X-Request-Id` response header, echoed only when the
 *   client sent a legal value (`envelope.go:99`). Never sourced from the body.
 *
 * The `message` is the envelope `message` field only — never the raw body,
 * so server-internal detail cannot leak through a fallback.
 */
export class XFlowApiError extends Error {
  override readonly name = "XFlowApiError";

  constructor(
    readonly status: number,
    message: string,
    readonly code?: string,
    readonly traceId?: string,
    readonly requestId?: string
  ) {
    super(message);
  }
}

function joinUrl(baseUrl: string, path: string): string {
  return `${baseUrl.replace(/\/$/, "")}${path}`;
}

interface Envelope<T> {
  success: boolean;
  code?: string;
  message?: string;
  data: T | null;
  trace_id?: string;
}

// Wire shapes — the server emits snake_case JSON inside `data`; these mirror
// `registerWorkflowResponse` / `executeWorkflowResponse` (module_control.go).
interface RegisterWorkflowWire {
  workflow_id: string;
  warnings?: string[];
}
interface ExecuteWorkflowWire {
  execution_id: string;
}

async function readJson<T>(response: Response): Promise<T> {
  const text = await response.text();
  return (text ? JSON.parse(text) : ({} as T)) as T;
}

/**
 * Perform an envelope-wrapped request and return the unwrapped `data` payload.
 *
 * On 2xx the envelope `data` is returned (callers must read the server's
 * snake_case fields). On non-2xx an {@link XFlowApiError} is thrown, sourced
 * from the envelope `code`/`message`/`trace_id` and the `X-Request-Id`
 * response header — never from the raw body.
 *
 * Internal: the transport signature is not part of this package's public API.
 * Tests reach it through the client methods, which is the only way production
 * code reaches it too.
 */
async function request<T>(fetcher: typeof fetch, url: string, init?: RequestInit): Promise<T> {
  const response = await fetcher(url, init);
  const body = await readJson<Envelope<T>>(response);
  if (!response.ok) {
    const requestId = response.headers.get("x-request-id") ?? undefined;
    throw new XFlowApiError(
      response.status,
      body.message ?? response.statusText,
      body.code,
      body.trace_id,
      requestId
    );
  }
  // success strictly tracks 2xx (spec §3): there is no 2xx + success:false.
  return (body.data ?? null) as T;
}

const LIST_NOT_IMPLEMENTED =
  "listWorkflows is not implemented: the server has no registered list endpoint yet " +
  "(see docs/design/API-SPECIFICATION.md §9.6).";

export function createXFlowApiClient(options: XFlowApiClientOptions): XFlowApiClient {
  const fetcher = options.fetcher ?? fetch;

  return {
    listWorkflows() {
      // Server has no list capability yet (spec §9.6). Keep the method on the
      // interface so the gap stays discoverable instead of silently failing.
      return Promise.reject(new Error(LIST_NOT_IMPLEMENTED));
    },
    createWorkflow(workflow) {
      return request<RegisterWorkflowWire>(fetcher, joinUrl(options.baseUrl, "/workflows"), {
        method: "POST",
        headers: {
          "content-type": "application/json"
        },
        body: JSON.stringify(workflow ?? {})
      }).then((data) => ({
        workflowId: data.workflow_id,
        warnings: data.warnings
      }));
    },
    getWorkflow(id) {
      return request<WorkflowDef>(fetcher, joinUrl(options.baseUrl, `/workflows/${id}`));
    },
    saveWorkflow(workflow) {
      if (!workflow.id) {
        // A plain Error, not XFlowApiError: no request was ever sent, so there
        // is no HTTP status to report. Fabricating status:400 would make a
        // caller branching on `e.status` believe the server rejected this —
        // the same field-as-costume defect the listWorkflows gap avoids.
        return Promise.reject(new Error("workflow id is required before saving"));
      }
      return request<RegisterWorkflowWire>(
        fetcher,
        joinUrl(options.baseUrl, `/workflows/${workflow.id}`),
        {
          method: "PUT",
          headers: {
            "content-type": "application/json"
          },
          body: JSON.stringify(workflow)
        }
      ).then((data) => ({
        workflowId: data.workflow_id,
        warnings: data.warnings
      }));
    },
    runWorkflow(workflowId) {
      return request<ExecuteWorkflowWire>(
        fetcher,
        joinUrl(options.baseUrl, `/workflows/${workflowId}/execute`),
        { method: "POST" }
      ).then((data) => ({
        executionId: data.execution_id
      }));
    }
  };
}
