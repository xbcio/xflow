import type {
  NodeStatus as RuntimeNodeStatus,
  RuntimeNodeSnapshot,
  RuntimeSnapshot,
  WorkflowDef,
  WorkflowStatus as RuntimeWorkflowStatus
} from "@xflow/core";

/** One summary row returned by GET /v1/workflows. */
export interface WorkflowSummary {
  id: string;
  name: string;
  version: string;
  definitionHash: string;
  /** Opaque registry revision supplied by the server for workflow replacement. */
  registryRevision: number;
}

/** Offset-pagination options for GET /v1/workflows. */
export interface ListWorkflowsOptions {
  /** 1-based page number. Omit to let the server use its default. */
  page?: number;
  /** Number of rows per page. Sent to the server as `page_size`. */
  pageSize?: number;
}

/** The `{list,total}` payload returned by GET /v1/workflows. */
export interface WorkflowListPage {
  list: WorkflowSummary[];
  total: number;
}

/** The status values returned by execution-detail and wait endpoints. */
export type ExecutionStatus =
  | "pending"
  | "running"
  | "success"
  | "failed"
  | "canceling"
  | "canceled"
  | "timeout";

/** The status values returned for a node in an execution detail. */
export type ExecutionNodeStatus =
  | "pending"
  | "running"
  | "committing"
  | "success"
  | "failed"
  | "skipped"
  | "suspended"
  | "continued"
  | "canceled"
  | "waiting";

/** One node entry from an execution detail. */
export interface ExecutionNodeDetail {
  name: string;
  status: ExecutionNodeStatus;
  attempt?: number;
  port?: string;
  error?: string;
  /** Structured failure data emitted by the current Go execution handler. */
  errorDetails?: Record<string, unknown>;
  output?: Record<string, unknown>;
}

/** Audit snapshot returned by GET /v1/executions/{id}. */
export interface ExecutionDetail {
  executionId: string;
  status: ExecutionStatus;
  error?: string;
  nodes?: ExecutionNodeDetail[];
}

/** Optional Go-duration query parameter for GET /v1/executions/{id}/wait. */
export interface WaitExecutionOptions {
  timeout?: string;
}

/** The normal 202 payload when an execution did not become terminal in time. */
export interface WaitExecutionTimeout {
  executionId: string;
  status: ExecutionStatus;
  timedOut: true;
}

/**
 * Result of waiting for an execution. A 200 returns an {@link ExecutionDetail};
 * a 202 returns {@link WaitExecutionTimeout} instead of throwing.
 */
export type WaitExecutionResult = ExecutionDetail | WaitExecutionTimeout;

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
  /**
   * Compatibility form for existing callers: fetches the server-default page
   * and returns only its rows.
   */
  listWorkflows(): Promise<WorkflowSummary[]>;
  /** Fetches a paginated workflow collection and retains its exact `total`. */
  listWorkflows(options: ListWorkflowsOptions): Promise<WorkflowListPage>;
  createWorkflow(workflow?: WorkflowDef): Promise<RegisterWorkflowResult>;
  getWorkflow(id: string): Promise<WorkflowDef>;
  saveWorkflow(workflow: WorkflowDef): Promise<RegisterWorkflowResult>;
  runWorkflow(workflowId: string): Promise<ExecuteWorkflowResult>;
  getExecution(id: string): Promise<ExecutionDetail>;
  waitExecution(id: string, options?: WaitExecutionOptions): Promise<WaitExecutionResult>;
}

/**
 * Error thrown when a user-facing endpoint returns a non-2xx envelope or a
 * successful response violates the envelope contract.
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
 * The `message` is an envelope `message` when a valid failure envelope is
 * available; otherwise it is a fixed transport message and never raw body text.
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

const REQUEST_FAILED_MESSAGE = "request failed";
const INVALID_RESPONSE_MESSAGE = "invalid API response";

const executionStatuses = new Set<string>([
  "pending",
  "running",
  "success",
  "failed",
  "canceling",
  "canceled",
  "timeout"
]);

const executionNodeStatuses = new Set<string>([
  "pending",
  "running",
  "committing",
  "success",
  "failed",
  "skipped",
  "suspended",
  "continued",
  "canceled",
  "waiting"
]);

function joinUrl(baseUrl: string, path: string): string {
  return `${baseUrl.replace(/\/$/, "")}${path}`;
}

function pathSegment(value: string): string {
  return encodeURIComponent(value);
}

function withQuery(path: string, values: Record<string, string | undefined>): string {
  const query = new URLSearchParams();
  for (const [key, value] of Object.entries(values)) {
    if (value !== undefined) {
      query.set(key, value);
    }
  }
  const serialized = query.toString();
  return serialized ? `${path}?${serialized}` : path;
}

function workflowPath(id: string): string {
  return `/workflows/${pathSegment(id)}`;
}

function executionPath(id: string): string {
  return `/executions/${pathSegment(id)}`;
}

interface Envelope<T> {
  success: boolean;
  code?: string;
  message?: string;
  data?: T;
  trace_id?: string;
}

interface ApiResponse<T> {
  status: number;
  data: T;
  traceId?: string;
  requestId?: string;
}

// Wire shapes use the names emitted by the Go API. They are parsed into the
// public camelCase types below rather than being exposed directly.
interface RegisterWorkflowWire {
  workflow_id: string;
  warnings?: string[];
}

interface ExecuteWorkflowWire {
  execution_id: string;
}

interface WorkflowListItemWire {
  id: string;
  name: string;
  version: string;
  definition_hash: string;
  registry_revision: number;
}

interface WorkflowListWire {
  list: WorkflowListItemWire[];
  total: number;
}

interface ExecutionNodeDetailWire {
  name: string;
  status: ExecutionNodeStatus;
  attempt?: number;
  port?: string;
  error?: string;
  error_details?: Record<string, unknown>;
  output?: Record<string, unknown>;
}

interface ExecutionDetailWire {
  execution_id: string;
  status: ExecutionStatus;
  error?: string;
  nodes?: ExecutionNodeDetailWire[];
}

interface WaitExecutionTimeoutWire {
  execution_id: string;
  status: ExecutionStatus;
  timed_out: true;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function hasOwn(object: object, key: string): boolean {
  return Object.prototype.hasOwnProperty.call(object, key);
}

function isEnvelope(value: unknown): value is Envelope<unknown> {
  return (
    isRecord(value) &&
    typeof value.success === "boolean" &&
    (value.code === undefined || typeof value.code === "string") &&
    (value.message === undefined || typeof value.message === "string") &&
    (value.trace_id === undefined || typeof value.trace_id === "string")
  );
}

async function readJson(response: Response): Promise<unknown> {
  const text = await response.text();
  if (!text) {
    return undefined;
  }
  try {
    return JSON.parse(text) as unknown;
  } catch {
    return undefined;
  }
}

function invalidResponse(response: ApiResponse<unknown>): never {
  throw new XFlowApiError(
    response.status,
    INVALID_RESPONSE_MESSAGE,
    undefined,
    response.traceId,
    response.requestId
  );
}

function requireRecord(value: unknown, response: ApiResponse<unknown>): Record<string, unknown> {
  if (!isRecord(value)) {
    invalidResponse(response);
  }
  return value;
}

function requireString(record: Record<string, unknown>, key: string, response: ApiResponse<unknown>): string {
  const value = record[key];
  if (typeof value !== "string") {
    invalidResponse(response);
  }
  return value;
}

function optionalString(record: Record<string, unknown>, key: string, response: ApiResponse<unknown>): string | undefined {
  const value = record[key];
  if (value === undefined) {
    return undefined;
  }
  if (typeof value !== "string") {
    invalidResponse(response);
  }
  return value;
}

function requireInteger(record: Record<string, unknown>, key: string, response: ApiResponse<unknown>): number {
  const value = record[key];
  if (typeof value !== "number" || !Number.isSafeInteger(value)) {
    invalidResponse(response);
  }
  return value;
}

function optionalInteger(record: Record<string, unknown>, key: string, response: ApiResponse<unknown>): number | undefined {
  const value = record[key];
  if (value === undefined) {
    return undefined;
  }
  if (typeof value !== "number" || !Number.isSafeInteger(value)) {
    invalidResponse(response);
  }
  return value;
}

function optionalRecord(record: Record<string, unknown>, key: string, response: ApiResponse<unknown>): Record<string, unknown> | undefined {
  const value = record[key];
  if (value === undefined) {
    return undefined;
  }
  return requireRecord(value, response);
}

function optionalStringArray(record: Record<string, unknown>, key: string, response: ApiResponse<unknown>): string[] | undefined {
  const value = record[key];
  if (value === undefined) {
    return undefined;
  }
  if (!Array.isArray(value) || value.some((item) => typeof item !== "string")) {
    invalidResponse(response);
  }
  return value;
}

function requireExecutionStatus(value: unknown, response: ApiResponse<unknown>): ExecutionStatus {
  if (typeof value !== "string" || !executionStatuses.has(value)) {
    invalidResponse(response);
  }
  return value as ExecutionStatus;
}

function requireExecutionNodeStatus(value: unknown, response: ApiResponse<unknown>): ExecutionNodeStatus {
  if (typeof value !== "string" || !executionNodeStatuses.has(value)) {
    invalidResponse(response);
  }
  return value as ExecutionNodeStatus;
}

/**
 * Perform an envelope-wrapped request and return the unwrapped payload plus
 * status metadata. Invalid JSON, a malformed envelope, or `success:false` on
 * a 2xx response is surfaced as a safe {@link XFlowApiError}; raw bodies never
 * become error messages.
 */
async function request<T>(fetcher: typeof fetch, url: string, init?: RequestInit): Promise<ApiResponse<T>> {
  const response = await fetcher(url, init);
  const body = await readJson(response);
  const requestId = response.headers.get("x-request-id") ?? undefined;
  const envelope = isEnvelope(body) ? body : undefined;

  if (!response.ok) {
    const failure = envelope?.success === false ? envelope : undefined;
    throw new XFlowApiError(
      response.status,
      failure?.message ?? REQUEST_FAILED_MESSAGE,
      failure?.code,
      failure?.trace_id,
      requestId
    );
  }

  if (!envelope || envelope.success !== true || !hasOwn(envelope, "data")) {
    throw new XFlowApiError(response.status, INVALID_RESPONSE_MESSAGE, undefined, undefined, requestId);
  }

  return {
    status: response.status,
    data: envelope.data as T,
    traceId: envelope.trace_id,
    requestId
  };
}

function mapRegisterWorkflow(response: ApiResponse<unknown>): RegisterWorkflowResult {
  const data = requireRecord(response.data, response);
  const result: RegisterWorkflowResult = { workflowId: requireString(data, "workflow_id", response) };
  const warnings = optionalStringArray(data, "warnings", response);
  if (warnings !== undefined) {
    result.warnings = warnings;
  }
  return result;
}

function mapExecuteWorkflow(response: ApiResponse<unknown>): ExecuteWorkflowResult {
  const data = requireRecord(response.data, response);
  return { executionId: requireString(data, "execution_id", response) };
}

function mapWorkflowList(response: ApiResponse<unknown>): WorkflowListPage {
  const data = requireRecord(response.data, response);
  const list = data.list;
  if (!Array.isArray(list)) {
    invalidResponse(response);
  }

  return {
    list: list.map((value) => {
      const item = requireRecord(value, response);
      return {
        id: requireString(item, "id", response),
        name: requireString(item, "name", response),
        version: requireString(item, "version", response),
        definitionHash: requireString(item, "definition_hash", response),
        registryRevision: requireInteger(item, "registry_revision", response)
      };
    }),
    total: requireInteger(data, "total", response)
  };
}

function mapExecutionNode(value: unknown, response: ApiResponse<unknown>): ExecutionNodeDetail {
  const data = requireRecord(value, response);
  const node: ExecutionNodeDetail = {
    name: requireString(data, "name", response),
    status: requireExecutionNodeStatus(data.status, response)
  };
  const attempt = optionalInteger(data, "attempt", response);
  const port = optionalString(data, "port", response);
  const error = optionalString(data, "error", response);
  const errorDetails = optionalRecord(data, "error_details", response);
  const output = optionalRecord(data, "output", response);

  if (attempt !== undefined) {
    node.attempt = attempt;
  }
  if (port !== undefined) {
    node.port = port;
  }
  if (error !== undefined) {
    node.error = error;
  }
  if (errorDetails !== undefined) {
    node.errorDetails = errorDetails;
  }
  if (output !== undefined) {
    node.output = output;
  }
  return node;
}

function mapExecutionDetail(response: ApiResponse<unknown>): ExecutionDetail {
  const data = requireRecord(response.data, response);
  const detail: ExecutionDetail = {
    executionId: requireString(data, "execution_id", response),
    status: requireExecutionStatus(data.status, response)
  };
  const error = optionalString(data, "error", response);
  if (error !== undefined) {
    detail.error = error;
  }

  if (data.nodes !== undefined) {
    if (!Array.isArray(data.nodes)) {
      invalidResponse(response);
    }
    detail.nodes = data.nodes.map((node) => mapExecutionNode(node, response));
  }
  return detail;
}

function mapWaitExecutionTimeout(response: ApiResponse<unknown>): WaitExecutionTimeout {
  const data = requireRecord(response.data, response);
  if (data.timed_out !== true) {
    invalidResponse(response);
  }
  return {
    executionId: requireString(data, "execution_id", response),
    status: requireExecutionStatus(data.status, response),
    timedOut: true
  };
}

function listWorkflowPage(
  fetcher: typeof fetch,
  baseUrl: string,
  options?: ListWorkflowsOptions
): Promise<WorkflowListPage> {
  const path = withQuery("/workflows", {
    page: options?.page === undefined ? undefined : String(options.page),
    page_size: options?.pageSize === undefined ? undefined : String(options.pageSize)
  });
  return request<WorkflowListWire>(fetcher, joinUrl(baseUrl, path)).then(mapWorkflowList);
}

type ListWorkflowsFunction = {
  (): Promise<WorkflowSummary[]>;
  (options: ListWorkflowsOptions): Promise<WorkflowListPage>;
};

function createListWorkflows(
  fetcher: typeof fetch,
  baseUrl: string
): ListWorkflowsFunction {
  const listWorkflows = (options?: ListWorkflowsOptions): Promise<WorkflowSummary[] | WorkflowListPage> => (
    listWorkflowPage(fetcher, baseUrl, options).then((page) => (options === undefined ? page.list : page))
  );
  return listWorkflows as ListWorkflowsFunction;
}

/**
 * Converts an execution-detail response into the runtime projection consumed by
 * `@xflow/core` renderers. The execution service exposes two transitional
 * statuses that the core projection does not: `canceling` and `committing` are
 * represented as `running` until their terminal state is observed.
 */
export function executionDetailToRuntimeSnapshot(detail: ExecutionDetail): RuntimeSnapshot {
  const snapshot: RuntimeSnapshot = {
    status: executionStatusToRuntimeStatus(detail.status)
  };

  if (detail.nodes !== undefined) {
    const nodes: Record<string, RuntimeNodeSnapshot> = {};
    for (const node of detail.nodes) {
      const runtimeNode: RuntimeNodeSnapshot = {
        status: executionNodeStatusToRuntimeStatus(node.status)
      };
      if (node.attempt !== undefined) {
        runtimeNode.attempts = node.attempt;
      }
      if (node.error !== undefined) {
        runtimeNode.error = node.error;
      }
      nodes[node.name] = runtimeNode;
    }
    snapshot.nodes = nodes;
  }

  return snapshot;
}

function executionStatusToRuntimeStatus(status: ExecutionStatus): RuntimeWorkflowStatus {
  switch (status) {
    case "pending":
      return "pending";
    case "running":
    case "canceling":
      return "running";
    case "success":
      return "success";
    case "failed":
      return "failed";
    case "canceled":
      return "canceled";
    case "timeout":
      return "timeout";
  }
}

function executionNodeStatusToRuntimeStatus(status: ExecutionNodeStatus): RuntimeNodeStatus {
  switch (status) {
    case "pending":
      return "pending";
    case "running":
    case "committing":
      return "running";
    case "success":
      return "success";
    case "failed":
      return "failed";
    case "skipped":
      return "skipped";
    case "suspended":
      return "suspended";
    case "continued":
      return "continued";
    case "canceled":
      return "canceled";
    case "waiting":
      return "waiting";
  }
}

export function createXFlowApiClient(options: XFlowApiClientOptions): XFlowApiClient {
  const fetcher = options.fetcher ?? fetch;

  return {
    listWorkflows: createListWorkflows(fetcher, options.baseUrl),
    createWorkflow(workflow) {
      return request<RegisterWorkflowWire>(fetcher, joinUrl(options.baseUrl, "/workflows"), {
        method: "POST",
        headers: {
          "content-type": "application/json"
        },
        body: JSON.stringify(workflow ?? {})
      }).then(mapRegisterWorkflow);
    },
    getWorkflow(id) {
      return request<WorkflowDef>(fetcher, joinUrl(options.baseUrl, workflowPath(id))).then(({ data }) => data);
    },
    saveWorkflow(workflow) {
      if (!workflow.id) {
        // A plain Error, not XFlowApiError: no request was ever sent, so there
        // is no HTTP status to report. Fabricating status:400 would make a
        // caller branching on `e.status` believe the server rejected this.
        return Promise.reject(new Error("workflow id is required before saving"));
      }
      return request<RegisterWorkflowWire>(
        fetcher,
        joinUrl(options.baseUrl, workflowPath(workflow.id)),
        {
          method: "PUT",
          headers: {
            "content-type": "application/json"
          },
          body: JSON.stringify(workflow)
        }
      ).then(mapRegisterWorkflow);
    },
    runWorkflow(workflowId) {
      return request<ExecuteWorkflowWire>(
        fetcher,
        joinUrl(options.baseUrl, `${workflowPath(workflowId)}/execute`),
        { method: "POST" }
      ).then(mapExecuteWorkflow);
    },
    getExecution(id) {
      return request<ExecutionDetailWire>(fetcher, joinUrl(options.baseUrl, executionPath(id))).then(mapExecutionDetail);
    },
    waitExecution(id, waitOptions) {
      const path = withQuery(`${executionPath(id)}/wait`, {
        timeout: waitOptions?.timeout
      });
      return request<ExecutionDetailWire | WaitExecutionTimeoutWire>(fetcher, joinUrl(options.baseUrl, path)).then(
        (response) => {
          if (response.status === 200) {
            return mapExecutionDetail(response);
          }
          if (response.status === 202) {
            return mapWaitExecutionTimeout(response);
          }
          invalidResponse(response);
        }
      );
    }
  };
}
