import type {
  NodeFormSchema,
  NodeStatus as RuntimeNodeStatus,
  NodeTypesResponse,
  ParamIssue,
  ParamValidationMode,
  RuntimeNodeSnapshot,
  RuntimeSnapshot,
  Viewport,
  WorkflowDef,
  WorkflowEditorMetadata,
  WorkflowEditorSection,
  WorkflowStatus as RuntimeWorkflowStatus,
  WireWorkflowDef
} from "@xflow/core";
import { mergeEditorMetadata, splitEditorMetadata } from "@xflow/core";

/**
 * GET /v1/current-user payload (OpenAPI `CurrentUserResponse`): the caller's
 * own verified identity. Every field is server-issued from the presented
 * credential; no request parameter can name a principal, so this can only
 * ever describe the caller.
 */
export interface CurrentIdentity {
  subject: string;
  namespace: string;
  scopes: string[];
}

/**
 * Summary of a workflow's most recent execution (OpenAPI
 * `WorkflowLatestExecution`). It is a projection of the execution record:
 * status and timestamps only, no definition, params, output or error.
 */
export interface WorkflowLatestExecution {
  /** Server-assigned execution id (the `{id}` of GET /v1/executions/{id}). */
  executionId: string;
  status: ExecutionStatus;
  /** RFC3339 timestamp of when the execution was recorded. */
  createdAt: string;
  /** RFC3339 timestamp of the execution's latest accepted transition. */
  updatedAt: string;
}

/** One summary row returned by GET /v1/workflows. */
export interface WorkflowSummary {
  id: string;
  name: string;
  version: string;
  definitionHash: string;
  /** Opaque registry revision supplied by the server for workflow replacement. */
  registryRevision: number;
  /**
   * The stored definition's one-line description. Absent when the definition
   * has none (or an older server does not publish it). Metadata only: it is
   * excluded from `definitionHash`.
   */
  description?: string;
  /**
   * The distinct `expression` parameters of the definition's
   * `xflow.trigger.cron` nodes, deduplicated, trimmed and sorted by the
   * server. Absent when the workflow has no cron trigger; no other trigger
   * parameter is exposed here.
   */
  cronExpressions?: string[];
  /**
   * The distinct trigger kinds in the stored definition (`timer`, `cron`,
   * `webhook`, `kafka`, `redis`, or a future `xflow.trigger.*` suffix),
   * deduplicated and sorted by the server. An `xflow.start` entry node is not
   * a trigger. Always present as an array; `[]` means the workflow has no
   * trigger node. Older servers/mocks that omit the field map to `[]`.
   */
  triggers: string[];
  /**
   * RFC3339 timestamp of when the registry last registered or REPLACED this
   * workflow. Absent for a record written before the registry stamped it —
   * absence means UNKNOWN, not the zero instant. It is excluded from
   * `definitionHash`.
   */
  updatedAt?: string;
  /**
   * The most recent execution recorded for this workflow. Absent means the
   * server could not answer the lookup (no execution store, degraded read, or
   * no execution yet) — never "this workflow never ran". A caller that must
   * tell those apart reads GET /v1/executions with `workflow_id`.
   */
  latestExecution?: WorkflowLatestExecution;
  /**
   * Execution statistics over the requested window (OpenAPI
   * `WorkflowExecutionStats`). Present only when the list request asked for
   * `include=stats` AND the server could answer — a caller without
   * `execution.read`, or a deployment without an execution store, gets the
   * page WITHOUT this field rather than a 403. Absent therefore means UNKNOWN;
   * `runs: 0` means a known zero.
   */
  stats?: WorkflowExecutionStats;
  /**
   * The workflow's newest executions, newest first (at most 10). Present only
   * with `include=recent` and the same availability rules as {@link stats};
   * absent also when the workflow has no execution in the caller's namespace.
   */
  recentExecutions?: WorkflowRecentExecution[];
}

/** Windows accepted by `include=stats` (OpenAPI `window`). */
export type WorkflowStatsWindow = "24h" | "7d" | "30d";

/** The execution-derived workflow-list fields a caller can request. */
export type WorkflowListInclude = "stats" | "recent";

/**
 * Per-workflow execution statistics (OpenAPI `WorkflowExecutionStats`).
 *
 * `failed` counts both `failed` and `timeout`. Every terminal status is in the
 * success-rate denominator (`succeeded / (succeeded + failed + canceled)`), so
 * a canceled run cannot inflate the rate. `successRate: null` means the window
 * has NO terminal run — null means "no data", never zero.
 *
 * `p50Ms`/`p95Ms` are nearest-rank percentiles of the durations of the most
 * recent `sampleSize` terminal runs (≤ 100). When `sampleSize` is smaller than
 * the terminal run count, they describe a sample rather than the whole window.
 * Durations are currently approximated as `updated_at − created_at`.
 */
export interface WorkflowExecutionStats {
  window: WorkflowStatsWindow;
  runs: number;
  succeeded: number;
  failed: number;
  successRate: number | null;
  p50Ms?: number | null;
  p95Ms?: number | null;
  sampleSize: number;
}

/**
 * One element of a workflow's `recentExecutions` (OpenAPI
 * `WorkflowRecentExecution`): a minimal projection, not a full execution.
 */
export interface WorkflowRecentExecution {
  executionId: string;
  status: ExecutionStatus;
  /** RFC3339 timestamp of when the execution was recorded. */
  createdAt: string;
  /**
   * Approximate wall-clock duration in milliseconds. Present only for a
   * TERMINAL execution (absent, never 0, for a non-terminal one).
   */
  durationMs?: number | null;
}

/** Offset-pagination options for GET /v1/workflows. */
export interface ListWorkflowsOptions {
  /** 1-based page number. Omit to let the server use its default. */
  page?: number;
  /** Number of rows per page. Sent to the server as `page_size`. */
  pageSize?: number;
  /**
   * Case-insensitive substring filter on the workflow name, sent as `q`.
   * Trimmed first; an absent, empty or whitespace-only value is not sent. The
   * server applies it before pagination, so `page` and `total` both describe
   * the filtered collection.
   */
  q?: string;
  /**
   * Execution-derived fields to attach to each row, sent comma-separated as
   * `include`. An empty or absent list is not sent. The server omits the
   * fields when the caller lacks `execution.read` or the deployment cannot
   * answer; it never fails the page.
   */
  include?: WorkflowListInclude[];
  /**
   * The window `include: ["stats"]` aggregates over (default `"7d"`).
   * Sent as `window`; the server validates it even when `include` is absent.
   */
  window?: WorkflowStatsWindow;
}

/** Options for GET /v1/node-types. */
export interface ListNodeTypesOptions {
  /**
   * Namespace the caller edits in. Sent as `?namespace=`; an omitted or empty
   * value requests the server registry only.
   */
  namespace?: string;
}

/** Options for GET /v1/node-types/{type}. */
export interface GetNodeTypeOptions {
  /**
   * Namespace the caller edits in. Sent as `?namespace=`; with it the server
   * can also answer from the types live runners entitled to that namespace
   * report. An omitted or empty value looks up the server registry only.
   */
  namespace?: string;
}

/** The `{list,total}` payload returned by GET /v1/workflows. */
export interface WorkflowListPage {
  list: WorkflowSummary[];
  total: number;
}

/** The status values returned by execution-detail and wait endpoints. */
export type ExecutionStatus =
  | "pending"
  /**
   * A queue-mode submission waiting for a workflow concurrency slot: persisted
   * in full, but nothing of it is deliverable until a slot frees. Deliberately
   * not terminal — a queued execution can still be waited on and canceled
   * (`types.ExecutionStatusQueued`).
   */
  | "queued"
  /**
   * An operator suspended dispatch. Not terminal: the execution can still be
   * resumed or canceled, and `waitExecution` keeps waiting on it
   * (`types.ExecutionStatusPaused`).
   */
  | "paused"
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
  /** Served from pin_data: the handler did not run; output is the mock. */
  | "pinned"
  /**
   * A member of a transient group whose records expired: the execution is
   * terminal and the group's artifacts have passed their completion TTL, so
   * nothing about the node is readable any more.
   */
  | "expired"
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
  /** First lease granted (B2b). Absent when the node never started. */
  startedAt?: string;
  /** First terminal status (B2b). Absent while the node has not finished. */
  finishedAt?: string;
  /** Structured failure data emitted by the current Go execution handler. */
  errorDetails?: Record<string, unknown>;
  output?: Record<string, unknown>;
  /**
   * The child execution a sub-workflow node waited on (`child_execution_id`),
   * absent on every other node. It survives the node's commit, so a finished
   * node's child stays drillable.
   */
  childExecutionId?: string;
}

/** Audit snapshot returned by GET /v1/executions/{id}. */
export interface ExecutionDetail {
  executionId: string;
  /**
   * The workflow definition this execution ran, captured at submission. Absent
   * when the submission carried no identity (an inline definition with no
   * server id, or a run recorded before the field existed): an unattributed
   * execution must not be reported under a guessed workflow.
   */
  workflowId?: string;
  /**
   * The version label captured with {@link workflowId}; absent when there is no
   * recorded identity or the definition declares no version. The editor reads
   * the run's definition snapshot with `GET /v1/workflows/{id}/versions/{version}`.
   */
  workflowVersion?: string;
  /**
   * The external tracing system's correlation id captured at submission
   * (`trace_id`): a handle for opening the run's trace in a tracing UI, not
   * business data. Absent when the submission carried no trace context —
   * absence means unknown, never a value to reconstruct.
   */
  traceId?: string;
  status: ExecutionStatus;
  /** When the backend recorded the execution (B2b). Absent on older rows. */
  createdAt?: string;
  /** Wall clock of the latest accepted status transition (B2b). */
  updatedAt?: string;
  /**
   * The execution this one derives from. Set on a rerun result (together with
   * `rerunFrom`) and on a sub-workflow child (together with `parentNode`).
   */
  parentExecutionId?: string;
  /** The node a rerun started at; upstream results were copied, not rerun. */
  rerunFrom?: string;
  /** The parent's `xflow.subworkflow` node this child execution satisfies. */
  parentNode?: string;
  /** 1-based place in the workflow's concurrency queue while `queued`. */
  queuePosition?: number;
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

/** Offset-pagination and filters for GET /v1/executions. */
export interface ListExecutionsOptions {
  /** 1-based page number. Omit to let the server use its default. */
  page?: number;
  /** Number of rows per page. Sent to the server as `page_size`. */
  pageSize?: number;
  /**
   * Narrow the page to one lifecycle status. The server narrows `total` too;
   * an unknown value is a 400 `execution_status_invalid`, never an empty page.
   */
  status?: ExecutionStatus;
  /**
   * Narrow the page to ANY of several lifecycle statuses (wire: the `status`
   * parameter repeated). Combined with {@link status}, every listed value is
   * ORed — a caller should normally use one of the two forms, not both.
   */
  statuses?: ExecutionStatus[];
  /**
   * Execution-id prefix search (`q` on the wire). Restricted server-side to at
   * most 64 characters drawn from letters, digits, `_` and `-`; anything else
   * is a 400 `execution_query_invalid`. Matched as a literal prefix — LIKE
   * metacharacters are escaped, not interpreted.
   */
  q?: string;
  /**
   * Narrow to one workflow identity (B1). The comparison is exact and a row
   * recorded without a definition id (`workflow_id: ""`) matches no value, so
   * an unattributed execution is never guessed into a workflow.
   */
  workflowId?: string;
  /** Lower bound on `created_at`, RFC3339 and exclusive (server-side). */
  createdAfter?: string;
  /** Upper bound on `created_at`, RFC3339 and exclusive (server-side). */
  createdBefore?: string;
}

/**
 * One row of GET /v1/executions (OpenAPI `ExecutionListItem`). It is a summary,
 * not a snapshot: `workflow_name` is a display string recorded at submit time,
 * while `workflow_id` is the immutable identity the list can be filtered by.
 */
export interface ExecutionListItem {
  executionId: string;
  /** The server-issued isolation scope the row belongs to. */
  namespace: string;
  /** Server-assigned id of the workflow the execution ran; `""` = unattributed. */
  workflowId: string;
  workflowName: string;
  status: ExecutionStatus;
  /** Failure message when the execution failed; `""` otherwise. */
  error: string;
  /** RFC3339 timestamp; the listing's descending primary sort key. */
  createdAt: string;
  /** RFC3339 timestamp of the row's last update. */
  updatedAt: string;
  /**
   * 1-based place in the workflow's concurrency queue while `queued`, read
   * from live queue state at list time. Absent for every other status and when
   * the server could not read it — absence means unknown, never the head of
   * the line (the head is `1`). Same semantics as
   * {@link ExecutionDetail.queuePosition}.
   */
  queuePosition?: number;
  /**
   * Wall-clock duration of a TERMINAL execution in whole milliseconds. Exact
   * when {@link finishedAt} is present; for rows written before that column
   * existed it is the documented estimate `updated_at − created_at`. Absent
   * for a live row: absence means "not finished", not zero.
   */
  durationMs?: number;
  /**
   * RFC3339 completion instant, write-once on the first terminal transition.
   * Absent for a live row and for rows written before the column existed — in
   * that case the completion time is UNKNOWN, never the zero instant.
   */
  finishedAt?: string;
  /**
   * The server-derived path that started the run (`api`, `rerun`,
   * `subworkflow`, `timer`, `cron`, `webhook`, `kafka`, `redis`). Absent when
   * the row predates the column, so absence means UNKNOWN.
   */
  triggerKind?: string;
  /**
   * Extra node attempts the execution consumed (the sum of `attempt − 1` over
   * its node rows). Absent when the configured store cannot answer the batch
   * node read — absence means UNKNOWN, not zero.
   */
  retries?: number;
  /**
   * How many of the execution's nodes reached a terminal status. Absent when
   * the store cannot answer the batch read or the graph size is unknown.
   */
  nodeProgress?: ExecutionNodeProgress;
}

/** Node-progress aggregate of one execution list row (OpenAPI `ExecutionNodeProgress`). */
export interface ExecutionNodeProgress {
  /** Node rows that reached a terminal status. */
  done: number;
  /** The graph's node count recorded when the execution was created. */
  total: number;
}

/**
 * The `data` payload of POST /v1/executions/{id}/cancel (OpenAPI
 * `AcceptedResponse`). `accepted` means the cancellation request was taken —
 * not that the execution is already terminal; poll `getExecution` for that.
 */
export interface CancelExecutionResult {
  accepted: boolean;
}

/**
 * A signal delivered to an execution (OpenAPI `SignalRequest`). The approval
 * node names its signals `<node>/approval`, `<node>/approval/<subject>` and
 * `<node>/approval/force`; the *action* (`approve`, `reject`, `return`,
 * `delegate`, `add_signer`, `force`) travels in `data.action`. Never put an
 * approver identity in `data`: the server injects the verified `_actor` and
 * refuses a payload that carries the reserved key.
 */
export interface SignalDelivery {
  name: string;
  data?: Record<string, unknown>;
}

/** The `data` payload of POST /v1/executions/{id}/signals. */
export interface DeliverSignalResult {
  accepted: boolean;
}

/**
 * The `data` payload of DELETE /v1/executions/{id}/signals/{name}. `revoked`
 * answers the delete itself; a 409 `signal_consumed` means the node already
 * consumed it or it was never queued.
 */
export interface RevokeSignalResult {
  revoked: boolean;
}

/** The `{list,total}` payload returned by GET /v1/executions. */
export interface ExecutionListPage {
  list: ExecutionListItem[];
  total: number;
}

/**
 * A count per lifecycle status (OpenAPI `ExecutionStatusCounts`). Every one of
 * the nine statuses is present; a status with no execution in the window is
 * `0`, never omitted, and the server rejects nothing here — the map is
 * zero-filled server-side.
 */
export type ExecutionStatusCounts = Record<ExecutionStatus, number>;

/** One histogram bucket of GET /v1/executions/stats (OpenAPI `ExecutionStatsBucket`). */
export interface ExecutionStatsBucket {
  /** Inclusive UTC-aligned start; the bucket covers `[start, start + width)`. */
  start: string;
  counts: ExecutionStatusCounts;
}

/**
 * The `data` payload of GET /v1/executions/stats (OpenAPI `ExecutionStats`):
 * status counts for one `[createdAfter, createdBefore)` window in the caller's
 * namespace. `total` equals the sum of `countsByStatus` (and of every bucket's
 * counts). `histogram` is absent unless the request named a `bucket`.
 */
export interface ExecutionStats {
  /** The request's `created_after` — inclusive lower bound. */
  windowStart: string;
  /** The request's `created_before` — EXCLUSIVE upper bound. */
  windowEnd: string;
  total: number;
  countsByStatus: ExecutionStatusCounts;
  histogram?: ExecutionStatsBucket[];
}

/** Bucket widths accepted by GET /v1/executions/stats (OpenAPI `bucket`). */
export type ExecutionStatsBucketWidth = "5m" | "1h" | "1d";

/**
 * Options for GET /v1/executions/stats. Both bounds are required and
 * exclusive; their span may not exceed 30 days. `bucket` is optional — omit it
 * for counts without a histogram.
 */
export interface GetExecutionStatsOptions {
  /** Inclusive lower bound on `created_at`, RFC3339 (wire `created_after`). */
  createdAfter: string;
  /** Exclusive upper bound on `created_at`, RFC3339 (wire `created_before`). */
  createdBefore: string;
  /** Exact-match workflow id filter (max 64 characters). */
  workflowId?: string;
  /** Bucket width; adds `histogram` to the response. Max 168 buckets. */
  bucket?: ExecutionStatsBucketWidth;
}

/** Windows accepted by GET /v1/overview (OpenAPI `window`). */
export type OverviewWindow = "24h" | "7d";

/** Options for GET /v1/overview. */
export interface GetOverviewOptions {
  /**
   * Aggregation window; defaults to the server's `24h`. The histogram and the
   * sparkline always have 24 buckets (1h or 7h wide respectively).
   */
  window?: OverviewWindow;
}

/** The 24-point series behind the overview charts (OpenAPI `OverviewSparkline`). */
export interface OverviewSparkline {
  /** Executions per bucket, aligned with `histogram`. */
  runs: number[];
  /** Success rate per bucket; `null` when the bucket has no terminal run. */
  successRate: Array<number | null>;
}

/**
 * The overview's KPI block (OpenAPI `OverviewKPIs`). `successRate`, `p50Ms` and
 * `p95Ms` are `null` when there was no terminal run to compute them from —
 * `null` means "no data", never zero (an all-failed window is a real `0`).
 */
export interface OverviewKPIs {
  /** Executions created in the window, terminal or not. */
  runs: number;
  /** Executions created in the immediately preceding, equally sized window. */
  runsPrevious: number;
  /** Failed plus timeout executions in that same preceding window. */
  failedPrevious: number;
  successRate: number | null;
  /** Nearest-rank P50 duration of terminal runs, in milliseconds. */
  p50Ms: number | null;
  /** Nearest-rank P95 duration of terminal runs, in milliseconds. */
  p95Ms: number | null;
  /** DISTINCT workflow ids with at least one execution in the window. */
  activeWorkflows: number;
  sparkline: OverviewSparkline;
}

/** One entry of the overview's failing-workflow list (OpenAPI `OverviewFailingWorkflow`). */
export interface OverviewFailingWorkflow {
  workflowId: string;
  /** Best-effort registry name; absent when the registry cannot name the id. */
  workflowName?: string;
  /** Failed plus timeout executions of this workflow in the window. */
  failed: number;
  /** The newest failure's message, truncated to 200 characters server-side. */
  lastError?: string;
  /** RFC3339 creation time of the newest failed/timeout execution. */
  lastFailedAt?: string;
}

/** Capacity aggregate over the live runners (OpenAPI `OverviewRunnerSummary`). */
export interface OverviewRunnerSummary {
  online: number;
  /** Directory members, online or not. */
  total: number;
  /** Summed task capacity of the online runners. */
  capacity: number;
  /** Summed tasks currently in flight on the online runners. */
  inFlight: number;
}

/**
 * The cluster segment of the overview (OpenAPI `OverviewCluster`). The whole
 * segment is absent unless the caller ALSO holds `management.runner.list`; the
 * runner roster is cluster-wide and must not leak to a namespace-scoped
 * principal. It is also absent when the deployment has no runner directory.
 */
export interface OverviewCluster {
  isLeader: boolean;
  runners: OverviewRunnerSummary;
}

/**
 * The `data` payload of GET /v1/overview (OpenAPI `OverviewResponse`): one
 * dashboard over the caller's own namespace. The histogram and the sparkline
 * are derived from the same buckets, so they cannot disagree. `generatedAt` is
 * when the payload was BUILT; the response is cached for up to 30 seconds per
 * (namespace, window, cluster permission).
 */
export interface OverviewResponse {
  window: OverviewWindow;
  generatedAt: string;
  kpis: OverviewKPIs;
  histogram: ExecutionStatsBucket[];
  topFailing: OverviewFailingWorkflow[];
  cluster?: OverviewCluster;
}

/**
 * Options for POST /v1/executions/{id}/rerun (OpenAPI
 * `RerunExecutionRequest`). Everything upstream of `fromNode` is copied from
 * the source execution; that node and everything downstream run again.
 */
export interface RerunExecutionOptions {
  /** The node the new execution starts at (wire `from_node`, required). */
  fromNode: string;
  /**
   * Accept a `success` source. `failed`, `canceled` and `timeout` sources are
   * accepted either way; every other status stays 409
   * `execution_not_rerunnable`.
   */
  includeSuccess?: boolean;
  /**
   * Retry-safe key sent as the `Idempotency-Key` header (1-255 characters of
   * [A-Za-z0-9._:-]). A retry with the same key and same request answers
   * `duplicate: true` with the execution the first attempt created.
   */
  idempotencyKey: string;
}

/** The `data` payload of a rerun response (OpenAPI `RerunExecutionResponse`). */
export interface RerunExecutionResult {
  /** The NEW execution. The source execution is unchanged. */
  executionId: string;
  status?: ExecutionStatus;
  /** 1-based queue position while `queued`; absent for executions that never queued. */
  queuePosition?: number;
  /** True when an idempotent retry returned the execution the first attempt created. */
  duplicate?: boolean;
}

/**
 * Result of reading a workflow definition (GET /v1/workflows/{id}).
 * `workflow` is the editor's internal (merged) model: `editor_metadata`'s
 * `positions`/`ui`/`notes` are already folded onto their nodes
 * (`mergeEditorMetadata`, ADR-D4 §2.5). `viewport` and `sections` are carried
 * alongside because they are workflow-level and have no field on
 * `WorkflowDef` to merge onto; each is absent when the stored
 * `editor_metadata` had none.
 */
export interface GetWorkflowResult {
  workflow: WorkflowDef;
  viewport?: Viewport;
  /**
   * Visual canvas sections, in author order. Grouping boxes only -- not the
   * scheduling `groups[]` (see `WorkflowEditorSection` in `@xflow/core`).
   * Absent when the stored `editor_metadata` carried none.
   */
  sections?: WorkflowEditorSection[];
}

/**
 * One row of GET /v1/workflows/{id}/versions (OpenAPI `WorkflowVersionListItem`):
 * a label, not a definition. The definition is read one version at a time with
 * `getWorkflowVersion`.
 */
export interface WorkflowVersion {
  /** The version label. Immutable once published. */
  version: string;
  /** Opaque identifier of this version's stored definition; compare only for equality. */
  definitionHash: string;
  /**
   * The current row's live compare-and-swap revision, or an archived row's
   * retirement order (newest first down the list).
   */
  registryRevision: number;
  /** Whether this row is the version the live record represents. */
  current: boolean;
  /** Whether this row is a retired history entry (complementary to `current`). */
  archived: boolean;
}

/** The `{list,total}` payload of GET /v1/workflows/{id}/versions. */
export interface WorkflowVersionPage {
  /** Current version first, then retired versions newest-retirement first. */
  list: WorkflowVersion[];
  /** Exact history size (current + retained archived), independent of the page. */
  total: number;
}

/** Offset-pagination options for GET /v1/workflows/{id}/versions. */
export interface ListWorkflowVersionsOptions {
  /** 1-based page number. Omit to let the server use its default (1). */
  page?: number;
  /** Rows per page. Sent as `page_size`; the server clamps above 200. */
  pageSize?: number;
}

/**
 * The `data` payload of POST /v1/workflows/{id}/rollback (OpenAPI
 * `RollbackWorkflowResponse`). The rollback installs an archived version as the
 * live record; the version it replaced goes to history.
 */
export interface RollbackWorkflowResult {
  /** The workflow id — unchanged by a rollback (version ownership follows the id). */
  workflowId: string;
  /** The version that is now current. */
  version: string;
  /**
   * True when nothing was written because the requested version already was
   * the current one (including on a replayed request).
   */
  unchanged: boolean;
}

/**
 * Options for `createWorkflow`/`saveWorkflow` carrying editor-only state that
 * has no field on `WorkflowDef`: the canvas pan/zoom to persist in
 * `editor_metadata.viewport` and the visual canvas sections to persist in
 * `editor_metadata.sections`, alongside the per-node `positions`/`ui`/`notes`
 * `splitEditorMetadata` already derives from the definition's nodes.
 */
export interface SaveWorkflowOptions {
  viewport?: Viewport;
  /**
   * Visual canvas sections to persist. An empty array is omitted from the
   * request (Go's `omitempty` encodes it exactly like an absent value), so a
   * caller can round-trip `GetWorkflowResult.sections` without special-casing
   * the empty case.
   */
  sections?: WorkflowEditorSection[];
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
  /**
   * ParamSpec findings (`param_issues`) the server reported without rejecting
   * the definition (warn mode). Absent when there are none.
   */
  paramIssues?: ParamIssue[];
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
  /**
   * GET /v1/current-user: the caller's own verified identity and scopes. A
   * console maps `scopes` onto editor capabilities; the server stays the
   * authority and still refuses a route those scopes do not cover.
   */
  getCurrentUser(): Promise<CurrentIdentity>;
  createWorkflow(workflow?: WorkflowDef, options?: SaveWorkflowOptions): Promise<RegisterWorkflowResult>;
  /**
   * Reads a workflow definition and merges its `editor_metadata` sibling
   * (ADR-D4 §2.5): `positions`/`ui`/`notes` land back on their nodes in the
   * returned `workflow`, and the canvas `viewport` -- which has no field on
   * `WorkflowDef` to merge onto -- is returned alongside it so a host can
   * restore it (e.g. `XFlowEditor`'s `defaultViewport` prop).
   */
  getWorkflow(id: string): Promise<GetWorkflowResult>;
  saveWorkflow(workflow: WorkflowDef, options?: SaveWorkflowOptions): Promise<RegisterWorkflowResult>;
  /**
   * GET /v1/workflows/{id}/versions: one offset-paginated page of the
   * workflow's version history — the current version first, then retired
   * versions newest-retirement first. Rows are labels; read a definition with
   * {@link getWorkflowVersion}. Exactly the current version until the first
   * replacement.
   */
  listWorkflowVersions(id: string, options?: ListWorkflowVersionsOptions): Promise<WorkflowVersionPage>;
  /**
   * GET /v1/workflows/{id}/versions/{version}: one version's definition, in
   * the same merged shape as {@link getWorkflow}. An unknown version rejects
   * 404 `workflow_version_not_found` (distinct from a missing workflow's
   * `workflow_not_found`).
   */
  getWorkflowVersion(id: string, version: string): Promise<GetWorkflowResult>;
  /**
   * POST /v1/workflows/{id}/rollback: install an archived version as the live
   * record. `unchanged: true` means the requested version already was current.
   * A version the current compiler cannot reproduce rejects 409
   * `workflow_version_unavailable`; the live record is left untouched.
   */
  rollbackWorkflow(id: string, version: string): Promise<RollbackWorkflowResult>;
  runWorkflow(workflowId: string): Promise<ExecuteWorkflowResult>;
  getExecution(id: string): Promise<ExecutionDetail>;
  /**
   * GET /v1/executions/stats: status counts for one bounded window in the
   * caller's own namespace, optionally with a UTC-aligned histogram. Both
   * bounds are required and exclusive; the span may not exceed 30 days. The
   * server answers 501 `execution_stats_unsupported` when the deployment has
   * no execution store (or its store cannot aggregate).
   */
  getExecutionStats(options: GetExecutionStatsOptions): Promise<ExecutionStats>;
  /**
   * GET /v1/overview: one dashboard over the caller's own namespace — KPIs
   * (runs and the previous window's runs, success rate, P50/P95, active
   * workflows, a 24-point sparkline), the 24-bucket status histogram, the top
   * failing workflows, and — only when the caller ALSO holds
   * `management.runner.list` — a cluster segment. The server caches the
   * payload for up to 30 seconds per (namespace, window, cluster permission);
   * `generatedAt` is the build instant, so it can lag the response by that
   * much. A deployment without an execution store answers 501
   * `overview_unsupported`.
   */
  getOverview(options?: GetOverviewOptions): Promise<OverviewResponse>;
  waitExecution(id: string, options?: WaitExecutionOptions): Promise<WaitExecutionResult>;
  /**
   * POST /v1/executions/{id}/pause: suspends dispatch and answers with the
   * resulting detail (`status: "paused"`). Pausing an already-paused execution
   * is idempotent; a queued or terminal one rejects 409
   * `execution_not_pausable`. Requires the `execution.control` scope.
   */
  pauseExecution(id: string): Promise<ExecutionDetail>;
  /**
   * POST /v1/executions/{id}/resume: releases the dispatch a pause held and
   * answers with the resulting detail. Anything but `paused` rejects 409
   * `execution_not_paused`. Requires the `execution.control` scope.
   */
  resumeExecution(id: string): Promise<ExecutionDetail>;
  /**
   * POST /v1/executions/{id}/cancel: requests cancellation. `accepted` only
   * says the request was taken — the execution reaches `canceled` later, so a
   * caller that needs the terminal state polls {@link getExecution}.
   */
  cancelExecution(id: string): Promise<CancelExecutionResult>;
  /**
   * POST /v1/executions/{id}/signals: delivers one named signal. A 200 only
   * means the signal was accepted for delivery — the node may still refuse it
   * (an approver not in the chain, a signal name for the wrong mode), which is
   * visible only in the node's published `ignored` trail. Requires the
   * `execution` scope.
   */
  deliverSignal(id: string, signal: SignalDelivery): Promise<DeliverSignalResult>;
  /**
   * DELETE /v1/executions/{id}/signals/{name}: removes a delivered signal the
   * node has not consumed yet (e.g. while the execution is paused). A signal
   * already consumed rejects 409 `signal_consumed`.
   */
  revokeSignal(id: string, name: string): Promise<RevokeSignalResult>;
  /**
   * GET /v1/executions: one offset-paginated page of the caller's namespace,
   * newest first. Wraps `page`, `page_size`, one or more lifecycle statuses
   * (`status` / `statuses`), the execution-id prefix `q`, the `workflow_id`
   * identity filter and the exclusive `created_after` / `created_before`
   * RFC3339 bounds.
   */
  listExecutions(options?: ListExecutionsOptions): Promise<ExecutionListPage>;
  /**
   * POST /v1/executions/{id}/rerun: derive a new execution from a terminal
   * one, starting at `fromNode`. Safe to retry with the same Idempotency-Key.
   */
  rerunExecution(id: string, options: RerunExecutionOptions): Promise<RerunExecutionResult>;
  /**
   * GET /v1/node-types: the NodeFormSchema of every node type registered in
   * the server process, plus the server's param validation mode. With a
   * `namespace`, the server authorizes a read of that namespace and also
   * merges the types live runners entitled to it report (`source: "runner"`,
   * `runner_pools`); without one it serves the server registry only.
   * Responses carry an ETag and `Cache-Control: no-cache`, so the browser HTTP
   * cache revalidates them.
   */
  listNodeTypes(options?: ListNodeTypesOptions): Promise<NodeTypesResponse>;
  /**
   * GET /v1/node-types/{type}?version=&namespace=: one schema. An omitted (or
   * 0) version selects the latest; an unknown type or version rejects with
   * status 404. A `namespace` scopes the lookup like listNodeTypes, so a
   * runner-reported type resolves too.
   */
  getNodeType(type: string, version?: number, options?: GetNodeTypeOptions): Promise<NodeFormSchema>;
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
 * - `paramIssues`: `data.param_issues` of a failure envelope — carried by the
 *   400 `workflow_param_invalid` rejection in enforce mode, so a caller can
 *   place every issue on its field. Absent for every other failure.
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
    readonly requestId?: string,
    readonly paramIssues?: ParamIssue[]
  ) {
    super(message);
  }
}

const REQUEST_FAILED_MESSAGE = "request failed";
const INVALID_RESPONSE_MESSAGE = "invalid API response";

const executionStatuses = new Set<string>([
  "pending",
  "queued",
  "paused",
  "running",
  "success",
  "failed",
  "canceling",
  "canceled",
  "timeout"
]);

/**
 * The nine statuses a count map carries. Listed once so the mapper can require
 * every key: a missing status is a broken response, never "0".
 */
const executionStatusCountKeys: readonly ExecutionStatus[] = [
  "pending",
  "queued",
  "paused",
  "running",
  "success",
  "failed",
  "canceling",
  "canceled",
  "timeout"
];

const workflowStatsWindows = new Set<string>(["24h", "7d", "30d"]);

const executionNodeStatuses = new Set<string>([
  "pending",
  "running",
  "committing",
  "success",
  "failed",
  "skipped",
  "pinned",
  "expired",
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

function withQuery(path: string, values: Record<string, string | string[] | undefined>): string {
  const query = new URLSearchParams();
  for (const [key, value] of Object.entries(values)) {
    if (value === undefined) {
      continue;
    }
    if (Array.isArray(value)) {
      for (const entry of value) {
        query.append(key, entry);
      }
      continue;
    }
    query.set(key, value);
  }
  const serialized = query.toString();
  return serialized ? `${path}?${serialized}` : path;
}

function workflowPath(id: string): string {
  return `/workflows/${pathSegment(id)}`;
}

function nodeTypePath(type: string): string {
  return `/node-types/${pathSegment(type)}`;
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
  param_issues?: ParamIssue[];
}

/**
 * The GET/POST/PUT /v1/workflows[/{id}] wire body: a `WireWorkflowDef` plus
 * an optional `editor_metadata` sibling (ADR-D4 §2.1/§2.3, D2;
 * `WorkflowDefWithEditorMetadata` in api/openapi/xflow-v1.yaml).
 * `editor_metadata` is never a field of the Go `WorkflowDef` type, so it is
 * modeled here the same way: a sibling, not a nested property.
 */
type WireWorkflowDefWithMetadata = WireWorkflowDef & { editor_metadata?: WorkflowEditorMetadata };

interface ExecuteWorkflowWire {
  execution_id: string;
}

interface WorkflowLatestExecutionWire {
  execution_id: string;
  status: ExecutionStatus;
  created_at: string;
  updated_at: string;
}

interface WorkflowListItemWire {
  id: string;
  name: string;
  version: string;
  definition_hash: string;
  registry_revision: number;
  description?: string;
  cron_expressions?: string[];
  triggers?: string[];
  updated_at?: string;
  latest_execution?: WorkflowLatestExecutionWire;
}

interface WorkflowListWire {
  list: WorkflowListItemWire[];
  total: number;
}

interface WorkflowVersionListItemWire {
  version: string;
  definition_hash: string;
  registry_revision: number;
  current: boolean;
  archived: boolean;
}

interface WorkflowVersionListWire {
  list: WorkflowVersionListItemWire[];
  total: number;
}

interface RollbackWorkflowWire {
  workflow_id: string;
  version: string;
  unchanged: boolean;
}

interface ExecutionNodeDetailWire {
  name: string;
  status: ExecutionNodeStatus;
  attempt?: number;
  port?: string;
  error?: string;
  error_details?: Record<string, unknown>;
  output?: Record<string, unknown>;
  child_execution_id?: string;
  started_at?: string;
  finished_at?: string;
}

interface AcceptedWire {
  accepted: boolean;
}

interface RevokeSignalWire {
  revoked: boolean;
}

interface ExecutionDetailWire {
  execution_id: string;
  /** `engine.ExecutionDetail` JSON; `omitempty` on the server. */
  workflow_id?: string;
  workflow_version?: string;
  /** External tracing correlation id; `omitempty` on the server. */
  trace_id?: string;
  status: ExecutionStatus;
  created_at?: string;
  updated_at?: string;
  parent_execution_id?: string;
  rerun_from?: string;
  parent_node?: string;
  queue_position?: number;
  error?: string;
  nodes?: ExecutionNodeDetailWire[];
}

interface WaitExecutionTimeoutWire {
  execution_id: string;
  status: ExecutionStatus;
  timed_out: true;
}

interface ExecutionListItemWire {
  execution_id: string;
  namespace: string;
  workflow_id: string;
  workflow_name: string;
  status: ExecutionStatus;
  error: string;
  created_at: string;
  updated_at: string;
  queue_position?: number;
  duration_ms?: number;
  finished_at?: string;
  trigger_kind?: string;
  retries?: number;
  node_progress?: { done: number; total: number };
}

interface ExecutionListWire {
  list: ExecutionListItemWire[];
  total: number;
}

interface OverviewKPIsWire {
  runs: number;
  runs_previous: number;
  failed_previous: number;
  success_rate: number | null;
  p50_ms: number | null;
  p95_ms: number | null;
  active_workflows: number;
  sparkline: { runs: number[]; success_rate: Array<number | null> };
}

interface OverviewFailingWorkflowWire {
  workflow_id: string;
  workflow_name?: string;
  failed: number;
  last_error?: string;
  last_failed_at?: string;
}

interface OverviewWire {
  window: OverviewWindow;
  generated_at: string;
  kpis: OverviewKPIsWire;
  histogram: Array<{ start: string; counts: unknown }>;
  top_failing: OverviewFailingWorkflowWire[];
  cluster?: { is_leader: boolean; runners: { online: number; total: number; capacity: number; in_flight: number } };
}

interface RerunExecutionWire {
  execution_id: string;
  status?: ExecutionStatus;
  queue_position?: number;
  duplicate?: boolean;
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

/** Reads a non-negative, safe integer count. */
function requireCount(record: Record<string, unknown>, key: string, response: ApiResponse<unknown>): number {
  const value = requireInteger(record, key, response);
  if (value < 0) {
    invalidResponse(response);
  }
  return value;
}

function requireBoolean(record: Record<string, unknown>, key: string, response: ApiResponse<unknown>): boolean {
  const value = record[key];
  if (typeof value !== "boolean") {
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

/**
 * Reads a non-negative integer that the server may render as `null`
 * (`nullable: true`) and may omit (`omitempty`). The three states stay
 * distinct: `undefined` = absent, `null` = explicitly no value, a number =
 * the value. Negative or fractional numbers are a broken response.
 */
function optionalNullableCount(
  record: Record<string, unknown>,
  key: string,
  response: ApiResponse<unknown>
): number | null | undefined {
  const value = record[key];
  if (value === undefined || value === null) {
    return value;
  }
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    invalidResponse(response);
  }
  return value;
}

/** Reads a finite number in [0, 1] — a rate, not a count. */
function nullableRate(record: Record<string, unknown>, key: string, response: ApiResponse<unknown>): number | null {
  const value = record[key];
  if (value === null) {
    return null;
  }
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0 || value > 1) {
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
      requestId,
      failureParamIssues(failure)
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

const paramIssueSeverities = new Set<string>(["error", "warning"]);

/** Parses one wire ParamIssue; undefined when it does not have the Go shape. */
function parseParamIssue(value: unknown): ParamIssue | undefined {
  if (!isRecord(value)) return undefined;
  const { node, path, code, message, severity } = value;
  if (
    typeof node !== "string" ||
    typeof path !== "string" ||
    typeof code !== "string" ||
    typeof message !== "string" ||
    typeof severity !== "string" ||
    !paramIssueSeverities.has(severity)
  ) {
    return undefined;
  }
  return { node, path, code, message, severity: severity as ParamIssue["severity"] };
}

/**
 * `data.param_issues` of a failure envelope. Best effort: the error is being
 * thrown anyway, so a malformed list is dropped rather than masking the
 * server's message with "invalid API response".
 */
function failureParamIssues(failure: Envelope<unknown> | undefined): ParamIssue[] | undefined {
  const list = isRecord(failure?.data) ? failure.data.param_issues : undefined;
  if (!Array.isArray(list)) return undefined;
  const issues = list.map(parseParamIssue);
  return issues.every((issue): issue is ParamIssue => issue !== undefined) ? issues : undefined;
}

/**
 * Splits an editor-side `WorkflowDef` into the `WireWorkflowDefWithMetadata`
 * body `createWorkflow`/`saveWorkflow` send (ADR-D4 §2.5): `position`/`ui`/
 * `notes` move off each node and into the `editor_metadata` sibling, and the
 * explicit `viewport` and `sections` (caller-owned canvas state that has no
 * field on `WorkflowDef`) are folded into the same sibling. An empty
 * `sections` array is omitted so the encoded body matches Go's `omitempty`
 * (empty and absent are the same value on the wire). `splitEditorMetadata`'s
 * diagnostics (`NODE_METADATA_KEYED_BY_NAME`) are dropped here -- the server
 * re-derives and returns the same findings via `RegisterWorkflowResult.warnings`,
 * so surfacing them twice would double every id-less-node warning for one save.
 */
function splitForWire(
  workflow: WorkflowDef,
  viewport?: Viewport,
  sections?: WorkflowEditorSection[]
): WireWorkflowDefWithMetadata {
  const { def, metadata } = splitEditorMetadata(workflow);
  if (viewport !== undefined) metadata.viewport = viewport;
  if (sections !== undefined && sections.length > 0) metadata.sections = sections;
  return Object.keys(metadata).length > 0 ? { ...def, editor_metadata: metadata } : def;
}

/**
 * Merges a `WireWorkflowDefWithMetadata` response body back into the editor's
 * internal (merged) `WorkflowDef` shape (ADR-D4 §2.5): `editor_metadata`'s
 * `positions`/`ui`/`notes` are restored onto their nodes and the sibling
 * field itself is dropped. `viewport` and `sections` are returned alongside,
 * since they are workflow-level and have no field on `WorkflowDef` to merge
 * onto. `mergeEditorMetadata`'s diagnostics are discarded for the same reason
 * as `splitForWire`'s: they mirror server-side findings a caller already sees
 * in `warnings` on write, and GET has no `warnings` slot to place them in.
 */
function mapWorkflowDef(wire: WireWorkflowDefWithMetadata): GetWorkflowResult {
  const { editor_metadata: metadata, ...rest } = wire;
  const result: GetWorkflowResult = { workflow: mergeEditorMetadata(rest, metadata).def };
  if (metadata?.viewport !== undefined) result.viewport = metadata.viewport;
  // Under Go's `omitempty` an empty slice and an absent one are the same
  // value, so normalize `[]` to absent: callers see one representation for
  // one value.
  if (metadata?.sections !== undefined && metadata.sections.length > 0) result.sections = metadata.sections;
  return result;
}

function mapRegisterWorkflow(response: ApiResponse<unknown>): RegisterWorkflowResult {
  const data = requireRecord(response.data, response);
  const result: RegisterWorkflowResult = { workflowId: requireString(data, "workflow_id", response) };
  const warnings = optionalStringArray(data, "warnings", response);
  if (warnings !== undefined) {
    result.warnings = warnings;
  }
  const rawIssues = data.param_issues;
  if (rawIssues !== undefined) {
    if (!Array.isArray(rawIssues)) invalidResponse(response);
    const issues = rawIssues.map((issue) => parseParamIssue(issue) ?? invalidResponse(response));
    if (issues.length > 0) result.paramIssues = issues;
  }
  return result;
}

function mapExecuteWorkflow(response: ApiResponse<unknown>): ExecuteWorkflowResult {
  const data = requireRecord(response.data, response);
  return { executionId: requireString(data, "execution_id", response) };
}

/**
 * Parses one `latest_execution` summary. All four fields are required by the
 * OpenAPI contract; a present-but-malformed object is a broken response rather
 * than "no recent run", so a caller never renders an invented status or time.
 */
function mapWorkflowLatestExecution(value: unknown, response: ApiResponse<unknown>): WorkflowLatestExecution {
  const data = requireRecord(value, response);
  return {
    executionId: requireString(data, "execution_id", response),
    status: requireExecutionStatus(data.status, response),
    createdAt: requireString(data, "created_at", response),
    updatedAt: requireString(data, "updated_at", response)
  };
}

/**
 * Parses one workflow row's `stats` object. Every count and `sample_size` is
 * required; `success_rate` is required but NULLABLE (null = the window has no
 * terminal run, which is not 0). The percentiles are optional/nullable
 * (`omitempty` server-side, `nullable: true` in the contract).
 */
function mapWorkflowExecutionStats(value: unknown, response: ApiResponse<unknown>): WorkflowExecutionStats {
  const data = requireRecord(value, response);
  const window = requireString(data, "window", response);
  if (!workflowStatsWindows.has(window)) {
    invalidResponse(response);
  }
  const stats: WorkflowExecutionStats = {
    window: window as WorkflowStatsWindow,
    runs: requireCount(data, "runs", response),
    succeeded: requireCount(data, "succeeded", response),
    failed: requireCount(data, "failed", response),
    successRate: nullableRate(data, "success_rate", response),
    sampleSize: requireCount(data, "sample_size", response)
  };
  const p50 = optionalNullableCount(data, "p50_ms", response);
  if (p50 !== undefined) {
    stats.p50Ms = p50;
  }
  const p95 = optionalNullableCount(data, "p95_ms", response);
  if (p95 !== undefined) {
    stats.p95Ms = p95;
  }
  return stats;
}

/**
 * Parses one workflow row's `recent_executions` array. `duration_ms` is
 * optional and nullable: a non-terminal execution has none, which is different
 * from a zero-length run.
 */
function mapWorkflowRecentExecutions(value: unknown, response: ApiResponse<unknown>): WorkflowRecentExecution[] {
  if (!Array.isArray(value)) {
    invalidResponse(response);
  }
  return value.map((entry) => {
    const data = requireRecord(entry, response);
    const row: WorkflowRecentExecution = {
      executionId: requireString(data, "execution_id", response),
      status: requireExecutionStatus(data.status, response),
      createdAt: requireString(data, "created_at", response)
    };
    const duration = optionalNullableCount(data, "duration_ms", response);
    if (duration !== undefined) {
      row.durationMs = duration;
    }
    return row;
  });
}

/** Reads a count map that must carry all nine status keys, each a non-negative integer. */
function mapExecutionStatusCounts(value: unknown, response: ApiResponse<unknown>): ExecutionStatusCounts {
  const data = requireRecord(value, response);
  const counts = {} as ExecutionStatusCounts;
  for (const status of executionStatusCountKeys) {
    counts[status] = requireCount(data, status, response);
  }
  return counts;
}

/**
 * Parses GET /v1/executions/stats. Both window bounds, `total` and the counts
 * map are required; `histogram` is present only when a `bucket` was requested,
 * and its buckets must carry the same complete counts map.
 */
function mapExecutionStats(response: ApiResponse<unknown>): ExecutionStats {
  const data = requireRecord(response.data, response);
  const stats: ExecutionStats = {
    windowStart: requireString(data, "window_start", response),
    windowEnd: requireString(data, "window_end", response),
    total: requireCount(data, "total", response),
    countsByStatus: mapExecutionStatusCounts(data.counts_by_status, response)
  };
  const histogram = data.histogram;
  if (histogram !== undefined) {
    if (!Array.isArray(histogram)) {
      invalidResponse(response);
    }
    stats.histogram = histogram.map((value) => {
      const bucket = requireRecord(value, response);
      return {
        start: requireString(bucket, "start", response),
        counts: mapExecutionStatusCounts(bucket.counts, response)
      };
    });
  }
  return stats;
}

/**
 * Parses GET /v1/overview. Every KPI the schema marks nullable is read through
 * the same three-state rule as elsewhere: `null` is a real "no terminal run",
 * never 0, and a missing required field is a broken response.
 */
function mapOverview(response: ApiResponse<OverviewWire>): OverviewResponse {
  const data = requireRecord(response.data, response);
  if (data.window !== "24h" && data.window !== "7d") {
    invalidResponse(response);
  }
  const kpis = requireRecord(data.kpis, response);
  const sparkline = requireRecord(kpis.sparkline, response);
  const sparkRuns = sparkline.runs;
  if (!Array.isArray(sparkRuns) || sparkRuns.some((v) => typeof v !== "number" || !Number.isSafeInteger(v) || v < 0)) {
    invalidResponse(response);
  }
  const sparkRates = sparkline.success_rate;
  if (
    !Array.isArray(sparkRates) ||
    sparkRates.some((v) => v !== null && (typeof v !== "number" || !Number.isFinite(v) || v < 0 || v > 1))
  ) {
    invalidResponse(response);
  }
  if (!Array.isArray(data.histogram)) {
    invalidResponse(response);
  }
  if (!Array.isArray(data.top_failing)) {
    invalidResponse(response);
  }

  const requiredNullableCount = (record: Record<string, unknown>, key: string): number | null => {
    const value = optionalNullableCount(record, key, response);
    if (value === undefined) {
      invalidResponse(response);
    }
    return value;
  };

  const overview: OverviewResponse = {
    window: data.window,
    generatedAt: requireString(data, "generated_at", response),
    kpis: {
      runs: requireCount(kpis, "runs", response),
      runsPrevious: requireCount(kpis, "runs_previous", response),
      failedPrevious: requireCount(kpis, "failed_previous", response),
      successRate: nullableRate(kpis, "success_rate", response),
      p50Ms: requiredNullableCount(kpis, "p50_ms"),
      p95Ms: requiredNullableCount(kpis, "p95_ms"),
      activeWorkflows: requireCount(kpis, "active_workflows", response),
      sparkline: {
        runs: sparkRuns,
        successRate: sparkRates
      }
    },
    histogram: data.histogram.map((value) => {
      const bucket = requireRecord(value, response);
      return {
        start: requireString(bucket, "start", response),
        counts: mapExecutionStatusCounts(bucket.counts, response)
      };
    }),
    topFailing: data.top_failing.map((value) => {
      const row = requireRecord(value, response);
      const failing: OverviewFailingWorkflow = {
        workflowId: requireString(row, "workflow_id", response),
        failed: requireCount(row, "failed", response)
      };
      const name = optionalString(row, "workflow_name", response);
      if (name !== undefined && name !== "") {
        failing.workflowName = name;
      }
      const lastError = optionalString(row, "last_error", response);
      if (lastError !== undefined && lastError !== "") {
        failing.lastError = lastError;
      }
      const lastFailedAt = optionalString(row, "last_failed_at", response);
      if (lastFailedAt !== undefined && lastFailedAt !== "") {
        failing.lastFailedAt = lastFailedAt;
      }
      return failing;
    })
  };

  // The cluster segment is absent for a caller without management.runner.list;
  // a present one is validated strictly like every other object.
  if (data.cluster !== undefined) {
    const cluster = requireRecord(data.cluster, response);
    const runners = requireRecord(cluster.runners, response);
    overview.cluster = {
      isLeader: requireBoolean(cluster, "is_leader", response),
      runners: {
        online: requireCount(runners, "online", response),
        total: requireCount(runners, "total", response),
        capacity: requireCount(runners, "capacity", response),
        inFlight: requireCount(runners, "in_flight", response)
      }
    };
  }
  return overview;
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
      // `triggers` is required by the service contract, but an older server or
      // a stale mock may omit it; absence means "no trigger node" (`[]`), and
      // any other malformed shape is still a broken response. A present
      // `latest_execution` is validated strictly; absence stays absence.
      const triggers = optionalStringArray(item, "triggers", response) ?? [];
      const summary: WorkflowSummary = {
        id: requireString(item, "id", response),
        name: requireString(item, "name", response),
        version: requireString(item, "version", response),
        definitionHash: requireString(item, "definition_hash", response),
        registryRevision: requireInteger(item, "registry_revision", response),
        triggers
      };
      // Both metadata fields are optional on the wire (Go omits empty ones).
      // An empty value and an absent one mean the same thing to a caller, so
      // they normalize to absent rather than being stored as "" / [].
      const description = optionalString(item, "description", response);
      if (description !== undefined && description !== "") {
        summary.description = description;
      }
      const cronExpressions = optionalStringArray(item, "cron_expressions", response);
      if (cronExpressions !== undefined && cronExpressions.length > 0) {
        summary.cronExpressions = cronExpressions;
      }
      // Absent means the record predates the registry timestamp; never
      // substitute the zero instant for it.
      const updatedAt = optionalString(item, "updated_at", response);
      if (updatedAt !== undefined && updatedAt !== "") {
        summary.updatedAt = updatedAt;
      }
      const latest = item.latest_execution;
      if (latest !== undefined) {
        summary.latestExecution = mapWorkflowLatestExecution(latest, response);
      }
      // The execution-derived projections are absent when the server could not
      // (or may not) answer; a present one is validated strictly. An empty
      // recent list is normalized to absent for the same reason as `[]`/absent
      // metadata above.
      const stats = item.stats;
      if (stats !== undefined) {
        summary.stats = mapWorkflowExecutionStats(stats, response);
      }
      const recent = item.recent_executions;
      if (recent !== undefined) {
        const rows = mapWorkflowRecentExecutions(recent, response);
        if (rows.length > 0) {
          summary.recentExecutions = rows;
        }
      }
      return summary;
    }),
    total: requireInteger(data, "total", response)
  };
}

function mapWorkflowVersionList(response: ApiResponse<unknown>): WorkflowVersionPage {
  const data = requireRecord(response.data, response);
  const list = data.list;
  if (!Array.isArray(list)) {
    invalidResponse(response);
  }

  return {
    list: list.map((value) => {
      const item = requireRecord(value, response);
      return {
        version: requireString(item, "version", response),
        definitionHash: requireString(item, "definition_hash", response),
        registryRevision: requireInteger(item, "registry_revision", response),
        current: requireBoolean(item, "current", response),
        archived: requireBoolean(item, "archived", response)
      };
    }),
    total: requireInteger(data, "total", response)
  };
}

function mapRollbackWorkflow(response: ApiResponse<unknown>): RollbackWorkflowResult {
  const data = requireRecord(response.data, response);
  return {
    workflowId: requireString(data, "workflow_id", response),
    version: requireString(data, "version", response),
    unchanged: requireBoolean(data, "unchanged", response)
  };
}

const paramValidationModes = new Set<string>(["off", "warn", "enforce"]);

/**
 * Checks the NodeFormSchema envelope fields the editor keys on (identity and
 * the field list). Field contents are not re-validated here: the compiler
 * degrades unknown widgets, types and rules with warnings (Doc C §3.2).
 */
function mapNodeFormSchema(value: unknown, response: ApiResponse<unknown>): NodeFormSchema {
  const data = requireRecord(value, response);
  requireString(data, "spec", response);
  requireString(data, "node_type", response);
  requireInteger(data, "node_version", response);
  requireString(data, "kind", response);
  if (!Array.isArray(data.fields) || data.fields.some((field) => !isRecord(field))) {
    invalidResponse(response);
  }
  return data as unknown as NodeFormSchema;
}

function mapNodeTypes(response: ApiResponse<unknown>): NodeTypesResponse {
  const data = requireRecord(response.data, response);
  const mode = data.param_validation_mode;
  if (typeof mode !== "string" || !paramValidationModes.has(mode)) {
    invalidResponse(response);
  }
  if (!Array.isArray(data.node_types)) {
    invalidResponse(response);
  }
  return {
    param_validation_mode: mode as ParamValidationMode,
    node_types: data.node_types.map((schema) => mapNodeFormSchema(schema, response))
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
  const childExecutionId = optionalString(data, "child_execution_id", response);
  const startedAt = optionalString(data, "started_at", response);
  const finishedAt = optionalString(data, "finished_at", response);

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
  if (childExecutionId !== undefined) {
    node.childExecutionId = childExecutionId;
  }
  if (startedAt !== undefined) {
    node.startedAt = startedAt;
  }
  if (finishedAt !== undefined) {
    node.finishedAt = finishedAt;
  }
  return node;
}

function mapExecutionDetail(response: ApiResponse<unknown>): ExecutionDetail {
  const data = requireRecord(response.data, response);
  const detail: ExecutionDetail = {
    executionId: requireString(data, "execution_id", response),
    status: requireExecutionStatus(data.status, response)
  };
  const workflowId = optionalString(data, "workflow_id", response);
  if (workflowId !== undefined) {
    detail.workflowId = workflowId;
  }
  const workflowVersion = optionalString(data, "workflow_version", response);
  if (workflowVersion !== undefined) {
    detail.workflowVersion = workflowVersion;
  }
  // Strictly a string when present: a malformed trace id must not be coerced
  // into a link target, and an absent one must stay absent rather than "".
  const traceId = optionalString(data, "trace_id", response);
  if (traceId !== undefined) {
    detail.traceId = traceId;
  }
  const createdAt = optionalString(data, "created_at", response);
  if (createdAt !== undefined) {
    detail.createdAt = createdAt;
  }
  const updatedAt = optionalString(data, "updated_at", response);
  if (updatedAt !== undefined) {
    detail.updatedAt = updatedAt;
  }
  const parentExecutionId = optionalString(data, "parent_execution_id", response);
  if (parentExecutionId !== undefined) {
    detail.parentExecutionId = parentExecutionId;
  }
  const rerunFrom = optionalString(data, "rerun_from", response);
  if (rerunFrom !== undefined) {
    detail.rerunFrom = rerunFrom;
  }
  const parentNode = optionalString(data, "parent_node", response);
  if (parentNode !== undefined) {
    detail.parentNode = parentNode;
  }
  const error = optionalString(data, "error", response);
  if (error !== undefined) {
    detail.error = error;
  }
  // `omitempty` on the server: absent for every execution that is not queued,
  // so an absent field must stay absent (a 0 is not a real place in line).
  const queuePosition = optionalInteger(data, "queue_position", response);
  if (queuePosition !== undefined) {
    detail.queuePosition = queuePosition;
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

function mapExecutionListItem(value: unknown, response: ApiResponse<unknown>): ExecutionListItem {
  const row = requireRecord(value, response);
  const item: ExecutionListItem = {
    executionId: requireString(row, "execution_id", response),
    namespace: requireString(row, "namespace", response),
    workflowId: requireString(row, "workflow_id", response),
    workflowName: requireString(row, "workflow_name", response),
    status: requireExecutionStatus(row.status, response),
    error: requireString(row, "error", response),
    createdAt: requireString(row, "created_at", response),
    updatedAt: requireString(row, "updated_at", response)
  };
  const queuePosition = optionalInteger(row, "queue_position", response);
  if (queuePosition !== undefined) {
    item.queuePosition = queuePosition;
  }
  // The statistics-era fields are all optional on the wire: each is omitted
  // when it is UNKNOWN (live row, store without the capability, pre-column
  // row), and absence must stay absence rather than becoming 0 / "".
  const durationMs = optionalInteger(row, "duration_ms", response);
  if (durationMs !== undefined) {
    item.durationMs = durationMs;
  }
  const finishedAt = optionalString(row, "finished_at", response);
  if (finishedAt !== undefined) {
    item.finishedAt = finishedAt;
  }
  const triggerKind = optionalString(row, "trigger_kind", response);
  if (triggerKind !== undefined && triggerKind !== "") {
    item.triggerKind = triggerKind;
  }
  const retries = optionalInteger(row, "retries", response);
  if (retries !== undefined) {
    item.retries = retries;
  }
  const progress = optionalRecord(row, "node_progress", response);
  if (progress !== undefined) {
    item.nodeProgress = {
      done: requireCount(progress, "done", response),
      total: requireCount(progress, "total", response)
    };
  }
  return item;
}

function mapExecutionList(response: ApiResponse<unknown>): ExecutionListPage {
  const data = requireRecord(response.data, response);
  if (!Array.isArray(data.list)) {
    invalidResponse(response);
  }
  return {
    list: data.list.map((row) => mapExecutionListItem(row, response)),
    total: requireInteger(data, "total", response)
  };
}

function mapRerunExecution(response: ApiResponse<unknown>): RerunExecutionResult {
  const data = requireRecord(response.data, response);
  const result: RerunExecutionResult = {
    executionId: requireString(data, "execution_id", response)
  };
  if (data.status !== undefined) {
    result.status = requireExecutionStatus(data.status, response);
  }
  const queuePosition = optionalInteger(data, "queue_position", response);
  if (queuePosition !== undefined) {
    result.queuePosition = queuePosition;
  }
  if (data.duplicate !== undefined) {
    if (typeof data.duplicate !== "boolean") {
      invalidResponse(response);
    }
    result.duplicate = data.duplicate;
  }
  return result;
}

function mapCancelExecution(response: ApiResponse<unknown>): CancelExecutionResult {
  const data = requireRecord(response.data, response);
  return { accepted: requireBoolean(data, "accepted", response) };
}

function mapRevokeSignal(response: ApiResponse<unknown>): RevokeSignalResult {
  const data = requireRecord(response.data, response);
  return { revoked: requireBoolean(data, "revoked", response) };
}

function listWorkflowPage(
  fetcher: typeof fetch,
  baseUrl: string,
  options?: ListWorkflowsOptions
): Promise<WorkflowListPage> {
  const include = options?.include && options.include.length > 0 ? options.include.join(",") : undefined;
  const path = withQuery("/workflows", {
    page: options?.page === undefined ? undefined : String(options.page),
    page_size: options?.pageSize === undefined ? undefined : String(options.pageSize),
    include,
    window: options?.window
  });
  // `q` is the only free-text parameter, so it is encoded with
  // encodeURIComponent rather than folded into `withQuery`'s URLSearchParams
  // (which would send a space as `+`). Absent, empty and whitespace-only values
  // are not sent: the server would treat them as no filter anyway.
  const q = options?.q?.trim();
  const requested = q ? `${path}${path.includes("?") ? "&" : "?"}q=${encodeURIComponent(q)}` : path;
  return request<WorkflowListWire>(fetcher, joinUrl(baseUrl, requested)).then(mapWorkflowList);
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
 * `@xflow/core` renderers. Three transitional statuses are collapsed:
 * `canceling` (execution) and `committing` (node) are represented as `running`
 * until their terminal state is observed, and `queued` (execution) as
 * `pending`, since a parked submission has run nothing yet. Every other status
 * is carried through unchanged, `expired` included: reporting it as `pending`
 * would assert that a node whose records are gone never ran, which is the one
 * thing the status exists to deny (types/execution.go).
 */
export function executionDetailToRuntimeSnapshot(detail: ExecutionDetail): RuntimeSnapshot {
  const snapshot: RuntimeSnapshot = {
    status: executionStatusToRuntimeStatus(detail.status)
  };
  // Diagnostic metadata, carried only when the server recorded it: the 运行日志
  // tab offers it for copying and for an external trace link.
  if (detail.traceId !== undefined) {
    snapshot.traceId = detail.traceId;
  }

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
      // The Inspector's 运行 tab reads these from the runtime projection: the
      // editor has no execution model of its own (it never imports @xflow/api).
      if (node.port !== undefined) {
        runtimeNode.port = node.port;
      }
      if (node.errorDetails !== undefined) {
        runtimeNode.errorDetails = node.errorDetails;
      }
      if (node.output !== undefined) {
        runtimeNode.output = node.output;
      }
      if (node.childExecutionId !== undefined) {
        runtimeNode.childExecutionId = node.childExecutionId;
      }
      // B2b stamps the node's first lease and first terminal status. A
      // duration only exists when both are present and ordered; otherwise it
      // stays absent, which the Inspector renders as "-" rather than a guess.
      if (node.startedAt !== undefined && node.finishedAt !== undefined) {
        const started = Date.parse(node.startedAt);
        const finished = Date.parse(node.finishedAt);
        if (Number.isFinite(started) && Number.isFinite(finished) && finished >= started) {
          runtimeNode.durationMs = finished - started;
        }
      }
      nodes[node.name] = runtimeNode;
    }
    snapshot.nodes = nodes;
  }

  return snapshot;
}

/**
 * Projects one server-side execution status into the runtime vocabulary. It is
 * exported for hosts that hold an execution response without a node list — a
 * `waitExecution` call that timed out, for instance — so the collapse table
 * lives in one place instead of being re-stated per host, where a status added
 * later would be silently missed.
 */
export function executionStatusToRuntimeStatus(status: ExecutionStatus): RuntimeWorkflowStatus {
  switch (status) {
    case "pending":
      return "pending";
    case "queued":
      // Accepted but parked behind the workflow's concurrency limit: nothing
      // has run, so the runtime vocabulary's "not started yet" value is the
      // honest projection. It stays a distinct server-side status because the
      // queue is what a caller waits on or cancels.
      return "pending";
    // A paused execution has started and is still live: dispatch is suspended,
    // not undone, so "not started yet" would be false. The runtime vocabulary
    // has no paused member; live is the honest projection.
    case "running":
    case "canceling":
    case "paused":
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
    // A pinned node completed with its pin_data mock as output; the runtime
    // status vocabulary has no separate state for it. The comment sits above
    // the group rather than between the two clauses: an empty clause followed
    // by a comment reads as a fallthrough to `no-fallthrough`.
    case "success":
    case "pinned":
      return "success";
    case "expired":
      // The node's records are gone (a transient group past its completion
      // TTL). The member ran, or may have; nothing about it is readable any
      // more, and "pending" would assert precisely the thing that is not
      // known, so the status is carried through for the renderers to show.
      return "expired";
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

/**
 * Parses GET /v1/current-user. `scopes` is required by the contract; an
 * absent list is a broken response rather than "no scopes" (the server sends
 * `[]` for that). Keeping the two distinct stops a malformed payload from
 * silently demoting the caller to read-only.
 */
function mapCurrentUser(response: ApiResponse<unknown>): CurrentIdentity {
  const data = requireRecord(response.data, response);
  const scopes = optionalStringArray(data, "scopes", response);
  if (scopes === undefined) {
    invalidResponse(response);
  }
  return {
    subject: requireString(data, "subject", response),
    namespace: requireString(data, "namespace", response),
    scopes
  };
}

export function createXFlowApiClient(options: XFlowApiClientOptions): XFlowApiClient {
  const fetcher = options.fetcher ?? fetch;

  return {
    listWorkflows: createListWorkflows(fetcher, options.baseUrl),
    getCurrentUser() {
      return request<unknown>(fetcher, joinUrl(options.baseUrl, "/current-user")).then(mapCurrentUser);
    },
    createWorkflow(workflow, saveOptions) {
      return request<RegisterWorkflowWire>(fetcher, joinUrl(options.baseUrl, "/workflows"), {
        method: "POST",
        headers: {
          "content-type": "application/json"
        },
        body: JSON.stringify(workflow ? splitForWire(workflow, saveOptions?.viewport, saveOptions?.sections) : {})
      }).then(mapRegisterWorkflow);
    },
    getWorkflow(id) {
      return request<WireWorkflowDefWithMetadata>(fetcher, joinUrl(options.baseUrl, workflowPath(id))).then(
        ({ data }) => mapWorkflowDef(data)
      );
    },
    saveWorkflow(workflow, saveOptions) {
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
          body: JSON.stringify(splitForWire(workflow, saveOptions?.viewport, saveOptions?.sections))
        }
      ).then(mapRegisterWorkflow);
    },
    listWorkflowVersions(id, listOptions) {
      const path = withQuery(`${workflowPath(id)}/versions`, {
        page: listOptions?.page === undefined ? undefined : String(listOptions.page),
        page_size: listOptions?.pageSize === undefined ? undefined : String(listOptions.pageSize)
      });
      return request<WorkflowVersionListWire>(fetcher, joinUrl(options.baseUrl, path)).then(mapWorkflowVersionList);
    },
    getWorkflowVersion(id, version) {
      // Same wire body as GET /workflows/{id}: the definition plus its
      // editor_metadata sibling, merged the same way (ADR-D4 §2.5).
      return request<WireWorkflowDefWithMetadata>(
        fetcher,
        joinUrl(options.baseUrl, `${workflowPath(id)}/versions/${pathSegment(version)}`)
      ).then(({ data }) => mapWorkflowDef(data));
    },
    rollbackWorkflow(id, version) {
      return request<RollbackWorkflowWire>(fetcher, joinUrl(options.baseUrl, `${workflowPath(id)}/rollback`), {
        method: "POST",
        headers: {
          "content-type": "application/json"
        },
        body: JSON.stringify({ version })
      }).then(mapRollbackWorkflow);
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
    getExecutionStats(statsOptions) {
      const path = withQuery("/executions/stats", {
        created_after: statsOptions.createdAfter,
        created_before: statsOptions.createdBefore,
        workflow_id: statsOptions.workflowId,
        bucket: statsOptions.bucket
      });
      return request<unknown>(fetcher, joinUrl(options.baseUrl, path)).then(mapExecutionStats);
    },
    getOverview(overviewOptions) {
      const path = withQuery("/overview", { window: overviewOptions?.window });
      return request<OverviewWire>(fetcher, joinUrl(options.baseUrl, path)).then(mapOverview);
    },
    pauseExecution(id) {
      return request<ExecutionDetailWire>(
        fetcher,
        joinUrl(options.baseUrl, `${executionPath(id)}/pause`),
        { method: "POST" }
      ).then(mapExecutionDetail);
    },
    resumeExecution(id) {
      return request<ExecutionDetailWire>(
        fetcher,
        joinUrl(options.baseUrl, `${executionPath(id)}/resume`),
        { method: "POST" }
      ).then(mapExecutionDetail);
    },
    cancelExecution(id) {
      return request<AcceptedWire>(
        fetcher,
        joinUrl(options.baseUrl, `${executionPath(id)}/cancel`),
        { method: "POST" }
      ).then(mapCancelExecution);
    },
    deliverSignal(id, signal) {
      return request<AcceptedWire>(
        fetcher,
        joinUrl(options.baseUrl, `${executionPath(id)}/signals`),
        {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ name: signal.name, data: signal.data })
        }
      ).then(mapCancelExecution);
    },
    revokeSignal(id, name) {
      // The name is one path segment and may contain `/` (an approval signal
      // is `<node>/approval/<subject>`), so it is percent-encoded: an unencoded
      // slash would make the mux match a different route.
      return request<RevokeSignalWire>(
        fetcher,
        joinUrl(options.baseUrl, `${executionPath(id)}/signals/${pathSegment(name)}`),
        { method: "DELETE" }
      ).then(mapRevokeSignal);
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
    },
    listExecutions(listOptions) {
      // One `status` parameter per selected value: the server ORs them. The
      // scalar `status` is kept first for backward compatibility with callers
      // (and tests) that only know the single-value form.
      const statusParams = [
        ...(listOptions?.status === undefined ? [] : [listOptions.status]),
        ...(listOptions?.statuses ?? [])
      ];
      const path = withQuery("/executions", {
        page: listOptions?.page === undefined ? undefined : String(listOptions.page),
        page_size: listOptions?.pageSize === undefined ? undefined : String(listOptions.pageSize),
        status: statusParams.length === 0 ? undefined : statusParams,
        q: listOptions?.q,
        workflow_id: listOptions?.workflowId,
        created_after: listOptions?.createdAfter,
        created_before: listOptions?.createdBefore
      });
      return request<ExecutionListWire>(fetcher, joinUrl(options.baseUrl, path)).then(mapExecutionList);
    },
    rerunExecution(id, rerunOptions) {
      return request<RerunExecutionWire>(
        fetcher,
        joinUrl(options.baseUrl, `${executionPath(id)}/rerun`),
        {
          method: "POST",
          headers: {
            "content-type": "application/json",
            "Idempotency-Key": rerunOptions.idempotencyKey
          },
          body: JSON.stringify({
            from_node: rerunOptions.fromNode,
            include_success: rerunOptions.includeSuccess === true
          })
        }
      ).then(mapRerunExecution);
    },
    listNodeTypes(listOptions) {
      const path = withQuery("/node-types", {
        namespace: listOptions?.namespace ? listOptions.namespace : undefined
      });
      return request<NodeTypesResponse>(fetcher, joinUrl(options.baseUrl, path)).then(mapNodeTypes);
    },
    getNodeType(type, version, getOptions) {
      const path = withQuery(nodeTypePath(type), {
        version: version === undefined ? undefined : String(version),
        namespace: getOptions?.namespace ? getOptions.namespace : undefined
      });
      return request<NodeFormSchema>(fetcher, joinUrl(options.baseUrl, path)).then((response) =>
        mapNodeFormSchema(response.data, response)
      );
    }
  };
}
