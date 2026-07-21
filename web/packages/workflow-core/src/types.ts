// NOTE: 手写对照 Go types/workflow.go 与 api/openapi/xflow-v1.yaml。
// C0 已冻结 WorkflowEditorMetadata / Diagnostic / WorkflowDefinitionRuntime 等 wire contract；
// 新增管理类型（WorkflowDefinition / WorkflowDraft / WorkflowDefinitionVersion / ExecutionSnapshot）
// 与 OpenAPI 一一对应。openapi-types.ts 由 openapi-typescript 生成，用于 CI diff 校验。

export type NodeKind = "action" | "trigger";

export type RunnerSelectorMode = "default" | "required";

export interface Position {
  x?: number;
  y?: number;
}

export interface PortDecl {
  name?: string;
  required?: boolean;
}

export interface Connection {
  node?: string;
  input?: string;
}

// Connections: map[source_node]map[output_port][]Connection(target)
export type Connections = Record<string, Record<string, Connection[]>>;

export interface WorkflowContext {
  vars?: Record<string, unknown>;
  config?: Record<string, unknown>;
}

export interface RetrySettings {
  enabled?: boolean;
  max_attempts?: number;
  strategy?: string;
  initial_interval?: number;
  max_interval?: number;
  multiplier?: number;
}

export interface WorkflowSettings {
  timeout?: number;
  concurrency?: number;
  timezone?: string;
  on_error?: string;
  pin_data_mode?: string;
  retry?: RetrySettings;
}

export interface RunnerSelector {
  mode?: RunnerSelectorMode;
  matchLabels?: Record<string, string>;
}

export interface WorkflowOptions {
  allow_cycles?: boolean;
  max_auto_depth?: number;
  experimental_expand?: boolean;
}

export interface CredentialDef {
  name?: string;
  type?: string;
}

export interface ParamDef {
  type?: string;
  required?: boolean;
  display_name?: string;
  default?: unknown;
  validation?: Record<string, unknown>;
}

export interface NodeTemplate {
  type?: string;
  parameters?: Record<string, unknown>;
}

export interface WorkflowOutput {
  value?: unknown;
  display_name?: string;
}

export interface NodeDef {
  id?: string;
  name?: string;
  type?: string;
  kind?: NodeKind;
  version?: number;
  template?: string;
  position?: Position;
  disabled?: boolean;
  on_error?: string;
  runnerSelector?: RunnerSelector;
  notes?: string;
  inputs?: PortDecl[];
  output_schema?: Record<string, unknown>;
  parameters?: Record<string, unknown>;
  ui?: Record<string, unknown>;
  retry?: RetrySettings;
}

export interface WorkflowDef {
  id?: string;
  namespace?: string;
  name?: string;
  version?: string;
  description?: string;
  spec?: string;
  runnerSelector?: RunnerSelector;
  context?: WorkflowContext;
  settings?: WorkflowSettings;
  options?: WorkflowOptions;
  credentials?: Record<string, CredentialDef>;
  params?: Record<string, ParamDef>;
  node_templates?: Record<string, NodeTemplate>;
  nodes?: NodeDef[];
  connections?: Connections;
  outputs?: Record<string, WorkflowOutput>;
  pin_data?: Record<string, unknown>;
}

export interface Diagnostic {
  code: string;
  severity: "error" | "warning" | "info";
  message: string;
  path?: string;
  nodeId?: string;
  connectionRef?: { node: string; input: string };
}

// ADR D4: Editor metadata separated from runtime definition.
export interface WorkflowEditorMetadata {
  positions?: Record<string, Position>;
  viewport?: {
    x?: number;
    y?: number;
    zoom?: number;
  };
  ui?: Record<string, Record<string, unknown>>;
  notes?: Record<string, string>;
  /**
   * Read-only derived cache of `WorkflowDef.pin_data` for UI display
   * convenience. NOT the authoritative source — `WorkflowDef.pin_data`
   * remains canonical. `splitEditorMetadata` does not remove `pin_data`
   * from the runtime def; `mergeEditorMetadata` does not overwrite
   * `WorkflowDef.pin_data` with this field (def wins on conflict).
   * See ADR D4 / F0-A2.
   */
  pinData?: Readonly<Record<string, unknown>>;
}

export type ExecutionStatus =
  | "pending"
  | "running"
  | "success"
  | "failed"
  | "canceling"
  | "canceled"
  | "timeout";

export type NodeStatus =
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

export interface WorkflowDefinition {
  id: string;
  namespace: string;
  name: string;
  description?: string;
  createdAt: string;
  updatedAt: string;
  createdBy?: string;
  tenantId?: string;
  latestVersion?: string;
  draftRevision?: number;
}

export interface WorkflowDefinitionList {
  items: WorkflowDefinition[];
  nextPageToken?: string;
}

export interface WorkflowDefinitionRuntime {
  namespace: string;
  name: string;
  versionTag?: string;
  description?: string;
  spec?: string;
  runnerSelector?: RunnerSelector;
  context?: WorkflowContext;
  settings?: WorkflowSettings;
  options?: WorkflowOptions;
  credentials?: Record<string, CredentialDef>;
  params?: Record<string, ParamDef>;
  node_templates?: Record<string, NodeTemplate>;
  nodes?: RuntimeNodeDef[];
  connections?: Connections;
  outputs?: Record<string, WorkflowOutput>;
  pin_data?: Record<string, unknown>;
}

export interface RuntimeNodeDef {
  name: string;
  type: string;
  kind?: NodeKind;
  version?: number;
  template?: string;
  disabled?: boolean;
  on_error?: string;
  runnerSelector?: RunnerSelector;
  inputs?: PortDecl[];
  output_schema?: Record<string, unknown>;
  parameters?: Record<string, unknown>;
  retry?: RetrySettings;
}

export interface WorkflowDraft {
  definition: WorkflowDefinitionRuntime;
  editorMetadata?: WorkflowEditorMetadata;
  revision: number;
  definitionHash?: string;
  auditFingerprint?: string;
  updatedAt?: string;
}

export interface WorkflowDefinitionVersion extends WorkflowDraft {
  version: string;
  publishedAt?: string;
  publishedBy?: string;
  immutable: boolean;
}

export interface WorkflowDefinitionVersionList {
  items: WorkflowDefinitionVersion[];
  nextPageToken?: string;
}

export interface ValidationResult {
  valid: boolean;
  definitionHash?: string;
  diagnostics?: Diagnostic[];
}

export interface ExecutionSnapshot {
  id: string;
  status: ExecutionStatus;
  params?: Record<string, unknown>;
  nodeStatuses?: Record<string, NodeStatus>;
  parentId?: string;
  traceId?: string;
  startedAt?: string;
  finishedAt?: string;
}

export interface PublishWorkflowVersionRequest {
  version: string;
}

export interface InvokeWorkflowRequest {
  entry: string;
  input?: Record<string, unknown>;
}

export interface InvokeWorkflowResponse {
  executionId: string;
}

export interface XflowError {
  code: string;
  message: string;
  details?: Record<string, unknown>;
}
