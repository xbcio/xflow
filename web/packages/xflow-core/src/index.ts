import { foundationTokens } from "./tokens";

export type NodeKind = "action" | "trigger" | "supply";
export type ErrorPolicy = "stop" | "error_output" | "main_output" | "continue";
export type RetryStrategy = "fixed" | "exponential";
export type RunnerSelectorMode = "default" | "required";
export type ConnectionType = "data" | "dependency";

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

/**
 * Connections from one output port. Data ports may use the compact array form;
 * dependency ports use the object form and are intentionally outside dataflow
 * topology. An omitted type in the object form retains the backend's data-port
 * default.
 */
export interface PortConnections {
  type?: ConnectionType;
  targets: Connection[];
}

export type ConnectionTargets = Connection[] | PortConnections;

export type Connections = Record<string, Record<string, ConnectionTargets>>;

export interface RetryPolicy {
  enabled?: boolean;
  max_attempts?: number;
  strategy?: RetryStrategy;
  initial_interval?: number;
  max_interval?: number;
  multiplier?: number;
}

/** Values of `ConcurrencySettings.mode`; empty means "queue". */
export type ConcurrencyMode = "queue" | "reject";

/**
 * The object form of `settings.concurrency`. The wire also accepts a bare int,
 * which is the shorthand for `{ limit: N, mode: "queue" }`. `max_queued` and
 * `queue_timeout` apply only in queue mode; `queue_timeout` is in nanoseconds.
 */
export interface ConcurrencySettings {
  /** Maximum concurrent executions; 0 or absent means unlimited. */
  limit?: number;
  /** Admission policy for submissions over the limit. Defaults to "queue". */
  mode?: ConcurrencyMode;
  /** Queue-mode backlog cap; 0 or absent means unbounded. */
  max_queued?: number;
  /** Queue-mode wait bound in nanoseconds; 0 or absent means unbounded. */
  queue_timeout?: number;
}

export interface WorkflowSettings {
  timeout?: string | number;
  /**
   * How many executions of this workflow may run at once. A bare int
   * (`concurrency: 10`) means queue mode, the shorthand the server preserves
   * across a round trip; the object form adds the admission mode and the
   * queue controls.
   */
  concurrency?: number | ConcurrencySettings;
  timezone?: string;
  on_error?: ErrorPolicy;
  pin_data_mode?: "test_only" | "always" | "disabled";
  retry?: RetryPolicy;
}

export interface WorkflowOptions {
  allow_cycles?: boolean;
  max_auto_depth?: number;
  experimental_node_group?: boolean;
  transient?: boolean;
  transient_ttl?: number;
  transient_completion_ttl?: number;
  /**
   * FAF is best-effort, non-persistent fire-and-forget: it creates no Redis/MySQL
   * execution, node, output, lease, outbox, retry, audit, or result state; it provides
   * no durable delivery, retry/recovery, cross-process dataflow, status inspection/wait/cancel,
   * or workflow result.
   */
  faf?: boolean;
}

export interface RunnerSelector {
  mode?: RunnerSelectorMode;
  match_labels?: Record<string, string>;
}

export interface WorkflowCredential {
  name?: string;
  type?: string;
}

export interface WorkflowParam {
  type?: "string" | "number" | "boolean" | "object" | "array" | string;
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

export interface NodeOutputPolicy {
  private?: boolean;
}

export interface GroupDef {
  name?: string;
  members?: string[];
  runner_selector?: RunnerSelector;
  on_error?: "stop" | "continue";
  retry?: RetryPolicy;
  timeout?: number;
  mode?: "transient";
  activation_replicas?: number;
}

export type WorkflowGroup = GroupDef;

export interface DependencyEdge {
  node: string;
  supply: string;
}

/**
 * A node on the wire: the shape `WireWorkflowDef.nodes` carries over
 * GET/POST/PUT /v1/workflows (ADR-D4 §2.1-§2.2). It has no `position`, `ui`,
 * or `notes` -- those are editor-only and live exclusively in
 * `WorkflowEditorMetadata` (see `./editorMetadata`), kept server-side on the
 * same record but never inline on a node.
 */
export interface WireWorkflowNode {
  id?: string;
  name?: string;
  type?: string;
  kind?: NodeKind;
  version?: number;
  template?: string;
  disabled?: boolean;
  on_error?: ErrorPolicy;
  runner_selector?: RunnerSelector;
  inputs?: PortDecl[];
  output_schema?: Record<string, unknown>;
  output?: NodeOutputPolicy;
  retry?: RetryPolicy;
  timeout?: number;
  activation_replicas?: number;
  parameters?: Record<string, unknown>;
}

/**
 * The wire shape of a workflow definition: no node carries `position`, `ui`,
 * or `notes` (ADR-D4 §2.1-§2.2). This is what `@xflow/api` sends and receives
 * as the `WorkflowDefWithEditorMetadata`'s `WorkflowDef` part; its sibling
 * `editor_metadata` is a separate top-level field, modeled by
 * `WorkflowEditorMetadata` (see `./editorMetadata`), never a field of this type.
 */
export interface WireWorkflowDef {
  spec?: string;
  id?: string;
  namespace?: string;
  name?: string;
  version?: string;
  description?: string;
  runner_selector?: RunnerSelector;
  context?: {
    vars?: Record<string, unknown>;
    config?: Record<string, unknown>;
  };
  settings?: WorkflowSettings;
  options?: WorkflowOptions;
  credentials?: Record<string, WorkflowCredential>;
  params?: Record<string, WorkflowParam>;
  node_templates?: Record<string, NodeTemplate>;
  nodes?: WireWorkflowNode[];
  groups?: GroupDef[];
  connections?: Connections;
  outputs?: Record<string, WorkflowOutput>;
  pin_data?: Record<string, unknown>;
  dependency_edges?: DependencyEdge[];
}

/**
 * A node in the editor's internal (merged) model: everything a
 * `WireWorkflowNode` has, plus the editor-only `position`, `ui`, and `notes`
 * fields merged in from `WorkflowEditorMetadata` (ADR-D4 §2.5). This is the
 * shape `@xflow/editor` and `@xflow/preview` operate on; it is unchanged from
 * before ADR-D4 so editor UI code needs no churn. `@xflow/api` is the only
 * place that converts between this and `WireWorkflowNode`
 * (`splitEditorMetadata` / `mergeEditorMetadata` in `./editorMetadata`).
 */
export interface WorkflowNode extends WireWorkflowNode {
  position?: Position;
  notes?: string;
  ui?: Record<string, unknown>;
}

/**
 * The editor's internal (merged) workflow model: a `WireWorkflowDef` whose
 * nodes may carry `position`/`ui`/`notes` inline. Unchanged from before
 * ADR-D4 (ADR §2.5) -- `@xflow/editor`, `@xflow/preview`, and `toGraphModel`
 * below all read and write this shape. Only `@xflow/api`'s `getWorkflow` /
 * `saveWorkflow` / `createWorkflow` convert at the network boundary via
 * `splitEditorMetadata` / `mergeEditorMetadata`.
 */
export interface WorkflowDef extends Omit<WireWorkflowDef, "nodes"> {
  nodes?: WorkflowNode[];
}

export type WorkflowStatus =
  | "pending"
  | "running"
  | "success"
  | "failed"
  | "canceled"
  | "timeout";

export type NodeStatus =
  | "pending"
  | "running"
  /**
   * A lease holder is committing this node's results: the attempt is over but
   * its write has not landed yet. Transitional; `@xflow/api`'s
   * `executionDetailToRuntimeSnapshot` still reports it as `running`.
   */
  | "committing"
  | "success"
  | "failed"
  | "skipped"
  | "pinned"
  | "continued"
  | "suspended"
  | "waiting"
  /**
   * The node's records are gone: a member of a `groups[].mode: transient`
   * group whose execution is terminal and whose artifacts passed the
   * completion TTL. A read-side verdict derived at inspect time, never stored
   * (`types.NodeStatusExpired` in the Go contracts).
   */
  | "expired"
  | "canceled";

export interface RuntimeNodeSnapshot {
  status: NodeStatus;
  attempts?: number;
  durationMs?: number;
  error?: string;
  /**
   * The output port that carried the node's result (the server's node `port`).
   * `@xflow/api`'s `executionDetailToRuntimeSnapshot` copies it here so the
   * Inspector's 运行 tab can name the port without its own execution model.
   */
  port?: string;
  /** Structured failure detail (the server's `error_details`), rendered flat. */
  errorDetails?: Record<string, unknown>;
  /** The node's public output map (the server's `output`). */
  output?: Record<string, unknown>;
  /**
   * The child execution a sub-workflow node waited on. Metadata, not node
   * content: it survives the node's commit, so a finished node stays drillable.
   */
  childExecutionId?: string;
}

export interface RuntimeSnapshot {
  status?: WorkflowStatus;
  /**
   * The external tracing system's correlation id for the run that produced this
   * snapshot (the server's `ExecutionDetail.trace_id`, carried here by
   * `@xflow/api`'s `executionDetailToRuntimeSnapshot`). Diagnostic metadata,
   * not business data: it is absent when the submission carried no trace
   * context, and absence means unknown rather than a value to reconstruct.
   */
  traceId?: string;
  nodes?: Record<string, RuntimeNodeSnapshot>;
}

export interface GraphNode {
  id: string;
  name: string;
  type: string;
  kind: NodeKind;
  label: string;
  position: Required<Position>;
  disabled: boolean;
  notes?: string;
  inputs: Required<PortDecl>[];
  /** Output ports the node declares on its own, before any connection is drawn. */
  outputs: string[];
}

export interface GraphEdge {
  id: string;
  source: string;
  sourceName: string;
  sourcePort: string;
  target: string;
  targetName: string;
  targetPort: string;
}

export interface GraphModel {
  workflowId?: string;
  name?: string;
  nodes: GraphNode[];
  edges: GraphEdge[];
}

/** The stable id a workflow node takes on the canvas. */
export function nodeKey(node: WorkflowNode): string {
  return node.id ?? node.name ?? "unnamed";
}

function nodeName(node: WorkflowNode): string {
  return node.name ?? node.id ?? "unnamed";
}

function nodeLabel(node: WorkflowNode, fallback: string): string {
  return typeof node.ui?.label === "string" && node.ui.label.trim() ? node.ui.label : fallback;
}

/** True when the definition pins at least one coordinate of the node. */
export function hasNodePosition(node: WorkflowNode): boolean {
  return node.position?.x !== undefined || node.position?.y !== undefined;
}

function normalizeInputs(inputs: PortDecl[] | undefined): Required<PortDecl>[] {
  return (inputs ?? []).map((input) => ({
    name: input.name ?? "main",
    required: input.required ?? false
  }));
}

function stringArray(value: unknown): string[] {
  return Array.isArray(value) ? value.filter((item): item is string => typeof item === "string" && item.trim().length > 0) : [];
}

/**
 * Output ports a node declares on its own, independent of what is connected.
 * `hasMain` is false for types that replace the single default output with a set
 * of named branches, so callers can tell "no main" apart from "no ports yet".
 * Ports named by the node's own `outputs` parameter come first, then the ones
 * its type implies.
 */
export function declaredOutputPorts(node: {
  type?: string;
  parameters?: Record<string, unknown>;
}): { ports: string[]; hasMain: boolean } {
  const type = node.type ?? "";
  const dynamicPorts = stringArray(node.parameters?.outputs);
  switch (type) {
    case "xflow.if":
      return { ports: [...dynamicPorts, "true", "false"], hasMain: false };
    case "xflow.approval":
      // "returned" is the port an approver's `return` action leaves by; the
      // list has to match the node's declared outputs or the editor refuses to
      // draw the edge the engine accepts.
      return { ports: [...dynamicPorts, "approved", "rejected", "returned", "timeout"], hasMain: false };
    case "xflow.wait":
      return { ports: [...dynamicPorts, "timeout", "error"], hasMain: true };
    case "xflow.http":
    case "xflow.grpc":
    case "xflow.database":
    case "xflow.function":
    case "xflow.script":
    case "xflow.notification":
      return { ports: [...dynamicPorts, "error"], hasMain: true };
    case "xflow.end":
      return { ports: [], hasMain: false };
    default:
      return { ports: dynamicPorts, hasMain: true };
  }
}

function dataConnectionTargets(value: ConnectionTargets | undefined): Connection[] {
  if (Array.isArray(value)) {
    return value;
  }
  if (!value || !Array.isArray(value.targets)) {
    return [];
  }
  return value.type === undefined || value.type === "data" ? value.targets : [];
}

function buildDepths(workflow: WorkflowDef, names: string[]): Map<string, number> {
  const known = new Set(names);
  const depths = new Map(names.map((name) => [name, 0]));
  const edges: Array<{ source: string; target: string }> = [];

  for (const [source, ports] of Object.entries(workflow.connections ?? {})) {
    if (!known.has(source)) continue;
    for (const portConnections of Object.values(ports)) {
      for (const target of dataConnectionTargets(portConnections)) {
        if (target.node && known.has(target.node)) {
          edges.push({ source, target: target.node });
        }
      }
    }
  }

  for (let pass = 0; pass < names.length; pass += 1) {
    let changed = false;
    for (const edge of edges) {
      const nextDepth = (depths.get(edge.source) ?? 0) + 1;
      if (nextDepth > (depths.get(edge.target) ?? 0)) {
        depths.set(edge.target, nextDepth);
        changed = true;
      }
    }
    if (!changed) break;
  }

  return depths;
}

/** A node card's box in canvas units, as React Flow measured it. */
export interface AutoLayoutBox {
  width: number;
  height: number;
}

export interface AutoLayoutOptions {
  /** Measured box of one node, by graph node id ({@link nodeKey}). */
  boxOf?: (nodeId: string) => AutoLayoutBox | undefined;
  /** Return positions only for nodes whose definition pins no coordinate. */
  onlyUnpositioned?: boolean;
}

/** Column and row gaps of the fallback layout, in canvas units. */
const autoLayoutGapX = 96;
const autoLayoutGapY = 32;

/**
 * The box the fallback layout assumes before React Flow has measured a card:
 * the card's 264px CSS width and the 159px full card measured on the 26-node
 * purchase-approval workflow, rounded up. It is a stand-in for one frame only —
 * callers that can measure (the preview) pass the real boxes through `boxOf`.
 */
const defaultAutoLayoutBox: AutoLayoutBox = {
  width: Number.parseInt(foundationTokens.nodeWidth, 10),
  height: 160
};

/**
 * Columns from the longest data path, rows sorted by name; x accumulates the
 * widest box per column and y the height of each box, so cards never share a
 * slot whatever their content makes them. Nodes that pin a coordinate are
 * still placed for spacing, and returned only when `onlyUnpositioned` is off.
 */
export function autoLayoutPositions(
  workflow: WorkflowDef,
  options: AutoLayoutOptions = {}
): Map<string, Required<Position>> {
  return buildAutoPositions(workflow, options);
}

function buildAutoPositions(
  workflow: WorkflowDef,
  options: AutoLayoutOptions = {}
): Map<string, Required<Position>> {
  const sourceNodes = workflow.nodes ?? [];
  const entries = sourceNodes.map((node) => ({
    node,
    name: nodeName(node),
    key: nodeKey(node),
    box: options.boxOf?.(nodeKey(node)) ?? defaultAutoLayoutBox
  }));
  const depths = buildDepths(workflow, entries.map((entry) => entry.name));
  const byDepth = new Map<number, typeof entries>();

  for (const entry of entries) {
    const depth = depths.get(entry.name) ?? 0;
    const bucket = byDepth.get(depth) ?? [];
    bucket.push(entry);
    byDepth.set(depth, bucket);
  }

  for (const bucket of byDepth.values()) {
    bucket.sort((a, b) => a.name.localeCompare(b.name));
  }

  const positions = new Map<string, Required<Position>>();
  let columnX = 0;
  for (const depth of [...byDepth.keys()].sort((a, b) => a - b)) {
    const bucket = byDepth.get(depth) ?? [];
    const columnWidth = Math.max(...bucket.map((entry) => entry.box.width));
    let cursorY = 0;
    for (const entry of bucket) {
      if (!options.onlyUnpositioned || !hasNodePosition(entry.node)) {
        positions.set(entry.key, { x: columnX, y: cursorY });
      }
      // A positioned node still takes its row: the unpositioned neighbours must
      // keep the slots the definition has not pinned.
      cursorY += entry.box.height + autoLayoutGapY;
    }
    columnX += columnWidth + autoLayoutGapX;
  }

  return positions;
}

function nodePosition(
  node: WorkflowNode,
  fallback: Required<Position>
): Required<Position> {
  if (hasNodePosition(node)) {
    return {
      x: node.position?.x ?? fallback.x,
      y: node.position?.y ?? fallback.y
    };
  }
  return fallback;
}

export function toGraphModel(workflow: WorkflowDef): GraphModel {
  const autoPositions = autoLayoutPositions(workflow);
  const nodes = (workflow.nodes ?? []).map<GraphNode>((node) => {
    const name = nodeName(node);
    const declared = declaredOutputPorts(node);
    // §6.2: the red error outlet belongs to nodes whose effective policy routes
    // failures there. An edge already leaving `error` keeps the port visible (the
    // preview merges edge ports back in); declaredOutputPorts stays untouched
    // because connection validation still accepts the port.
    const errorOutput = (node.on_error ?? workflow.settings?.on_error) === "error_output";
    return {
      id: nodeKey(node),
      name,
      type: node.type ?? "xflow.unknown",
      kind: node.kind ?? "action",
      label: nodeLabel(node, name),
      position: nodePosition(node, autoPositions.get(nodeKey(node)) ?? { x: 0, y: 0 }),
      disabled: node.disabled ?? false,
      notes: node.notes,
      inputs: normalizeInputs(node.inputs),
      outputs: [
        ...(declared.hasMain ? ["main"] : []),
        ...declared.ports.filter((port) => port !== "error" || errorOutput)
      ]
    };
  });

  const byName = new Map(nodes.map((node) => [node.name, node]));
  const edges: GraphEdge[] = [];
  // The engine compiles every repeated identical target as its own edge (each
  // one adds to the target's in-degree), so duplicates are kept rather than
  // deduped; later occurrences get a "#n" suffix so ids stay unique.
  const idOccurrences = new Map<string, number>();

  for (const [sourceName, ports] of Object.entries(workflow.connections ?? {})) {
    const source = byName.get(sourceName);
    for (const [sourcePort, portConnections] of Object.entries(ports)) {
      for (const targetRef of dataConnectionTargets(portConnections)) {
        const targetName = targetRef.node ?? "unknown";
        const target = byName.get(targetName);
        const targetPort = targetRef.input ?? "main";
        const baseId = `${sourceName}:${sourcePort}->${targetName}:${targetPort}`;
        const occurrence = (idOccurrences.get(baseId) ?? 0) + 1;
        idOccurrences.set(baseId, occurrence);
        edges.push({
          id: occurrence === 1 ? baseId : `${baseId}#${occurrence}`,
          source: source?.id ?? sourceName,
          sourceName,
          sourcePort,
          target: target?.id ?? targetName,
          targetName,
          targetPort
        });
      }
    }
  }

  return {
    workflowId: workflow.id,
    name: workflow.name,
    nodes,
    edges
  };
}

export * from "./nodeForm";
export * from "./nodeSummary";
export * from "./editorMetadata";
export * from "./nodeHelpers";
export * from "./tokens";
export * from "./antdTheme";
export * from "./statusMeta";
export * from "./categoryMeta";
export * from "./breakpoints";
export * from "./appearance";
export * from "./capabilities";
export * from "./approval";
