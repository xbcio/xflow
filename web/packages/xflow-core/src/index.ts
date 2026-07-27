export type NodeKind = "action" | "trigger";
export type ErrorPolicy = "stop" | "error_output" | "main_output" | "continue";
export type RetryStrategy = "fixed" | "exponential";
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

export type Connections = Record<string, Record<string, Connection[]>>;

export interface RetryPolicy {
  enabled?: boolean;
  max_attempts?: number;
  strategy?: RetryStrategy;
  initial_interval?: number;
  max_interval?: number;
  multiplier?: number;
}

export interface WorkflowSettings {
  timeout?: string | number;
  concurrency?: number;
  timezone?: string;
  on_error?: ErrorPolicy;
  pin_data_mode?: "test_only" | "always" | "disabled";
  retry?: RetryPolicy;
}

export interface WorkflowOptions {
  allow_cycles?: boolean;
  max_auto_depth?: number;
  experimental_expand?: boolean;
}

export interface RunnerSelector {
  mode?: RunnerSelectorMode;
  matchLabels?: Record<string, string>;
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

export interface WorkflowNode {
  id?: string;
  name?: string;
  type?: string;
  kind?: NodeKind;
  version?: number;
  template?: string;
  position?: Position;
  disabled?: boolean;
  on_error?: ErrorPolicy;
  runnerSelector?: RunnerSelector;
  notes?: string;
  inputs?: PortDecl[];
  output_schema?: Record<string, unknown>;
  retry?: RetryPolicy;
  parameters?: Record<string, unknown>;
  ui?: Record<string, unknown>;
}

export interface WorkflowDef {
  spec?: string;
  id?: string;
  namespace?: string;
  name?: string;
  version?: string;
  description?: string;
  runnerSelector?: RunnerSelector;
  context?: {
    vars?: Record<string, unknown>;
    config?: Record<string, unknown>;
  };
  settings?: WorkflowSettings;
  options?: WorkflowOptions;
  credentials?: Record<string, WorkflowCredential>;
  params?: Record<string, WorkflowParam>;
  node_templates?: Record<string, NodeTemplate>;
  nodes?: WorkflowNode[];
  connections?: Connections;
  outputs?: Record<string, WorkflowOutput>;
  pin_data?: Record<string, unknown>;
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
  | "success"
  | "failed"
  | "skipped"
  | "pinned"
  | "continued"
  | "suspended"
  | "waiting"
  | "canceled";

export interface RuntimeNodeSnapshot {
  status: NodeStatus;
  attempts?: number;
  durationMs?: number;
  error?: string;
}

export interface RuntimeSnapshot {
  status?: WorkflowStatus;
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

function nodeKey(node: WorkflowNode): string {
  return node.id ?? node.name ?? "unnamed";
}

function nodeName(node: WorkflowNode): string {
  return node.name ?? node.id ?? "unnamed";
}

function nodeLabel(node: WorkflowNode, fallback: string): string {
  return typeof node.ui?.label === "string" && node.ui.label.trim() ? node.ui.label : fallback;
}

function hasPosition(node: WorkflowNode): boolean {
  return node.position?.x !== undefined || node.position?.y !== undefined;
}

function normalizeInputs(inputs: PortDecl[] | undefined): Required<PortDecl>[] {
  return (inputs ?? []).map((input) => ({
    name: input.name ?? "main",
    required: input.required ?? false
  }));
}

function buildDepths(workflow: WorkflowDef, names: string[]): Map<string, number> {
  const known = new Set(names);
  const depths = new Map(names.map((name) => [name, 0]));
  const edges: Array<{ source: string; target: string }> = [];

  for (const [source, ports] of Object.entries(workflow.connections ?? {})) {
    if (!known.has(source)) continue;
    for (const targets of Object.values(ports)) {
      for (const target of targets) {
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

function buildAutoPositions(workflow: WorkflowDef): Map<string, Required<Position>> {
  const sourceNodes = workflow.nodes ?? [];
  const names = sourceNodes.map(nodeName);
  const depths = buildDepths(workflow, names);
  const byDepth = new Map<number, string[]>();

  for (const name of names) {
    const depth = depths.get(name) ?? 0;
    const bucket = byDepth.get(depth) ?? [];
    bucket.push(name);
    byDepth.set(depth, bucket);
  }

  for (const bucket of byDepth.values()) {
    bucket.sort((a, b) => a.localeCompare(b));
  }

  const positions = new Map<string, Required<Position>>();
  for (const [depth, bucket] of byDepth) {
    bucket.forEach((name, row) => {
      positions.set(name, { x: depth * 280, y: row * 120 });
    });
  }

  return positions;
}

function nodePosition(
  node: WorkflowNode,
  fallback: Required<Position>
): Required<Position> {
  if (hasPosition(node)) {
    return {
      x: node.position?.x ?? fallback.x,
      y: node.position?.y ?? fallback.y
    };
  }
  return fallback;
}

export function toGraphModel(workflow: WorkflowDef): GraphModel {
  const autoPositions = buildAutoPositions(workflow);
  const nodes = (workflow.nodes ?? []).map<GraphNode>((node) => {
    const name = nodeName(node);
    return {
      id: nodeKey(node),
      name,
      type: node.type ?? "xflow.unknown",
      kind: node.kind ?? "action",
      label: nodeLabel(node, name),
      position: nodePosition(node, autoPositions.get(name) ?? { x: 0, y: 0 }),
      disabled: node.disabled ?? false,
      notes: node.notes,
      inputs: normalizeInputs(node.inputs)
    };
  });

  const byName = new Map(nodes.map((node) => [node.name, node]));
  const edges: GraphEdge[] = [];

  for (const [sourceName, ports] of Object.entries(workflow.connections ?? {})) {
    const source = byName.get(sourceName);
    for (const [sourcePort, targets] of Object.entries(ports)) {
      for (const targetRef of targets) {
        const targetName = targetRef.node ?? "unknown";
        const target = byName.get(targetName);
        const targetPort = targetRef.input ?? "main";
        edges.push({
          id: `${sourceName}:${sourcePort}->${targetName}:${targetPort}`,
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
