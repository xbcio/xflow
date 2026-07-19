import type { NodeDef, Position, WorkflowDef, Diagnostic } from "@xflow/workflow-core";

/**
 * Placeholder execution snapshot type.
 * M2/M3 will replace this with the canonical ExecutionSnapshot from workflow-provider.
 */
export interface ExecutionSnapshot {
  nodeStatuses?: Record<string, "running" | "completed" | "failed" | "suspended">;
}

export interface NodeData extends Record<string, unknown> {
  nodeDef: NodeDef;
  status?: "running" | "completed" | "failed" | "suspended";
  selected?: boolean;
  diagnostics?: Diagnostic[];
}

export interface DanglingTarget {
  source: string;
  port: string;
  target: string;
  input?: string;
}

export interface FlowViewModel {
  nodes: Array<{
    id: string;
    position: { x: number; y: number };
    data: NodeData;
    type: string;
  }>;
  edges: Array<{
    id: string;
    source: string;
    target: string;
    sourceHandle?: string;
    targetHandle?: string;
  }>;
  /** Edges that reference a target node not present in the workflow definition. */
  danglingTargets: DanglingTarget[];
}

export type ResolvedNodeType =
  | "start"
  | "end"
  | "http"
  | "grpc"
  | "function"
  | "database"
  | "if"
  | "switch"
  | "merge"
  | "wait"
  | "approval"
  | "generic"
  | "unknown";

export interface WorkflowCanvasProps {
  definition: WorkflowDef;
  executionSnapshot?: ExecutionSnapshot;
  diagnostics?: Diagnostic[];
  readOnly?: boolean;
  className?: string;
  nodeTypes?: Record<string, React.ComponentType<unknown>>;
  overlays?: React.ComponentType<WorkflowCanvasOverlayProps>[];
  /** Allow selecting nodes even when readOnly is true. Defaults to true. */
  selectable?: boolean;
  /** Controlled selection. If omitted, selection is managed internally. */
  selectedNodeIds?: string[];
  /** Called when the selection changes (node clicks or controlled updates). */
  onSelectionChange?: (nodeIds: string[]) => void;
}

export interface WorkflowCanvasOverlayProps {
  definition: WorkflowDef;
  executionSnapshot?: ExecutionSnapshot;
  diagnostics?: Diagnostic[];
  selectedNodeIds?: string[];
}

export { NodeDef, Position, WorkflowDef, Diagnostic };
