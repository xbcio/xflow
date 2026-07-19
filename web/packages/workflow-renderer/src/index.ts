export { WORKFLOW_CORE_VERSION } from "@xflow/workflow-core";

export const WORKFLOW_RENDERER_VERSION = "0.1.0";

export { WorkflowCanvas } from "./WorkflowCanvas";
export { workflowToFlow, flowToWorkflow, resolveNodeType } from "./transform";
export { GenericNode } from "./nodes/GenericNode";
export { UnknownNode } from "./nodes/UnknownNode";
export { UnknownPort } from "./nodes/UnknownPort";
export {
  selectionOverlay,
  executionOverlay,
  diagnosticOverlay,
} from "./overlays";

export type {
  ExecutionSnapshot,
  NodeData,
  FlowViewModel,
  ResolvedNodeType,
  WorkflowCanvasProps,
  WorkflowCanvasOverlayProps,
} from "./types";
export type { GenericNodeProps } from "./nodes/GenericNode";
export type { UnknownNodeProps } from "./nodes/UnknownNode";
export type { UnknownPortProps } from "./nodes/UnknownPort";

import "./styles.css";
