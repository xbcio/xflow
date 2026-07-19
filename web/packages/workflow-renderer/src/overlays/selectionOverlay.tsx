import * as React from "react";
import type { WorkflowCanvasOverlayProps } from "../types";

/**
 * Selection overlay: highlights selected nodes.
 * In read-only mode the selection is still visible but cannot be changed by the user.
 */
export const selectionOverlay: React.FC<WorkflowCanvasOverlayProps> = () => {
  // This overlay is a declarative marker; WorkflowCanvas wires selectedNodeIds into node data.
  return null;
};

selectionOverlay.displayName = "selectionOverlay";
