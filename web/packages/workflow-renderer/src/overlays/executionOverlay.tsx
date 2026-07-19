import * as React from "react";
import type { WorkflowCanvasOverlayProps } from "../types";

/**
 * Execution overlay: execution snapshot status is wired into node data by WorkflowCanvas.
 * This component is a no-op marker kept for the compositional overlays array.
 */
export const executionOverlay: React.FC<WorkflowCanvasOverlayProps> = () => {
  return null;
};

executionOverlay.displayName = "executionOverlay";
