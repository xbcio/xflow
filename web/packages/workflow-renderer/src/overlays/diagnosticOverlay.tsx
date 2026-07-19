import * as React from "react";
import type { WorkflowCanvasOverlayProps } from "../types";

/**
 * Diagnostic overlay placeholder.
 * Diagnostic markers are wired into node data by WorkflowCanvas.
 * Full diagnostic badges will be added when workflow-core diagnostics are surfaced in the UI.
 */
export const diagnosticOverlay: React.FC<WorkflowCanvasOverlayProps> = () => {
  return null;
};

diagnosticOverlay.displayName = "diagnosticOverlay";
