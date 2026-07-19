import * as React from "react";
import type { WorkflowCanvasOverlayProps } from "../types";

/**
 * Selection overlay: displays a compact badge with the number of selected
 * nodes and their ids. The actual selection highlight is wired into each
 * node's data by WorkflowCanvas; this overlay provides a readable summary.
 */
export const selectionOverlay: React.FC<WorkflowCanvasOverlayProps> = ({
  selectedNodeIds,
}) => {
  const count = selectedNodeIds?.length ?? 0;
  return (
    <div
      className="xf-overlay xf-overlay--selection"
      data-testid="selection-overlay"
      aria-live="polite"
    >
      <span className="xf-overlay__label">Selected</span>
      <span className="xf-overlay__value" data-testid="selection-count">
        {count}
      </span>
      {count > 0 && (
        <span className="xf-overlay__detail" data-testid="selection-names">
          {selectedNodeIds!.join(", ")}
        </span>
      )}
    </div>
  );
};

selectionOverlay.displayName = "selectionOverlay";
