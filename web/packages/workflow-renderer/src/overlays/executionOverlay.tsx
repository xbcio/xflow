import * as React from "react";
import type { WorkflowCanvasOverlayProps } from "../types";

/**
 * Execution overlay: shows a compact summary of the execution snapshot.
 * Node-level status colors are applied by WorkflowCanvas via node data;
 * this overlay surfaces the aggregate counts.
 */
export const executionOverlay: React.FC<WorkflowCanvasOverlayProps> = ({
  executionSnapshot,
}) => {
  const statuses = executionSnapshot?.nodeStatuses ?? {};
  const counts = {
    running: 0,
    completed: 0,
    failed: 0,
    suspended: 0,
  };
  for (const status of Object.values(statuses)) {
    if (status && status in counts) {
      counts[status as keyof typeof counts]++;
    }
  }

  const total = Object.values(counts).reduce((a, b) => a + b, 0);

  return (
    <div
      className="xf-overlay xf-overlay--execution"
      data-testid="execution-overlay"
      aria-live="polite"
    >
      <span className="xf-overlay__label">Execution</span>
      <span className="xf-overlay__value" data-testid="execution-total">
        {total}
      </span>
      {total > 0 && (
        <span className="xf-overlay__detail" data-testid="execution-breakdown">
          {counts.running > 0 && `R:${counts.running} `}
          {counts.completed > 0 && `C:${counts.completed} `}
          {counts.failed > 0 && `F:${counts.failed} `}
          {counts.suspended > 0 && `S:${counts.suspended} `}
        </span>
      )}
    </div>
  );
};

executionOverlay.displayName = "executionOverlay";
