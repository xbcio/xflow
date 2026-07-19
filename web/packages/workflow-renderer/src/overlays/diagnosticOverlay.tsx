import * as React from "react";
import { workflowToFlow } from "../transform";
import type { WorkflowCanvasOverlayProps } from "../types";
import { UnknownPort } from "../nodes/UnknownPort";

/**
 * Diagnostic overlay: renders an aggregate badge for diagnostics and a list
 * of dangling targets using UnknownPort. Per-node markers are rendered by
 * GenericNode/UnknownNode; this overlay provides the canvas-level summary.
 */
export const diagnosticOverlay: React.FC<WorkflowCanvasOverlayProps> = ({
  definition,
  diagnostics,
}) => {
  const items = diagnostics ?? [];
  const errors = items.filter((d) => d.severity === "error").length;
  const warnings = items.filter((d) => d.severity === "warning").length;
  const infos = items.filter((d) => d.severity === "info").length;

  const { danglingTargets } = workflowToFlow(definition);

  return (
    <div
      className="xf-overlay xf-overlay--diagnostic"
      data-testid="diagnostic-overlay"
      aria-live="polite"
    >
      <span className="xf-overlay__label">Diagnostics</span>
      <span className="xf-overlay__value" data-testid="diagnostic-total">
        {items.length}
      </span>
      {(errors > 0 || warnings > 0 || infos > 0) && (
        <span className="xf-overlay__detail" data-testid="diagnostic-breakdown">
          {errors > 0 && `E:${errors} `}
          {warnings > 0 && `W:${warnings} `}
          {infos > 0 && `I:${infos} `}
        </span>
      )}

      {danglingTargets.length > 0 && (
        <ul
          className="xf-overlay__list"
          data-testid="dangling-targets"
          aria-label="Dangling targets"
        >
          {danglingTargets.map((target) => (
            <li key={`${target.source}-${target.port}-${target.target}`}>
              <UnknownPort nodeId={target.target} portId={target.input ?? target.port} />
              <span className="xf-overlay__list-text">
                {target.source} → {target.target}
                {target.input ? `:${target.input}` : ""}
              </span>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
};

diagnosticOverlay.displayName = "diagnosticOverlay";
