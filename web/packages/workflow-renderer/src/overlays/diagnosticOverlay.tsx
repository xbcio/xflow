import * as React from "react";
import { workflowToFlow } from "../transform";
import type { WorkflowCanvasOverlayProps } from "../types";

/**
 * Diagnostic overlay: renders an aggregate badge for diagnostics and a list
 * of dangling targets. Per-node markers are rendered by
 * GenericNode/UnknownNode (which render real React Flow `<Handle>` elements
 * via UnknownPort); this overlay provides the canvas-level summary and uses
 * plain `<span>` markers — it lives outside the React Flow node tree, so it
 * cannot host `<Handle>` instances.
 */
export const diagnosticOverlay: React.FC<WorkflowCanvasOverlayProps> = ({
  definition,
  diagnostics,
}) => {
  const items = diagnostics ?? [];
  const errors = items.filter((d) => d.severity === "error").length;
  const warnings = items.filter((d) => d.severity === "warning").length;
  const infos = items.filter((d) => d.severity === "info").length;

  const { danglingTargets, missingSources } = workflowToFlow(definition);
  const missingList = missingSources ?? [];

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
            <li key={`dangling-${target.source}-${target.port}-${target.target}`}>
              <span
                className="xf-port xf-port--unknown"
                data-testid={target.input ? `port-${target.input}` : `port-${target.port}`}
                aria-hidden="true"
              >
                !
              </span>
              <span className="xf-overlay__list-text">
                {target.source} → {target.target}
                {target.input ? `:${target.input}` : ""}
              </span>
            </li>
          ))}
        </ul>
      )}

      {missingList.length > 0 && (
        <ul
          className="xf-overlay__list"
          data-testid="missing-sources"
          aria-label="Missing sources"
        >
          {missingList.map((src) => (
            <li key={`missing-${src.source}-${src.port}-${src.target}`}>
              <span
                className="xf-port xf-port--unknown"
                data-testid={`missing-port-${src.source}`}
                aria-hidden="true"
              >
                ?
              </span>
              <span className="xf-overlay__list-text">
                {src.source} → {src.target}
                {src.input ? `:${src.input}` : ""}
              </span>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
};

diagnosticOverlay.displayName = "diagnosticOverlay";
