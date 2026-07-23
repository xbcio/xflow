import * as React from "react";
import { Handle, Position as RfPosition } from "@xyflow/react";

export interface SourceHandlesProps {
  nodeId: string;
  sourcePorts?: string[];
  hasDefaultSourcePort?: boolean;
}

/**
 * Render the source Handles for a workflow node.
 *
 * - Named source ports each get their own Handle.
 * - When `hasDefaultSourcePort` is true, an additional default Handle is
 *   rendered alongside the named ports.
 * - When no named ports are provided, a single default Handle is rendered.
 *
 * Every Handle carries a stable `data-testid` derived from the node id so
 * tests can avoid relying on React Flow internals such as `data-handleid`.
 */
export const SourceHandles = React.memo(function SourceHandles({
  nodeId,
  sourcePorts,
  hasDefaultSourcePort,
}: SourceHandlesProps) {
  if (!sourcePorts || sourcePorts.length === 0) {
    return (
      <Handle
        type="source"
        position={RfPosition.Right}
        className="xf-port xf-port--source"
        data-testid={`source-handle-${nodeId}-default`}
      />
    );
  }

  return (
    <>
      {sourcePorts.map((handleId) => (
        <Handle
          key={handleId}
          type="source"
          position={RfPosition.Right}
          id={handleId}
          className="xf-port xf-port--source"
          data-testid={`source-handle-${nodeId}-${handleId}`}
        />
      ))}
      {hasDefaultSourcePort && (
        <Handle
          type="source"
          position={RfPosition.Right}
          className="xf-port xf-port--source"
          data-testid={`source-handle-${nodeId}-default`}
        />
      )}
    </>
  );
});
