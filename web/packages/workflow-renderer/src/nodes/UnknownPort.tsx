import * as React from "react";
import { Handle, Position as RfPosition } from "@xyflow/react";

export interface UnknownPortProps {
  /** Target node id referenced by a dangling edge. */
  nodeId?: string;
  /** Port name referenced by a dangling edge. */
  portId?: string;
}

/**
 * Safe fallback for unknown ports / dangling edges.
 *
 * Renders a real React Flow `<Handle>` (with `isConnectable={false}` so users
 * cannot drag new edges onto it) instead of a plain span, so it participates
 * in edge routing for diagnostic display and is reachable from screen
 * readers via the title attribute. The visual marker is provided by the
 * `xf-port--unknown` CSS class.
 */
export const UnknownPort = React.memo(function UnknownPort({
  nodeId,
  portId,
}: UnknownPortProps) {
  const handleId = portId ?? "unknown";
  return (
    <Handle
      type="target"
      position={RfPosition.Left}
      id={handleId}
      isConnectable={false}
      className="xf-port xf-port--unknown"
      data-testid={portId ? `port-${portId}` : "port-unknown"}
      data-dangling-node={nodeId ?? ""}
      title={`Unknown port: ${portId ?? "?"} on node ${nodeId ?? "?"}`}
    />
  );
});
