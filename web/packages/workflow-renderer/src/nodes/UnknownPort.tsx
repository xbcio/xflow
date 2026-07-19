import * as React from "react";

export interface UnknownPortProps {
  /** Target node id referenced by a dangling edge. */
  nodeId?: string;
  /** Port name referenced by a dangling edge. */
  portId?: string;
}

/**
 * Safe fallback for unknown ports / dangling edges.
 * Does not crash and shows a tiny warning marker.
 */
export const UnknownPort = React.memo(function UnknownPort({
  nodeId,
  portId,
}: UnknownPortProps) {
  return (
    <span
      className="xf-port xf-port--unknown"
      data-testid={portId ? `port-${portId}` : "port-unknown"}
      data-dangling-node={nodeId ?? ""}
      title={`Unknown port: ${portId ?? "?"} on node ${nodeId ?? "?"}`}
    >
      !
    </span>
  );
});
