import * as React from "react";
import { Handle, Position as RfPosition } from "@xyflow/react";
import type { NodeData } from "../types";

export interface UnknownNodeProps {
  id: string;
  data: NodeData;
  selected?: boolean;
}

export const UnknownNode = React.memo(function UnknownNode({
  id,
  data,
  selected,
}: UnknownNodeProps) {
  const nodeDef = data.nodeDef;
  const type = nodeDef.type ?? "unknown";
  const name = nodeDef.name ?? id;

  return (
    <div
      className={[
        "xf-node",
        "xf-node--unknown",
        selected ? "xf-node--selected" : "",
      ]
        .filter(Boolean)
        .join(" ")}
      data-testid={`node-${id}`}
      data-node-type={type}
      data-node-kind="unknown"
      role="status"
      aria-label={`Unknown node type: ${type}`}
    >
      <Handle type="target" position={RfPosition.Left} className="xf-port xf-port--target" />
      <div className="xf-node__header">
        <span className="xf-node__icon" aria-hidden="true">!</span>
        <span className="xf-node__name" title={name}>{name}</span>
      </div>
      <div className="xf-node__body">
        <span className="xf-node__type">{type}</span>
        <span className="xf-node__hint">unknown node</span>
      </div>
      <Handle type="source" position={RfPosition.Right} className="xf-port xf-port--source" />
    </div>
  );
});
