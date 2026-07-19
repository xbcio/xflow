import * as React from "react";
import { Handle, Position as RfPosition } from "@xyflow/react";
import type { NodeData } from "../types";

export interface GenericNodeProps {
  id: string;
  data: NodeData;
  selected?: boolean;
}

function statusClass(status?: NodeData["status"]): string {
  switch (status) {
    case "running":
      return "xf-node--running";
    case "completed":
      return "xf-node--completed";
    case "failed":
      return "xf-node--failed";
    case "suspended":
      return "xf-node--suspended";
    default:
      return "";
  }
}

export const GenericNode = React.memo(function GenericNode({
  id,
  data,
  selected,
}: GenericNodeProps) {
  const nodeDef = data.nodeDef;
  const type = nodeDef.type ?? "unknown";
  const name = nodeDef.name ?? id;
  const status = data.status;
  const hasError = (data.diagnostics ?? []).some((d) => d.severity === "error");

  return (
    <div
      className={[
        "xf-node",
        "xf-node--generic",
        statusClass(status),
        selected ? "xf-node--selected" : "",
        hasError ? "xf-node--error" : "",
      ]
        .filter(Boolean)
        .join(" ")}
      data-testid={`node-${id}`}
      data-node-type={type}
      data-node-status={status ?? ""}
    >
      <Handle type="target" position={RfPosition.Left} className="xf-port xf-port--target" />
      <div className="xf-node__header">
        <span className="xf-node__name" title={name}>{name}</span>
      </div>
      <div className="xf-node__body">
        <span className="xf-node__type">{type}</span>
        {status && (
          <span className="xf-node__status" data-testid={`node-status-${id}`}>
            {status}
          </span>
        )}
      </div>
      <Handle type="source" position={RfPosition.Right} className="xf-port xf-port--source" />
    </div>
  );
});
