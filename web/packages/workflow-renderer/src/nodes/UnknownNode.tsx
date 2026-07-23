import * as React from "react";
import { Handle, Position as RfPosition } from "@xyflow/react";
import type { NodeData } from "../types";
import { SourceHandles } from "./SourceHandles";
import { UnknownPort } from "./UnknownPort";
import { collectUnknownPorts, targetHandleIds } from "./nodeCommon";

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
  const status = data.status;
  const diagnostics = data.diagnostics ?? [];
  const hasError = diagnostics.some((d) => d.severity === "error");
  const hasWarning = diagnostics.some((d) => d.severity === "warning");
  const unknownPorts = collectUnknownPorts(id, diagnostics);
  const targetIds = targetHandleIds(data);
  const sourcePorts = data.sourcePorts;
  const hasDefaultSourcePort = data.hasDefaultSourcePort ?? false;

  return (
    <div
      className={[
        "xf-node",
        "xf-node--unknown",
        selected ? "xf-node--selected" : "",
        hasError ? "xf-node--error" : "",
        hasWarning ? "xf-node--warning" : "",
      ]
        .filter(Boolean)
        .join(" ")}
      data-testid={`node-${id}`}
      data-node-type={type}
      data-node-kind="unknown"
      data-node-status={status ?? ""}
      role="status"
      aria-label={`Unknown node type: ${type}`}
    >
      {targetIds ? (
        targetIds.map((handleId) => (
          <Handle
            key={handleId}
            type="target"
            position={RfPosition.Left}
            id={handleId}
            className="xf-port xf-port--target"
            data-testid={`target-handle-${id}-${handleId}`}
          />
        ))
      ) : (
        <Handle
          type="target"
          position={RfPosition.Left}
          className="xf-port xf-port--target"
          data-testid={`target-handle-${id}-default`}
        />
      )}
      <div className="xf-node__header">
        <span className="xf-node__icon" aria-hidden="true">!</span>
        <span className="xf-node__name" title={name}>{name}</span>
        {diagnostics.length > 0 && (
          <span
            className="xf-node__diagnostic-badge"
            data-testid={`node-diagnostics-${id}`}
            title={diagnostics.map((d) => `${d.severity}: ${d.message}`).join("\n")}
          >
            {diagnostics.length}
          </span>
        )}
      </div>
      <div className="xf-node__body">
        <span className="xf-node__type">{type}</span>
        {unknownPorts.length > 0 && (
          <div className="xf-node__unknown-ports" data-testid={`node-unknown-ports-${id}`}>
            {unknownPorts.map((port) => (
              <UnknownPort key={port} nodeId={id} portId={port} />
            ))}
          </div>
        )}
        {status && (
          <span className="xf-node__status" data-testid={`node-status-${id}`}>
            {status}
          </span>
        )}
        <span className="xf-node__hint">unknown node</span>
      </div>
      <SourceHandles
        nodeId={id}
        sourcePorts={sourcePorts}
        hasDefaultSourcePort={hasDefaultSourcePort}
      />
    </div>
  );
});
