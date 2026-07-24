import * as React from "react";
import { Handle, Position as RfPosition } from "@xyflow/react";
import type { Diagnostic, NodeData } from "../types";
import { UnknownPort } from "./UnknownPort";

export interface UnknownNodeProps {
  id: string;
  data: NodeData;
  selected?: boolean;
}

function collectUnknownPorts(id: string, diagnostics?: Diagnostic[]): string[] {
  if (!diagnostics) return [];
  const ports = new Set<string>();
  for (const d of diagnostics) {
    if (d.connectionRef && d.connectionRef.node === id && d.connectionRef.input) {
      ports.add(d.connectionRef.input);
    }
  }
  return Array.from(ports);
}

function targetHandleIds(data: NodeData): string[] | undefined {
  const inputs = data.nodeDef.inputs;
  if (!inputs || inputs.length === 0) return undefined;
  const named = inputs
    .map((input) => input.name)
    .filter((name): name is string => typeof name === "string" && name.length > 0);
  return named.length > 0 ? named : undefined;
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
          />
        ))
      ) : (
        <Handle
          type="target"
          position={RfPosition.Left}
          className="xf-port xf-port--target"
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
      {/* Single default source Handle (no id); see GenericNode for rationale. */}
      <Handle type="source" position={RfPosition.Right} className="xf-port xf-port--source" />
    </div>
  );
});
