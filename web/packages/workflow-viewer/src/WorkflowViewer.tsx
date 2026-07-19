import * as React from "react";
import { ReactFlowProvider, useReactFlow } from "@xyflow/react";
import type { WorkflowDef, Diagnostic } from "@xflow/workflow-core";
import { WorkflowCanvas } from "@xflow/workflow-renderer";
import type { ExecutionSnapshot } from "@xflow/workflow-renderer";
import { workflowToFlow } from "@xflow/workflow-renderer";

export interface WorkflowViewerProps {
  definition: WorkflowDef;
  executionSnapshot?: ExecutionSnapshot;
  diagnostics?: Diagnostic[];
  className?: string;
}

interface FlowControllerRef {
  focusNode: (id: string) => void;
}

function FlowViewportController(_props: unknown, ref: React.Ref<FlowControllerRef>) {
  const rf = useReactFlow();

  React.useImperativeHandle(
    ref,
    () => ({
      focusNode: (id: string) => {
        if (!rf) return;
        const node = rf.getNode(id);
        if (!node) return;
        rf.fitView({ nodes: [{ id }], duration: 300, padding: 0.2 });
      },
    }),
    [rf]
  );

  return null;
}

const ForwardedFlowViewportController = React.forwardRef(FlowViewportController);

export function WorkflowViewer({
  definition,
  executionSnapshot,
  diagnostics,
  className,
}: WorkflowViewerProps) {
  const [keyword, setKeyword] = React.useState("");
  const [selectedNodeIds, setSelectedNodeIds] = React.useState<string[]>([]);
  const controllerRef = React.useRef<FlowControllerRef>(null);

  const flowModel = React.useMemo(() => workflowToFlow(definition), [definition]);
  const nodeById = React.useMemo(() => {
    const map = new Map<string, (typeof flowModel.nodes)[number]>();
    for (const node of flowModel.nodes) {
      map.set(node.id, node);
    }
    return map;
  }, [flowModel]);

  const filteredNodes = React.useMemo(() => {
    const normalized = keyword.trim().toLowerCase();
    if (!normalized) return flowModel.nodes;
    return flowModel.nodes.filter((node) => {
      const name = node.data.nodeDef.name ?? node.id;
      return (
        name.toLowerCase().includes(normalized) ||
        (node.data.nodeDef.type?.toLowerCase().includes(normalized) ?? false)
      );
    });
  }, [flowModel, keyword]);

  const selectedNode =
    selectedNodeIds.length === 1 ? nodeById.get(selectedNodeIds[0]) : undefined;

  const handleSelect = React.useCallback(
    (id: string) => {
      setSelectedNodeIds([id]);
      controllerRef.current?.focusNode(id);
    },
    []
  );

  const handleSelectionChange = React.useCallback((ids: string[]) => {
    setSelectedNodeIds(ids);
  }, []);

  return (
    <div
      style={{
        width: "100%",
        height: "100%",
        position: "relative",
        overflow: "hidden",
      }}
      className={className}
      data-testid="workflow-viewer"
    >
      <ReactFlowProvider>
        <ForwardedFlowViewportController ref={controllerRef} />
        <WorkflowCanvas
          definition={definition}
          executionSnapshot={executionSnapshot}
          diagnostics={diagnostics}
          readOnly
          selectable
          selectedNodeIds={selectedNodeIds}
          onSelectionChange={handleSelectionChange}
          className="workflow-viewer-canvas"
        />
      </ReactFlowProvider>

      <div
        style={{
          position: "absolute",
          top: 12,
          left: 12,
          zIndex: 10,
          display: "flex",
          flexDirection: "column",
          gap: 8,
          maxWidth: 260,
          background: "rgba(255, 255, 255, 0.95)",
          padding: 12,
          borderRadius: 8,
          boxShadow: "0 1px 4px rgba(0, 0, 0, 0.15)",
        }}
      >
        <input
          type="text"
          value={keyword}
          onChange={(event) => setKeyword(event.target.value)}
          placeholder="Search nodes..."
          aria-label="Search nodes"
          style={{
            width: "100%",
            padding: "6px 8px",
            border: "1px solid #d1d5db",
            borderRadius: 4,
          }}
        />
        {keyword.trim() ? (
          <ul
            style={{
              listStyle: "none",
              margin: 0,
              padding: 0,
              maxHeight: 200,
              overflow: "auto",
            }}
            role="listbox"
            aria-label="Search results"
          >
            {filteredNodes.map((node) => {
              const name = node.data.nodeDef.name ?? node.id;
              return (
                <li key={node.id} role="option">
                  <button
                    type="button"
                    onClick={() => handleSelect(node.id)}
                    style={{
                      width: "100%",
                      textAlign: "left",
                      padding: "4px 6px",
                      border: "none",
                      background: "transparent",
                      cursor: "pointer",
                    }}
                  >
                    {name}
                  </button>
                </li>
              );
            })}
            {filteredNodes.length === 0 && (
              <li style={{ color: "#6b7280", fontSize: 12 }}>No matching nodes</li>
            )}
          </ul>
        ) : null}
      </div>

      {selectedNode ? (
        <div
          style={{
            position: "absolute",
            top: 12,
            right: 12,
            zIndex: 10,
            width: 280,
            maxHeight: "calc(100% - 24px)",
            overflow: "auto",
            background: "rgba(255, 255, 255, 0.95)",
            padding: 16,
            borderRadius: 8,
            boxShadow: "0 1px 4px rgba(0, 0, 0, 0.15)",
          }}
          data-testid="node-detail-panel"
        >
          <h3 style={{ margin: "0 0 8px", fontSize: 16 }}>
            {selectedNode.data.nodeDef.name ?? selectedNode.id}
          </h3>
          <p style={{ margin: "0 0 12px", color: "#6b7280", fontSize: 12 }}>
            {selectedNode.data.nodeDef.type}
          </p>
          {executionSnapshot?.nodeStatuses?.[selectedNode.id] ? (
            <p style={{ margin: "0 0 12px", fontSize: 12 }}>
              Status:{" "}
              <strong data-testid={`selected-node-status-${selectedNode.id}`}>
                {executionSnapshot.nodeStatuses[selectedNode.id]}
              </strong>
            </p>
          ) : null}

          <section style={{ marginBottom: 12 }}>
            <h4 style={{ margin: "0 0 4px", fontSize: 13 }}>Inputs</h4>
            {selectedNode.data.nodeDef.inputs &&
            selectedNode.data.nodeDef.inputs.length > 0 ? (
              <ul style={{ margin: 0, paddingLeft: 16, fontSize: 12 }}>
                {selectedNode.data.nodeDef.inputs.map((input, index) => (
                  <li key={index}>
                    {input.name}
                    {input.required ? " *" : ""}
                  </li>
                ))}
              </ul>
            ) : (
              <p style={{ margin: 0, color: "#6b7280", fontSize: 12 }}>None</p>
            )}
          </section>

          <section style={{ marginBottom: 12 }}>
            <h4 style={{ margin: "0 0 4px", fontSize: 13 }}>Output Schema</h4>
            {selectedNode.data.nodeDef.output_schema &&
            Object.keys(selectedNode.data.nodeDef.output_schema).length > 0 ? (
              <pre style={{ margin: 0, fontSize: 11, overflow: "auto" }}>
                {JSON.stringify(selectedNode.data.nodeDef.output_schema, null, 2)}
              </pre>
            ) : (
              <p style={{ margin: 0, color: "#6b7280", fontSize: 12 }}>None</p>
            )}
          </section>

          <section style={{ marginBottom: 12 }}>
            <h4 style={{ margin: "0 0 4px", fontSize: 13 }}>Parameters</h4>
            {selectedNode.data.nodeDef.parameters &&
            Object.keys(selectedNode.data.nodeDef.parameters).length > 0 ? (
              <pre style={{ margin: 0, fontSize: 11, overflow: "auto" }}>
                {JSON.stringify(selectedNode.data.nodeDef.parameters, null, 2)}
              </pre>
            ) : (
              <p style={{ margin: 0, color: "#6b7280", fontSize: 12 }}>None</p>
            )}
          </section>

          <section>
            <h4 style={{ margin: "0 0 4px", fontSize: 13 }}>Execution</h4>
            <p style={{ margin: 0, color: "#6b7280", fontSize: 12 }}>
              Attempt / error details placeholder (M2/M3).
            </p>
          </section>
        </div>
      ) : null}
    </div>
  );
}
