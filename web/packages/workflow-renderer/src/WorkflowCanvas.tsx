import * as React from "react";
import {
  Background,
  Controls,
  MiniMap,
  ReactFlow,
  type Edge,
  type Node,
  type OnSelectionChangeParams,
} from "@xyflow/react";
import type { WorkflowDef } from "@xflow/workflow-core";
import { GenericNode } from "./nodes/GenericNode";
import { UnknownNode } from "./nodes/UnknownNode";
import {
  selectionOverlay,
  executionOverlay,
  diagnosticOverlay,
  type WorkflowCanvasOverlayProps,
} from "./overlays";
import { workflowToFlow } from "./transform";
import type { ExecutionSnapshot, NodeData, WorkflowCanvasProps } from "./types";

const defaultNodeTypes = {
  start: GenericNode,
  end: GenericNode,
  http: GenericNode,
  grpc: GenericNode,
  function: GenericNode,
  database: GenericNode,
  if: GenericNode,
  switch: GenericNode,
  merge: GenericNode,
  wait: GenericNode,
  approval: GenericNode,
  generic: GenericNode,
  unknown: UnknownNode,
};

function applyExecutionSnapshot(
  nodes: Node<NodeData>[],
  snapshot?: ExecutionSnapshot
): Node<NodeData>[] {
  if (!snapshot?.nodeStatuses) return nodes;
  return nodes.map((node) => {
    const status = snapshot.nodeStatuses?.[node.id];
    if (!status) return node;
    return {
      ...node,
      data: { ...node.data, status },
    };
  });
}

function useFlowModel(
  definition: WorkflowDef,
  executionSnapshot?: ExecutionSnapshot
): { nodes: Node<NodeData>[]; edges: Edge[] } {
  return React.useMemo(() => {
    const { nodes, edges } = workflowToFlow(definition);
    const rfNodes: Node<NodeData>[] = nodes.map((n) => ({
      id: n.id,
      position: n.position,
      data: n.data,
      type: n.type,
    }));
    const rfEdges: Edge[] = edges.map((e) => ({
      id: e.id,
      source: e.source,
      target: e.target,
      sourceHandle: e.sourceHandle,
      targetHandle: e.targetHandle,
    }));
    return {
      nodes: applyExecutionSnapshot(rfNodes, executionSnapshot),
      edges: rfEdges,
    };
  }, [definition, executionSnapshot]);
}

export const WorkflowCanvas = React.forwardRef<HTMLDivElement, WorkflowCanvasProps>(
  function WorkflowCanvas(
    {
      definition,
      executionSnapshot,
      readOnly = true,
      className,
      nodeTypes,
      overlays = [selectionOverlay, executionOverlay, diagnosticOverlay],
    },
    ref
  ) {
    const [selectedNodeIds, setSelectedNodeIds] = React.useState<string[]>([]);
    const { nodes, edges } = useFlowModel(definition, executionSnapshot);

    const handleSelectionChange = React.useCallback(
      (params: OnSelectionChangeParams) => {
        setSelectedNodeIds(params.nodes.map((n) => n.id));
      },
      [setSelectedNodeIds]
    );

    const nodesWithSelection = React.useMemo(() => {
      const selected = new Set(selectedNodeIds);
      return nodes.map((node) =>
        selected.has(node.id)
          ? { ...node, data: { ...node.data, selected: true }, selected: true }
          : node
      );
    }, [nodes, selectedNodeIds]);

    const mergedNodeTypes = React.useMemo(
      () => ({ ...defaultNodeTypes, ...nodeTypes }),
      [nodeTypes]
    );

    const overlayProps: WorkflowCanvasOverlayProps = {
      definition,
      executionSnapshot,
      selectedNodeIds,
    };

    return (
      <div ref={ref} className={["xflow-root", className].filter(Boolean).join(" ")}>
        <ReactFlow
          nodes={nodesWithSelection}
          edges={edges}
          nodeTypes={mergedNodeTypes}
          fitView
          nodesDraggable={!readOnly}
          nodesConnectable={!readOnly}
          nodesFocusable={!readOnly}
          edgesFocusable={!readOnly}
          elementsSelectable={!readOnly}
          onSelectionChange={handleSelectionChange}
          attributionPosition="bottom-right"
        >
          <Background gap={16} size={1} />
          <Controls />
          <MiniMap />
        </ReactFlow>
        {overlays.map((Overlay, index) => (
          <Overlay key={Overlay.displayName ?? index} {...overlayProps} />
        ))}
      </div>
    );
  }
);
