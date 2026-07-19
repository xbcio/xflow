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
import type { Diagnostic, WorkflowDef } from "@xflow/workflow-core";
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

function applyDiagnostics(
  nodes: Node<NodeData>[],
  diagnostics?: Diagnostic[]
): Node<NodeData>[] {
  if (!diagnostics || diagnostics.length === 0) return nodes;

  const byNode = new Map<string, Diagnostic[]>();
  for (const d of diagnostics) {
    const nodeId = d.nodeId ?? d.connectionRef?.node;
    if (!nodeId) continue;
    if (!byNode.has(nodeId)) {
      byNode.set(nodeId, []);
    }
    byNode.get(nodeId)!.push(d);
  }

  return nodes.map((node) => {
    const nodeDiagnostics = byNode.get(node.id);
    if (!nodeDiagnostics || nodeDiagnostics.length === 0) return node;
    return {
      ...node,
      data: { ...node.data, diagnostics: nodeDiagnostics },
    };
  });
}

function useFlowModel(
  definition: WorkflowDef,
  executionSnapshot?: ExecutionSnapshot,
  diagnostics?: Diagnostic[]
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
    const withStatus = applyExecutionSnapshot(rfNodes, executionSnapshot);
    return {
      nodes: applyDiagnostics(withStatus, diagnostics),
      edges: rfEdges,
    };
  }, [definition, executionSnapshot, diagnostics]);
}

export const WorkflowCanvas = React.forwardRef<HTMLDivElement, WorkflowCanvasProps>(
  function WorkflowCanvas(
    {
      definition,
      executionSnapshot,
      diagnostics,
      readOnly = true,
      className,
      nodeTypes,
      overlays = [selectionOverlay, executionOverlay, diagnosticOverlay],
      selectable = true,
      selectedNodeIds: controlledSelectedNodeIds,
      onSelectionChange,
    },
    ref
  ) {
    const [internalSelectedNodeIds, setInternalSelectedNodeIds] = React.useState<string[]>([]);
    const isSelectionControlled = controlledSelectedNodeIds !== undefined;
    const selectedNodeIds = isSelectionControlled
      ? controlledSelectedNodeIds
      : internalSelectedNodeIds;
    const { nodes, edges } = useFlowModel(definition, executionSnapshot, diagnostics);

    const handleSelectionChange = React.useCallback(
      (params: OnSelectionChangeParams) => {
        const ids = params.nodes.map((n) => n.id);
        if (!isSelectionControlled) {
          setInternalSelectedNodeIds(ids);
        }
        onSelectionChange?.(ids);
      },
      [isSelectionControlled, onSelectionChange]
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
      diagnostics,
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
          elementsSelectable={selectable}
          onSelectionChange={handleSelectionChange}
          attributionPosition="bottom-right"
        >
          <Background gap={16} size={1} />
          {!readOnly && <Controls />}
          <MiniMap />
        </ReactFlow>
        {overlays.map((Overlay, index) => (
          <Overlay key={Overlay.displayName ?? index} {...overlayProps} />
        ))}
      </div>
    );
  }
);
