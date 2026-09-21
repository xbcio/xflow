import * as React from "react";
import {
  AuditOutlined,
  BranchesOutlined,
  ClockCircleOutlined,
  CloudServerOutlined,
  CodeOutlined,
  DatabaseOutlined,
  GlobalOutlined,
  LinkOutlined,
  PlayCircleOutlined,
  QuestionCircleOutlined,
  ThunderboltOutlined
} from "@ant-design/icons";
import {
  Background,
  ControlButton,
  Controls,
  Handle,
  MiniMap,
  Position,
  ReactFlow,
  type Connection as ReactFlowConnection,
  type Edge,
  type EdgeChange,
  type IsValidConnection,
  type Node,
  type NodeProps,
  type OnBeforeDelete,
  type ReactFlowInstance,
  useReactFlow
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import {
  toGraphModel,
  type GraphEdge,
  type GraphNode,
  type NodeStatus,
  type RuntimeNodeSnapshot,
  type RuntimeSnapshot,
  type WorkflowDef
} from "@xflow/core";
import "./styles.css";

/** A data-flow connection expressed with workflow node names and port names. */
export interface PreviewConnection {
  source: string;
  sourcePort: string;
  target: string;
  targetPort: string;
}

export type XFlowPreviewInteractionMode = "select" | "connect";

export interface XFlowPreviewProps {
  workflow: WorkflowDef;
  runtime?: RuntimeSnapshot;
  className?: string;
  editable?: boolean;
  /**
   * Restricts an editable canvas to one intentional interaction at a time.
   * Omit it to retain the package's legacy editable behavior (drag + connect).
   */
  interactionMode?: XFlowPreviewInteractionMode;
  selectedNodeId?: string;
  onSelectNode?: (nodeId: string) => void;
  onNodePositionChange?: (nodeId: string, position: { x: number; y: number }) => void;
  /** Requests that the owner add a data-flow connection. The preview never mutates the workflow. */
  onConnect?: (connection: PreviewConnection) => void;
  /** Requests that the owner remove one data-flow connection. The preview never mutates the workflow. */
  onDeleteConnection?: (connection: PreviewConnection) => void;
}

const statusLabel: Record<string, string> = {
  pending: "pending",
  running: "running",
  success: "success",
  failed: "failed",
  skipped: "skipped",
  pinned: "pinned",
  continued: "continued",
  suspended: "suspended",
  waiting: "waiting",
  canceled: "canceled",
  timeout: "timeout"
};

function runtimeForNode(runtime: RuntimeSnapshot | undefined, node: GraphNode): RuntimeNodeSnapshot {
  return runtime?.nodes?.[node.name] ?? runtime?.nodes?.[node.id] ?? { status: "pending" };
}

function nodeMeta(snapshot: RuntimeNodeSnapshot): string {
  if (snapshot.attempts && snapshot.attempts > 1) {
    return `${snapshot.attempts} attempts`;
  }
  if (snapshot.durationMs !== undefined) {
    return `${snapshot.durationMs} ms`;
  }
  return "ready";
}

function nodeIcon(type: string): React.ReactNode {
  if (type.includes("start")) return <PlayCircleOutlined />;
  if (type.includes("webhook")) return <LinkOutlined />;
  if (type.includes("kafka")) return <CloudServerOutlined />;
  if (type.includes("cron") || type.includes("wait")) return <ClockCircleOutlined />;
  if (type.includes("signal")) return <ThunderboltOutlined />;
  if (type.includes("switch")) return <BranchesOutlined />;
  if (type.includes("database")) return <DatabaseOutlined />;
  if (type.includes("approval")) return <AuditOutlined />;
  if (type.includes("function")) return <CodeOutlined />;
  if (type.includes("http")) return <GlobalOutlined />;
  if (type.includes("code")) return <CodeOutlined />;
  return <QuestionCircleOutlined />;
}

function nodeTone(type: string): "blue" | "cyan" | "green" | "amber" | "violet" {
  if (type.includes("kafka") || type.includes("merge")) return "cyan";
  if (type.includes("start") || type.includes("database")) return "green";
  if (type.includes("cron") || type.includes("wait") || type.includes("approval")) return "amber";
  if (type.includes("signal") || type.includes("function") || type.includes("code")) return "violet";
  return "blue";
}

function inputSummary(node: GraphNode): string | undefined {
  if (node.inputs.length === 0) return undefined;
  return `input: ${node.inputs.map((input) => input.name).join(", ")}`;
}

function miniMapNodeColor(node: Node<PreviewNodeData>): string {
  const status = node.data.runtime.status;
  if (status === "running" || status === "waiting") return "var(--xflow-preview-status-running)";
  if (status === "success") return "var(--xflow-preview-status-success)";
  if (status === "failed") return "var(--xflow-preview-status-error)";
  if (status === "pinned" || status === "continued") return "var(--xflow-preview-status-warning)";
  if (status === "skipped" || status === "canceled") return "var(--xflow-preview-status-disabled)";
  return "var(--xflow-preview-node-bg)";
}

interface PreviewNodeData extends Record<string, unknown> {
  graphNode: GraphNode;
  runtime: RuntimeNodeSnapshot;
  sourcePorts: string[];
  targetPorts: string[];
  canConnect: boolean;
  selected: boolean;
  onSelect: (nodeId: string) => void;
}

type PreviewFlowNode = Node<PreviewNodeData, "xflowPreviewNode">;

const nodeTypes = {
  xflowPreviewNode: PreviewNode
};

const initialFitViewOptions = { padding: 0.18 };

function fitMeasuredNodes(flow: ReactFlowInstance<PreviewFlowNode, Edge>): boolean {
  const nodes = flow.getNodes();
  if (nodes.length === 0) return false;

  const bounds = flow.getNodesBounds(nodes);
  if (bounds.width <= 0 || bounds.height <= 0) return false;

  void flow.fitBounds(bounds, initialFitViewOptions);
  return true;
}

function useFitMeasuredNodes(): () => void {
  const flow = useReactFlow<PreviewFlowNode, Edge>();

  return React.useCallback(() => {
    fitMeasuredNodes(flow);
  }, [flow]);
}

/** Fits once after the controlled nodes have received measured dimensions. */
function InitialViewFitter({
  flow
}: {
  flow: ReactFlowInstance<PreviewFlowNode, Edge> | null;
}): null {
  const hasFittedInitialView = React.useRef(false);
  const fitViewFrame = React.useRef<number | undefined>(undefined);

  React.useEffect(() => {
    if (!flow || hasFittedInitialView.current) return;

    let attempts = 0;
    const tryFit = () => {
      fitViewFrame.current = undefined;
      if (hasFittedInitialView.current) return;

      if (fitMeasuredNodes(flow)) {
        hasFittedInitialView.current = true;
        return;
      }

      attempts += 1;
      if (attempts < 12) {
        fitViewFrame.current = requestAnimationFrame(tryFit);
      }
    };

    fitViewFrame.current = requestAnimationFrame(tryFit);
    return () => {
      if (fitViewFrame.current !== undefined) {
        cancelAnimationFrame(fitViewFrame.current);
        fitViewFrame.current = undefined;
      }
    };
  }, [flow]);

  return null;
}

/** Uses fitBounds so the control works with the preview's controlled nodes. */
function PreviewControls(): React.ReactElement {
  const fitMeasuredNodes = useFitMeasuredNodes();

  return (
    <Controls showFitView={false} showInteractive={false}>
      <ControlButton
        aria-label="fit view"
        title="fit view"
        onClick={fitMeasuredNodes}
      >
        <span aria-hidden="true">⌗</span>
      </ControlButton>
    </Controls>
  );
}

function addPort(portsByNode: Map<string, string[]>, nodeId: string, port: string): void {
  const ports = portsByNode.get(nodeId) ?? [];
  if (!ports.includes(port)) {
    ports.push(port);
    portsByNode.set(nodeId, ports);
  }
}

function portsWithMain(...portGroups: string[][]): string[] {
  const ports = ["main"];
  for (const portGroup of portGroups) {
    for (const port of portGroup) {
      if (port && !ports.includes(port)) {
        ports.push(port);
      }
    }
  }
  return ports;
}

function isSupplyNode(node: GraphNode | undefined): boolean {
  return node?.kind === "supply" || node?.type.startsWith("xflow.supply.") === true;
}

function toFlowNodes(
  graphNodes: GraphNode[],
  graphEdges: GraphEdge[],
  runtime: RuntimeSnapshot | undefined,
  selectedNodeId: string | undefined,
  editable: boolean,
  canConnect: boolean,
  onSelect: (nodeId: string) => void
): PreviewFlowNode[] {
  const sourcePortsByNode = new Map<string, string[]>();
  const targetPortsByNode = new Map<string, string[]>();
  for (const edge of graphEdges) {
    addPort(sourcePortsByNode, edge.source, edge.sourcePort);
    addPort(targetPortsByNode, edge.target, edge.targetPort);
  }

  return graphNodes.map((node) => {
    const nodeRuntime = runtimeForNode(runtime, node);
    const isSupply = isSupplyNode(node);
    const nodeCanConnect = canConnect && !isSupply;
    return {
      id: node.id,
      type: "xflowPreviewNode",
      position: node.position,
      draggable: editable,
      selectable: false,
      connectable: nodeCanConnect,
      deletable: false,
      ariaLabel: `${node.name} node ${nodeRuntime.status}`,
      data: {
        graphNode: node,
        runtime: nodeRuntime,
        // Read-only data edges still need their handles for React Flow to locate them.
        // Supply nodes intentionally have neither source nor target data handles.
        sourcePorts: isSupply ? [] : portsWithMain(sourcePortsByNode.get(node.id) ?? []),
        targetPorts: isSupply
          ? []
          : portsWithMain(
            node.inputs.map((input) => input.name),
            targetPortsByNode.get(node.id) ?? []
          ),
        canConnect: nodeCanConnect,
        selected: selectedNodeId === node.id,
        onSelect
      }
    };
  });
}

function edgeConnection(edge: GraphEdge): PreviewConnection {
  return {
    source: edge.sourceName,
    sourcePort: edge.sourcePort,
    target: edge.targetName,
    targetPort: edge.targetPort
  };
}

function previewConnectionFromFlow(
  connection: Pick<ReactFlowConnection, "source" | "target"> & {
    sourceHandle?: string | null;
    targetHandle?: string | null;
  },
  graphNodesById: ReadonlyMap<string, GraphNode>
): PreviewConnection | undefined {
  const source = graphNodesById.get(connection.source);
  const target = graphNodesById.get(connection.target);
  if (!source || !target) return undefined;

  return {
    source: source.name,
    sourcePort: connection.sourceHandle ?? "main",
    target: target.name,
    targetPort: connection.targetHandle ?? "main"
  };
}

function toFlowEdges(
  graphEdges: GraphEdge[],
  runtime: RuntimeSnapshot | undefined,
  canDeleteConnections: boolean,
  selectedEdgeIds: ReadonlySet<string>
): Edge[] {
  return graphEdges.map((edge) => {
    const targetRuntime = runtime?.nodes?.[edge.targetName] ?? runtime?.nodes?.[edge.target];
    const isActive = targetRuntime?.status === "running" || targetRuntime?.status === "waiting";
    const isError = edge.sourcePort === "error";
    const showLabel = edge.sourcePort !== "main";
    const connectionLabel = `Connection from ${edge.sourceName} ${edge.sourcePort} to ${edge.targetName} ${edge.targetPort}`;

    return {
      id: edge.id,
      source: edge.source,
      target: edge.target,
      sourceHandle: edge.sourcePort,
      targetHandle: edge.targetPort,
      type: "smoothstep",
      animated: isActive,
      label: showLabel ? edge.sourcePort : undefined,
      selectable: canDeleteConnections,
      focusable: canDeleteConnections,
      deletable: canDeleteConnections,
      selected: canDeleteConnections && selectedEdgeIds.has(edge.id),
      ariaLabel: canDeleteConnections
        ? `${connectionLabel}; press Enter to select, then Delete or Backspace to remove`
        : connectionLabel,
      interactionWidth: 16,
      style: {
        stroke: isError
          ? "var(--xflow-preview-edge-error)"
          : isActive
            ? "var(--xflow-preview-edge-running)"
            : "var(--xflow-preview-edge)",
        strokeDasharray: isError ? "5 4" : undefined,
        opacity: isError ? 0.78 : 1,
        strokeWidth: isActive || isError ? 1.8 : 1.2
      },
      labelStyle: {
        fill: isError ? "var(--xflow-preview-status-error)" : "var(--xflow-preview-edge-label)",
        fontWeight: 700
      },
      labelBgStyle: {
        fill: "var(--xflow-preview-edge-label-bg)"
      }
    };
  });
}

function EdgeList({ edges }: { edges: GraphEdge[] }): React.ReactElement {
  if (edges.length === 0) {
    return <p className="xflow-preview-empty">No connections</p>;
  }

  return (
    <ul className="xflow-preview-edges" aria-label="Workflow connections">
      {edges.map((edge) => (
        <li
          key={edge.id}
          aria-label={`${edge.sourceName} ${edge.sourcePort} to ${edge.targetName} ${edge.targetPort}`}
        >
          <span>{edge.sourceName}</span>
          <span>{edge.sourcePort}</span>
          <span aria-hidden="true">→</span>
          <span>{edge.targetName}</span>
          <span>{edge.targetPort}</span>
        </li>
      ))}
    </ul>
  );
}

function handleTop(index: number, count: number): string {
  return `${((index + 1) / (count + 1)) * 100}%`;
}

function activateHandle(
  event: React.KeyboardEvent<HTMLDivElement>,
  canConnect: boolean
): void {
  if (!canConnect || (event.key !== "Enter" && event.key !== " ")) return;
  event.preventDefault();
  event.currentTarget.click();
}

function PreviewNode({ data }: NodeProps<PreviewFlowNode>): React.ReactElement {
  const { graphNode: node, runtime, sourcePorts, targetPorts, canConnect, selected, onSelect } = data;
  const inputLabel = inputSummary(node);
  const visiblePorts = sourcePorts.filter((port) => port !== "main" && port !== "error");

  return (
    <article
      className="xflow-preview-node"
      data-selected={selected}
      data-status={runtime.status}
    >
      <div
        className="xflow-preview-node__select"
        role="button"
        tabIndex={0}
        aria-label={`${node.name} node ${runtime.status}`}
        onClick={() => onSelect(node.id)}
        onKeyDown={(event) => {
          if (event.key === "Enter" || event.key === " ") {
            event.preventDefault();
            onSelect(node.id);
          }
        }}
      >
        <div className="xflow-preview-node__header">
          <span className="xflow-preview-node__icon" data-tone={nodeTone(node.type)} aria-label={`${node.type} icon`}>
            {nodeIcon(node.type)}
          </span>
          <div>
            <strong>{node.label}</strong>
            {node.label !== node.name ? <span>{node.name}</span> : null}
          </div>
          <span className="xflow-preview-node__status">{statusLabel[runtime.status]}</span>
        </div>
        <div className="xflow-preview-node__body">
          <div className="xflow-preview-node__meta">
            <span>{node.kind}</span>
            <code>{node.type}</code>
          </div>
          <div className="xflow-preview-node__ports">
            {inputLabel ? <span>{inputLabel}</span> : <span>input: -</span>}
            <span>{nodeMeta(runtime)}</span>
          </div>
          {visiblePorts.length > 0 ? (
            <div className="xflow-preview-node__port-chips">
              {visiblePorts.map((port) => (
                <span key={port}>{port}</span>
              ))}
            </div>
          ) : null}
        </div>
        {runtime.error ? <p className="xflow-preview-node__error">{runtime.error}</p> : null}
      </div>
      {targetPorts.map((port, index) => (
        <Handle
          key={`target:${port}`}
          id={port}
          type="target"
          position={Position.Left}
          className="xflow-preview-node__handle xflow-preview-node__handle--target"
          style={{ top: handleTop(index, targetPorts.length) }}
          isConnectable={canConnect}
          isConnectableStart={false}
          isConnectableEnd={canConnect}
          role="button"
          tabIndex={canConnect ? 0 : -1}
          aria-disabled={!canConnect}
          aria-label={`${node.name} input ${port}; press Enter or Space to complete a connection`}
          onKeyDown={(event) => activateHandle(event, canConnect)}
        />
      ))}
      {sourcePorts.map((port, index) => (
        <Handle
          key={`source:${port}`}
          id={port}
          type="source"
          position={Position.Right}
          className="xflow-preview-node__handle xflow-preview-node__handle--source"
          style={{ top: handleTop(index, sourcePorts.length) }}
          isConnectable={canConnect}
          isConnectableStart={canConnect}
          isConnectableEnd={false}
          role="button"
          tabIndex={canConnect ? 0 : -1}
          aria-disabled={!canConnect}
          aria-label={`${node.name} output ${port}; press Enter or Space to start a connection`}
          onKeyDown={(event) => activateHandle(event, canConnect)}
        />
      ))}
    </article>
  );
}

interface RuntimeSummary {
  total: number;
  counts: Partial<Record<NodeStatus, number>>;
}

function summarizeRuntime(nodes: GraphNode[], runtime?: RuntimeSnapshot): RuntimeSummary {
  const counts: Partial<Record<NodeStatus, number>> = {};
  for (const node of nodes) {
    const status = runtimeForNode(runtime, node).status;
    counts[status] = (counts[status] ?? 0) + 1;
  }
  return { total: nodes.length, counts };
}

function SummaryPill({ label }: { label: string }): React.ReactElement {
  return <span className="xflow-preview-summary__pill">{label}</span>;
}

function RunSummary({ summary }: { summary: RuntimeSummary }): React.ReactElement {
  const statuses: NodeStatus[] = [
    "running",
    "failed",
    "waiting",
    "suspended",
    "success",
    "skipped",
    "pending",
    "canceled"
  ];

  return (
    <div className="xflow-preview-summary" aria-label="Run summary">
      <span>Run summary</span>
      <div>
        <SummaryPill label={`${summary.total} nodes`} />
        {statuses.map((status) => {
          const count = summary.counts[status] ?? 0;
          return count > 0 ? <SummaryPill key={status} label={`${count} ${status}`} /> : null;
        })}
      </div>
    </div>
  );
}

function SelectedNodeDetails({
  node,
  runtime
}: {
  node?: GraphNode;
  runtime?: RuntimeNodeSnapshot;
}): React.ReactElement | null {
  if (!node || !runtime) return null;

  return (
    <section className="xflow-preview-details" aria-label="Selected node details">
      <span>Selected node</span>
      <strong>{node.name}</strong>
      <code>{node.type}</code>
      <p>{node.kind}</p>
      <p>{statusLabel[runtime.status]}</p>
      {node.disabled ? <p>Disabled</p> : null}
      {node.notes ? <p>{node.notes}</p> : null}
      <p>Inputs {node.inputs.length}</p>
      <p>Attempts {runtime.attempts ?? 1}</p>
      {runtime.durationMs !== undefined ? <p>Duration {runtime.durationMs} ms</p> : null}
      {runtime.error ? <p className="xflow-preview-node__error">{runtime.error}</p> : null}
    </section>
  );
}

export function XFlowPreview({
  workflow,
  runtime,
  className,
  editable = false,
  interactionMode,
  selectedNodeId: selectedNodeIdProp,
  onSelectNode,
  onNodePositionChange,
  onConnect,
  onDeleteConnection
}: XFlowPreviewProps): React.ReactElement {
  const graph = React.useMemo(() => toGraphModel(workflow), [workflow]);
  const [internalSelectedNodeId, setInternalSelectedNodeId] = React.useState<string | undefined>();
  const [selectedEdgeIds, setSelectedEdgeIds] = React.useState<ReadonlySet<string>>(() => new Set());
  const [flowInstance, setFlowInstance] = React.useState<ReactFlowInstance<PreviewFlowNode, Edge> | null>(null);
  const selectionViewportRef = React.useRef<{
    flow: ReactFlowInstance<PreviewFlowNode, Edge> | null;
    selectedNodeId: string | undefined;
  }>({ flow: null, selectedNodeId: undefined });
  const selectedNodeId = selectedNodeIdProp ?? internalSelectedNodeId;
  // Preserve the original standalone API when no mode is supplied. The editor
  // opts into a mode explicitly so selecting nodes never accidentally starts a
  // connection, and connecting never drags a node.
  const canDragNodes = editable && (interactionMode === undefined || interactionMode === "select");
  const canConnect = editable && (interactionMode === undefined || interactionMode === "connect") && Boolean(onConnect);
  const canDeleteConnections = editable && Boolean(onDeleteConnection);
  const graphNodesById = React.useMemo(
    () => new Map(graph.nodes.map((node) => [node.id, node])),
    [graph.nodes]
  );
  const canvasGraphEdges = React.useMemo(
    () => graph.edges.filter((edge) => (
      !isSupplyNode(graphNodesById.get(edge.source)) && !isSupplyNode(graphNodesById.get(edge.target))
    )),
    [graph.edges, graphNodesById]
  );
  const graphEdgesById = React.useMemo(
    () => new Map(canvasGraphEdges.map((edge) => [edge.id, edge])),
    [canvasGraphEdges]
  );
  const handleSelectNode = React.useCallback((nodeId: string) => {
    if (selectedNodeIdProp === undefined) {
      setInternalSelectedNodeId(nodeId);
    }
    onSelectNode?.(nodeId);
  }, [onSelectNode, selectedNodeIdProp]);
  const handleFlowInit = React.useCallback((instance: ReactFlowInstance<PreviewFlowNode, Edge>) => {
    setFlowInstance(instance);
  }, []);
  React.useEffect(() => {
    const previous = selectionViewportRef.current;
    if (!flowInstance) {
      selectionViewportRef.current = { flow: null, selectedNodeId };
      return;
    }

    // Initial fitting is intentionally owned by InitialViewFitter. Recording
    // the initial selection here prevents an immediately selected node from
    // replacing that measured fit as soon as React Flow initializes.
    if (previous.flow !== flowInstance) {
      selectionViewportRef.current = { flow: flowInstance, selectedNodeId };
      return;
    }
    if (previous.selectedNodeId === selectedNodeId) return;

    selectionViewportRef.current = { flow: flowInstance, selectedNodeId };
    if (!selectedNodeId) return;

    const selectedNode = flowInstance.getNodes().find((node) => node.id === selectedNodeId);
    if (!selectedNode) return;
    const bounds = flowInstance.getNodesBounds([selectedNode]);
    void flowInstance.fitBounds(bounds, { padding: 0.28 });
  }, [flowInstance, selectedNodeId]);
  const isValidConnection = React.useCallback<IsValidConnection<Edge>>((connection) => {
    const source = graphNodesById.get(connection.source);
    const target = graphNodesById.get(connection.target);
    return Boolean(
      source
      && target
      && source.id !== target.id
      && !isSupplyNode(source)
      && !isSupplyNode(target)
    );
  }, [graphNodesById]);
  const handleConnect = React.useCallback((connection: ReactFlowConnection) => {
    if (!canConnect || !isValidConnection(connection)) return;
    const previewConnection = previewConnectionFromFlow(connection, graphNodesById);
    if (previewConnection) {
      onConnect?.(previewConnection);
    }
  }, [canConnect, graphNodesById, isValidConnection, onConnect]);
  const handleEdgesChange = React.useCallback((changes: EdgeChange[]) => {
    if (!canDeleteConnections) return;

    setSelectedEdgeIds((current) => {
      const next = new Set(current);
      for (const change of changes) {
        if (change.type !== "select" || !graphEdgesById.has(change.id)) continue;
        if (change.selected) {
          next.add(change.id);
        } else {
          next.delete(change.id);
        }
      }
      return next.size === current.size && [...next].every((id) => current.has(id)) ? current : next;
    });
  }, [canDeleteConnections, graphEdgesById]);
  const handleEdgesDelete = React.useCallback((deletedEdges: Edge[]) => {
    if (!canDeleteConnections || !onDeleteConnection) return;

    const deletedIds = new Set<string>();
    for (const deletedEdge of deletedEdges) {
      if (deletedIds.has(deletedEdge.id)) continue;
      const graphEdge = graphEdgesById.get(deletedEdge.id);
      if (!graphEdge) continue;
      deletedIds.add(graphEdge.id);
      onDeleteConnection(edgeConnection(graphEdge));
    }
    if (deletedIds.size > 0) {
      setSelectedEdgeIds((current) => {
        const next = new Set([...current].filter((id) => !deletedIds.has(id)));
        return next.size === current.size ? current : next;
      });
    }
  }, [canDeleteConnections, graphEdgesById, onDeleteConnection]);
  const handleBeforeDelete = React.useCallback<OnBeforeDelete<PreviewFlowNode>>(async ({ edges }) => ({
    nodes: [],
    edges: canDeleteConnections ? edges.filter((edge) => graphEdgesById.has(edge.id)) : []
  }), [canDeleteConnections, graphEdgesById]);
  React.useEffect(() => {
    setSelectedEdgeIds((current) => {
      if (!canDeleteConnections) {
        return current.size === 0 ? current : new Set();
      }
      const next = new Set([...current].filter((id) => graphEdgesById.has(id)));
      return next.size === current.size ? current : next;
    });
  }, [canDeleteConnections, graphEdgesById]);
  const flowNodes = React.useMemo(
    () => toFlowNodes(graph.nodes, canvasGraphEdges, runtime, selectedNodeId, canDragNodes, canConnect, handleSelectNode),
    [graph.nodes, canvasGraphEdges, runtime, selectedNodeId, canDragNodes, canConnect, handleSelectNode]
  );
  const flowEdges = React.useMemo(
    () => toFlowEdges(canvasGraphEdges, runtime, canDeleteConnections, selectedEdgeIds),
    [canvasGraphEdges, runtime, canDeleteConnections, selectedEdgeIds]
  );
  const summary = React.useMemo(() => summarizeRuntime(graph.nodes, runtime), [graph.nodes, runtime]);
  const selectedNode = React.useMemo(
    () => graph.nodes.find((node) => node.id === selectedNodeId),
    [graph.nodes, selectedNodeId]
  );
  const selectedRuntime = selectedNode ? runtimeForNode(runtime, selectedNode) : undefined;
  const title = workflow.name ?? "Untitled workflow";
  const workflowStatus = runtime?.status ?? "pending";

  return (
    <section
      className={["xflow-preview", className].filter(Boolean).join(" ")}
      aria-label={`${title} preview`}
    >
      <header className="xflow-preview-toolbar">
        <div>
          <p>Preview</p>
          <h2>{title}</h2>
        </div>
        <span className="xflow-preview-status" data-status={workflowStatus}>
          {statusLabel[workflowStatus]}
        </span>
      </header>

      <div className="xflow-preview-shell">
        <aside className="xflow-preview-sidebar" aria-label="Workflow nodes">
          <span>Nodes</span>
          <strong>{graph.nodes.length}</strong>
        </aside>

        <div className="xflow-preview-canvas">
          {graph.nodes.length === 0 ? (
            <p className="xflow-preview-empty">No nodes to preview</p>
          ) : (
            <ReactFlow
              nodes={flowNodes}
              edges={flowEdges}
              nodeTypes={nodeTypes}
              onInit={handleFlowInit}
              minZoom={0.4}
              maxZoom={1.6}
              nodesDraggable={canDragNodes}
              nodesConnectable={canConnect}
              edgesFocusable={canDeleteConnections}
              nodesFocusable={false}
              elementsSelectable={canDeleteConnections}
              connectOnClick={canConnect}
              deleteKeyCode={canDeleteConnections ? ["Backspace", "Delete"] : null}
              onConnect={canConnect ? handleConnect : undefined}
              onEdgesChange={canDeleteConnections ? handleEdgesChange : undefined}
              onEdgesDelete={canDeleteConnections ? handleEdgesDelete : undefined}
              onBeforeDelete={canDeleteConnections ? handleBeforeDelete : undefined}
              isValidConnection={canConnect ? isValidConnection : undefined}
              aria-label={`${title} canvas`}
              onNodeDragStop={canDragNodes ? (_, node) => {
                onNodePositionChange?.(node.id, node.position);
              } : undefined}
              attributionPosition="bottom-left"
            >
              <InitialViewFitter flow={flowInstance} />
              <Background color="var(--xflow-preview-canvas-grid)" gap={20} size={1} />
              <PreviewControls />
              <MiniMap
                pannable
                zoomable
                maskColor="var(--xflow-preview-minimap-mask)"
                nodeColor={miniMapNodeColor}
                nodeStrokeColor="var(--xflow-preview-node-border)"
                nodeStrokeWidth={2}
              />
            </ReactFlow>
          )}
        </div>

        <aside className="xflow-preview-panel" aria-label="Workflow status">
          <RunSummary summary={summary} />
          <SelectedNodeDetails node={selectedNode} runtime={selectedRuntime} />
          <span>Connections</span>
          <EdgeList edges={canvasGraphEdges} />
        </aside>
      </div>
    </section>
  );
}
