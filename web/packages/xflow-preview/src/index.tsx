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
  Controls,
  Handle,
  MiniMap,
  Position,
  ReactFlow,
  type Edge,
  type Node,
  type NodeProps
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

export interface XFlowPreviewProps {
  workflow: WorkflowDef;
  runtime?: RuntimeSnapshot;
  className?: string;
  editable?: boolean;
  selectedNodeId?: string;
  onSelectNode?: (nodeId: string) => void;
  onNodePositionChange?: (nodeId: string, position: { x: number; y: number }) => void;
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
  if (status === "running" || status === "waiting") return "#2f7cff";
  if (status === "success") return "#35c98b";
  if (status === "failed") return "#ff5f57";
  if (status === "pinned" || status === "continued") return "#f7b955";
  if (status === "skipped" || status === "canceled") return "#747b86";
  return "#d6d9de";
}

interface PreviewNodeData extends Record<string, unknown> {
  graphNode: GraphNode;
  runtime: RuntimeNodeSnapshot;
  outgoingPorts: string[];
  selected: boolean;
  onSelect: (nodeId: string) => void;
}

type PreviewFlowNode = Node<PreviewNodeData, "xflowPreviewNode">;

const nodeTypes = {
  xflowPreviewNode: PreviewNode
};

function toFlowNodes(
  graphNodes: GraphNode[],
  graphEdges: GraphEdge[],
  runtime: RuntimeSnapshot | undefined,
  selectedNodeId: string | undefined,
  editable: boolean,
  onSelect: (nodeId: string) => void
): PreviewFlowNode[] {
  const portsByNode = new Map<string, string[]>();
  for (const edge of graphEdges) {
    const ports = portsByNode.get(edge.source) ?? [];
    if (!ports.includes(edge.sourcePort)) {
      ports.push(edge.sourcePort);
      portsByNode.set(edge.source, ports);
    }
  }

  return graphNodes.map((node) => ({
    id: node.id,
    type: "xflowPreviewNode",
    position: node.position,
    draggable: editable,
    selectable: true,
    data: {
      graphNode: node,
      runtime: runtimeForNode(runtime, node),
      outgoingPorts: portsByNode.get(node.id) ?? [],
      selected: selectedNodeId === node.id,
      onSelect
    }
  }));
}

function toFlowEdges(graphEdges: GraphEdge[], runtime?: RuntimeSnapshot): Edge[] {
  return graphEdges.map((edge) => {
    const targetRuntime = runtime?.nodes?.[edge.targetName] ?? runtime?.nodes?.[edge.target];
    const isActive = targetRuntime?.status === "running" || targetRuntime?.status === "waiting";
    const isError = edge.sourcePort === "error";
    const showLabel = edge.sourcePort !== "main" && (!isError || edge.sourceName === "route_by_amount");

    return {
      id: edge.id,
      source: edge.source,
      target: edge.target,
      type: "smoothstep",
      animated: isActive,
      label: showLabel ? edge.sourcePort : undefined,
      style: {
        stroke: isError ? "#ff5f57" : isActive ? "#2878ff" : "#5b5b5b",
        strokeDasharray: isError ? "5 4" : undefined,
        opacity: isError ? 0.78 : 1,
        strokeWidth: isActive || isError ? 1.8 : 1.2
      },
      labelStyle: {
        fill: isError ? "#ff8b84" : "#f7b955",
        fontWeight: 700
      },
      labelBgStyle: {
        fill: "#202226"
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

function PreviewNode({ data }: NodeProps<PreviewFlowNode>): React.ReactElement {
  const { graphNode: node, runtime, outgoingPorts, selected, onSelect } = data;
  const inputLabel = inputSummary(node);
  const visiblePorts = outgoingPorts.filter((port) => port !== "main" && port !== "error");

  return (
    <article
      className="xflow-preview-node"
      data-selected={selected}
      data-status={runtime.status}
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
      <Handle type="target" position={Position.Left} isConnectable={false} />
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
      <Handle type="source" position={Position.Right} isConnectable={false} />
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
  selectedNodeId: selectedNodeIdProp,
  onSelectNode,
  onNodePositionChange
}: XFlowPreviewProps): React.ReactElement {
  const graph = React.useMemo(() => toGraphModel(workflow), [workflow]);
  const [internalSelectedNodeId, setInternalSelectedNodeId] = React.useState<string | undefined>();
  const selectedNodeId = selectedNodeIdProp ?? internalSelectedNodeId;
  const handleSelectNode = React.useCallback((nodeId: string) => {
    if (selectedNodeIdProp === undefined) {
      setInternalSelectedNodeId(nodeId);
    }
    onSelectNode?.(nodeId);
  }, [onSelectNode, selectedNodeIdProp]);
  const flowNodes = React.useMemo(
    () => toFlowNodes(graph.nodes, graph.edges, runtime, selectedNodeId, editable, handleSelectNode),
    [graph.nodes, graph.edges, runtime, selectedNodeId, editable, handleSelectNode]
  );
  const flowEdges = React.useMemo(() => toFlowEdges(graph.edges, runtime), [graph.edges, runtime]);
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
              fitView
              fitViewOptions={{ padding: 0.18 }}
              minZoom={0.4}
              maxZoom={1.6}
              nodesDraggable={editable}
              nodesConnectable={false}
              edgesFocusable={false}
              nodesFocusable={false}
              onNodeDragStop={(_, node) => {
                onNodePositionChange?.(node.id, node.position);
              }}
              attributionPosition="bottom-left"
            >
              <Background color="#3b3b3b" gap={20} size={1} />
              <Controls showInteractive={false} />
              <MiniMap
                pannable
                zoomable
                maskColor="rgba(16, 18, 20, .58)"
                nodeColor={miniMapNodeColor}
                nodeStrokeColor="#202226"
                nodeStrokeWidth={2}
              />
            </ReactFlow>
          )}
        </div>

        <aside className="xflow-preview-panel" aria-label="Workflow status">
          <RunSummary summary={summary} />
          <SelectedNodeDetails node={selectedNode} runtime={selectedRuntime} />
          <span>Connections</span>
          <EdgeList edges={graph.edges} />
        </aside>
      </div>
    </section>
  );
}
