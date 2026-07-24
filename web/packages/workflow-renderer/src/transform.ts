import type { Connection, Connections, NodeDef, WorkflowDef } from "@xflow/workflow-core";
import { topologicalSort } from "@xflow/workflow-core";
import type { FlowViewModel, ResolvedNodeType } from "./types";

const NODE_SIZE = { width: 180, height: 60 };
const LAYER_GAP_X = 220;
const NODE_GAP_Y = 80;

const SPECIFIC_NODE_TYPES: Record<string, ResolvedNodeType> = {
  "xflow.start": "start",
  "xflow.end": "end",
  "xflow.http": "http",
  "xflow.grpc": "grpc",
  "xflow.function": "function",
  "xflow.database": "database",
  "xflow.if": "if",
  "xflow.switch": "switch",
  "xflow.merge": "merge",
  "xflow.wait": "wait",
  "xflow.approval": "approval",
  "approval.request": "approval",
  "approval.human": "approval",
  "xflow.generic": "generic",
};

export function resolveNodeType(nodeDef: NodeDef): ResolvedNodeType {
  const raw = nodeDef.type ?? "";
  // Normalize common prefixes.
  const normalized = raw
    .replace(/^node:/, "xflow.")
    .replace(/^@xflow\//, "xflow.");
  // Only built-in types or an explicit xflow.generic resolve to a concrete
  // renderer type. Unregistered xflow.* types degrade to "unknown" so they
  // are rendered by UnknownNode rather than silently treated as generic.
  return SPECIFIC_NODE_TYPES[normalized] ?? SPECIFIC_NODE_TYPES[raw] ?? "unknown";
}

function computeLayerIndex(
  nodeName: string,
  predecessors: Record<string, Set<string>>,
  visiting = new Set<string>(),
  memo = new Map<string, number>()
): number {
  if (memo.has(nodeName)) return memo.get(nodeName)!;
  if (visiting.has(nodeName)) {
    // Cycle detected: break it by treating this node as layer 0 for this branch.
    return 0;
  }
  const preds = predecessors[nodeName] ?? new Set<string>();
  if (preds.size === 0) return 0;

  visiting.add(nodeName);
  let max = -1;
  for (const pred of preds) {
    max = Math.max(max, computeLayerIndex(pred, predecessors, visiting, memo));
  }
  visiting.delete(nodeName);
  const layer = max + 1;
  memo.set(nodeName, layer);
  return layer;
}

function autoLayout(def: WorkflowDef): Record<string, { x: number; y: number }> {
  const nodesByName = new Map<string, NodeDef>();
  for (const node of def.nodes ?? []) {
    if (node.name) {
      nodesByName.set(node.name, node);
    }
  }

  // Build predecessor map manually (without cycles to avoid recursion overflow).
  const predecessors: Record<string, Set<string>> = {};
  for (const node of def.nodes ?? []) {
    if (node.name) {
      predecessors[node.name] = new Set<string>();
    }
  }
  for (const [sourceNode, ports] of Object.entries(def.connections ?? {})) {
    for (const targets of Object.values(ports)) {
      for (const target of targets ?? []) {
        const targetNode = target.node;
        if (targetNode && predecessors[targetNode]) {
          predecessors[targetNode].add(sourceNode);
        }
      }
    }
  }

  let order: string[] = [];
  try {
    order = topologicalSort(def);
  } catch {
    // Cyclic or invalid graph: fall back to declaration order.
    order = Array.from(nodesByName.keys());
  }

  const layers = new Map<number, string[]>();
  for (const nodeName of order) {
    const layer = computeLayerIndex(nodeName, predecessors);
    if (!layers.has(layer)) {
      layers.set(layer, []);
    }
    layers.get(layer)!.push(nodeName);
  }

  const positions: Record<string, { x: number; y: number }> = {};
  for (const [layer, names] of layers) {
    for (let i = 0; i < names.length; i++) {
      positions[names[i]] = {
        x: layer * LAYER_GAP_X,
        y: i * (NODE_SIZE.height + NODE_GAP_Y),
      };
    }
  }
  return positions;
}

// Canonical "default" port names: these normalize to undefined so React Flow
// uses the single default Handle (no id) on a node. Any other value is a
// named port and must match a Handle with that id.
const DEFAULT_PORT_NAMES = new Set(["", "default", "main"]);

function normalizePort(port: string | undefined): string | undefined {
  if (port === undefined) return undefined;
  if (DEFAULT_PORT_NAMES.has(port)) return undefined;
  return port;
}

function expandEdges(
  connections: Connections,
  nodeNames: Set<string>
): Pick<FlowViewModel, "edges" | "danglingTargets" | "missingSources"> {
  const edges: FlowViewModel["edges"] = [];
  const danglingTargets: FlowViewModel["danglingTargets"] = [];
  const missingSources: FlowViewModel["missingSources"] = [];
  let edgeIndex = 0;

  function edgeId(conn: Connection, source: string, port: string, index: number): string {
    const target = conn.node ?? "unknown";
    const input = conn.input ?? "default";
    return `e-${source}-${port}-${index}-${target}-${input}-${edgeIndex++}`;
  }

  for (const [sourceNode, ports] of Object.entries(connections ?? {})) {
    // If the source node itself isn't in the workflow, record every outgoing
    // connection as a missing source. We must NOT emit dangling edges whose
    // source React Flow cannot resolve to a node — those would surface as
    // "couldn't create edge" warnings.
    const sourceMissing = !nodeNames.has(sourceNode);

    for (const [port, targets] of Object.entries(ports)) {
      for (let i = 0; i < (targets ?? []).length; i++) {
        const conn = targets[i];
        const targetNode = conn.node;
        if (!targetNode) continue;

        if (sourceMissing) {
          missingSources.push({
            source: sourceNode,
            port,
            target: targetNode,
            input: conn.input,
          });
          continue;
        }

        if (!nodeNames.has(targetNode)) {
          danglingTargets.push({
            source: sourceNode,
            port,
            target: targetNode,
            input: conn.input,
          });
          continue;
        }

        edges.push({
          id: edgeId(conn, sourceNode, port, i),
          source: sourceNode,
          // Node defs declare only `output_schema`, not named output ports.
          // GenericNode/UnknownNode render a single default source Handle (no
          // id), so every edge must use sourceHandle=undefined. Named source
          // ports in the workflow def are recorded for diagnostics but the
          // rendered edge uses the default Handle.
          sourceHandle: undefined,
          target: targetNode,
          targetHandle: normalizePort(conn.input),
        });
      }
    }
  }
  return { edges, danglingTargets, missingSources };
}

export function workflowToFlow(def: WorkflowDef): FlowViewModel {
  const autoPositions = autoLayout(def);
  const nodes: FlowViewModel["nodes"] = [];
  const nodeNames = new Set<string>();

  for (const node of def.nodes ?? []) {
    const name = node.name ?? node.id ?? `node-${nodes.length}`;
    nodeNames.add(name);
    const position =
      node.position?.x != null && node.position?.y != null
        ? { x: node.position.x, y: node.position.y }
        : autoPositions[name] ?? { x: 0, y: 0 };

    nodes.push({
      id: name,
      position,
      data: { nodeDef: node },
      type: resolveNodeType(node),
    });
  }

  const { edges, danglingTargets, missingSources } = expandEdges(
    def.connections ?? {},
    nodeNames
  );
  return { nodes, edges, danglingTargets, missingSources };
}

/**
 * Reverse transform: React Flow nodes/edges -> WorkflowDef.
 * M4.1 is read-only; full editor reverse transform is reserved for M5.
 */
export function flowToWorkflow(
  _nodes: FlowViewModel["nodes"],
  _edges: FlowViewModel["edges"]
): WorkflowDef {
  // TODO(M5): implement reverse conversion preserving positions, ports and node definitions.
  return { name: "converted" };
}
