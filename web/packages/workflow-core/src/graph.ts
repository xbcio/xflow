import type { Connections, Diagnostic, NodeDef, WorkflowDef } from "./types";

export interface Adjacency {
  /** node -> set of predecessor node names */
  predecessors: Record<string, Set<string>>;
  /** node -> set of successor node names */
  successors: Record<string, Set<string>>;
  /** node -> output port -> list of target connections */
  edges: Connections;
}

function ensureNode(nodes: Record<string, Set<string>>, nodeName: string): Set<string> {
  if (!nodes[nodeName]) {
    nodes[nodeName] = new Set();
  }
  return nodes[nodeName];
}

export function buildAdjacency(def: WorkflowDef): Adjacency {
  const predecessors: Record<string, Set<string>> = {};
  const successors: Record<string, Set<string>> = {};
  const edges: Connections = {};

  for (const node of def.nodes ?? []) {
    const name = node.name;
    if (!name) continue;
    ensureNode(predecessors, name);
    ensureNode(successors, name);
  }

  for (const [sourceNode, ports] of Object.entries(def.connections ?? {})) {
    for (const [port, targets] of Object.entries(ports)) {
      for (const target of targets ?? []) {
        const targetNode = target.node;
        if (!targetNode) continue;

        ensureNode(successors, sourceNode).add(targetNode);
        ensureNode(predecessors, targetNode).add(sourceNode);

        if (!edges[sourceNode]) {
          edges[sourceNode] = {};
        }
        if (!edges[sourceNode][port]) {
          edges[sourceNode][port] = [];
        }
        edges[sourceNode][port].push(target);
      }
    }
  }

  return { predecessors, successors, edges };
}

export function detectCycle(def: WorkflowDef): string[] | null {
  if (def.options?.allow_cycles) {
    return null;
  }

  const adjacency = buildAdjacency(def);
  const successors = adjacency.successors;
  const WHITE = 0;
  const GRAY = 1;
  const BLACK = 2;
  const color: Record<string, number> = {};
  const parent: Record<string, string | undefined> = {};

  for (const node of Object.keys(successors)) {
    color[node] = WHITE;
  }

  function dfs(node: string): string[] | null {
    color[node] = GRAY;

    for (const next of successors[node] ?? []) {
      if (color[next] === GRAY) {
        // Found cycle; reconstruct path
        const cycle: string[] = [next];
        let cur = node;
        while (cur !== next && cur !== undefined) {
          cycle.push(cur);
          cur = parent[cur]!;
        }
        cycle.push(next);
        return cycle.reverse();
      }
      if (color[next] === WHITE) {
        parent[next] = node;
        const result = dfs(next);
        if (result) return result;
      }
    }

    color[node] = BLACK;
    return null;
  }

  for (const node of Object.keys(successors)) {
    if (color[node] === WHITE) {
      const result = dfs(node);
      if (result) return result;
    }
  }

  return null;
}

export function topologicalSort(def: WorkflowDef): string[] {
  const cycle = detectCycle(def);
  if (cycle) {
    throw new Error(`Cycle detected: ${cycle.join(" -> ")}`);
  }

  const adjacency = buildAdjacency(def);
  const predecessors: Record<string, number> = {};

  for (const node of Object.keys(adjacency.successors)) {
    predecessors[node] = (adjacency.predecessors[node]?.size) ?? 0;
  }

  const queue: string[] = Object.keys(predecessors)
    .filter((n) => predecessors[n] === 0)
    .sort();
  const result: string[] = [];

  while (queue.length > 0) {
    const node = queue.shift()!;
    result.push(node);

    const nextNodes = Array.from(adjacency.successors[node] ?? []).sort();
    for (const next of nextNodes) {
      predecessors[next] -= 1;
      if (predecessors[next] === 0) {
        queue.push(next);
        queue.sort();
      }
    }
  }

  if (result.length !== Object.keys(predecessors).length) {
    throw new Error("Topological sort failed: graph contains a cycle");
  }

  return result;
}

export function upstream(def: WorkflowDef, nodeName: string): string[] {
  const adjacency = buildAdjacency(def);
  return Array.from(adjacency.predecessors[nodeName] ?? []).sort();
}

export function downstream(def: WorkflowDef, nodeName: string): string[] {
  const adjacency = buildAdjacency(def);
  return Array.from(adjacency.successors[nodeName] ?? []).sort();
}

export function reachable(def: WorkflowDef, fromNode: string): string[] {
  const adjacency = buildAdjacency(def);
  const visited = new Set<string>();
  const queue: string[] = [fromNode];

  while (queue.length > 0) {
    const node = queue.shift()!;
    if (visited.has(node)) continue;
    visited.add(node);

    for (const next of adjacency.successors[node] ?? []) {
      if (!visited.has(next)) {
        queue.push(next);
      }
    }
  }

  visited.delete(fromNode);
  return Array.from(visited).sort();
}

function collectNodeNames(def: WorkflowDef): Set<string> {
  return new Set(
    (def.nodes ?? []).map((n) => n.name).filter((n): n is string => Boolean(n))
  );
}

function collectInputPorts(node: NodeDef): Set<string> {
  return new Set(
    (node.inputs ?? []).map((p) => p.name).filter((n): n is string => Boolean(n))
  );
}

export function validatePorts(def: WorkflowDef): Diagnostic[] {
  const diagnostics: Diagnostic[] = [];
  const nodeMap = new Map<string, NodeDef>();
  const nodeNames = collectNodeNames(def);

  for (const node of def.nodes ?? []) {
    if (node.name) {
      nodeMap.set(node.name, node);
    }
  }

  for (const [sourceNode, ports] of Object.entries(def.connections ?? {})) {
    if (!nodeNames.has(sourceNode)) {
      diagnostics.push({
        code: "PORT_DANGLING_SOURCE",
        severity: "error",
        message: `Connection source node "${sourceNode}" does not exist`,
        path: `/connections/${sourceNode}`,
        connectionRef: { node: sourceNode, input: "" },
      });
    }

    for (const [port, targets] of Object.entries(ports)) {
      for (let i = 0; i < (targets ?? []).length; i++) {
        const target = targets[i];
        const targetNode = target.node;
        const targetInput = target.input;

        if (!targetNode) {
          diagnostics.push({
            code: "PORT_MISSING_TARGET_NODE",
            severity: "error",
            message: `Connection target node is missing`,
            path: `/connections/${sourceNode}/${port}/${i}`,
            connectionRef: { node: targetNode ?? "", input: targetInput ?? "" },
          });
          continue;
        }

        if (!nodeNames.has(targetNode)) {
          diagnostics.push({
            code: "PORT_DANGLING_TARGET",
            severity: "error",
            message: `Connection target node "${targetNode}" does not exist`,
            path: `/connections/${sourceNode}/${port}/${i}`,
            connectionRef: { node: targetNode, input: targetInput ?? "" },
          });
          continue;
        }

        if (targetInput) {
          const targetDef = nodeMap.get(targetNode);
          const validInputs = collectInputPorts(targetDef ?? {});
          if (validInputs.size > 0 && !validInputs.has(targetInput)) {
            diagnostics.push({
              code: "PORT_UNKNOWN_INPUT",
              severity: "error",
              message: `Target node "${targetNode}" has no input port "${targetInput}"`,
              path: `/connections/${sourceNode}/${port}/${i}`,
              nodeId: targetNode,
              connectionRef: { node: targetNode, input: targetInput },
            });
          }
        }
      }
    }
  }

  return diagnostics;
}

export function validateWorkflow(def: WorkflowDef): Diagnostic[] {
  const diagnostics: Diagnostic[] = [];

  // Required fields
  if (!def.name) {
    diagnostics.push({
      code: "WORKFLOW_MISSING_NAME",
      severity: "error",
      message: "Workflow name is required",
      path: "/name",
    });
  }

  const nodeNames = new Set<string>();
  for (const node of def.nodes ?? []) {
    if (!node.name) {
      diagnostics.push({
        code: "NODE_MISSING_NAME",
        severity: "error",
        message: "Node name is required",
        path: "/nodes",
      });
      continue;
    }
    if (nodeNames.has(node.name)) {
      diagnostics.push({
        code: "NODE_DUPLICATE_NAME",
        severity: "error",
        message: `Duplicate node name "${node.name}"`,
        path: "/nodes",
        nodeId: node.name,
      });
    }
    nodeNames.add(node.name);
  }

  // DAG validation
  const cycle = detectCycle(def);
  if (cycle) {
    diagnostics.push({
      code: "WORKFLOW_CYCLE",
      severity: "error",
      message: `Workflow contains a cycle: ${cycle.join(" -> ")}`,
      path: "/connections",
    });
  }

  // Port validation
  diagnostics.push(...validatePorts(def));

  return diagnostics;
}
