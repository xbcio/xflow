import { foundationTokens, type WorkflowDef } from "@xflow/core";
import { listConnectionReferences, nodeKey, nodeName } from "./workflowOps";

/**
 * Layered ("Sugiyama-lite") auto layout for the editor's own 自动布局 command.
 *
 * The graph is a DAG in every realistic workflow, but the writer is the user's
 * canvas, so it can hold a cycle: the depth walk marks the nodes on the current
 * path and skips the edge that closes the loop, which keeps the traversal
 * finite instead of hanging on a back edge.
 *
 * x comes from the longest-path depth (one column per depth, each column as
 * wide as its widest node); y from the input order inside the column, each
 * column vertically centred on the tallest one. The result is deterministic:
 * the same workflow always lays out the same way.
 */

export interface NodeBox {
  width: number;
  height: number;
}

export interface LayoutPoint {
  x: number;
  y: number;
}

export interface LayoutOptions {
  gapX?: number;
  gapY?: number;
  originX?: number;
  originY?: number;
}

/**
 * The canvas node card's own box, used when nothing measured it yet. The width is
 * the card's CSS width (`--xflow-node-width`, from core's foundation tokens): it
 * must stay equal to it, or unmeasured cards are laid out narrower than they draw.
 */
export const defaultNodeBox: NodeBox = { width: parseInt(foundationTokens.nodeWidth, 10), height: 64 };
const defaultGapX = 96;
const defaultGapY = 32;
const defaultOrigin = 48;

export function layoutGraph(
  nodes: { key: string; box: NodeBox }[],
  edges: { source: string; target: string }[],
  options: LayoutOptions = {}
): Map<string, LayoutPoint> {
  const gapX = options.gapX ?? defaultGapX;
  const gapY = options.gapY ?? defaultGapY;
  const originX = options.originX ?? defaultOrigin;
  const originY = options.originY ?? defaultOrigin;

  const known = new Set(nodes.map((node) => node.key));
  const outgoing = new Map<string, string[]>();
  const incomingKeys = new Map<string, string[]>();
  for (const node of nodes) {
    outgoing.set(node.key, []);
    incomingKeys.set(node.key, []);
  }
  for (const edge of edges) {
    // An edge to a node that is not in the list (a dangling port target) is
    // ignored rather than inventing a depth-0 column for it.
    if (!known.has(edge.source) || !known.has(edge.target) || edge.source === edge.target) continue;
    outgoing.get(edge.source)?.push(edge.target);
    incomingKeys.get(edge.target)?.push(edge.source);
  }

  const depth = new Map<string, number>();
  const incoming = new Map<string, number>();
  for (const node of nodes) incoming.set(node.key, 0);
  for (const node of nodes) {
    for (const successor of outgoing.get(node.key) ?? []) {
      incoming.set(successor, (incoming.get(successor) ?? 0) + 1);
    }
  }
  // Longest path over the DAG, in topological order.
  const queue = nodes.filter((node) => incoming.get(node.key) === 0).map((node) => node.key);
  for (const key of queue) depth.set(key, 0);
  for (let index = 0; index < queue.length; index += 1) {
    const key = queue[index];
    for (const successor of outgoing.get(key) ?? []) {
      depth.set(successor, Math.max(depth.get(successor) ?? 0, (depth.get(key) ?? 0) + 1));
      const remaining = (incoming.get(successor) ?? 0) - 1;
      incoming.set(successor, remaining);
      if (remaining === 0) queue.push(successor);
    }
  }
  // A cycle leaves its members out of the queue. The edges that close the loop
  // are exactly the ones between those members, so the residual graph becomes a
  // DAG once the members are ordered (an edge pointing back in that order is a
  // back edge and is skipped): every surviving predecessor is then already
  // assigned and a single pass settles the depths.
  const cyclic = nodes.map((node) => node.key).filter((key) => !depth.has(key));
  if (cyclic.length > 0) {
    const order = new Map(cyclic.map((key, index) => [key, index]));
    for (const key of cyclic) {
      const base = (incomingKeys.get(key) ?? []).reduce((best, pred) => {
        if (pred === key) return best;
        if (order.has(pred) && (order.get(pred) ?? 0) >= (order.get(key) ?? 0)) return best;
        return Math.max(best, (depth.get(pred) ?? 0) + 1);
      }, 0);
      depth.set(key, base);
    }
  }

  const layers = new Map<number, { key: string; box: NodeBox }[]>();
  for (const node of nodes) {
    const level = depth.get(node.key) ?? 0;
    const layer = layers.get(level);
    if (layer) layer.push(node);
    else layers.set(level, [node]);
  }

  const levels = [...layers.keys()].sort((a, b) => a - b);
  const columnX = new Map<number, number>();
  let cursorX = originX;
  for (const level of levels) {
    columnX.set(level, cursorX);
    const widest = Math.max(...(layers.get(level) ?? []).map((node) => node.box.width));
    cursorX += widest + gapX;
  }
  const tallest = Math.max(
    ...levels.map((level) =>
      (layers.get(level) ?? []).reduce((total, node, index) => total + node.box.height + (index > 0 ? gapY : 0), 0)
    )
  );

  const positions = new Map<string, LayoutPoint>();
  for (const level of levels) {
    const layer = layers.get(level) ?? [];
    const columnHeight = layer.reduce((total, node, index) => total + node.box.height + (index > 0 ? gapY : 0), 0);
    let cursorY = originY + Math.max(0, (tallest - columnHeight) / 2);
    for (const node of layer) {
      positions.set(node.key, { x: columnX.get(level) ?? originX, y: cursorY });
      cursorY += node.box.height + gapY;
    }
  }
  return positions;
}

/**
 * The same layout for a whole workflow: node keys, edges and boxes come from
 * the definition, and the caller only supplies a measured box per node.
 */
export function layoutWorkflowPositions(
  workflow: WorkflowDef,
  boxOf: (key: string) => NodeBox = () => defaultNodeBox,
  options: LayoutOptions = {}
): Map<string, LayoutPoint> {
  const nodes = (workflow.nodes ?? []).map((node, index) => ({
    key: nodeKey(node, index),
    box: boxOf(nodeKey(node, index))
  }));
  const keyByName = new Map<string, string>();
  (workflow.nodes ?? []).forEach((node, index) => keyByName.set(nodeName(node, index), nodeKey(node, index)));
  const edges = listConnectionReferences(workflow)
    .map((reference) => ({
      source: keyByName.get(reference.sourceName),
      target: keyByName.get(reference.targetName)
    }))
    .filter((edge): edge is { source: string; target: string } => Boolean(edge.source && edge.target));
  return layoutGraph(nodes, edges, options);
}
