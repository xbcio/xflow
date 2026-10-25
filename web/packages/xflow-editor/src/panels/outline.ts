import type { NodeCategory, WorkflowDef, WorkflowEditorSection } from "@xflow/core";
import { categoryOf } from "@xflow/core";
import { connectionTypeFor, nodeKey, nodeLabel, nodeName, targetsFor } from "./workflowOps";

/**
 * The outline's rows (spec §6.0.1): the workflow's nodes in execution order,
 * grouped under derived sections -- 入口 / 流程 / 结束 -- or, when the workflow
 * carries visual canvas sections (`editor_metadata.sections`), under those
 * author-named sections with the derived ones filling in around them.
 *
 * Order. The graph is walked depth-first from every node that has no incoming
 * data edge, and a node is placed only once every predecessor has been placed,
 * so a merge lands after the last branch that feeds it. Edges that close a
 * cycle (they point at a node still on the walk's path) are not part of the
 * order; nodes the walk never reaches (a cycle with no way in) are appended in
 * array order. Every node appears exactly once.
 *
 * Indent. A node with more than one distinct successor, or successors on more
 * than one port, opens a branch: its successors sit one level deeper. A
 * straight chain keeps its level, and a merge steps back out one level of its
 * shallowest incoming branch. When a node has several output ports, each port
 * with a node placed under it gets a heading row naming it.
 *
 * Sections. 入口 holds trigger-category nodes and nodes with no incoming data
 * edge; 结束 holds the rest that have no outgoing data edge, or are
 * `xflow.end`; 流程 holds everything else. A section with no member has no row.
 * Dependency edges are not data flow and take no part in any of this.
 *
 * Visual sections (§6.2 / WB-02) are read from the third argument. When at
 * least one of them covers a live node, a node listed by a visual section
 * moves under that section's title (the first section listing it wins, so no
 * node is drawn twice); a node no section lists stays in its derived bucket.
 * Groups keep topological order: a group appears where its first member is
 * met. With no sections, or none covering a node, the rows are exactly the
 * derived ones -- the pre-sections behaviour.
 */

export type OutlineSectionId = "entry" | "middle" | "end";

export interface OutlineSectionRow {
  kind: "section";
  id: string;
  /** A derived bucket, or `visual` for an author-defined canvas section. */
  section: OutlineSectionId | "visual";
  /** The stored section id (`WorkflowEditorSection.id`) when `section` is `visual`. */
  sectionId?: string;
  label: string;
  /** Items in the section. */
  count: number;
  /** The category most of the section's nodes belong to; the first seen wins a tie. */
  category: NodeCategory;
}

/** A heading naming the output port the nodes below it hang from. */
export interface OutlinePortRow {
  kind: "port";
  id: string;
  port: string;
  indent: number;
}

export interface OutlineItemRow {
  kind: "item";
  id: string;
  /** The key canvas selection and `fitNode` use (the node's id, else its name). */
  key: string;
  name: string;
  label: string;
  type: string;
  category: NodeCategory;
  /** Branch depth. 0 is flush; anything deeper is drawn at the one indent step. */
  indent: number;
  disabled: boolean;
}

export type OutlineRow = OutlineSectionRow | OutlinePortRow | OutlineItemRow;

const sectionOrder: readonly OutlineSectionId[] = ["entry", "middle", "end"];

const sectionLabel: Record<OutlineSectionId, string> = {
  entry: "入口",
  middle: "流程",
  end: "结束"
};

/** The category most of `members` belong to; the first seen wins a tie. */
function dominantCategory(
  members: readonly number[],
  categoryOfNode: (index: number) => NodeCategory
): NodeCategory {
  const tally = new Map<NodeCategory, number>();
  for (const index of members) {
    const category = categoryOfNode(index);
    tally.set(category, (tally.get(category) ?? 0) + 1);
  }
  let category: NodeCategory = "unknown";
  let best = 0;
  for (const [candidate, count] of tally) {
    if (count > best) {
      best = count;
      category = candidate;
    }
  }
  return category;
}

interface Edge {
  port: string;
  to: number;
}

interface Placement {
  index: number;
  depth: number;
  /** The port heading to draw right above this node. */
  port?: string;
}

function isEndType(type: string | undefined): boolean {
  return type === "xflow.end" || type?.startsWith("xflow.end.") === true;
}

export function buildOutline(
  workflow: WorkflowDef,
  nodeCategories?: Readonly<Record<string, string>>,
  sections?: readonly WorkflowEditorSection[]
): OutlineRow[] {
  const nodes = workflow.nodes ?? [];
  if (nodes.length === 0) return [];
  const categoryOfNode = (index: number): NodeCategory =>
    categoryOf(nodes[index].type, nodes[index].type ? nodeCategories?.[nodes[index].type] : undefined);

  // Connections are keyed by node name; a repeated name resolves to its first node.
  const indexByName = new Map<string, number>();
  nodes.forEach((node, index) => {
    const name = nodeName(node, index);
    if (!indexByName.has(name)) indexByName.set(name, index);
  });

  const out: Edge[][] = nodes.map(() => []);
  const hasIn: boolean[] = nodes.map(() => false);
  for (const [sourceName, ports] of Object.entries(workflow.connections ?? {})) {
    const from = indexByName.get(sourceName);
    if (from === undefined) continue;
    for (const [port, connections] of Object.entries(ports)) {
      if (connectionTypeFor(connections) !== "data") continue;
      for (const target of targetsFor(connections)) {
        const to = target.node === undefined ? undefined : indexByName.get(target.node);
        if (to === undefined) continue;
        out[from].push({ port, to });
        hasIn[to] = true;
      }
    }
  }

  // Edges that close a cycle, found by a walk from the roots (gray = on the path).
  const roots = nodes.map((_, index) => index).filter((index) => !hasIn[index]);
  const color = new Uint8Array(nodes.length);
  const back = new Set<Edge>();
  // Iterative on purpose: a long chain must not overflow the call stack.
  const classify = (start: number): void => {
    const stack: { u: number; next: number }[] = [{ u: start, next: 0 }];
    color[start] = 1;
    while (stack.length > 0) {
      const frame = stack[stack.length - 1];
      const edge = out[frame.u][frame.next];
      if (edge === undefined) {
        color[frame.u] = 2;
        stack.pop();
        continue;
      }
      frame.next += 1;
      if (color[edge.to] === 1) back.add(edge);
      else if (color[edge.to] === 0) {
        color[edge.to] = 1;
        stack.push({ u: edge.to, next: 0 });
      }
    }
  };
  for (const root of roots) if (color[root] === 0) classify(root);

  // A node is ready once all of its reachable predecessors are placed.
  const preds: Set<number>[] = nodes.map(() => new Set<number>());
  out.forEach((edges, from) => {
    if (color[from] === 0) return;
    for (const edge of edges) if (!back.has(edge)) preds[edge.to].add(from);
  });
  const pending = preds.map((set) => set.size);

  const placed: Placement[] = [];
  const done: boolean[] = nodes.map(() => false);
  const depthOf: number[] = nodes.map(() => 0);
  const place = (index: number, depth: number, port?: string): void => {
    done[index] = true;
    depthOf[index] = depth;
    placed.push({ index, depth, port });
  };
  interface Frame {
    u: number;
    edges: Edge[];
    next: number;
    childDepth: number;
    ports: number;
    counted: Set<number>;
    headed: Set<string>;
  }
  const open = (u: number): Frame => {
    const edges = out[u].filter((edge) => !back.has(edge));
    const portCount = new Set(edges.map((edge) => edge.port)).size;
    const targetCount = new Set(edges.map((edge) => edge.to)).size;
    return {
      u,
      edges,
      next: 0,
      childDepth: depthOf[u] + (portCount > 1 || targetCount > 1 ? 1 : 0),
      ports: portCount,
      counted: new Set<number>(),
      headed: new Set<string>()
    };
  };
  // Depth-first without recursion: a placed node's own expansion runs before
  // its parent's next edge, exactly as the recursive walk did.
  const expand = (start: number): void => {
    const stack: Frame[] = [open(start)];
    while (stack.length > 0) {
      const frame = stack[stack.length - 1];
      const edge = frame.edges[frame.next];
      if (edge === undefined) {
        stack.pop();
        continue;
      }
      frame.next += 1;
      // Two ports into one node are one predecessor, not two.
      if (frame.counted.has(edge.to)) continue;
      frame.counted.add(edge.to);
      pending[edge.to] -= 1;
      if (pending[edge.to] > 0 || done[edge.to]) continue;
      const merge = preds[edge.to].size > 1;
      const depth = merge
        ? Math.max(0, Math.min(...[...preds[edge.to]].map((pred) => depthOf[pred])) - 1)
        : frame.childDepth;
      // A merge belongs to no single port, so it takes no heading.
      const heading = frame.ports > 1 && !merge && !frame.headed.has(edge.port) ? edge.port : undefined;
      if (heading !== undefined) frame.headed.add(heading);
      place(edge.to, depth, heading);
      stack.push(open(edge.to));
    }
  };
  for (const root of roots) {
    place(root, 0);
    expand(root);
  }
  nodes.forEach((_, index) => {
    if (!done[index]) place(index, 0);
  });

  const sectionOf = (index: number): OutlineSectionId => {
    const node = nodes[index];
    if (categoryOfNode(index) === "trigger" || !hasIn[index]) return "entry";
    if (out[index].length === 0 || isEndType(node.type)) return "end";
    return "middle";
  };

  const itemRow = (index: number, depth: number): OutlineItemRow => {
    const node = nodes[index];
    return {
      kind: "item",
      id: `item:${index}`,
      key: nodeKey(node, index),
      name: nodeName(node, index),
      label: nodeLabel(node, index),
      type: node.type ?? "",
      category: categoryOfNode(index),
      indent: depth,
      disabled: node.disabled ?? false
    };
  };

  const pushMembers = (target: OutlineRow[], members: Placement[]): void => {
    for (const { index, depth, port } of members) {
      if (port !== undefined) target.push({ kind: "port", id: `port:${index}`, port, indent: depth });
      target.push(itemRow(index, depth));
    }
  };

  const rows: OutlineRow[] = [];

  // WB-02: a visual section owns the live nodes it lists. The first section
  // listing a node wins, so a key repeated across sections never draws the
  // node twice; a section covering no live node is not drawn at all (the
  // server drops it, and a hand-written definition can still carry one).
  const liveKeys = new Set(nodes.map((node, index) => nodeKey(node, index)));
  const visualByNode = new Map<string, string>();
  const visuals = new Map<string, { id?: string; label: string }>();
  for (const [index, section] of (sections ?? []).entries()) {
    const keys = Array.isArray(section.node_keys) ? section.node_keys : [];
    const covered: string[] = [];
    for (const key of keys) {
      if (typeof key !== "string" || !liveKeys.has(key) || visualByNode.has(key)) continue;
      covered.push(key);
    }
    if (covered.length === 0) continue;
    const groupKey = `visual:${index}`;
    visuals.set(groupKey, {
      id: section.id,
      label: typeof section.title === "string" && section.title.trim() ? section.title.trim() : "未命名分区"
    });
    for (const key of covered) visualByNode.set(key, groupKey);
  }

  if (visuals.size === 0) {
    // No visual section covers anything: the rows are exactly the pre-sections
    // ones, in the fixed 入口 / 流程 / 结束 order.
    const buckets: Record<OutlineSectionId, Placement[]> = { entry: [], middle: [], end: [] };
    for (const placement of placed) buckets[sectionOf(placement.index)].push(placement);

    for (const section of sectionOrder) {
      const members = buckets[section];
      if (members.length === 0) continue;
      rows.push({
        kind: "section",
        id: `section:${section}`,
        section,
        label: sectionLabel[section],
        count: members.length,
        category: dominantCategory(members.map(({ index }) => index), categoryOfNode)
      });
      pushMembers(rows, members);
    }
    return rows;
  }

  // A group appears where its first member is met, keeping the tree in
  // topological order while never splitting one group into two blocks. An
  // uncovered node keeps its derived bucket, so 入口 / 流程 / 结束 fill in
  // around the author's sections.
  interface Group {
    key: string;
    heading: OutlineSectionRow;
    members: Placement[];
  }
  const groups: Group[] = [];
  const groupByKey = new Map<string, Group>();
  for (const placement of placed) {
    const key = nodeKey(nodes[placement.index], placement.index);
    const visualKey = visualByNode.get(key);
    const groupKey = visualKey ?? sectionOf(placement.index);
    let group = groupByKey.get(groupKey);
    if (!group) {
      const visual = visualKey === undefined ? undefined : visuals.get(visualKey);
      group = {
        key: groupKey,
        heading: visual
          ? {
            kind: "section",
            id: `section:${groupKey}`,
            section: "visual",
            sectionId: visual.id,
            label: visual.label,
            count: 0,
            category: "unknown"
          }
          : {
            kind: "section",
            id: `section:${groupKey}`,
            section: groupKey as OutlineSectionId,
            label: sectionLabel[groupKey as OutlineSectionId],
            count: 0,
            category: "unknown"
          },
        members: []
      };
      groupByKey.set(groupKey, group);
      groups.push(group);
    }
    group.members.push(placement);
  }

  for (const group of groups) {
    group.heading.count = group.members.length;
    group.heading.category = dominantCategory(group.members.map(({ index }) => index), categoryOfNode);
    rows.push(group.heading);
    pushMembers(rows, group.members);
  }
  return rows;
}
