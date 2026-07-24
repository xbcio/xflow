import type { Connections, WorkflowDef } from "./types";

/**
 * Rename a node in the workflow definition.
 *
 * This function performs an immutable update (does not mutate the input def).
 *
 * What is updated:
 * - `nodes[].name` where name === oldName
 * - `connections` keys that use oldName as source node
 * - `connections[].node` references where target node === oldName
 *
 * What is NOT updated (boundary / TODO):
 * - Expression references inside `parameters`, `output_schema`, `ui`, `outputs[].value`,
 *   `context.vars/config`, etc. cannot be statically analyzed because they are
 *   `map[string]any` and may contain expressions like `nodes.X.output.Y`.
 * - When Monaco/AST-based expression editor is integrated (M5), we should walk
 *   those values and rewrite references as well.
 */
export function renameNode(
  def: WorkflowDef,
  oldName: string,
  newName: string
): WorkflowDef {
  if (oldName === newName) {
    return def;
  }

  const renamedNodes = (def.nodes ?? []).map((node) => {
    if (node.name === oldName) {
      return { ...node, name: newName };
    }
    return node;
  });

  const renamedConnections: Connections = {};
  for (const [sourceNode, ports] of Object.entries(def.connections ?? {})) {
    const newSourceNode = sourceNode === oldName ? newName : sourceNode;
    renamedConnections[newSourceNode] = {};

    for (const [port, targets] of Object.entries(ports)) {
      renamedConnections[newSourceNode][port] = (targets ?? []).map((target) => ({
        ...target,
        node: target.node === oldName ? newName : target.node,
      }));
    }
  }

  return {
    ...def,
    nodes: renamedNodes,
    connections: renamedConnections,
    // TODO(M5): Rewrite expression references inside parameters/output_schema/ui
    // once we have an AST-aware expression parser.
  };
}
