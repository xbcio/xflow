import type { NodeDef, Position, WorkflowDef, WorkflowEditorMetadata } from "./types";

/**
 * Split editor metadata from a WorkflowDef.
 *
 * Runtime fields that affect execution semantics remain in the returned def.
 * Position, UI, notes, and pin_data are moved into WorkflowEditorMetadata.
 *
 * This is an immutable update.
 */
export function splitEditorMetadata(def: WorkflowDef): {
  def: WorkflowDef;
  metadata: WorkflowEditorMetadata;
} {
  const positions: Record<string, Position> = {};
  const ui: Record<string, Record<string, unknown>> = {};
  const notes: Record<string, string> = {};
  const strippedNodes: NodeDef[] = [];

  for (const node of def.nodes ?? []) {
    const nodeName = node.name;
    const { position, ui: nodeUi, notes: nodeNotes, ...rest } = node;

    if (nodeName) {
      if (position) {
        positions[nodeName] = position;
      }
      if (nodeUi) {
        ui[nodeName] = nodeUi;
      }
      if (nodeNotes) {
        notes[nodeName] = nodeNotes;
      }
    }

    strippedNodes.push(rest);
  }

  const metadata: WorkflowEditorMetadata = {
    positions,
    ui,
    notes,
    pinData: def.pin_data,
  };

  const restDef = Object.fromEntries(
    Object.entries(def).filter(([key]) => key !== "pin_data")
  ) as WorkflowDef;

  return {
    def: {
      ...restDef,
      nodes: strippedNodes,
    },
    metadata,
  };
}

/**
 * Merge editor metadata back into a WorkflowDef.
 *
 * This is the inverse of splitEditorMetadata. It is immutable.
 */
export function mergeEditorMetadata(
  def: WorkflowDef,
  metadata: WorkflowEditorMetadata
): WorkflowDef {
  const mergedNodes: NodeDef[] = [];

  for (const node of def.nodes ?? []) {
    const nodeName = node.name;
    const extra: Partial<NodeDef> = {};
    if (nodeName) {
      if (metadata.positions?.[nodeName]) {
        extra.position = metadata.positions[nodeName];
      }
      if (metadata.ui?.[nodeName]) {
        extra.ui = metadata.ui[nodeName];
      }
      if (metadata.notes?.[nodeName]) {
        extra.notes = metadata.notes[nodeName];
      }
    }
    mergedNodes.push({ ...node, ...extra });
  }

  return {
    ...def,
    nodes: mergedNodes,
    pin_data: metadata.pinData,
  };
}
