import type {
  Diagnostic,
  NodeDef,
  Position,
  WorkflowDef,
  WorkflowEditorMetadata,
} from "./types";

/**
 * Resolve the editor-metadata key for a node.
 *
 * Per ADR D4: metadata MUST be keyed by `NodeDef.ID` (stable editor
 * identity), falling back to `NodeDef.Name` only when `id` is absent
 * (legacy/backward-compat). Returns `undefined` when neither is present.
 */
function nodeMetadataKey(node: NodeDef): string | undefined {
  if (node.id && node.id.length > 0) {
    return node.id;
  }
  if (node.name && node.name.length > 0) {
    return node.name;
  }
  return undefined;
}

/**
 * Build a fallback-name diagnostic for a node that lacks a stable id.
 *
 * Emitted once per node when `nodeMetadataKey` falls back to `node.name`
 * because `node.id` is absent. The host can surface this to warn authors
 * that editor metadata will be keyed by an unstable identifier and may
 * silently cross-contaminate if the node name is later renamed or
 * duplicated. See ADR D4 / F0-A2.
 */
function fallbackNameDiagnostic(node: NodeDef, index: number): Diagnostic {
  return {
    code: "NODE_METADATA_KEYED_BY_NAME",
    severity: "warning",
    message: `editor metadata for node "${node.name}" is keyed by name because node.id is missing; set a stable id to avoid metadata collisions on rename or duplicate`,
    path: `nodes[${index}]`,
  };
}

/**
 * Split editor metadata from a WorkflowDef.
 *
 * Runtime fields that affect execution semantics — including `pin_data` —
 * REMAIN in the returned def. Only editor-only fields (`position`, `ui`,
 * `notes`) are extracted into `WorkflowEditorMetadata`.
 *
 * `WorkflowEditorMetadata.pinData` is a read-only derived cache populated
 * from `def.pin_data` for UI display convenience; it is NOT the canonical
 * source. See ADR D4 / F0-A2.
 *
 * Metadata keys use `node.id` when present, falling back to `node.name`
 * for backward compatibility. A `NODE_METADATA_KEYED_BY_NAME` diagnostic
 * is emitted once per node that falls back to `node.name`, so hosts can
 * surface the missing-stable-id condition.
 *
 * This is an immutable update.
 */
export function splitEditorMetadata(def: WorkflowDef): {
  def: WorkflowDef;
  metadata: WorkflowEditorMetadata;
  diagnostics: Diagnostic[];
} {
  const positions: Record<string, Position> = {};
  const ui: Record<string, Record<string, unknown>> = {};
  const notes: Record<string, string> = {};
  const strippedNodes: NodeDef[] = [];
  const diagnostics: Diagnostic[] = [];

  for (const [index, node] of (def.nodes ?? []).entries()) {
    const key = nodeMetadataKey(node);
    const { position, ui: nodeUi, notes: nodeNotes, ...rest } = node;

    // Diagnostic: id absent but name present → metadata keyed by name.
    // Fires exactly once per node that falls back, regardless of how
    // many editor-only fields it carries.
    if ((!node.id || node.id.length === 0) && node.name && node.name.length > 0) {
      diagnostics.push(fallbackNameDiagnostic(node, index));
    }

    if (key) {
      if (position) {
        positions[key] = position;
      }
      if (nodeUi) {
        ui[key] = nodeUi;
      }
      if (nodeNotes) {
        notes[key] = nodeNotes;
      }
    }

    strippedNodes.push(rest);
  }

  const metadata: WorkflowEditorMetadata = {
    positions,
    ui,
    notes,
    // Read-only derived cache; def.pin_data remains canonical.
    pinData: def.pin_data,
  };

  // pin_data is runtime-semantic; do NOT strip it from the runtime def.
  return {
    def: {
      ...def,
      nodes: strippedNodes,
    },
    metadata,
    diagnostics,
  };
}

/**
 * Merge editor metadata back into a WorkflowDef.
 *
 * This is the inverse of splitEditorMetadata for editor-only fields
 * (`position`, `ui`, `notes`). It is immutable.
 *
 * `pin_data` is NOT overwritten by `metadata.pinData` — `WorkflowDef.pin_data`
 * is canonical. If `def.pin_data` is absent and `metadata.pinData` is present,
 * the metadata-derived cache is NOT written back; callers must ensure the
 * runtime def already carries `pin_data` (e.g. by not stripping it during
 * split). See ADR D4 / F0-A2.
 */
export function mergeEditorMetadata(
  def: WorkflowDef,
  metadata: WorkflowEditorMetadata
): { def: WorkflowDef; diagnostics: Diagnostic[] } {
  const mergedNodes: NodeDef[] = [];
  const diagnostics: Diagnostic[] = [];

  for (const [index, node] of (def.nodes ?? []).entries()) {
    const key = nodeMetadataKey(node);
    const extra: Partial<NodeDef> = {};

    // Symmetrical diagnostic: a node on the def being merged into
    // metadata lacks a stable id and falls back to name. This catches
    // the case where the caller merges metadata onto a freshly-built
    // def that never went through `splitEditorMetadata`.
    if ((!node.id || node.id.length === 0) && node.name && node.name.length > 0) {
      diagnostics.push(fallbackNameDiagnostic(node, index));
    }

    if (key) {
      if (metadata.positions?.[key]) {
        extra.position = metadata.positions[key];
      }
      if (metadata.ui?.[key]) {
        extra.ui = metadata.ui[key];
      }
      if (metadata.notes?.[key]) {
        extra.notes = metadata.notes[key];
      }
    }
    mergedNodes.push({ ...node, ...extra });
  }

  // Preserve existing def.pin_data (canonical). Do not overwrite with
  // metadata.pinData (read-only derived cache).
  return {
    def: {
      ...def,
      nodes: mergedNodes,
    },
    diagnostics,
  };
}
