import type { Position, WorkflowDef, WorkflowNode, WireWorkflowDef, WireWorkflowNode } from "./index";

/**
 * The editor-only sibling of a wire `WorkflowDef` (ADR-D4 §2.2-§2.3): visual
 * node positions, canvas viewport, per-node UI theme/configuration, and
 * author notes. Mirrors Go's `types.WorkflowEditorMetadata`
 * (`types/editor_metadata.go`) field for field, including the lack of a
 * `pinData` field (ADR-D4 "Deviations" item 1: `pin_data` has no runtime
 * consumer, so it is read directly off the returned `WorkflowDef.pin_data`
 * rather than cached here too).
 *
 * Every node-keyed map (`positions`, `ui`, `notes`) is keyed by the stable
 * node identity: `WorkflowNode.id` when the node has one, else
 * `WorkflowNode.name` (ADR §2.4).
 */
export interface WorkflowEditorMetadata {
  positions?: Record<string, Position>;
  viewport?: Viewport;
  ui?: Record<string, unknown>;
  notes?: Record<string, string>;
}

/** The editor canvas pan/zoom state. Mirrors Go's `types.Viewport`. */
export interface Viewport {
  x?: number;
  y?: number;
  zoom?: number;
}

/**
 * Diagnostic code emitted when an editor metadata key is resolved against a
 * node that has no `id`, falling back to `name`. Matches Go's
 * `types.DiagNodeMetadataKeyedByName`.
 */
export const NODE_METADATA_KEYED_BY_NAME = "NODE_METADATA_KEYED_BY_NAME";

function nodeIdentityKeys(nodes: WorkflowNode[] | undefined): { idKeys: Set<string>; nameKeys: Set<string> } {
  const idKeys = new Set<string>();
  const nameKeys = new Set<string>();
  for (const node of nodes ?? []) {
    if (node.id) {
      idKeys.add(node.id);
    } else if (node.name) {
      nameKeys.add(node.name);
    }
  }
  return { idKeys, nameKeys };
}

/**
 * Checks a metadata key against a definition's nodes (ADR §2.4), mirroring
 * Go's `types.ValidateEditorMetadata`:
 * - a key matching a node by `id` is kept silently;
 * - a key matching a node by `name` (that node has no `id`) is kept, and a
 *   `NODE_METADATA_KEYED_BY_NAME` diagnostic is emitted (once per key);
 * - a key matching no node is dropped silently, with no diagnostic.
 */
function checkKeyFactory(
  idKeys: Set<string>,
  nameKeys: Set<string>,
  diagnostics: string[],
  warnedNames: Set<string>
): (key: string) => boolean {
  return (key: string): boolean => {
    if (idKeys.has(key)) return true;
    if (nameKeys.has(key)) {
      if (!warnedNames.has(key)) {
        warnedNames.add(key);
        diagnostics.push(
          `${NODE_METADATA_KEYED_BY_NAME}: editor metadata key "${key}" is keyed by node name because the node has no stable id; ` +
            "renaming or duplicating the node may cross-contaminate its metadata"
        );
      }
      return true;
    }
    return false;
  };
}

function filterByKey<T>(
  source: Record<string, T> | undefined,
  checkKey: (key: string) => boolean
): Record<string, T> | undefined {
  if (!source || Object.keys(source).length === 0) return undefined;
  const out: Record<string, T> = {};
  for (const [key, value] of Object.entries(source)) {
    if (checkKey(key)) out[key] = value;
  }
  return Object.keys(out).length > 0 ? out : undefined;
}

function nodeKey(node: WorkflowNode): string | undefined {
  return node.id ?? node.name;
}

/**
 * Splits an editor-side `WorkflowDef` (nodes may carry `position`/`ui`/`notes`
 * inline) into the wire shape plus its `WorkflowEditorMetadata` sibling (ADR
 * §2.5). `pin_data` and every runtime-semantic field are left untouched on
 * `def`.
 *
 * Diagnostics mirror `types.ValidateEditorMetadata`'s `NODE_METADATA_KEYED_BY_NAME`
 * warnings: a node with no `id` contributes its position/ui/notes keyed by
 * `name`, with one diagnostic per such node that actually has metadata to carry.
 */
export function splitEditorMetadata(def: WorkflowDef): {
  def: WireWorkflowDef;
  metadata: WorkflowEditorMetadata;
  diagnostics: string[];
} {
  const diagnostics: string[] = [];
  const positions: Record<string, Position> = {};
  const ui: Record<string, unknown> = {};
  const notes: Record<string, string> = {};
  const warnedNames = new Set<string>();

  const wireNodes: WireWorkflowNode[] = (def.nodes ?? []).map((node) => {
    const { position, ui: nodeUi, notes: nodeNotes, ...wireNode } = node;
    const key = nodeKey(node);
    if (key) {
      let hasMetadata = false;
      if (position !== undefined) {
        positions[key] = position;
        hasMetadata = true;
      }
      if (nodeUi !== undefined) {
        ui[key] = nodeUi;
        hasMetadata = true;
      }
      if (nodeNotes !== undefined) {
        notes[key] = nodeNotes;
        hasMetadata = true;
      }
      if (hasMetadata && !node.id && node.name && !warnedNames.has(key)) {
        warnedNames.add(key);
        diagnostics.push(
          `${NODE_METADATA_KEYED_BY_NAME}: editor metadata key "${key}" is keyed by node name because the node has no stable id; ` +
            "renaming or duplicating the node may cross-contaminate its metadata"
        );
      }
    }
    return wireNode;
  });

  const metadata: WorkflowEditorMetadata = {};
  if (Object.keys(positions).length > 0) metadata.positions = positions;
  if (Object.keys(ui).length > 0) metadata.ui = ui;
  if (Object.keys(notes).length > 0) metadata.notes = notes;

  const { nodes: _droppedNodes, ...restOfDef } = def;
  return {
    def: { ...restOfDef, nodes: wireNodes },
    metadata,
    diagnostics: diagnostics.sort()
  };
}

/**
 * Merges a wire `WorkflowDef` with its `WorkflowEditorMetadata` sibling into
 * the editor's internal (merged) model, restoring `position`/`ui`/`notes`
 * onto each node keyed by `id` (or `name` as a fallback). `pin_data` is left
 * untouched. Mirrors `types.ValidateEditorMetadata`'s key resolution and
 * `NODE_METADATA_KEYED_BY_NAME` diagnostics.
 *
 * An absent `metadata` merges to the same `def`, converted to the editor
 * shape, with no diagnostics.
 */
export function mergeEditorMetadata(
  def: WireWorkflowDef,
  metadata: WorkflowEditorMetadata | undefined
): { def: WorkflowDef; diagnostics: string[] } {
  const nodes: WorkflowNode[] = def.nodes ?? [];
  if (!metadata) {
    return { def: { ...def, nodes }, diagnostics: [] };
  }

  const { idKeys, nameKeys } = nodeIdentityKeys(nodes);
  const diagnostics: string[] = [];
  const warnedNames = new Set<string>();
  const checkKey = checkKeyFactory(idKeys, nameKeys, diagnostics, warnedNames);

  const positions = filterByKey(metadata.positions, checkKey);
  const ui = filterByKey(metadata.ui, checkKey);
  const notes = filterByKey(metadata.notes, checkKey);

  const mergedNodes = nodes.map((node) => {
    const key = nodeKey(node);
    if (!key) return node;
    const position = positions?.[key];
    const nodeUi = ui?.[key];
    const nodeNotes = notes?.[key];
    if (position === undefined && nodeUi === undefined && nodeNotes === undefined) return node;
    return {
      ...node,
      ...(position !== undefined ? { position } : {}),
      ...(nodeUi !== undefined ? { ui: nodeUi as Record<string, unknown> } : {}),
      ...(nodeNotes !== undefined ? { notes: nodeNotes } : {})
    };
  });

  return {
    def: { ...def, nodes: mergedNodes },
    diagnostics: diagnostics.sort()
  };
}
