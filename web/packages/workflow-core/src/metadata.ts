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
 *
 * Callers that need deterministic stable keys should run `migrateNodeIds`
 * first so that every node has an `id`.
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
 * Normalize a node name into a URL-safe, deterministic slug.
 *
 * The slug is used as the human-readable part of a generated stable id.
 * Collisions are impossible at generation time because the index is also
 * included; the slug only makes the id recognizable.
 */
function slugify(name: string): string {
  return (
    name
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-|-$/g, "") || "node"
  );
}

/**
 * Generate a deterministic stable id for a node that lacks one or that
 * participates in a duplicate-id collision.
 *
 * The id is derived from the node's index in the `nodes` array and its
 * `name` (or "node" if no name is present). Because the index is unique,
 * generated ids never collide with each other. In the extremely unlikely
 * case that a generated id collides with an existing id that is being
 * preserved, a deterministic counter suffix is appended.
 */
function generateStableId(
  node: NodeDef,
  index: number,
  reserved: Set<string>
): string {
  const nameSlug = node.name && node.name.length > 0 ? slugify(node.name) : "node";
  const candidates = [
    `node-${index}-${nameSlug}`,
    `node-${index}-${nameSlug}-migrated`,
  ];
  for (const candidate of candidates) {
    if (!reserved.has(candidate)) {
      reserved.add(candidate);
      return candidate;
    }
  }
  let counter = 1;
  while (true) {
    const candidate = `node-${index}-${nameSlug}-migrated-${counter}`;
    if (!reserved.has(candidate)) {
      reserved.add(candidate);
      return candidate;
    }
    counter++;
  }
}

/**
 * Migrate missing or duplicate `NodeDef.ID` values to deterministic stable
 * ids.
 *
 * Rules:
 * - A node with no `id` (empty or absent) gets a generated stable id.
 * - A node whose `id` is shared by two or more nodes gets a generated stable
 *   id; all colliding nodes are migrated so metadata can be keyed uniquely.
 * - A node with a unique, non-empty `id` is preserved unchanged.
 *
 * Diagnostics:
 * - `NODE_METADATA_ID_MIGRATED` per node that had a missing id.
 * - `NODE_METADATA_DUPLICATE_IDS_MIGRATED` per node that had a duplicate id.
 *
 * This is an immutable update.
 */
function migrateNodeIds(nodes: NodeDef[]): {
  nodes: NodeDef[];
  diagnostics: Diagnostic[];
} {
  const diagnostics: Diagnostic[] = [];

  // Count how many times each id appears so we can detect duplicates.
  const idCounts = new Map<string, number>();
  for (const node of nodes) {
    if (node.id && node.id.length > 0) {
      idCounts.set(node.id, (idCounts.get(node.id) ?? 0) + 1);
    }
  }

  // Decide which nodes need migration and reserve ids that will be kept.
  const reserved = new Set<string>();
  const needsMigration = nodes.map((node) => {
    if (!node.id || node.id.length === 0) {
      return true;
    }
    return (idCounts.get(node.id) ?? 0) > 1;
  });

  for (const [index, node] of nodes.entries()) {
    if (!needsMigration[index] && node.id) {
      reserved.add(node.id);
    }
  }

  const migratedNodes = nodes.map((node, index) => {
    if (!needsMigration[index]) {
      return node;
    }

    const hadId = node.id && node.id.length > 0;
    const newId = generateStableId(node, index, reserved);
    const code = hadId
      ? "NODE_METADATA_DUPLICATE_IDS_MIGRATED"
      : "NODE_METADATA_ID_MIGRATED";
    const message = hadId
      ? `node "${node.name ?? ""}" at index ${index} had duplicate id "${node.id}"; assigned deterministic id "${newId}"`
      : `node "${node.name ?? ""}" at index ${index} had no stable id; assigned deterministic id "${newId}"`;

    diagnostics.push({
      code,
      severity: "warning",
      message,
      path: `nodes[${index}]`,
      nodeId: newId,
    });

    return { ...node, id: newId };
  });

  return { nodes: migratedNodes, diagnostics };
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
 * Missing or duplicate `NodeDef.ID` values are migrated to deterministic
 * stable ids before metadata is keyed. Diagnostics are emitted for every
 * migrated node:
 * - `NODE_METADATA_ID_MIGRATED` for nodes that lacked an id.
 * - `NODE_METADATA_DUPLICATE_IDS_MIGRATED` for nodes that had a duplicate id.
 *
 * This is an immutable update.
 */
export function splitEditorMetadata(def: WorkflowDef): {
  def: WorkflowDef;
  metadata: WorkflowEditorMetadata;
  diagnostics: Diagnostic[];
} {
  const { nodes: migratedNodes, diagnostics: migrationDiags } = migrateNodeIds(
    def.nodes ?? []
  );

  const positions: Record<string, Position> = {};
  const ui: Record<string, Record<string, unknown>> = {};
  const notes: Record<string, string> = {};
  const strippedNodes: NodeDef[] = [];

  for (const node of migratedNodes) {
    const key = nodeMetadataKey(node);
    const { position, ui: nodeUi, notes: nodeNotes, ...rest } = node;

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
    diagnostics: migrationDiags,
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
 *
 * Missing or duplicate `NodeDef.ID` values are migrated to the same
 * deterministic stable ids used by `splitEditorMetadata` before metadata is
 * looked up, so split/merge round-trips are stable. Diagnostics are emitted
 * for every migrated node.
 */
export function mergeEditorMetadata(
  def: WorkflowDef,
  metadata: WorkflowEditorMetadata
): { def: WorkflowDef; diagnostics: Diagnostic[] } {
  const { nodes: migratedNodes, diagnostics: migrationDiags } = migrateNodeIds(
    def.nodes ?? []
  );

  const mergedNodes: NodeDef[] = [];
  const diagnostics: Diagnostic[] = [...migrationDiags];

  for (const [index, node] of migratedNodes.entries()) {
    const idKey = node.id && node.id.length > 0 ? node.id : undefined;
    const nameKey = node.name && node.name.length > 0 ? node.name : undefined;

    // Prefer stable-id-keyed metadata. If nothing is found under the stable id,
    // fall back to the legacy name-keyed metadata so old persisted editor
    // metadata is not silently lost.
    const hasIdMetadata =
      idKey &&
      (metadata.positions?.[idKey] !== undefined ||
        metadata.ui?.[idKey] !== undefined ||
        metadata.notes?.[idKey] !== undefined);

    let key: string | undefined;
    let usedNameFallback = false;
    if (hasIdMetadata) {
      key = idKey;
    } else if (
      nameKey &&
      (metadata.positions?.[nameKey] !== undefined ||
        metadata.ui?.[nameKey] !== undefined ||
        metadata.notes?.[nameKey] !== undefined)
    ) {
      key = nameKey;
      usedNameFallback = true;
    }

    const extra: Partial<NodeDef> = {};
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

    if (usedNameFallback) {
      diagnostics.push({
        code: "NODE_METADATA_NAME_KEY_FALLBACK",
        severity: "warning",
        message: `node "${node.name ?? ""}" at index ${index} had no metadata under stable id "${idKey ?? ""}"; restored editor metadata keyed by name "${nameKey ?? ""}"`,
        path: `nodes[${index}]`,
        nodeId: idKey,
      });
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
