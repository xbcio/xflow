# ADR D4: Runtime / Editor Metadata Split

| Item | Value |
|------|-------|
| Status | Accepted / Implemented (F0-A2) |
| Owner | xflow team |
| Related | `types/workflow.go`, `types/workflow_management.go`, `sdk/xflow/workflow_identity.go`, `web/packages/workflow-core/src/metadata.ts`, `web/packages/workflow-core/src/types.ts` |

## 1. Context

`WorkflowDef` is the canonical workflow definition structure used by both the runtime engine and the visual editor. Historically, editor-only fields (visual position, UI theme, notes) lived inside `NodeDef`, which caused two problems:

1. **Runtime identity drift** — moving a node on the canvas changed the definition hash and forced a new workflow version.
2. **Metadata loss on round-trip** — the server had no place to store editor metadata without making it part of the runtime contract.

This ADR defines the split between runtime-semantic fields and editor-only metadata, and how the two representations are converted, keyed, and hashed.

## 2. Decision

Introduce a separate `WorkflowEditorMetadata` structure that lives alongside the runtime definition. The runtime definition keeps only fields that can affect execution output. Editor metadata is keyed by the stable editor identity of each node (`NodeDef.ID`, with a backward-compatible fallback to `NodeDef.Name`).

### 2.1 Runtime-semantic fields

These fields remain in `WorkflowDef` and participate in the runtime hash:

- Top-level: `namespace`, `name`, `version`, `spec`, `runnerSelector`, `context`, `settings`, `options`, `credentials`, `params`, `node_templates`, `connections`, `outputs`, `pin_data`.
- Per node (`NodeDef`): `name`, `type`, `kind`, `version`, `template`, `disabled`, `on_error`, `runnerSelector`, `inputs`, `output_schema`, `parameters`, `retry`.

`pin_data` is runtime-semantic because it fixes node outputs and therefore affects execution behavior. It is retained in `WorkflowDef` and included in the runtime hash.

### 2.2 Editor-only fields

These fields are extracted into `WorkflowEditorMetadata` and excluded from the runtime hash:

- `WorkflowDef.description` — human documentation, no execution effect.
- `NodeDef.id` — durable editor-assigned handle. Re-importing a workflow must not invalidate its registry record just because the editor assigned a different stable ID.
- `NodeDef.position` — visual coordinates on the canvas.
- `NodeDef.ui` — node-level UI theme/configuration.
- `NodeDef.notes` — free-text author notes.

### 2.3 `WorkflowEditorMetadata` schema

```go
type WorkflowEditorMetadata struct {
    Positions map[string]Position `json:"positions,omitempty"`
    Viewport  *Viewport           `json:"viewport,omitempty"`
    UI        map[string]any      `json:"ui,omitempty"`
    Notes     map[string]string   `json:"notes,omitempty"`
    // Read-only derived cache of WorkflowDef.PinData for UI display convenience.
    // WorkflowDef.PinData remains authoritative.
    PinData   map[string]any      `json:"pinData,omitempty"`
}

type Viewport struct {
    X    float64 `json:"x,omitempty"`
    Y    float64 `json:"y,omitempty"`
    Zoom float64 `json:"zoom,omitempty"`
}
```

All node-level maps (`positions`, `ui`, `notes`, `pinData`) are keyed by the stable node identity as defined in §2.4.

### 2.4 Metadata key: stable node ID

Metadata keys MUST use `NodeDef.ID` when present. If `NodeDef.ID` is absent, the implementation falls back to `NodeDef.Name` for backward compatibility. A diagnostic `NODE_METADATA_KEYED_BY_NAME` is emitted so hosts can warn authors that renaming or duplicating a node may silently cross-contaminate metadata.

### 2.5 Conversion functions

The TypeScript implementation provides two inverses:

- `splitEditorMetadata(def)` — returns `{ def, metadata, diagnostics }`. The returned `def` has `position`, `ui`, and `notes` stripped from each node; `pin_data` is left in place. `metadata.pinData` is populated as a read-only view of `def.pin_data`.
- `mergeEditorMetadata(def, metadata)` — returns `{ def, diagnostics }`. It restores `position`, `ui`, and `notes` onto nodes keyed by `NodeDef.ID` (or `NodeDef.Name`). It intentionally does NOT overwrite `def.pin_data` with `metadata.pinData`; the runtime field remains canonical.

## 3. Runtime Hash

The canonical runtime hash is computed over the runtime-semantic subset only:

- Prefix: `runtime-sha256:v1:`.
- Excludes: `WorkflowDef.ID`, `WorkflowDef.TenantID`, `WorkflowDef.Description`, `NodeDef.ID`, `NodeDef.Position`, `NodeDef.UI`, `NodeDef.Notes`.
- Includes: everything else, including `WorkflowDef.PinData` and the runtime subset of each node.

A separate audit fingerprint (`sha256:audit:v1:`) is computed over the full `WorkflowDef` JSON (including editor metadata) for export/audit traceability. It must NOT be used for conflict detection.

## 4. Consequences

- Moving or restyling a node no longer changes the runtime hash or triggers a version conflict.
- Re-importing a workflow with newly generated `NodeDef.ID` values keeps the same runtime identity.
- The server stores `WorkflowDraft`/`WorkflowDefinitionVersion` as `{ definition, editorMetadata }`, so editor state survives server-side round-trips without polluting the runtime contract.
- Callers must not rely on `WorkflowEditorMetadata.pinData` as authoritative; `WorkflowDef.pin_data` is always the source of truth.
