# ADR D4: Runtime / Editor Metadata Split

| Item | Value |
|------|-------|
| Status | Accepted / Partially Implemented — Go side complete; TypeScript conversion layer and server storage not yet implemented |
| Owner | xflow team |
| Related | `types/workflow.go`, `backend/workflowhash/`, `node/builtin_specs.go` |

## Implementation Status

**Go side (implemented):**
- Runtime hash (`workflowhash.Runtime`) and its payload mirrors live in `backend/workflowhash/workflowhash.go`; every registration path (SDK Engine, HTTP `POST`/`PUT /v1/workflows`, embedded `Server.AddWorkflow`/`ReplaceWorkflow`) hashes through it. `sdk/xflow/workflow_identity.go` keeps thin wrappers.
- Editor fields (`NodeDef.Position`, `NodeDef.UI`, `NodeDef.Notes`, `NodeDef.ID`) excluded from runtime hash per §2.1–§2.2
- `Groups` and `DependencyEdges` included in runtime hash via `canonicalizeGroups`/`canonicalizeDependencyEdges`
- Separate audit fingerprint (`workflowhash.Audit`), recorded as `WorkflowRecord.AuditFingerprint` on every path
- Hash-local `runtimeSelectorHashPayload` mirror with frozen pre-§9.4 tags to decouple wire rename from hash
- Canonical builtin defaults before hashing (§3.1): `workflowhash.Canonical` over `node.BuiltinParamSpecs`
- Legacy-hash reconcile with CAS upgrade (`workflowhash.Reconcile` / `ReconcileAdd`), shared by the SDK and the apiserver

**Not yet implemented:**
- `WorkflowEditorMetadata` Go struct (§2.3) — zero hits in codebase
- TypeScript `splitEditorMetadata` / `mergeEditorMetadata` (§2.5) — not in `web/packages/xflow-core/`; `web/packages/workflow-core/` referenced in the Related table does not exist (package is `xflow-core`)
- Go server storage types `WorkflowDraft` / `WorkflowDefinitionVersion` (§4) — zero hits in codebase

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

- Prefix: `runtime-sha256:v1:` for a definition that sets no node `Timeout` and no private node `Output`; `runtime-sha256:v2:` otherwise. v2 added those two node fields to the hash; a definition that sets neither hashes to the same bytes under both, so it keeps its v1 hash.
- Excludes: `WorkflowDef.ID`, `WorkflowDef.Description`, `NodeDef.ID`, `NodeDef.Position`, `NodeDef.UI`, `NodeDef.Notes`.
- Includes: everything else, including `WorkflowDef.PinData` and the runtime subset of each node (with `NodeDef.Timeout` and `NodeDef.Output` since v2). Re-registering the same `name@version` with only a node timeout or output policy changed is therefore a conflict.

A separate audit fingerprint (`sha256:audit:v1:`) is computed over the full `WorkflowDef` JSON (including editor metadata) for export/audit traceability. It must NOT be used for conflict detection.

### 3.1 Canonical builtin defaults

The runtime hash is taken over a canonical form of the definition in which every **builtin** node's missing or `null` parameter is filled with its `ParamSpec.Default`. An omitted builtin parameter and the same parameter spelled out as its Default are therefore one runtime identity, whichever path registered the workflow: the SDK builder writes Defaults into the definition, while the HTTP path stores the body as sent.

- The canonical form exists only in memory. The stored definition is never modified, and execution still reads the stored definition.
- The fill rule mirrors the SDK builder's: a missing or `null` parameter is filled; an explicit `""` is kept; only top-level ParamSpecs are filled; a body-bearing parent (`params.body` is an `xflow.subgraph`) is skipped while its body members are filled; disabled nodes are filled; lookup is kind-aware (action, trigger, supply).
- Only builtin node types are canonicalized, from a fixed table (`node.BuiltinParamSpecs`), never from the live node registry or runner-reported descriptors. The hash must not depend on what a process registered, and "omitted equals Default" is proven only for builtins, whose Defaults equal their handler fallbacks. A custom node type with a Default therefore stays path-sensitive: the SDK form and the same body with that parameter omitted conflict.
- A builtin `ParamSpec.Default` is part of hash identity. Changing one is a hash-format change and needs a prefix bump.
- On an SDK build the canonical form is the identity (the builder already wrote those Defaults), so no SDK hash changed. Records hashed before this (bare `sha256:` hashes written by the HTTP path) are recomputed on their next registration and CAS-upgraded to the `runtime-sha256:` form; there is no migration pass.
- Registration through `POST /v1/workflows` or the embedded `AddWorkflow` is idempotent on the runtime identity: a definition that differs from the stored one only in editor metadata (§2.2) or by omitted builtin Defaults returns the existing workflow id and leaves the stored definition unchanged. Replace no-op checks (`PUT /v1/workflows/{id}`, embedded `ReplaceWorkflow`) compare audit fingerprints instead, so a metadata-only change is still written as a new revision.

## 4. Consequences

- Moving or restyling a node no longer changes the runtime hash or triggers a version conflict.
- Re-importing a workflow with newly generated `NodeDef.ID` values keeps the same runtime identity.
- The server stores `WorkflowDraft`/`WorkflowDefinitionVersion` as `{ definition, editorMetadata }`, so editor state survives server-side round-trips without polluting the runtime contract. **（planned — not yet implemented）**
- Callers must not rely on `WorkflowEditorMetadata.pinData` as authoritative; `WorkflowDef.pin_data` is always the source of truth.
