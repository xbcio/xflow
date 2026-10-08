# ADR D4: Runtime / Editor Metadata Split

| Item | Value |
|------|-------|
| Status | Implemented (Go + HTTP API + TypeScript web client/editor). Deferred: `WorkflowDraft`/publish split, `WorkflowDefinitionVersion` history (see "Not yet implemented" below). |
| Owner | xflow team |
| Related | `types/editor_metadata.go`, `types/workflow.go`, `backend/workflowhash/`, `backend/workflow_registry.go`, `backend/providers/distributed/internal/workflowreg/`, `service/apiserver/module_control.go`, `service/apiserver/module_control_editor_metadata.go`, `api/openapi/xflow-v1.yaml`, `node/builtin_specs.go`, `web/packages/xflow-core/src/editorMetadata.ts`, `web/packages/xflow-api/src/index.ts`, `web/packages/xflow-editor/src/index.tsx`, `web/packages/xflow-preview/src/index.tsx` |

## Implementation Status

**Go + HTTP API side (implemented):**
- `types.WorkflowEditorMetadata` (`types/editor_metadata.go`): `{Positions, Viewport, UI, Notes}`. No `PinData` field — see "Deviations from the original decision" below.
- `NodeDef.Position`, `NodeDef.UI`, `NodeDef.Notes` are REMOVED from `types.NodeDef` entirely (not deprecated, not dual-read). `NodeDef.ID` stays — it is the metadata key, not metadata — and `WorkflowDef.Description` stays on `WorkflowDef`. There is no legacy decoding, fallback, or migration of pre-existing Redis records that may have stored these fields inline; `json.Decoder` without `DisallowUnknownFields` silently ignores an inline `position`/`ui`/`notes` key in a request body (ADR §2.2 is enforced by field absence, not by stripping on read).
- `types.ValidateEditorMetadata(def, metadata)` validates metadata keys against the definition's nodes (keyed by `NodeDef.ID`, falling back to `NodeDef.Name`), drops a key that matches no node silently, and emits a `types.DiagNodeMetadataKeyedByName` (`NODE_METADATA_KEYED_BY_NAME`) warning string for a name-fallback match. Surfaced through the same `registrationDiagnostics.Warnings []string` channel as `graph.Compile`'s own compiler warnings (POST/PUT response `data.warnings`).
- Runtime hash (`workflowhash.Runtime`) and its payload mirrors live in `backend/workflowhash/workflowhash.go`; every registration path (SDK Engine, HTTP `POST`/`PUT /v1/workflows`, embedded `Server.AddWorkflow`/`ReplaceWorkflow`) hashes through it. `sdk/xflow/workflow_identity.go` keeps thin wrappers. `runtimeNodeHashPayload` needs no Position/UI/Notes exclusion any more — those fields no longer exist on `NodeDef` to exclude.
- `Groups` and `DependencyEdges` included in runtime hash via `canonicalizeGroups`/`canonicalizeDependencyEdges`.
- `workflowhash.Audit(def, metadata)` takes the editor metadata sibling as an explicit second argument (not a field read off `def`) and fingerprints `{definition, editor_metadata}` as one JSON payload. See "Deviations" for why this changed from a single-argument, bare-`WorkflowDef` fingerprint.
- `backend.WorkflowRecord.EditorMetadata *types.WorkflowEditorMetadata` is a new field threaded through the Redis-backed `workflowreg.Registry`'s `storedWorkflowRecord`/`marshalWorkflowRecordPayload`/`unmarshalWorkflowRecord` (D4). No SQL schema change. A legacy record with no stored `editor_metadata` key decodes to `nil` without error.
- `PUT /v1/workflows/{id}` authorizes as `OpWorkflowDefinitionUpdate` (`"workflowdefinition.update"`), mapped to the same `"workflow"` authz scope as `OpWorkflowRegister` (D5). This was already wired in `service/apiserver/authz.go`/`module_control.go` before this change; this change is what gives the operation its intended behavior (editor-metadata-aware replace) rather than leaving it a draft comment.
- `editor_metadata` is an optional object on the `GET /v1/workflows/{id}` response and the `POST`/`PUT /v1/workflows[/{id}]` request/response bodies, via a wrapper type (`workflowDefinitionWithMetadata` in `service/apiserver/module_control_editor_metadata.go`) rather than a field on `types.WorkflowDef` itself — keeping the runtime-hashed type free of any editor concern (D2). `api/openapi/xflow-v1.yaml` models this as `WorkflowDefWithEditorMetadata` (`allOf` over `WorkflowDef` plus `editor_metadata`), with new `WorkflowEditorMetadata`/`Viewport` schema components.
- A metadata-only PUT is a normal revision: `registry_revision` advances and the runtime conflict hash (`DefinitionHash`) does not change (D3). This reuses the existing audit-fingerprint-based replace no-op check (`sameAuditFingerprint`), now comparing `workflowhash.Audit(def, metadata)` instead of a bare definition hash.
- `MaxEditorMetadataBytes` (1 MiB, `service/apiserver/module_control_editor_metadata.go`) bounds the JSON-encoded size of one `editor_metadata` object. There is no general request body size limit on the workflow registration routes today, so this is a dedicated cap rather than a reused one.
- Hash-local `runtimeSelectorHashPayload` mirror with frozen pre-§9.4 tags to decouple wire rename from hash.
- Canonical builtin defaults before hashing (§3.1): `workflowhash.Canonical` over `node.BuiltinParamSpecs`.
- Legacy-hash reconcile with CAS upgrade (`workflowhash.Reconcile` / `ReconcileAdd`), shared by the SDK and the apiserver.

**TypeScript web client/editor side (implemented):**
- `splitEditorMetadata(def)` / `mergeEditorMetadata(wireDef, metadata)` (§2.5) in `web/packages/xflow-core/src/editorMetadata.ts`, mirroring `types.ValidateEditorMetadata`'s key-resolution semantics (id-keyed kept silently; name-fallback kept plus a byte-identical `NODE_METADATA_KEYED_BY_NAME` diagnostic; unmatched key dropped silently).
- `xflow-core`'s `WorkflowDef`/`WorkflowNode` stay the editor's internal merged model (position/ui/notes inline on nodes, as React Flow and the rest of the editor already expect); new `WireWorkflowNode`/`WireWorkflowDef` types (no position/ui/notes) model the actual wire shape, matching Go's `NodeDef`/`WorkflowDefWithEditorMetadata`.
- `xflow-api`'s `getWorkflow`/`createWorkflow`/`saveWorkflow` convert at the network boundary only: `splitForWire` on write, `mapWorkflowDef` on read. Diagnostics from split/merge are discarded at this boundary — the server independently re-derives and returns the same findings via `RegisterWorkflowResult.warnings` on write.
- Canvas viewport persistence: `XFlowPreview` accepts `defaultViewport`/`onViewportChange` (React Flow's native `defaultViewport` + `onMoveEnd`), threaded through `XFlowEditor` and wired into `xflow-admin`'s workflow detail page, which restores the saved viewport on load and sends the live viewport on save.

**Not yet implemented (deferred, out of this ADR's required scope):**
- `WorkflowDraft` / `WorkflowDefinitionVersion` as separate, independently-addressable storage objects (§4's literal phrasing) — not built. `EditorMetadata` instead lives as a field on the existing `WorkflowRecord`/Redis hash (D4), which is sufficient for everything this ADR requires; see "Deviations" for why a second storage object was not built. A draft/publish split and a definition version history remain future work if a product need for them arises.

## Deviations from the original decision (binding overrides)

The following resolve points the original ADR text left open, or override it outright. Each is a deliberate decision made during Go-side implementation, not an oversight.

1. **No `PinData` field on `WorkflowEditorMetadata`.** The original §2.3 draft included `PinData map[string]any` as "a read-only derived cache of `WorkflowDef.PinData`." It is dropped. `pin_data` has no runtime consumer (see `docs/design/DSL-SPECIFICATION.md`'s pin_data note), so caching it a second place would add split/merge bookkeeping for a field any caller can already read directly off the returned `WorkflowDef.pin_data` in the same response. If `pin_data` later gets a real runtime consumer and a product need for "what was pinned when this was saved" distinct from "what is pinned now," re-add `PinData` then, with that justification.
2. **No backward-compatible inline position/ui/notes.** The fields are removed from `types.NodeDef` outright, with no dual-read, no fallback, and no migration pass for Redis records written before this change. A pre-existing stored `WorkflowRecord.Definition` whose JSON still carries `position`/`ui`/`notes` keys on a node simply decodes those keys into nothing (Go's non-strict `json.Decoder` silently drops unknown fields); it does not error, and it does not resurrect them into `EditorMetadata`. This is a deliberate scope choice: the host accepted no backward-compatibility requirement for this change.
3. **Audit fingerprint signature and payload changed.** `workflowhash.Audit` is now `Audit(def *types.WorkflowDef, md *types.WorkflowEditorMetadata) (string, error)` — not `Audit(def)` reading metadata off `def`. Its payload is `{"definition": ..., "editor_metadata": ...}`, not a bare `json.Marshal(def)`. This is necessary because `WorkflowEditorMetadata` is never a field of `types.WorkflowDef`: there is nowhere on `def` for `Audit` to read it from. One consequence: the audit fingerprint is no longer byte-identical to a pre-this-ADR `sha256:` fingerprint of the bare definition, even for a definition with no metadata (`Audit(def, nil)` wraps `def` in an object with a `null` `editor_metadata` key, which is not the same bytes as `json.Marshal(def)` alone). This byte-level discontinuity is accepted under the "no backward compatibility" waiver; nothing depends on the audit fingerprint's bytes matching a specific historical algorithm, only on it changing when and only when the definition or its metadata changes.
4. **A nil and a zero-value `*WorkflowEditorMetadata` fingerprint differently.** `Audit(def, nil)` and `Audit(def, &types.WorkflowEditorMetadata{})` produce different fingerprints, because a nil pointer marshals its `editor_metadata` JSON slot as `null` while a non-nil zero value marshals as `{}` (every field is `omitempty`). This is intentional: "editor_metadata was never sent" and "editor_metadata was sent and is empty" are distinguishable wire inputs, and the replace no-op check (point 6 below) must not conflate them.
5. **`EditorMetadata` lives on `WorkflowRecord`, not a second Redis key.** Consistent with D4: one field on the registry's existing record and Redis hash, read/written atomically with `Definition`/`DefinitionHash`/`AuditFingerprint` in the same `CompareAndReplaceWorkflow`/`AddWorkflow` transaction. Metadata's lifetime is therefore tied 1:1 to the workflow record's lifetime (deleting the workflow deletes its metadata too) — there is no independent metadata lifecycle, no separate revision counter for metadata alone, and no SQL projection of it.
6. **Metadata-only PUT still advances `registry_revision`.** This is not a deviation from the original ADR text (which left it open) so much as a confirmed decision: `CompareAndReplaceWorkflow`'s one CAS token (`RegistryRevision`) is not split into "runtime changed" vs. "anything changed." The runtime hash — the actual guarantee this ADR provides — is unaffected either way (§3 below). A caller doing frequent small editor-metadata autosaves will see `registry_revision` advance on each one; this is bookkeeping overhead, not a conflict or a re-dispatch, since the compiled graph and entry activations are untouched.
7. **`OpWorkflowDefinitionUpdate` is the PUT authz operation**, not `OpWorkflowRegister`. Both map to the same `"workflow"` scope, so no token's effective permissions change; only the operation name recorded in authz admission/outcome audit rows changes for `PUT /v1/workflows/{id}`. Any audit consumer that filters or dashboards on the literal operation string for that route must be updated to look for `"workflowdefinition.update"`.
8. **Dedicated size cap, not a reused one.** `MaxEditorMetadataBytes` (1 MiB) is new; there is no pre-existing general request body size limit on the workflow registration HTTP routes to reuse. The cap applies to the JSON-encoded `editor_metadata` object only, not to the request body as a whole.
9. **`Server.ReplaceWorkflow` keeps clearing metadata; a new SDK method opts in to carrying it.** D6 above (no backward-compatible inline fields) does not, by itself, decide what an embedded replace should do with a record's existing `EditorMetadata` when the SDK has no metadata input to supply. The decision made here is non-destructive to that existing contract: `Server.ReplaceWorkflow` keeps writing nil, exactly as documented and pinned by `TestEmbeddedReplaceWorkflowClearsEditorMetadata`. A second method, `Server.ReplaceWorkflowWithMetadata(ctx, wf, metadata)`, is added alongside it for an embedded host that wants to carry an explicit `*types.WorkflowEditorMetadata` through its replace instead. It calls a new `APIServer.ReplaceWorkflowReportWithMetadata`, which is a thin wrapper over the already-existing `workflowControlModule.replaceWorkflowWithMetadata` (unchanged; it already accepted a metadata argument for the HTTP PUT path). No existing signature changed.

## 1. Context

`WorkflowDef` is the canonical workflow definition structure used by both the runtime engine and the visual editor. Historically, editor-only fields (visual position, UI theme, notes) lived inside `NodeDef`, which caused two problems:

1. **Runtime identity drift** — moving a node on the canvas changed the definition hash and forced a new workflow version.
2. **Metadata loss on round-trip** — the server had no place to store editor metadata without making it part of the runtime contract.

This ADR defines the split between runtime-semantic fields and editor-only metadata, and how the two representations are converted, keyed, and hashed.

## 2. Decision

Introduce a separate `WorkflowEditorMetadata` structure that lives alongside the runtime definition. The runtime definition keeps only fields that can affect execution output. Editor metadata is keyed by the stable editor identity of each node (`NodeDef.ID`, with a fallback to `NodeDef.Name` for nodes that have no ID).

### 2.1 Runtime-semantic fields

These fields remain in `WorkflowDef` and participate in the runtime hash:

- Top-level: `namespace`, `name`, `version`, `spec`, `runnerSelector`, `context`, `settings`, `options`, `credentials`, `params`, `node_templates`, `connections`, `outputs`, `pin_data`.
- Per node (`NodeDef`): `name`, `type`, `kind`, `version`, `template`, `disabled`, `on_error`, `runnerSelector`, `inputs`, `output_schema`, `parameters`, `retry`.

`pin_data` is runtime-semantic because it fixes node outputs and therefore affects execution behavior. It is retained in `WorkflowDef` and included in the runtime hash.

### 2.2 Editor-only fields

These fields exist ONLY in `WorkflowEditorMetadata` — they are not fields of `types.NodeDef` or `types.WorkflowDef` at all, so there is nothing for the runtime hash to exclude:

- `NodeDef.position` — visual coordinates on the canvas.
- `NodeDef.ui` — node-level UI theme/configuration.
- `NodeDef.notes` — free-text author notes.

`WorkflowDef.description` and `NodeDef.id` remain real fields on the runtime types (they are not editor metadata), but are still excluded from the runtime hash: `description` is human documentation with no execution effect, and `id` is a durable editor-assigned handle whose change must not invalidate a re-imported workflow's registry record.

### 2.3 `WorkflowEditorMetadata` schema

```go
type WorkflowEditorMetadata struct {
    Positions map[string]Position `json:"positions,omitempty"`
    Viewport  *Viewport           `json:"viewport,omitempty"`
    UI        map[string]any      `json:"ui,omitempty"`
    Notes     map[string]string   `json:"notes,omitempty"`
}

type Viewport struct {
    X    float64 `json:"x,omitempty"`
    Y    float64 `json:"y,omitempty"`
    Zoom float64 `json:"zoom,omitempty"`
}
```

No `PinData` field — see "Deviations" item 1 above. All node-level maps (`positions`, `ui`, `notes`) are keyed by the stable node identity as defined in §2.4.

### 2.4 Metadata key: stable node ID

Metadata keys MUST use `NodeDef.ID` when present. If `NodeDef.ID` is absent, the implementation falls back to `NodeDef.Name`. A diagnostic `NODE_METADATA_KEYED_BY_NAME` is emitted so hosts can warn authors that renaming or duplicating a node may silently cross-contaminate metadata. A key that matches no node (by ID, or by Name when the node has no ID) is dropped silently, with no diagnostic.

### 2.5 Conversion functions

Go needs no `SplitEditorMetadata`/`MergeEditorMetadata` pair: since `NodeDef` carries no position/UI/notes fields to split out or merge back in, there is nothing to convert between two representations of `WorkflowDef`. The only Go-side operation is validation: `types.ValidateEditorMetadata(def, metadata)` checks metadata keys against the definition's nodes and returns a sanitized copy plus diagnostics (§2.4).

The TypeScript implementation (`web/packages/xflow-core/src/editorMetadata.ts`) provides the two inverses, because the editor's in-memory `WorkflowNode`/`WorkflowDef` types in `web/packages/xflow-core/` are kept as the merged model — they carry `position`/`ui`/`notes` inline, matching what React Flow and the rest of the editor already expect — while the wire shape (`WireWorkflowNode`/`WireWorkflowDef`) carries neither:

- `splitEditorMetadata(def)` — returns `{ def, metadata, diagnostics }`. The returned `def` has `position`, `ui`, and `notes` stripped from each node; `pin_data` is left in place.
- `mergeEditorMetadata(def, metadata)` — returns `{ def, diagnostics }`. It restores `position`, `ui`, and `notes` onto nodes keyed by `NodeDef.ID` (or `NodeDef.Name`). It does not touch `def.pin_data`.

## 3. Runtime Hash

The canonical runtime hash is computed over the runtime-semantic subset only:

- Prefix: `runtime-sha256:v1:` for a definition that sets no node `Timeout` and no private node `Output`; `runtime-sha256:v2:` otherwise. v2 added those two node fields to the hash; a definition that sets neither hashes to the same bytes under both, so it keeps its v1 hash.
- Excludes: `WorkflowDef.ID`, `WorkflowDef.Description`, `NodeDef.ID`. (`NodeDef.Position`/`UI`/`Notes` need no exclusion — they do not exist on `NodeDef`.)
- Includes: everything else, including `WorkflowDef.PinData` and the runtime subset of each node (with `NodeDef.Timeout` and `NodeDef.Output` since v2). Re-registering the same `name@version` with only a node timeout or output policy changed is therefore a conflict.

A separate audit fingerprint (`sha256:audit:v1:`) is computed by `workflowhash.Audit(def, metadata)` over `{definition, editor_metadata}` for export/audit traceability and for the replace no-op check. It must NOT be used for conflict detection. See "Deviations" items 3-4 for the signature and payload-shape decisions.

### 3.1 Canonical builtin defaults

The runtime hash is taken over a canonical form of the definition in which every **builtin** node's missing or `null` parameter is filled with its `ParamSpec.Default`. An omitted builtin parameter and the same parameter spelled out as its Default are therefore one runtime identity, whichever path registered the workflow: the SDK builder writes Defaults into the definition, while the HTTP path stores the body as sent.

- The canonical form exists only in memory. The stored definition is never modified, and execution still reads the stored definition.
- The fill rule mirrors the SDK builder's: a missing or `null` parameter is filled; an explicit `""` is kept; only top-level ParamSpecs are filled; a body-bearing parent (`params.body` is an `xflow.subgraph`) is skipped while its body members are filled; disabled nodes are filled; lookup is kind-aware (action, trigger, supply).
- Only builtin node types are canonicalized, from a fixed table (`node.BuiltinParamSpecs`), never from the live node registry or runner-reported descriptors. The hash must not depend on what a process registered, and "omitted equals Default" is proven only for builtins, whose Defaults equal their handler fallbacks. A custom node type with a Default therefore stays path-sensitive: the SDK form and the same body with that parameter omitted conflict.
- A builtin `ParamSpec.Default` is part of hash identity. Changing one is a hash-format change and needs a prefix bump.
- On an SDK build the canonical form is the identity (the builder already wrote those Defaults), so no SDK hash changed. Records hashed before this (bare `sha256:` hashes written by the HTTP path) are recomputed on their next registration and CAS-upgraded to the `runtime-sha256:` form; there is no migration pass.
- Registration through `POST /v1/workflows` or the embedded `AddWorkflow` is idempotent on the runtime identity: a definition that differs from the stored one only in editor metadata (§2.2) or by omitted builtin Defaults returns the existing workflow id and leaves the stored definition unchanged. Replace no-op checks (`PUT /v1/workflows/{id}`, embedded `ReplaceWorkflow`) compare audit fingerprints instead, so a metadata-only change is still written as a new revision (D3; "Deviations" item 6).
- Because the replace no-op check compares the full stored definition plus metadata, an embedded `ReplaceWorkflow` of an SDK build over a record registered through HTTP with builtin Defaults omitted is a real replace: it writes a new record with a new workflow id, although the runtime hash is unchanged. A following `AddWorkflow` or `POST` of either form is idempotent on the replaced record.

### 3.2 Upgrade notes

These belong in the release notes of the release that introduces §3.1.

- **Mixed SDK versions on one Redis.** An SDK binary built before §3.1 re-checks a stored `runtime-sha256:v1:` hash by hashing the raw stored definition. On a record registered through HTTP with builtin Defaults omitted, that raw hash differs from the canonical one, so the old binary reports `ErrWorkflowConflict` (as it already did before §3.1) and best-effort CAS-rewrites the stored hash to its raw form. The next registration by a new binary or server upgrades it back. While both versions register the same workflow, its stored hash flaps between the two forms and each rewrite advances `registry_revision`. No definition and no `runtime-sha256:v2:` hash is ever changed by this. Upgrade every SDK binary that registers against a shared Redis before relying on cross-path idempotency.
- **One-time stale-revision conflict on PUT.** The first registration (`POST`, `AddWorkflow`) of a legacy `sha256:` record upgrades its hash in place, which advances `registry_revision`. A `PUT /v1/workflows/{id}` that read the record before that upgrade and writes after it fails once with a stale-revision 409; retrying it succeeds. Each legacy record can cause this at most once.
- **No migration pass.** Legacy hashes are upgraded lazily on their next registration. An identical PUT over a legacy record is a no-op and leaves the hash as it is, whether the stored definition was written by POST (no id) or by PUT (carrying its id). The one exception is an identical PUT that carries an `X-Request-Id` over a legacy record whose stored definition carries its id: it goes through the compare-and-swap so a retry of an earlier mutation with that id can replay from the operation ledger, and the CAS writes a new revision that upgrades the hash.
- **Legacy records with no stored `EditorMetadata`.** A `WorkflowRecord` written before this ADR's storage change has no `editor_metadata` key in its Redis hash at all. It decodes to a `nil` `EditorMetadata` without error; there is no migration pass to backfill one, and none is needed since `nil` is a valid "no editor metadata" state.

## 4. Consequences

- Moving or restyling a node no longer changes the runtime hash or triggers a version conflict.
- Re-importing a workflow with newly generated `NodeDef.ID` values keeps the same runtime identity.
- The server stores editor metadata as a field (`EditorMetadata`) on the existing `WorkflowRecord`/Redis hash, so editor state survives server-side round-trips without polluting the runtime contract. This is implemented as one field on one existing record (D4), not as separate `WorkflowDraft`/`WorkflowDefinitionVersion` storage objects with their own lifecycle — see "Deviations" item 5.
- `GET /v1/workflows/{id}` and `POST`/`PUT /v1/workflows[/{id}]` carry an optional `editor_metadata` object alongside the workflow definition fields (D2); a metadata-only `PUT` is a normal revision (D3; "Deviations" item 6).
- `PUT /v1/workflows/{id}` audits as `OpWorkflowDefinitionUpdate` rather than `OpWorkflowRegister` (D5; "Deviations" item 7); no scope or permission changes, only the recorded operation name.
- There is no backward-compatible inline `position`/`ui`/`notes` on `NodeDef` and no migration of pre-existing records that may have carried them (D6; "Deviations" item 2).
