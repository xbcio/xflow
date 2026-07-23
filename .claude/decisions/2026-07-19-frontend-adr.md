# ADR F0-A: Frontend Workflow Field Decision Matrix

| Item | Value |
|------|-------|
| Status | Accepted / Implemented (F0-A) |
| Owner | xflow team |
| Related | `types/workflow.go`, `sdk/xflow/workflow_identity.go`, `web/packages/workflow-core/src/metadata.ts`, `web/packages/workflow-core/src/types.ts` |

## 1. Context

The frontend workflow editor and the runtime engine share the same `WorkflowDef` / `NodeDef` schema, but they care about different subsets of its fields. A node moved two pixels to the left must not create a new workflow version; conversely, a pinned value that changes execution output must be detected by the registry conflict check. This ADR records the canonical field categorization and hash strategy for the F0 remediation.

## 2. Field matrix

The matrix below is exhaustive for the fields in `types.WorkflowDef` and `types.NodeDef` as of this decision. The "Hash" columns state whether the field is included in the canonical runtime hash (`runtime-sha256:v1:`) and/or the audit fingerprint (`sha256:audit:v1:`).

### 2.1 `WorkflowDef` top-level fields

| Field | Category | runtimeHash | auditFingerprint | Notes |
|-------|----------|-------------|------------------|-------|
| `Namespace` | workflow identity | yes | yes | Registry lookup key: `namespace/name@version`. |
| `Name` | workflow identity | yes | yes | Registry lookup key. |
| `Version` | workflow identity | yes | yes | Registry lookup key. |
| `ID` | instance identifier | no | yes | Runtime instance pointer; not part of the definition identity. |
| `TenantID` | instance identifier | no | yes | Server-injected scope; ignored on ingest (`json:"-"`). |
| `Description` | descriptive | no | yes | Human documentation; no execution effect. |
| `Spec` | runtime semantic | yes | yes | DSL spec selector. |
| `RunnerSelector` | runtime semantic | yes | yes | Affects where nodes execute. |
| `Context` | runtime semantic | yes | yes | Workflow-level variables/config available to nodes. |
| `Settings` | runtime semantic | yes | yes | Execution behavior (timeout, concurrency, timezone, pin-data mode, retry). |
| `Options` | runtime semantic | yes | yes | Advanced behavior such as cyclic mode and experimental expand. |
| `Credentials` | runtime semantic | yes | yes | Credential references used by nodes. |
| `Params` | runtime semantic | yes | yes | Workflow-level input declarations. |
| `NodeTemplates` | runtime semantic | yes | yes | Reusable node configuration snippets referenced by `NodeDef.Template`. |
| `Nodes` | runtime semantic | yes | yes | See per-node matrix below. |
| `Connections` | runtime semantic | yes | yes | Graph wiring; drives execution order and data flow. |
| `Outputs` | runtime semantic | yes | yes | Workflow outputs exposed to callers. |
| `PinData` | runtime semantic | yes | yes | Pinned node outputs directly affect execution behavior. |

### 2.2 `NodeDef` fields

| Field | Category | runtimeHash | auditFingerprint | Notes |
|-------|----------|-------------|------------------|-------|
| `ID` | editor-only | no | yes | Stable editor-assigned handle. Changing it must not invalidate the runtime identity. |
| `Name` | runtime semantic | yes | yes | Runtime identity used by connections and `pin_data`. |
| `Type` | runtime semantic | yes | yes | Node implementation selector. |
| `Kind` | runtime semantic | yes | yes | Runtime role (`action` / `trigger`). |
| `Version` | runtime semantic | yes | yes | Node implementation version. |
| `Template` | runtime semantic | yes | yes | References a `NodeTemplate`. |
| `Position` | editor-only | no | yes | Visual coordinates on the canvas. |
| `Disabled` | runtime semantic | yes | yes | Affects whether the node runs. |
| `OnError` | runtime semantic | yes | yes | Per-node error handling policy. |
| `RunnerSelector` | runtime semantic | yes | yes | Per-node runner selection. |
| `Notes` | editor-only | no | yes | Free-text author notes. |
| `Inputs` | runtime semantic | yes | yes | Input port declarations. |
| `OutputSchema` | runtime semantic | yes | yes | Declared output shape. |
| `Parameters` | runtime semantic | yes | yes | Node-specific configuration. |
| `UI` | editor-only | no | yes | Node-level UI theme/configuration. |
| `Retry` | runtime semantic | yes | yes | Per-node retry override. |

## 3. Removed stale ideas

The following ideas from earlier drafts are explicitly rejected:

1. **"Introduce TanStack Query"** — rejected. Data-fetching strategy is out of scope for the field/hash decision. This ADR governs schema and identity only.
2. **"`definitionHash` currently still contains `Position` / `UI`; migration to M2"** — rejected. The canonical runtime hash (`runtime-sha256:v1:`) excludes `Position`, `UI`, `Notes`, `Description`, `NodeDef.ID`, and instance identifiers from the start. There is no deferred M2 migration for hash purity.
3. **"Move `pinData` into editor metadata"** — rejected. `pin_data` is runtime-semantic because it fixes node outputs and changes execution behavior. It remains in `WorkflowDef` and is included in the runtime hash. `WorkflowEditorMetadata.pinData` is a read-only derived cache for UI convenience only; `WorkflowDef.pin_data` is authoritative.

## 4. `pin_data` is runtime-semantic

`WorkflowDef.PinData` pins node outputs to fixed values. Because it changes what the workflow produces, it stays in the runtime definition and is included in `runtimeHash`. `splitEditorMetadata` does **not** strip `pin_data` from the returned definition; it only copies it into `WorkflowEditorMetadata.pinData` as a read-only view. `mergeEditorMetadata` does **not** overwrite `def.pin_data` with the metadata cache.

## 5. `NodeDef.ID` is a stable editor identity

`NodeDef.ID` is a durable, editor-assigned handle used to bind editor metadata (position, UI, notes) to a node across saves, re-imports, and renames. It is intentionally excluded from `runtimeHash` so that re-importing a workflow with newly generated IDs does not create a spurious registry conflict. The runtime identity of a node is carried by `NodeDef.Name`, which is included in the runtime hash.

## 6. Three hash responsibilities

The system maintains three distinct hashes to avoid conflating runtime identity, audit traceability, and compiled structure:

| Hash | Prefix | Scope | Purpose |
|------|--------|-------|---------|
| Runtime hash | `runtime-sha256:v1:` | Runtime-semantic subset of `WorkflowDef` / `NodeDef` | Registry conflict detection and canonical workflow identity. |
| Audit fingerprint | `sha256:audit:v1:` | Full `WorkflowDef` JSON including editor metadata and instance identifiers | Export/audit traceability; must never be used for conflict detection. |
| Engine graph hash | (engine/graph) | Compiled graph IR (nodes, edges, order) | Structural compile identity; orthogonal to the JSON definition form. |

## 7. Legacy-hash compatibility

Records written before the runtime/audit split may carry a bare `sha256:` prefix in `WorkflowRecord.DefinitionHash`. The registry reconciles such legacy records on read:

- If the stored hash already starts with `runtime-sha256:v1:`, it is current and no rewrite is needed.
- If the stored hash has any other prefix (`sha256:`, `sha256:audit:v1:`, or an unrecognized prefix), the runtime hash is recomputed from the stored definition and returned with `needsUpgrade=true`.
- The caller must then persist the recomputed hash atomically via `UpdateDefinitionHash` so the record is upgraded without changing its runtime semantics.

This path ensures old records remain comparable to new registrations while the canonical hash format stays self-describing.

## 8. Consequences

- Editor-only changes (moving, restyling, adding notes, regenerating stable IDs) do not change the runtime hash or trigger version conflicts.
- Runtime-semantic changes (parameters, connections, outputs, `pin_data`, disabled flags, retry settings) are correctly detected by the registry.
- `WorkflowEditorMetadata` stores only editor-only state and a read-only view of `pin_data`; the runtime contract stays clean.
- Legacy records upgrade lazily and safely to the new runtime hash format.
