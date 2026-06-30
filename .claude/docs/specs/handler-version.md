# Spec: Handler Version Validation

**Status**: draft
**Tracks**: review concern #3 — handler version field stored but never enforced
**Severity**: high (verifier-confirmed)

## Problem

`NodeDef.Version` is captured at compile time (`sdk/xflow/builder.go:280-282`) and
flows through to the engine, but the runtime registry silently falls back to the
latest registered handler when the requested version is missing
(`execution/registry.go:56-79`). Symptoms:

- Old workflows can run on incompatible new handlers with no warning.
- `nodes/node/registry.go:100-105` `LookupTrigger` ignores `nd.Version` entirely
  and returns the most-recently registered trigger handler for a given type
  (verifier-confirmed in SDK dimension).
- No validation pass at workflow registration time — a workflow referencing a
  handler version that does not exist will compile and only fail at first
  task dispatch.

## Goals

- Make handler version a load-bearing field, not a comment.
- Fail fast at `AddWorkflow` if any referenced `(type, version)` is missing.
- At task dispatch, resolve the exact version; on miss, apply an explicit policy
  rather than silent fallback.
- Apply the same rules to trigger handlers.

## Non-goals

- Major-version semver semantics for the engine itself.
- Automatic migration of workflows across handler versions.

## Design

### Version policies

```go
// types/runtime.go (or sdk/xflow/option.go)
type VersionPolicy int

const (
    VersionStrict       VersionPolicy = iota // miss → ErrHandlerVersionMismatch
    VersionWarnFallback                      // miss → log + return latest
    VersionSilentFallback                    // current behavior, opt-in only
)
```

Default at `Engine` construction: `VersionWarnFallback` (avoids breaking
existing deployments). New `xflow.WithVersionPolicy(p)` SDK option.

### Registry changes

`nodes/node/registry.go`:

- Keep `versionedHandlers map[string]map[int]Handler` (already conceptually present).
- Add `LookupVersion(typ string, version int) (Handler, bool)` — exact match,
  no fallback. Existing `Lookup(typ)` keeps returning latest.
- Add the same pair for triggers: `LookupTriggerVersion`.

`execution/registry.go`:

- `Get(execID, nodeName, nodeType, version int)` becomes the policy-aware
  entry point:
  1. execution-scoped lookup (no version, current behavior)
  2. node-scoped lookup
  3. `nodes/node.LookupVersion(nodeType, version)` — exact
  4. on miss, apply `policy`:
     - `VersionStrict` → return `ErrHandlerVersionMismatch{Type, RequestedVersion, LatestAvailable}`
     - `VersionWarnFallback` → log + return latest
     - `VersionSilentFallback` → return latest, no log

### Registration-time pre-check

`sdk/xflow/xflow.go` `AddWorkflow`:

```go
func (x *XFlow) AddWorkflow(def *types.WorkflowDef) error {
    if err := x.preCheckVersions(def); err != nil {
        return err
    }
    // ... existing registration
}

func (x *XFlow) preCheckVersions(def *types.WorkflowDef) error {
    var missing []HandlerLocator
    for _, n := range def.Nodes {
        if n.Kind == types.NodeKindTrigger {
            if _, ok := node.LookupTriggerVersion(n.Type, n.Version); !ok {
                missing = append(missing, HandlerLocator{n.Type, n.Version})
            }
            continue
        }
        if _, ok := node.LookupVersion(n.Type, n.Version); !ok {
            missing = append(missing, HandlerLocator{n.Type, n.Version})
        }
    }
    if len(missing) > 0 {
        return ErrMissingHandlerVersions{Missing: missing}
    }
    return nil
}
```

Pre-check is **always** strict, regardless of `VersionPolicy`. Rationale: at
registration time, all handlers should be known; falling back at register-time
hides bugs, while at dispatch-time fallback can be a deployment-friction
mitigation.

### Trigger version path

`sdk/xflow/trigger_runtime.go` `ReconcileWorkflow` (currently uses only
`nd.Type`) must consume `nd.Version` and call `LookupTriggerVersion`. If
multiple workflows register the same trigger type with different versions, each
workflow gets its own resolved handler — no cross-workflow shadowing.

## API surface changes

- New errors: `ErrHandlerVersionMismatch`, `ErrMissingHandlerVersions`.
- New SDK option: `xflow.WithVersionPolicy(VersionPolicy)`.
- New registry methods: `LookupVersion`, `LookupTriggerVersion`.
- `execution.Registry.Get` signature unchanged; behavior changes.

## Migration

- Existing callers get `VersionWarnFallback` automatically — no code change
  required, but warnings will appear in logs for version misses.
- Callers can opt into `VersionStrict` once they've audited their deployment.

## Testing

- Unit: register v1, build workflow that pins v2 → `AddWorkflow` returns
  `ErrMissingHandlerVersions` listing the locator.
- Unit: `Strict` policy + dispatch with missing version → handler error
  propagated to `OnError` path.
- Unit: `WarnFallback` policy + dispatch with missing version → uses latest,
  emits warn log (capturable via `Logger` interface).
- Integration: two workflows register same trigger type with different
  versions; each gets its own handler at activation.

## Acceptance

- `engine`/`execution` packages still depend on only the existing public types.
- `nodes/node` test for trigger no longer relies on "latest wins".
- All existing tests pass after explicit `WithVersionPolicy(VersionWarnFallback)`
  is wired into test setup (or default).
