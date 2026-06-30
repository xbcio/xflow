# Spec: Loop/Split Expand — Compile-time Gate

**Status**: draft
**Tracks**: review concern #4 — `engine/expand.go` is pass-through stub
**Severity**: med (verifier-confirmed)

## Problem

`engine/expand.go` advertises loop/split support but is a pass-through stub:

- Lines 57–59 comment: *"A full implementation would compile and run a body sub-graph."*
- `ExecuteBatch` (97–102) hard-codes `result = {"items": items, "count": len(items)}`.
- `_batch_exec` marker is written into Payload but never consumed by any
  dispatcher (verifier walked the production path and confirmed no router).

Yet `xflow.loop` / `xflow.split` nodes register normally, so user workflows can
compile and submit them — landing in undefined behavior at runtime.

## Goals

- Block accidental production use of loop/split until body sub-graph is real.
- Keep the existing tests and `ExecuteBatch` code path alive as the future
  skeleton — do not delete.
- Provide a single explicit opt-in (`ExperimentalExpand`) for development.

## Non-goals

- Implementing body sub-graphs in this spec — that is a separate, much larger
  feature.

## Design

### Capability tags on node descriptors

`types/node.go` — add to `Descriptor`:

```go
type Descriptor struct {
    // ... existing fields
    Capabilities []string `json:"capabilities,omitempty"`
}
```

Conventions:

- `body_subgraph_required` — node needs a compiled body sub-graph at runtime.
  Set on `xflow.loop`, `xflow.split`.

This is a generic capability tag, not a feature flag. Future node types can
declare similar requirements without changing engine internals.

### Compile-time gate

`engine/graph/compile.go` — new pass after node table is built:

```go
func (c *Compiler) checkUnsupportedCapabilities(def *types.WorkflowDef) error {
    if def.Options != nil && def.Options.ExperimentalExpand {
        return nil
    }
    var blocked []string
    for _, n := range def.Nodes {
        d := c.descriptors[n.Type]
        if hasCap(d, "body_subgraph_required") {
            blocked = append(blocked, fmt.Sprintf("%s (%s)", n.Name, n.Type))
        }
    }
    if len(blocked) > 0 {
        return &ErrLoopSplitNotImplemented{Nodes: blocked}
    }
    return nil
}
```

### Workflow option

`types/workflow.go`:

```go
type WorkflowOptions struct {
    // ... existing
    ExperimentalExpand bool `json:"experimental_expand,omitempty"`
}
```

DSL surface: `options.experimental_expand: true`. Document it as **experimental,
no stability guarantees, no production use**.

### Error type

`engine/graph/compile.go`:

```go
type ErrLoopSplitNotImplemented struct {
    Nodes []string
}

func (e *ErrLoopSplitNotImplemented) Error() string {
    return fmt.Sprintf(
        "loop/split nodes are not yet implemented (use options.experimental_expand=true to opt in): %s",
        strings.Join(e.Nodes, ", "),
    )
}
```

### Existing tests

`engine/expand_test.go` — every test that exercises `expandLoopSplit` or
`ExecuteBatch` must construct workflows with `Options.ExperimentalExpand = true`.
This keeps the skeleton code alive and CI-green.

### Stub annotations

`engine/expand.go:57-59` and `97-102` — replace TODO-ish comments with:

```go
// EXPERIMENTAL: pass-through stub. Real body sub-graph execution is not
// implemented; loop/split nodes are blocked at compile time unless
// WorkflowOptions.ExperimentalExpand is set. See .claude/docs/specs/expand-gate.md.
```

## API surface changes

- New: `types.Descriptor.Capabilities`, `types.WorkflowOptions.ExperimentalExpand`,
  `engine/graph.ErrLoopSplitNotImplemented`.
- No removal: `ExecuteBatch` and `expandLoopSplit` retain their signatures.

## Testing

- Unit: workflow with `xflow.loop` and no opt-in → `Compile` returns
  `ErrLoopSplitNotImplemented` mentioning the node.
- Unit: same workflow with `ExperimentalExpand=true` → compiles, falls through
  to existing stub behavior (current test asserts unchanged).
- Negative: registering a new node type with `body_subgraph_required` capability
  without setting the option also gets blocked.

## Acceptance

- `go test ./engine/... -race -count=1` passes.
- A simple DSL sample using `xflow.loop` without the option returns a
  user-friendly error on submit.
- `docs/design/DSL-SPECIFICATION.md` gets one paragraph under loop/split
  noting their experimental status.
