# xflow Coding Standards

Derived from [Go style guide](<sas-repo-root>/docs/claude/development/go-style-guide.md) and [golang-standards/project-layout](https://github.com/golang-standards/project-layout), adapted to this repo's structure.

---

## Module & Directory Layout

### Multi-module structure

The repo contains four Go modules. Each has its own `go.mod`:

| Module path | Directory | Role |
|---|---|---|
| `github.com/xbcio/xflow` | `/` | Root — types, cmd |
| `github.com/xbcio/xflow/types` | `types/` | Shared data types (no deps) |
| `github.com/xbcio/xflow/nodes/node` | `nodes/node/` | Builtin task node constructors and implementations |
| `github.com/xbcio/xflow/sdk` | `sdk/` | Embedded engine SDK |

**Rules:**
- Use `go.work` at the repo root for local development across all modules. `go.work` is gitignored — create it locally:
  ```bash
  go work init
  go work use . ./types ./sdk
  ```
- `types/` must have zero imports from sibling modules (it is the leaf dependency).
- `nodes/node/` may import `types/` only.
- `sdk/` may import `nodes/node/` and `types/` only; never import `pkg/` or `internal/`.
- `cmd/` entry points must be thin: import from `internal/` and `pkg/`, do nothing else.

### Directory conventions (per golang-standards)

| Directory | Purpose | Rule |
|---|---|---|
| `cmd/<name>/` | Binary entry points | Must be minimal `main()`; no business logic |
| `internal/` | Private app code | `config/`, `storage/`, `middleware/` go here; not importable outside root module |
| `pkg/` | Shared library code safe for external use | `workflow/`, `scheduler/`, `state/`, `task/`, `expression/`, `monitor/`, `api/` |
| `api/` | OpenAPI specs, gRPC `.proto` files | One source of truth for service contracts |
| `configs/` | Config templates, default YAML | No secrets, no environment-specific values |
| `nodes/node/` | Builtin task nodes | Constructors + implementations for normal workflow nodes |
| `types/` | Shared data types (separate module) | Plain structs + constants; zero business logic |
| `sdk/` | Embedded engine (separate module) | Self-contained; carries its own runners |
| `docs/` | Design docs and specs | Keep in sync with code |

**Do not create:** `utils/`, `helpers/`, `common/`, `lib/`, `src/`.

---

## Comments & Documentation

### Language: English for all exported symbols

All exported identifiers (types, functions, methods, constants, variables) **must** have English doc comments. Chinese is acceptable only for unexported identifiers and inline logic comments inside function bodies.

```go
// ✅ Exported — English, starts with symbol name
// Register registers a handler in the global registry.
// Safe for concurrent use from multiple init() functions.
func Register(h ActionHandler) { ... }

// ✅ Unexported — Chinese acceptable
// nodeRef 是 Definition.New() 返回的内部 Builder 实现。
type nodeRef struct { ... }

// ❌ Exported but Chinese
// Register 注册 handler，通常在 init() 中调用。
func Register(h ActionHandler) { ... }
```

### Doc comment format

- First sentence starts with the symbol name (exported) or a lowercase verb (unexported).
- Complete sentences end with `.`, never `。`.
- One blank line between the last import and the first doc comment.
- Package-level doc comment goes in exactly one file per package (usually `doc.go` or the file matching the package name).

```go
// ✅
// ActionHandler is the runtime interface for action nodes.
// Implementations must be stateless and safe for concurrent use.
type ActionHandler interface { ... }

// ❌
// ActionHandler 核心执行接口
type ActionHandler interface { ... }
```

---

## Naming

### Typed constants over string literals

When a type alias like `OnError`, `WaitMode`, or `MergeMode` is defined, use it — never compare against raw string literals.

```go
// ✅
switch mode {
case string(node.WaitTimer):
    ...
case string(node.WaitSignal):
    ...
}

// ❌
switch mode {
case "timer":
    ...
case "signal":
    ...
}
```

### Key naming rules

- Initialisms are all-caps: `ExecutionID` not `ExecutionId`, `TraceID` not `TraceId`, `URL` not `Url`.
- Receivers: 1-2 letter abbreviation, consistent across all methods of a type.
  - `AsynqRunner` → `r`, `MemoryRunner` → `r`, `Engine` → `e`, `WorkflowBuilder` → `w`.
- No `Get` prefix on accessors: `Status()` not `GetStatus()`.
- Constructor returns concrete type: `func NewEngine(...) *Engine`, never `EngineInterface`.
- No generic package names: `util`, `helper`, `common`.

### Error messages

Follow Go convention throughout:

```go
// ✅
fmt.Errorf("enqueue node %q: %w", name, err)
fmt.Errorf("execution %q not found", id)

// ❌ — capitalized, Chinese, or ends with punctuation
fmt.Errorf("Execution not found.")
fmt.Errorf("节点入队失败: %w", err)
```

---

## Error Handling

- Always wrap with `%w` when callers need to inspect the error type.
- Use `errors.Is` / `errors.As` for checks — never `strings.Contains(err.Error(), ...)`.
- Early return pattern; never nest `if err == nil { ... }`.
- `nodeFailure` in AsynqRunner returns `nil` to suppress Asynq retries — document this at the call site with `_ =`.

---

## Concurrency

- Every goroutine must have a documented exit condition.
- Use `ctx.Done()` in select to make goroutines context-aware; never `time.Sleep` in a select loop.
- Prefer `time.NewTimer` + `defer timer.Stop()` over `time.After` inside a function that may exit early.
- Use `sync.RWMutex` for read-heavy maps (registry, executions). Always acquire the smallest lock scope.
- Goroutine closures must capture only snapshots, not shared mutable references.

---

## Interfaces

- Define interfaces at the consumer, not the provider.
- Keep interfaces minimal; prefer single-method or two-method interfaces.
- `EngineRunner` is the internal runner contract — do not expose it outside `internal/runner/`.
- `types.ActionHandler` is the primary interface action node authors implement; keep it stable.

---

## Testing

### Structure

Use `t.Run` for sub-cases; do not create separate `Test*_Case` functions.

```go
// ✅
func TestEngine_OnError(t *testing.T) {
    t.Run("Stop", func(t *testing.T) { ... })
    t.Run("ErrorOutput", func(t *testing.T) { ... })
    t.Run("MainOutput", func(t *testing.T) { ... })
}

// ❌
func TestEngine_OnError_Stop(t *testing.T) { ... }
func TestEngine_OnError_ErrorOutput(t *testing.T) { ... }
```

### Table-driven tests for parametric cases

```go
tests := []struct {
    name    string
    mode    string
    wantErr bool
}{
    {"signal without signal_name", "signal", true},
    {"timer without duration",    "timer",  true},
    {"valid signal",              "signal", false},
}
for _, tt := range tests {
    t.Run(tt.name, func(t *testing.T) { ... })
}
```

### Failure messages

Include inputs in the failure message:

```go
// ✅
t.Errorf("Wait(%q) status = %q, want %q", id, got.Status, types.ExecutionStatusSuccess)

// ❌
t.Errorf("wrong status")
```

### Test helpers

Call `t.Helper()` at the top of every helper function so failures point to the call site.

### No sleep in tests

Use buffered channels, `sync.WaitGroup`, or polling with `context.WithTimeout` instead of `time.Sleep`. (The Signal buffering mechanism in `MemoryRunner` makes `time.Sleep` unnecessary in wait-signal tests.)

### Test data registration

Handler types used only in tests must use a `test.` prefix (e.g., `test.engine.echo`) and be registered in `init()` inside `_test.go` files to avoid polluting the global registry.

---

## Module-specific rules

### `node/` module

- No runtime state in handlers — each handler is a singleton called concurrently.
- `Descriptor().Type` must be a dot-separated reverse-domain string: `xflow.wait`, `xflow.http`, `custom.myorg.name`.
- Use typed constants (`WaitMode`, `OnError`, `MergeMode`) in Descriptor param defaults.
- `port.go` — `OutputPort` and `InputPort` are value types; do not add methods that mutate them.

### `sdk/xflow` module (embedded engine)

- `EngineRunner` stays in `internal/runner/` — not part of the public API.
- Redis key helpers (`execKey`, `nodeKey`, `outputKey`, `signalKey`, `inDegreeKey`) are the single source of truth for key layout; never inline the format string elsewhere.
- `completedResults` is the retention store for post-completion queries; keep `executions` and `completedResults` always updated under the same `r.mu.Lock()`.
- AsynqRunner's `handleWaitNodeSignal` polling loop must be interruptible: use `select { case <-time.After(...): case <-ctx.Done(): }`.

### `types/` module

- Plain data types only — no business logic, no external dependencies.
- All fields use `omitempty` in JSON tags.
- Status constants (`StatusPending`, etc.) are the canonical state machine vocabulary; use them everywhere.

---

## What to avoid

| Anti-pattern | Instead |
|---|---|
| `time.Sleep` in select loops | `select { case <-time.After(d): case <-ctx.Done(): }` |
| `time.After` in long-lived loops | `time.NewTimer` + `defer Stop()` |
| `strings.Contains(err.Error(), ...)` | `errors.Is` / `errors.As` |
| String literals for typed constants | `string(node.WaitTimer)` |
| Chinese in exported doc comments | English, starts with symbol name |
| `util`, `helper`, `common` packages | Focused, domain-named packages |
| `_ = r.someFunc()` without comment | Add `// always returns nil to suppress Asynq retries` |
| Direct action handler in AsynqRunner | Define with `node.Define`, register workers with `xflow.WithNodes` |
| `t.Fatal` at top-level without `t.Run` | Wrap in `t.Run` subtests |
| Stray generated/output files at root | Add to `.gitignore` (e.g., `test_output.txt`) |
