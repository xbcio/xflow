# Naming Conventions

Package and identifier naming rules for this repo. Goal: eliminate **stutter**
(an exported identifier repeating its own package name), while preserving the
four idiomatic Go patterns that only *look* like stutter.

Validated against Go's own conventions: [Package names](https://go.dev/blog/package-names),
[Go Proverbs](https://go-proverbs.github.io/), and the standard library.

## Fix — true stutter

Drop the package-name suffix from exported identifiers.

| Pattern | Bad | Good |
|---------|-----|------|
| Primary constructor | `engine.NewEngine` | `engine.New` |
| Option type | `engine.EngineOption` | `engine.Option` |
| Domain interface | `store.ExecutionStore` | `store.Executions` |
| Composed interface | `store.ClusterStore` | `store.Store` |
| Bundle struct | `store.Stores` | `store.Set` |

- `New` is the primary constructor (cf. `list.New`); reserve `NewXxx` for
  secondary constructors (cf. `time.NewTicker`).
- store interfaces use **plural collection names** (`Executions`, `Nodes`,
  `Signals`); the flagship composed interface is `store.Store`; the per-tx
  bundle is `store.Set`.

## Do NOT touch — idiomatic, leave as-is

1. **Core type whose name equals the package** — `graph.Graph`, the
   `engine.Engine` type itself. Same shape as `list.List`, `bytes.Buffer`.
2. **Package names with a `store` suffix** — `sqlstore`, `mysqlstore`. They
   deliberately avoid colliding with `database/sql` and
   `gorm.io/driver/mysql` ("don't steal good names from the user", cf. `bufio`
   not `buf`).
3. **Path stutter where package name == module's last element** — `sdk/xflow`
   (`package xflow` under module `.../xflow`). Idiomatic, cf.
   `aws-sdk-go-v2/aws`.
4. **`package types`** — a generic name, but a zero-dependency, cohesive
   contract package (DSL + wire types), not a junk drawer. Used everywhere;
   renaming is high churn for no value.

## Trap: section-divider comments

A divider comment like `// ExecutionStore` may textually match a renamed type
but actually label methods of a *different* interface — e.g. in the cluster /
local adapters it labels `engine.StateBackend` methods operating on
`engine.ExecutionSnapshot`, not `store` methods. **Judge by the code below the
comment, not the text.** Renaming such a label would wrongly imply the type
implements the store interface.

## Workflow

Before any rename: gather (a) the authoritative Go convention and (b) the
repo's exported surface + every external consumer. After editing, always run
`go build ./... && go test ./...`, then grep for leftover old identifiers.
