# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common Commands

```bash
go mod download          # Dependencies
make build               # Build
make test                # Test
go fmt ./...             # Format
golangci-lint run        # Lint
```

## Project Structure

- **engine/** — Pure scheduling algorithm (zero IO deps): Graph IR, Scheduler, ErrorPolicy, Suspend
- **node/** — Node handler contracts (`TaskHandler`, `SuspendingHandler`) + all builtin node types (flat, not split into sub-packages)
- **types/** — DSL/wire contracts: `WorkflowDef`, `ExecutionID`, `Status`, `Result` (json-tagged, zero impl deps)
- **store/** — Public persistence interfaces + domain models
  - `memstore/` — in-memory implementation (test / local)
  - `sqlstore/` — dialect-agnostic GORM implementation; `sqlstore/mysqlstore/` — MySQL dialect entry (`mysqlstore.New`)
- **sdk/** — Public SDK grouping
  - `xflow/` — `package xflow`: `NewLocal` / `NewCluster` factories, WorkflowBuilder (import path末段=包名)
  - `internal/adapter/local/` — In-memory StateBackend + goroutine pool TaskQueue
  - `internal/adapter/cluster/` — Redis StateBackend + Asynq TaskQueue
  - `examples/` — runnable `.go` usage examples
- **cmd/server/** — Management server (Master node)
- **cmd/runner/** — Task runner (Execution node)
- **db/** — SQL schema
- **docs/** — `design/` specs, `dsl-samples/` (`.yaml` DSL samples), `references/`

## Key Constraints

- `engine/` must NOT import redis/asynq/mysql/sql — only stdlib + types + node
- `sdk/internal/adapter/` is SDK-private, cmd must not import it
- Graph IR is immutable after compile, shared lock-free at runtime
- Engine Core depends on exactly 2 interfaces: `StateBackend` + `TaskQueue`
- Future server/runner code goes under `service/` (a future module boundary); core packages (engine/node/types/store/sdk) must NEVER import `service/` or `cmd/` — dependencies flow one way only, so a later module split stays mechanical

## Detailed Documentation

Read before implementing core features:

- **[.claude/docs/architecture.md](.claude/docs/architecture.md)** — Layered architecture, dependency rules, interface design
- **[.claude/docs/implementation-guide.md](.claude/docs/implementation-guide.md)** — Engine core, node handlers, SuspendingHandler, ErrorPolicy
- **[.claude/docs/testing.md](.claude/docs/testing.md)** — Test commands, strategies, conventions
- **[.claude/docs/deployment-topologies.md](.claude/docs/deployment-topologies.md)** — SDK modes (local/cluster/remote) + server/runner cluster architecture; current vs planned
- **[.claude/docs/naming-conventions.md](.claude/docs/naming-conventions.md)** — Stutter policy: which package/identifier names to fix vs. the four idiomatic patterns to leave alone
- **[docs/CODING-STANDARDS.md](docs/CODING-STANDARDS.md)** — Naming, comments, error handling, concurrency
- **[docs/design/DSL-SPECIFICATION.md](docs/design/DSL-SPECIFICATION.md)** — Complete DSL syntax specification
