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
- **sdk/** — Public SDK entry point: `NewLocal` / `NewCluster` factories, WorkflowBuilder
  - `internal/adapter/local/` — In-memory StateBackend + goroutine pool TaskQueue
  - `internal/adapter/cluster/` — Redis StateBackend + Asynq TaskQueue
- **node/** — Node handler contracts: `TaskHandler`, `SuspendingHandler`, builtin node types
- **types/** — Shared types: `WorkflowDef`, `ExecutionID`, `Status`, `Result`
- **store/** — Public persistence interfaces (`ClusterStore`) + MySQL implementation
- **cmd/server/** — Management server (Master node)
- **cmd/worker/** — Task worker (Execution node)
- **db/** — SQL schema
- **examples/** — SDK usage examples

## Key Constraints

- `engine/` must NOT import redis/asynq/mysql/sql — only stdlib + types + node
- `sdk/internal/adapter/` is SDK-private, cmd must not import it
- Graph IR is immutable after compile, shared lock-free at runtime
- Engine Core depends on exactly 2 interfaces: `StateBackend` + `TaskQueue`

## Detailed Documentation

Read before implementing core features:

- **[.claude/docs/architecture.md](.claude/docs/architecture.md)** — Layered architecture, dependency rules, interface design
- **[.claude/docs/implementation-guide.md](.claude/docs/implementation-guide.md)** — Engine core, node handlers, SuspendingHandler, ErrorPolicy
- **[.claude/docs/testing.md](.claude/docs/testing.md)** — Test commands, strategies, conventions
- **[docs/CODING-STANDARDS.md](docs/CODING-STANDARDS.md)** — Naming, comments, error handling, concurrency
- **[docs/design/DSL-SPECIFICATION.md](docs/design/DSL-SPECIFICATION.md)** — Complete DSL syntax specification
- **[.claude/specs/2026-05-25-engine-core-refactor-design.md](.claude/specs/2026-05-25-engine-core-refactor-design.md)** — Engine refactor design spec (current)
