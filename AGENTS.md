# AGENTS.md

This file provides guidance to Codex (Codex.ai/code) when working with code in this repository.

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
- **nodes/node/** — Builtin task node constructors and implementations (`node.HTTP`, `node.Function`, etc.)
- **types/** — Public DSL/runtime contracts: `WorkflowDef`, handler interfaces (`ActionHandler`, `SuspendingHandler`), handler IO, descriptors, statuses, `Result` (json-tagged, zero impl deps)
- **store/** — Public persistence interfaces + domain models
  - `memstore/` — in-memory implementation (test / local)
  - `sqlstore/` — dialect-agnostic GORM implementation; `sqlstore/mysqlstore/` — MySQL dialect entry (`mysqlstore.New`)
- **backend/** — Reusable backend providers and implementations
  - `memory/` — In-memory StateStore + goroutine pool TaskQueue
  - `asynq/` — Redis StateStore + Asynq TaskQueue
- **execution/** — Reusable embedded task execution boundary: Dispatcher, Runner, Registry
- **sdk/** — Public SDK grouping
  - `xflow/` — `package xflow`: `NewLocal` / `NewCluster` factories, WorkflowBuilder, and production control APIs (`AddWorkflow`, `Invoke`, `Wait`, `Signal`, `RevokeSignal`, `Cancel`, `Inspect`)
  - `examples/` — runnable `.go` usage examples
- **cmd/server/** — Management server (Master node)
- **cmd/runner/** — Task runner (Execution node)
- **db/** — SQL schema
- **docs/** — `design/` specs, `dsl-samples/` (`.yaml` DSL samples), `references/`

## Key Constraints

- `engine/` must NOT import redis/asynq/mysql/sql — only stdlib + types + nodes/node
- Graph IR is immutable after compile, shared lock-free at runtime
- Engine Core depends on exactly 2 interfaces: `StateStore` + `TaskQueue`
- Future server/runner code goes under `service/` (a future module boundary); core packages (engine/nodes/types/store/sdk) must NEVER import `service/` or `cmd/` — dependencies flow one way only, so a later module split stays mechanical

## AI-Generated Documentation Placement

AI-generated/maintained design docs, specs, and review reports go under an
appropriate `.claude/` subdirectory (e.g. `.claude/specs/`), never under
`docs/`. `docs/` is reserved for human-maintained architecture docs
(`docs/design/`) and user-facing references (`docs/dsl-samples/`,
`docs/references/`). `.claude/` is gitignored, so these docs never enter
version control; follow the existing `.claude/specs/` naming convention:
`YYYY-MM-DD-<topic>-design.md`.

## Git Commits

Single-line subject, no body: `type(scope): what changed`. Full convention: **[docs/GIT-COMMITS.md](docs/GIT-COMMITS.md)**.

## Detailed Documentation

Read before implementing core features:

- **[docs/design/ARCHITECTURE.md](docs/design/ARCHITECTURE.md)** — Layered architecture, dependency rules, interface design
- **[docs/design/DEPLOYMENT-TOPOLOGIES.md](docs/design/DEPLOYMENT-TOPOLOGIES.md)** — SDK modes (local/cluster/remote) + server/runner cluster architecture; current vs planned
- **[docs/design/STORAGE-CONTRACT.md](docs/design/STORAGE-CONTRACT.md)** — Redis-as-system-of-record dual-write contract
- **[docs/design/CORE-COMPONENTS.md](docs/design/CORE-COMPONENTS.md)** — Target design for server clustering (Raft HA, Relay Gateway) — not yet implemented
- **[docs/TESTING.md](docs/TESTING.md)** — Test commands, strategies, conventions
- **[docs/NAMING-CONVENTIONS.md](docs/NAMING-CONVENTIONS.md)** — Stutter policy: which package/identifier names to fix vs. the four idiomatic patterns to leave alone
- **[docs/CODING-STANDARDS.md](docs/CODING-STANDARDS.md)** — Naming, comments, error handling, concurrency
- **[docs/design/DSL-SPECIFICATION.md](docs/design/DSL-SPECIFICATION.md)** — Complete DSL syntax specification
