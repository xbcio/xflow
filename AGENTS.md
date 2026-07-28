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

- **engine/** — Pure scheduling algorithm (zero business IO deps): Graph IR, Scheduler, ErrorPolicy, Suspend, lease/result semantics
- **node/** — Public node DSL and builtin implementations (`node.HTTP`, `node.Function`, `node.KafkaTrigger`, etc.)
- **types/** — Public DSL/runtime contracts: `WorkflowDef`, handler interfaces (`ActionHandler`, `SuspendingHandler`), handler IO, descriptors, statuses, `Result` (json-tagged, zero impl deps)
- **store/** — Public persistence interfaces + domain models
  - `memstore/` — in-memory implementation (test / local)
  - `sqlstore/` — dialect-agnostic GORM implementation; `sqlstore/mysqlstore/` — MySQL dialect entry (`mysqlstore.New`)
- **backend/** — Reusable backend provider abstractions
  - `providers/local/` — In-memory StateStore + goroutine pool TaskQueue
  - `providers/distributed/` — Redis StateStore + Asynq TaskQueue
- **namespace/** — Server-issued permission namespace type and context propagation primitives
- **execution/** — Reusable embedded task execution boundary: Dispatcher, Runner, Registry
- **sdk/**
  - `xflow/` — `package xflow`: `NewLocal` / `NewCluster` factories, `NewServer` embedded control-plane facade, WorkflowBuilder, and production control APIs (`AddWorkflow`, `Invoke`, `Wait`, `Signal`, `RevokeSignal`, `Cancel`, `Inspect`)
  - `examples/` — runnable `.go` usage examples
- **service/** — Server/runner process-level implementation boundary (cluster topology)
  - `runner/` — cluster task runner process (holds `ProtocolClient` + embedded `execution.Runner`)
  - `control/` — control plane: controlplane, dispatcher, auth, core connect
  - `protocol/` — wire protocol + `protocol/runnerpb/` generated gRPC
- **cmd/server/** — Management server (Master node) entrypoint
- **cmd/runner/** — Task runner (Execution node) entrypoint
- **db/** — SQL schema
- **docs/** — `design/` specs, `dsl-samples/` (`.yaml` DSL samples), `references/`

## Key Constraints

- `engine/` must NOT import redis/asynq/mysql/sql/network transports. It may depend on public contracts (`types`, `namespace`, `engine/graph`) and the narrow observability tracing facade only for engine-owned spans; it must not depend on concrete metrics/logging exporters or provider packages.
- Graph IR is immutable after compile, shared lock-free at runtime
- Engine Core depends on exactly 2 constructor interfaces: `StateStore` + `TaskQueue`. `StateStore` is intentionally a broad facade; optional capabilities (`AtomicStateStore`, durable suspend/signal, dead-letter, observers) must be explicitly documented and contract-tested by each backend.
- `service/` is the server/runner process-level implementation boundary (`runner/` / `control/` / `protocol/`). Core packages (`engine`, `node`, `types`, `store`) must NEVER import `service/` or `cmd/`. `sdk/xflow` is the only allowed exception: it may import `service/apiserver` and `service/control` solely to expose the supported `xflow.NewServer` embedded control-plane facade; reusable backend behavior must still live under `backend/`.
- `backend/providers/distributed/internal/rstate` is the Redis authority sub-system. Keep Redis state-machine changes grouped by contract area (execution/node, lease, outbox/dead-letter, suspend/signal, audit/projection, namespace) and extend the relevant backend contract tests when changing one.

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

<!-- antd-cli setup start -->
## Ant Design CLI Skill

Use the shared Ant Design skill at `.agents/skills/antd/SKILL.md` before working on Ant Design code in this repository.

The skill teaches agents when and how to call `@ant-design/cli` commands such as `antd info`, `antd doc`, `antd demo`, `antd token`, `antd semantic`, and `antd changelog`.

<!-- antd-cli setup end -->
