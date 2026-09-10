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
- **node/** — Public node DSL and builtin implementations (`node.HTTP`, `node.Function`, `node.Script`, etc.)
  - `trigger/` — trigger factories (`trigger.Timer()` … `trigger.Kafka()`); each kind in its own subpackage (`timer/`, `cron/`, `webhook/`, `kafka/`, `redis/`), each self-registering via `init()`
  - `trigger/triggertest/` — shared fakes for `types.TriggerRuntime` / `types.TriggerLock` (a normal package, not `_test`, so all five subpackages can import it)
- **types/** — Public DSL/runtime contracts: `WorkflowDef`, handler interfaces (`ActionHandler`, `SuspendingHandler`), handler IO, descriptors, statuses, `Result` (json-tagged, zero impl deps)
- **store/** — Public persistence interfaces + domain models
  - `memstore/` — in-memory implementation (test / local)
  - `sqlstore/` — dialect-agnostic GORM implementation; `sqlstore/mysqlstore/` — MySQL dialect entry (`mysqlstore.New`)
  - `objectstore/` — generic S3-shaped object storage contract (artifact/script blob store)
  - `storetest/` — cross-backend behavioural contract tests shared by `memstore` and `sqlstore`
- **backend/** — Reusable backend provider abstractions
  - `providers/local/` — In-memory StateStore + goroutine pool TaskQueue
  - `providers/distributed/` — Redis StateStore + Asynq TaskQueue
- **namespace/** — Server-issued permission namespace type and context propagation primitives
- **execution/** — Reusable embedded task execution boundary: Dispatcher, Runner, Registry; `subgraph/` handles nested graph execution
- **exprx/** — Expression evaluation helpers shared by builtin nodes (`xflow.if`, `xflow.switch`, `xflow.map`, `xflow.function`, `xflow.script`, etc.)
- **observability/** — Structured logging, Prometheus metrics, and OTLP tracing adapters shared across engine, dispatcher, and runner (`logger/`, `metrics/`, `tracing/`)
- **sdk/**
  - `xflow/` — `package xflow`: `NewLocal` / `NewCluster` factories, `NewServer` embedded control-plane facade, WorkflowBuilder, and production control APIs (`AddWorkflow`, `Invoke`, `Wait`, `Signal`, `RevokeSignal`, `Cancel`, `Inspect`)
  - `examples/` — runnable `.go` usage examples
- **service/** — Server/runner process-level implementation boundary (cluster topology)
  - `apiserver/` — HTTP API server (the `/v1` face served to external callers; `sdk/xflow.NewServer` assembles it)
  - `runner/` — cluster task runner process (holds `ProtocolClient` + embedded `execution.Runner`)
  - `control/` — control plane: controlplane, dispatcher, auth, core connect
  - `crypto/` — cryptography support: `masterkey/` (key management) and `supplyenc/` (supply value encryption)
  - `protocol/` — wire protocol + `protocol/runnerpb/` generated gRPC + `HTTPEntrySeedRuntime` (the entry-seed admission client for the DTOs in `entry_seed.go`)
- **api/openapi/** — OpenAPI specification for the `/v1` HTTP surface (`xflow-v1.yaml`; validated by `make validate-openapi`)
- **cmd/server/** — Management server (Master node) entrypoint
- **cmd/runner/** — Task runner (Execution node) entrypoint
- **cmd/xflow/** — CLI binary entry point (dead-letter inspection and other operator commands)
- **db/** — SQL schema
- **docs/** — `design/` specs, `dsl-samples/` (`.yaml` DSL samples), `references/`

## Key Constraints

- `engine/` must NOT import redis/asynq/mysql/sql/network transports. It may depend on public contracts (`types`, `namespace`, `engine/graph`) and the narrow observability tracing facade only for engine-owned spans; it must not depend on concrete metrics/logging exporters or provider packages.
- Graph IR is immutable after compile, shared lock-free at runtime
- Engine Core depends on exactly 2 constructor interfaces: `StateStore` + `TaskQueue`. `StateStore` is intentionally a broad facade; optional capabilities (`AtomicStateStore`, durable suspend/signal, dead-letter, observers) must be explicitly documented and contract-tested by each backend.
- `service/` is the server/runner process-level implementation boundary (`runner/` / `control/` / `protocol/`). Lower-layer packages (`engine`, `node`, `types`, `store`, `execution`, `observability`) must NEVER import `service/` or `cmd/`; additionally, `engine/` and `observability/` must not import concrete `backend/providers/`. `sdk/xflow` is the only allowed process-layer exception: it may import `service/apiserver` and `service/control` solely to expose the supported `xflow.NewServer` embedded control-plane facade; reusable backend behavior must still live under `backend/`. Enforced transitively for `node/` by `node/layering_test.go` and repository-wide for direct production imports by `test/architecture/production_dependencies_test.go`; no exceptions are allowlisted.
- `backend/providers/distributed/internal/rstate` is the Redis authority sub-system. Keep Redis state-machine changes grouped by contract area (execution/node, lease, outbox/dead-letter, suspend/signal, audit/projection, namespace) and extend the relevant backend contract tests when changing one.

## AI-Generated Documentation Placement

Agent-produced design docs, specs, plans, and review reports go in exactly two
places:

- `docs/specs/` — designs and specs, named `YYYY-MM-DD-<topic>-design.md`
- `docs/plans/` — implementation plans and their working notes, named
  `YYYY-MM-DD-<topic>-plan.md`

Both are gitignored, so these documents never enter version control. They are
process artifacts, not published material.

Do not create a third location. `.claude/specs/`, `.claude/plans/`, and
`docs/superpowers/` are historical paths that have been consolidated into the
two above; their ignore rules survive only to catch a tool that still writes
the old path. Do not put these documents under `docs/design/`, which is
reserved for human-maintained architecture docs that ARE tracked, alongside
user-facing references (`docs/dsl-samples/`, `docs/references/`).

One consequence worth stating: whoever clones this repository does not get
`docs/specs/` or `docs/plans/`, so a tracked file citing a path under them
gives that reader a dead link. Do not add new such citations; when tracked
code or a tracked doc needs to lean on one of these documents, promote the
content it needs into the tracked file itself.

Known outstanding violations, not yet resolved: `docs/design/RELEASE-GATES.md`
leans on a `docs/specs/` roadmap for its P0 Exit Gate criteria (marked inline),
and ten comments in production Go and the Makefile still cite four specs
(`lua-concurrency-tests.md`, `resource-pool.md`, `dual-write-contract.md`,
`handler-version.md`) that `06ed35c` deleted outright — those pointers have
resolved to nothing since then and need their content recovered from history
or the comment rewritten.

## Git Commits

`type(scope): what changed` — and nothing else. Four hard rules:

1. **Single-line subject, no body.** Not a short body, not a bullet list, not
   "just one line of context" — no body.
2. **Subject ≤ 70 characters**, counting the `type(scope): ` prefix.
3. **One commit, one change.** If the subject needs " and " or a comma to cover
   what you did, that is two commits.
4. **Scope is a Go package**, not a file or a topic: `control`, `runner`,
   `wasm`, `supply`, `protocol`. Repo-root and tooling changes take no scope.

Reasoning, trade-offs, and alternatives-considered belong in the PR description
or a `.claude/` doc — never in the commit message. Commits already in history
that carry a body are grandfathered, not a pattern to copy: `git log` is not the
convention, **[docs/GIT-COMMITS.md](docs/GIT-COMMITS.md)** is.

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

Use the shared Ant Design skill at `.claude/skills/antd/SKILL.md` before working on Ant Design code in this repository.

The skill teaches agents when and how to call `@ant-design/cli` commands such as `antd info`, `antd doc`, `antd demo`, `antd token`, `antd semantic`, and `antd changelog`.

<!-- antd-cli setup end -->
