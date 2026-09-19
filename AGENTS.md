# AGENTS.md

This file provides guidance to AI coding agents working with code in this repository.

## Common Commands

```bash
go mod download          # Dependencies
make build               # Build
make fmt                 # Format
make lint                # Lint (adds --build-tags soak so test/soak/ is seen)

# Verification — pick the smallest that covers your change (see below)
make vet                 # Vet every build tag
make test                # Full gate: race, uncached, all ordinary packages
```

### Test execution discipline

Do not run the full suite or `-race` unless the task requires it. Both are the
repository's slowest gates and are reserved for CI and pre-merge verification,
not the inner development loop. Skipping them is not skipping verification.

- Minimum for any change: `go build ./...`, `make vet` (it also vets the
  build-tagged code the default config cannot see), and the tests for the
  packages you touched: `go test ./path/to/pkg/... -count=1`.
- Add `-race` only when the change touches concurrency or shared state; leave it
  off for pure logic changes.
- Full-gate targets are opt-in and should be named in advance when started:
  `make test` (race-enabled, uncached, every ordinary package, 5m per package),
  `make test-script-wasm` (WASM package once in isolation, 45m watchdog; ~24m
  measured under `-race`), `make test-coverage`, `make test-integration*`.
- A bare `go test ./... -race` cannot carry the WASM package — Go's 10m default
  package timeout is far below its ~24m race cost, so it fails with a misleading
  timeout panic. Use the Makefile targets, which supply the watchdog.

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
  - `runner/` — `package runner`: reusable standalone-runner CLI/process facade (`NewCommand`, `Execute`, `ExecuteProfile`) over `xflow.NewRunner`
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
- `service/` is the server/runner process-level implementation boundary (`runner/` / `control/` / `protocol/`). Lower-layer packages (`engine`, `node`, `types`, `store`, `execution`, `observability`) must NEVER import `service/` or `cmd/`; additionally, `engine/` and `observability/` must not import concrete `backend/providers/`. `sdk/xflow` and `sdk/runner` are the only SDK process-layer facades. `sdk/xflow` may directly import only `service/apiserver`, `service/control`, `service/runner`, and `service/protocol`, solely for its supported embedded `xflow.NewServer` / `xflow.NewRunner` facades and shared runner-client support. `sdk/runner` may directly import only `service/runner` and `service/protocol`, solely to expose the reusable standalone-runner (`NewCommand`, `Execute`, `ExecuteProfile`) facade over `xflow.NewRunner`; it must not carry application or business logic. No other `sdk/` package may import `service/` or `cmd/`; reusable backend behavior must still live under `backend/`. These SDK facade exceptions do not relax the lower-layer constraints. The lower-layer prohibition is enforced transitively for `node/` by `node/layering_test.go` and by direct-import scanning of the listed lower-layer source roots in `test/architecture/production_dependencies_test.go`; no other exceptions are allowlisted.
- `backend/providers/distributed/internal/rstate` is the Redis authority sub-system. Keep Redis state-machine changes grouped by contract area (execution/node, lease, outbox/dead-letter, suspend/signal, audit/projection, namespace) and extend the relevant backend contract tests when changing one.

## AI-Generated Documentation Placement

Agent-produced design docs, specs, plans, and review reports go in exactly two
places, both gitignored process artifacts:

- `docs/specs/` — designs and specs, named `YYYY-MM-DD-<topic>-design.md`
- `docs/plans/` — implementation plans and working notes, named
  `YYYY-MM-DD-<topic>-plan.md`

Do not create a third location, and do not put these in `docs/design/` — that
is reserved for tracked, human-maintained architecture docs, alongside
`docs/dsl-samples/` and `docs/references/`. Two things deliberately stay where
they are: ADRs are durable tracked records of decisions the code already
embodies (live in `docs/design/`, named `ADR-<id>-<topic>.md`), and
`.claude/superpowers/sdd/` is per-run execution scratch owned by the
orchestration tooling.

Because `docs/specs/` and `docs/plans/` do not ship, a tracked file that cites a
path under them hands the reader a dead link. Do not add such citations; promote
whatever content is needed into the tracked file itself. The one deliberate
exception is `docs/design/RUNNER-IDENTITY-LIFECYCLE-TODO.md`, which names its
source spec only to say that path is gitignored and transcribes the rulings in
full — copy that shape when provenance is needed.

Background only: six legacy locations were consolidated on 2026-09-10
(`.claude/specs/`, `.claude/plans/`, `.claude/docs/specs/`, `.claude/decisions/`,
`.claude/superpowers/{specs,plans}`, `docs/superpowers/{specs,plans,notes}`);
their ignore rules remain only to catch tools still writing an old path. A
related unfixed habit: some comments cite a bare `design §6.1` / `§4.2` / `§7.1`
with no document named, and at least one matches no section anywhere.

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
