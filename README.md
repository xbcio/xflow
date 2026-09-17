# XFlow

XFlow is a Go workflow engine for SDK-embedded orchestration. The current
production focus is long-running approval workflows: DAG scheduling,
action-node execution, suspend/resume by signal, cancellation, inspection, and
Redis/Asynq-backed execution.

## Key Technologies

- **Task Scheduling**: Asynq (Redis-based)
- **Expression Engine**: Expr (expr-lang/expr)
- **State Storage**: Redis + MySQL (PostgreSQL: no typed error classifier or runtime evidence yet — see RELEASE-GATES §A3; not in G0/G1 support scope)
- **Monitoring**: Prometheus + Grafana
- **Tracing**: OpenTelemetry
- **API**: gRPC + HTTP
- **Logging**: Structured logging (zap)

## SDK API

Use `sdk/xflow` as the public entry point:

- `xflow.NewLocal()` starts an in-process engine for tests and embedded use.
- `xflow.NewCluster(...)` uses Redis/Asynq for distributed execution.
- `Engine.AddWorkflow` registers a workflow and returns a stable workflow ID.
- `Engine.Invoke` starts an execution from an explicit entry such as
  `xflow.Start()` or `xflow.Trigger(name)`.
- `Engine.Wait` waits for completion.
- `Engine.Signal` resumes suspended approval or wait nodes.
- `Engine.RevokeSignal` revokes a pre-delivered signal before it is consumed.
- `Engine.Cancel` cancels an active execution.
- `Engine.Inspect` returns execution and node status for audit/UI flows.

Approval workflows can use `node.Approval(...).WithTimeout("48h", "reject")`
and wait nodes can use `node.Wait("signal").WithTimeout("30m")`.

## Embedded Production Boundary

For production vulnerability approval systems, embed XFlow as the workflow
scheduler/runtime, not as the business approval system of record:

- The host service owns approval tickets, permissions, immutable approval
  events, audit logs, notification delivery, and idempotency keys.
- XFlow owns DAG scheduling, task execution, suspend/resume, timeout routing,
  cancellation, and inspection.
- In cluster mode, do not use `WorkflowBuilder.LocalNode`; every worker process
  must declare the same custom node capabilities through `xflow.WithNodes(...)`.
- API-only service instances can use `ClusterConfig{DisableConsumer: true}` so
  they register/invoke workflows, inspect, signal, and cancel without consuming
  workflow tasks.
- Worker instances should use the same Redis backend with consumers enabled and
  the full custom node definition set loaded.
- Complex approval nodes should pass an approval event ID in the signal payload
  and read the authoritative approval event set from the host service database.

## Delivery Semantics

XFlow is **at-least-once**, not exactly-once. Both the handler boundary and the
Runner Protocol may invoke the same work more than once: lease replay after a
runner reconnect, a node execution timeout, and failover to another runner can
each cause a duplicate handler invocation. The engine fences the DAG itself, so
a duplicate result does not advance a node twice, but it cannot deduplicate a
host-side side effect. **Business side effects are the host's responsibility**,
and the recommended idempotency key is `execution_id` + `node_name` (or an
equivalent business key, such as the source record's own ID).

The same contract reaches the Kafka trigger's source records. In
`aggregate` mode, `on_overflow` defaults to `discard`, which permanently drops
records that arrive while a partition is at its retained bound — that is loss,
not deferral. Any topic that cannot afford it must set `on_overflow`
explicitly; the alternatives are `block` (loses nothing, but halts fetching for
the whole assignment and is invisible to the lag gauges) and `dead_letter`
(republishes to `dead_letter_topic` before the offset may pass). The per-value
trade-offs are spelled out in
[DSL-SPECIFICATION.md](docs/design/DSL-SPECIFICATION.md) and
[kafka-batch-overflow.yaml](docs/dsl-samples/kafka-batch-overflow.yaml).

## Supported Topologies and Guarantees

Nothing outside the lists in this section is promised. When a capability below
is marked **planned** or **unproven**, treat it as unavailable rather than as a
configuration you can enable.

**What XFlow supports today**

- **Delivery is at-least-once** — handler invocation and Runner Protocol
  responses may both repeat; the host owns side-effect idempotency under the
  `execution_id` + `node_name` key. See [Delivery Semantics](#delivery-semantics)
  above.
- **local, cluster, and the server + runner split** are implemented. The
  split's durable Redis handoff (assignment claim, reconnect lease replay,
  fenced release) is exercised end-to-end against a real Redis/Asynq backend by
  the required integration shard (`make test-integration-required`), alongside
  the runner-reconnect replay coverage in
  [DEPLOYMENT-TOPOLOGIES.md](docs/design/DEPLOYMENT-TOPOLOGIES.md) §4.2.
- **HTTP long-poll is the production Runner Protocol channel**; gRPC is the
  experimental one (see below).
- **Runner-side enforcement** — bearer token / mTLS / runner policy allowlist,
  network-scoped runner placement, and server-issued workflow namespaces — is
  implemented. Capability matching is exact on `node_type` / `node_version`;
  tags, env, region, and weighted scheduling are **planned**.

**Topology support matrix**

| Topology | Status | Notes |
|---|---|---|
| SDK `local` — in-process, in-memory | Supported | No persistence; direct inline `ActionHandler` only here |
| SDK `cluster` — peer processes on Redis/Asynq | Supported | Every process submits and also executes; roles cannot be separated |
| `server` + `runner` split via Runner Protocol | Supported | HTTP is the recommended production path (see the gRPC note below) |
| Embedded `xflow.NewServer` / `xflow.NewRunner` | Supported | Same assembly as `cmd/server` / `cmd/runner` |
| SDK `remote` thin client | **Planned** | No factory or client exists yet; use `cluster` or the server HTTP API |
| Relay Gateway | **Planned** | No standalone process; runners must reach the server directly |
| Leader election (Redis lease) | Implemented, **not** HA | Gates leader-only maintenance only; see below |

The topology catalogue, its per-entry status labels, and the reasoning behind
each are maintained in
[DEPLOYMENT-TOPOLOGIES.md](docs/design/DEPLOYMENT-TOPOLOGIES.md) §1 and §7.

**Not supported, and not to be claimed**

- **No full control-plane HA.** Leader election (`RedisLeaderElector`) only
  elects the process allowed to run leader-only maintenance. It is not
  metadata replication, not cross-replica API ownership, and not a failover
  SLO.
- **No multi-namespace production isolation claim.** The namespace boundary is
  implemented in code and contract-tested, but production isolation is not
  accepted until a real multi-namespace environment is exercised.
- **Out of scope**: the remote SDK, the Relay Gateway, Raft, and any general
  low-code / ETL / browser-automation platform.

**What would be required to claim HA**

A real G2 control-plane HA soak: at least two servers, real Redis
Sentinel/Cluster, ≥ 2 runners, and a persistent store,
run through the existing fault matrix and written up in the existing HA soak
report. `make test-soak` in this repository is **not** that evidence — it is an
in-process run over a miniredis emulator, and its Redis-failover and
network-partition injectors are expected to report themselves as env-gated
because a single-node emulator cannot induce a real failover. The gate layering
that defines this boundary is in
[RELEASE-GATES.md](docs/design/RELEASE-GATES.md); the support matrix that this
statement would need to match, and the decisions still awaiting an approver,
are listed as open in
[RELEASE-GATES.md §6](docs/design/RELEASE-GATES.md#6-open-approvals未批准事项支持矩阵--迁移停机窗口--runbook-owner).

**Experimental capabilities**

- **gRPC Runner Protocol streaming and credit-flow control** are experimental
  transport optimizations. HTTP long-poll is the production channel. gRPC is
  also missing the activation-ack path, so a gated activation under a
  gRPC-only deployment can only be cleared by restarting the runner.
- **Loop / Split** expansion paths are experimental and are excluded from
  static-DAG completion guarantees.
- **Node Group co-location** is implemented but experimental/limited, and is
  opt-in through `WorkflowOptions.experimental_node_group`.

## DSL

XFlow uses an n8n-inspired DSL. Key concepts:

- **Nodes** — Individual workflow steps
- **Connections** — Explicit data flow between nodes (instead of `depends_on`)
- **Context** — Global variables, config, and secrets

```yaml
nodes:
  - name: validate
    type: xflow.http
  - name: process
    type: xflow.http

connections:
  validate:
    main:
      - node: process
```

### Expression Syntax

```yaml
$input.order_id                # Input parameters
$('node_name').json.field      # Another node's output
$ctx.api_base_url              # Global variables
$config.env                    # Configuration
$secret.api_key                # Secrets
$now()                         # Built-in functions
```

### Node Types

| Type | Identifier | Purpose |
|------|-----------|---------|
| HTTP Request | `xflow.http` | REST API calls |
| gRPC Call | `xflow.grpc` | Microservice communication |
| Function | `xflow.function` | Go function execution |
| Script | `xflow.script` | WASM/JS script execution |
| Database | `xflow.database` | CRUD operations |
| IF | `xflow.if` | Boolean branching |
| Switch | `xflow.switch` | Conditional branching |
| Wait | `xflow.wait` | External signals and timers |
| Approval | `xflow.approval` | Human approval gates |
| Merge | `xflow.merge` | Combine multiple branches |
| Notification | `xflow.notification` | Send notifications |
| Supply (static) | `xflow.supply.static` | Inject static supply values |
| Supply (external) | `xflow.supply.external` | Fetch supply values from external source |
| Transform | `xflow.transform.*` | Data transforms: `aggregate`, `filter`, `limit`, `pick`, `remove_duplicates`, `rename`, `set`, `sort` |
| Trigger | `xflow.trigger.*` | Entry triggers: `timer`, `cron`, `webhook`, `kafka`, `redis` |

For the complete node type reference including parameters and connection ports, see [docs/design/DSL-SPECIFICATION.md](docs/design/DSL-SPECIFICATION.md).

## Design Principles

1. **Explicit over Implicit** — Use connections to show data flow, not hidden dependencies
2. **Type Safety** — Leverage Expr's compile-time type checking
3. **Performance** — High concurrency, low latency, efficient resource usage
4. **Reliability** — Fault isolation, graceful degradation, automatic recovery
5. **Extensibility** — Plugin architecture for custom node types
6. **Observability** — Comprehensive monitoring, logging, and tracing
7. **Progressive Complexity** — Simple for basic workflows, powerful for complex ones

## Documentation

See `docs/` for detailed design documentation:

- [docs/design/DSL-SPECIFICATION.md](docs/design/DSL-SPECIFICATION.md) — Complete DSL syntax specification
- [docs/design/ARCHITECTURE.md](docs/design/ARCHITECTURE.md) — Current implemented architecture (engine/execution/backend layering)
- [docs/design/DEPLOYMENT-TOPOLOGIES.md](docs/design/DEPLOYMENT-TOPOLOGIES.md) — SDK modes, server/runner cluster, current vs planned
- [docs/design/CORE-COMPONENTS.md](docs/design/CORE-COMPONENTS.md) — Target design for server clustering
- [docs/dsl-samples/](docs/dsl-samples/) — Runnable DSL examples (e.g. `purchase-approval.yaml`)
