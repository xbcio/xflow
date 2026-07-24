# XFlow

XFlow is a Go workflow engine for SDK-embedded orchestration. The current
production focus is long-running approval workflows: DAG scheduling,
action-node execution, suspend/resume by signal, cancellation, inspection, and
Redis/Asynq-backed execution.

## Key Technologies

- **Task Scheduling**: Asynq (Redis-based)
- **Expression Engine**: Expr (expr-lang/expr)
- **State Storage**: Redis + PostgreSQL/MySQL
- **Monitoring**: Prometheus + Grafana
- **Tracing**: OpenTelemetry
- **API**: gRPC + HTTP
- **Logging**: Structured logging (logrus/zap)

## SDK API

Use `sdk/xflow` as the public entry point:

- `xflow.NewLocal()` starts an in-process engine for tests and embedded use.
- `xflow.NewCluster(...)` uses Redis/Asynq for distributed execution.
- `Engine.Submit` starts a workflow.
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
  they submit, inspect, signal, and cancel without consuming workflow tasks.
- Worker instances should use the same Redis backend with consumers enabled and
  the full custom node definition set loaded.
- Complex approval nodes should pass an approval event ID in the signal payload
  and read the authoritative approval event set from the host service database.

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
| Database | `xflow.database` | CRUD operations |
| IF | `xflow.if` | Boolean branching |
| Switch | `xflow.switch` | Conditional branching |
| Wait | `xflow.wait` | External signals and timers |
| Approval | `xflow.approval` | Human approval gates |
| Merge | `xflow.merge` | Combine multiple branches |

`xflow.loop` and `xflow.split` are currently experimental and should not be
used for production vulnerability approval flows yet.

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

- [docs/README.md](docs/README.md) — Complete overview
- [docs/00-design-summary.md](docs/00-design-summary.md) — 5-minute architecture overview
- [docs/DSL-SPECIFICATION.md](docs/DSL-SPECIFICATION.md) — Complete DSL syntax specification
- [docs/CORE-COMPONENTS.md](docs/CORE-COMPONENTS.md) — Complete component architecture
