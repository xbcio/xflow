# XFlow

XFlow is a distributed workflow engine built with Go, using a Master-Worker architecture powered by Asynq (Redis-based task queue). The system enables high-concurrency, highly-available, and scalable workflow orchestration and scheduling.

## Key Technologies

- **Task Scheduling**: Asynq (Redis-based)
- **Expression Engine**: Expr (expr-lang/expr)
- **State Storage**: Redis + PostgreSQL/MySQL
- **Monitoring**: Prometheus + Grafana
- **Tracing**: OpenTelemetry
- **API**: gRPC + HTTP
- **Logging**: Structured logging (logrus/zap)

## Architecture

```
Master Cluster (Orchestration & Scheduling)
    ├── Workflow Engine     - DSL parsing, DAG construction, workflow orchestration
    ├── Scheduler (Asynq)   - Task scheduling and queuing
    ├── State Manager       - Workflow and task state management (Redis)
    ├── Monitor             - Metrics, logging, tracing
    ├── API Server          - gRPC and HTTP endpoints
    └── Version Controller  - Version management and rollback
           ↓
    Asynq Queue (Redis)
           ↓
Worker Pool (Task Execution)
    ├── Task Handlers       - Execute different task types (HTTP, gRPC, database, etc.)
    ├── Expression Engine   - Evaluate Expr expressions in task parameters
    └── Plugin System       - Dynamically loadable custom task handlers
```

## DSL

XFlow uses an n8n-inspired DSL. Key concepts:

- **Nodes** — Individual workflow steps
- **Connections** — Explicit data flow between nodes (instead of `depends_on`)
- **Triggers** — How workflows are initiated (webhook, cron, event, queue)
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
| Notification | `xflow.notification` | Email, SMS, webhook |
| Switch | `xflow.switch` | Conditional branching |
| Parallel | `xflow.parallel` | Concurrent execution |
| Loop | `xflow.loop` | Iteration over data |
| Wait | `xflow.wait` | Human approval, external events |
| Merge | `xflow.merge` | Combine multiple branches |

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
