# SDK examples

These examples are executable Go tests for the embedded SDK surface.

## Files

- `basic_test.go` shows local-mode DAG basics: direct handlers, port routing, and error-output branching.
- `vulnerability_approval_test.go` shows the production-oriented approval shape for distributed services: typed node handlers, built-in approval gates, external signals, inspection, and rejected/approved branches.
- `cyclic_vulnerability_approval_test.go` shows an opt-in cyclic approval workflow: explicit `xflow.start`, rejected security review returning to remediation, validation looping back into review, and final change approval/deployment/closure.
- `runner_selector_test.go` shows workflow-level default runner placement with a node-level local approval override.

## Local vs cluster handlers

Use `wf.LocalNode` only for local development and single-process tests. It stores a Go function value in memory, so it cannot be serialized or executed by another service instance.

For distributed services, define reusable typed nodes and instantiate them in
the workflow:

```go
var NormalizeVulnerability = node.Define("demo.vuln.normalize",
	func(ctx context.Context, input *node.Input) (*node.Output, error) {
		return &node.Output{Data: input.Data}, nil
	},
)

wf := xflow.Workflow("vulnerability-approval")
start := wf.Node("start", node.Start())
normalize := wf.Node("NormalizeVulnerability", NormalizeVulnerability.New(nil))
approval := wf.Node("SecurityApproval",
	node.Approval([]string{"sec-owner", "app-owner"}, node.ApprovalSequential),
)
wf.Connect(start, normalize).Connect(normalize, approval)
```

`node.Define` is the single custom-node API. `Definition.New(params)`
stores only node type and params in the workflow definition, and exposes the
handler instance to the current process registry. The queued workflow remains
portable; the Go function value is never serialized.

## Embedded cluster topology

Worker-capable pods create a cluster engine and run workflows with typed handlers:

```go
eng, err := xflow.NewCluster(xflow.ClusterConfig{
	RedisAddr: "redis:6379",
}, xflow.WithNodes(NormalizeVulnerability))
```

API-only pods can register workflows, invoke executions, inspect, cancel, and signal without consuming tasks:

```go
api, err := xflow.NewCluster(xflow.ClusterConfig{
	RedisAddr:       "redis:6379",
	DisableConsumer: true,
})
```

In cluster mode, do not register workflows containing `LocalNode`; use typed
nodes (`node.Define`, built-in nodes). `AddWorkflow` registers workflow
metadata and workflow-declared handlers in the current process, `Invoke` starts
an execution from an explicit entry such as `xflow.Start()`, and
`xflow.WithNodes` declares consumer capabilities for processes that may execute
tasks registered by other service instances.

## Runner selectors

Runner selectors are workflow and node definition metadata. Do not pass runner
placement through `InvokeOption`.

```go
wf := xflow.Workflow("risk-review").
	RunnerSelector(xflow.DefaultRunnerSelector(map[string]string{
		"mode":   "remote",
		"env":    "prod",
		"tenant": "tenant-a",
	}))

approval := wf.Node("SecurityApproval",
	node.Approval([]string{"sec-owner"}, node.ApprovalAny),
).RunnerSelector(xflow.RunnerSelector(map[string]string{
	"mode": "local",
	"env":  "prod",
}))
```

`xflow.NewLocal` and the current embedded `xflow.NewCluster` path preserve and
validate selectors, but they do not enforce runner placement. Server plus
runner protocol is the placement enforcement path: runner labels declared by
`cmd/runner --label key=value` are matched against the effective selector after
capability matching.

### Kafka high-throughput transient mode

For Kafka ingestion workloads that can acknowledge messages once xflow accepts
a batch, use transient mode with Kafka partition aggregation:

```go
eng, err := xflow.NewCluster(cfg,
	xflow.WithExecutionMode(xflow.ExecutionModeTransient),
	xflow.WithConcurrency(128),
	xflow.WithTransientTTL(10*time.Minute),
	xflow.WithTransientCompletionTTL(30*time.Second),
)

kafka := node.KafkaTrigger().
	Brokers("broker-1:9092", "broker-2:9092").
	Topic("orders").
	Group("xflow-orders").
	MaxInflight(256).
	AggregateByPartition(100, 50*time.Millisecond)
```

The Kafka trigger commits offsets after `Emit` accepts the batch. Workloads
that need offset commit only after workflow success need a separate ack
strategy.
