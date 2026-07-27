package engine

import (
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// TaskType distinguishes initial execution from a resume after suspension.
type TaskType int

const (
	TaskTypeNodeExec   TaskType = iota // normal first-time execution
	TaskTypeNodeResume                 // resume after signal/timer
	// TaskTypeNodeAdvance is an engine-internal durable scheduling task. It
	// never reaches a handler or remote runner.
	TaskTypeNodeAdvance
	// TaskTypeNodeSkip is an engine-internal durable skip-cascade task. It
	// terminalizes a node only after all of its inbound routes were skipped.
	TaskTypeNodeSkip
	// TaskTypeNodeBatch is an engine-internal Loop/Split batch continuation.
	// It must be consumed by Engine.ExecuteBatch rather than routed to a node
	// handler or remote runner.
	TaskTypeNodeBatch
	// TaskTypeGroupExec 把一整个 co-location 组作为单元派发给 runner 执行。
	TaskTypeGroupExec
	// TaskTypeGroupResume 恢复一个 durable-suspended 组（里程碑 A 预留，暂不消费）。
	TaskTypeGroupResume
)

// Task is the unit of work dispatched to the queue.
type Task struct {
	ExecutionID types.ExecutionID `json:"execution_id"`
	NodeName    string            `json:"node_name"`
	NodeIdx     int               `json:"node_idx"`
	Type        TaskType          `json:"type"`

	// Payload is non-nil only for TaskTypeNodeResume tasks.
	Payload *types.SignalPayload `json:"payload,omitempty"`

	// AutoDepth is internal cyclic scheduling metadata. Backend implementations
	// may persist it out-of-band, but it is not part of the public runner JSON
	// contract.
	AutoDepth int `json:"-"`

	// ActivationID is internal cyclic scheduling metadata. It lets a node
	// re-enter after a terminal state while fencing stale queued or leased
	// tasks, but it is not exposed in the public runner JSON contract.
	ActivationID int `json:"-"`

	// UnitIdx 是任务所属 durable unit 的下标。普通 node 任务恒等于其 node 下标
	// （无 group 时 unit 索引 == node 索引，退化等价）；group 任务指向 group unit。
	UnitIdx int `json:"-"`
}

// LeaseID uniquely identifies one assignment of a queued task to a runner.
type LeaseID string

// LeaseToken is the fencing token required when committing a leased task result.
type LeaseToken string

// NodeResult is the runner-facing result submitted after a node task executes.
type NodeResult struct {
	Output  *types.Output      `json:"output,omitempty"`
	Suspend *types.SuspendSpec `json:"suspend,omitempty"`
	Error   error              `json:"-"`
}

// TaskResult is the protocol-facing task execution result.
type TaskResult = NodeResult

// CommitOutcome classifies runner result commits so control-plane directories
// can clean up assignment and capacity state without parsing errors.
type CommitOutcome string

const (
	// CommitOutcomeAccepted means the result advanced node state or parked a suspend.
	CommitOutcomeAccepted CommitOutcome = "accepted"
	// CommitOutcomeDuplicateTerminal means the result repeated an already-terminal commit.
	CommitOutcomeDuplicateTerminal CommitOutcome = "duplicate_terminal"
	// CommitOutcomeStaleToken means the lease token no longer fences the node.
	CommitOutcomeStaleToken CommitOutcome = "stale_token"
	// CommitOutcomeExecutionInactive means the execution is no longer running.
	CommitOutcomeExecutionInactive CommitOutcome = "execution_inactive"
	// CommitOutcomeTransientError means storage or scheduling failed before classification completed.
	CommitOutcomeTransientError CommitOutcome = "transient_error"
)

// ReleasesLeasedCapacity reports whether this outcome should release runner capacity.
func (o CommitOutcome) ReleasesLeasedCapacity() bool {
	switch o {
	case CommitOutcomeAccepted, CommitOutcomeDuplicateTerminal, CommitOutcomeStaleToken, CommitOutcomeExecutionInactive:
		return true
	default:
		return false
	}
}

// TaskLease is the server-side assignment sent to a runner through Runner Protocol.
type TaskLease struct {
	LeaseID     LeaseID       `json:"lease_id,omitempty"`
	LeaseToken  LeaseToken    `json:"lease_token,omitempty"`
	Attempt     int           `json:"attempt,omitempty"`
	Task        Task          `json:"task"`
	Input       *types.Input  `json:"input,omitempty"`
	NodeType    string        `json:"node_type"`
	NodeVersion int           `json:"node_version,omitempty"`
	IssuedAt    time.Time     `json:"issued_at"`
	TTL         time.Duration `json:"ttl,omitempty"`
	// Namespace is the authoritative namespace recorded on the assignment at
	// submit time. It is set by the control plane when building/recovering the
	// lease so the report/commit path (which has no principal resolver) can
	// inject it into ctx and read/write the correct Redis namespace. This is
	// NOT placed in W3C baggage (RELEASE-GATES §4.1); it travels in the lease
	// payload, not in trace propagation headers.
	Namespace namespace.Namespace `json:"namespace,omitempty"`
	// TraceCarrier holds W3C traceparent/tracestate propagation headers so the
	// runner can create properly-parented execution spans. Populated by the
	// control plane when dispatching; nil when tracing is disabled or unsampled.
	TraceCarrier map[string]string `json:"trace_carrier,omitempty"`
}

// TaskRouting is the side-effect-free routing metadata for a queued task. It is
// used by control-plane dispatchers to pick a capable runner before issuing a
// lease, so queue backpressure does not consume handler attempts.
type TaskRouting struct {
	NodeType       string `json:"node_type"`
	NodeVersion    int    `json:"node_version,omitempty"`
	RunnerSelector *types.RunnerSelector
}

// Deadline returns the wall-clock instant after which the lease is considered
// expired. Returns the zero time if either IssuedAt or TTL is unset.
func (l TaskLease) Deadline() time.Time {
	if l.IssuedAt.IsZero() || l.TTL <= 0 {
		return time.Time{}
	}
	return l.IssuedAt.Add(l.TTL)
}

// RunnerCapability describes a node type/version a runner can execute.
type RunnerCapability struct {
	NodeType string `json:"node_type"`
	Version  int    `json:"version,omitempty"`
}

// RunnerHeartbeat reports runner capacity and supported capabilities.
type RunnerHeartbeat struct {
	RunnerID     string             `json:"runner_id"`
	Capacity     int                `json:"capacity"`
	InFlight     int                `json:"in_flight"`
	Capabilities []RunnerCapability `json:"capabilities,omitempty"`
}

// ExecutionSnapshot is the engine's view of a running execution stored in the backend.
type ExecutionSnapshot struct {
	ID      types.ExecutionID
	Graph   *graph.Graph
	Status  types.ExecutionStatus
	Params  map[string]any
	Runtime *types.Runtime
	TraceID string
	SpanID  string
	// TraceCarrier holds the W3C traceparent/tracestate headers captured at
	// submission (xflow.workflow.submit / xflow.workflow.invoke) so a later,
	// asynchronous dispatch (xflow.task.dispatch, potentially in a different
	// goroutine or control-plane replica) can extract a REAL W3C remote parent
	// for the dispatch span — not a trace_id/span_id string reconstruction
	// (RELEASE-GATES §4 forbids faking a parent from raw id strings). The
	// carrier round-trips through the W3C propagator, which preserves
	// tracestate and the sampled flag.
	TraceCarrier map[string]string `json:"trace_carrier,omitempty"`
	ParentID     types.ExecutionID // non-empty for sub-executions
}

// NodeSnapshot is the engine's view of a single node's latest state stored in
// the backend.
type NodeSnapshot struct {
	ExecutionID types.ExecutionID
	Name        string
	NodeIdx     int
	Status      types.NodeStatus
	LeaseID     LeaseID
	LeaseToken  LeaseToken
	Attempt     int
	// ActivationID is the latest cyclic activation version for this node.
	ActivationID int
	// AutoDepth is the automatic scheduling depth associated with the latest
	// activation. It is runtime metadata, not business history.
	AutoDepth int
	// LeaseIssuedAt / LeaseTTL track when the current lease was handed out and
	// for how long it is valid. The sweeper uses these to reclaim leases whose
	// runner crashed mid-execute. Both are zero for nodes without an active
	// lease.
	LeaseIssuedAt time.Time
	LeaseTTL      time.Duration
	// LeaseTaskType and LeasePayload preserve the exact queued task while a
	// lease is active or committing. They let crash recovery replay a resume
	// task without silently dropping its signal payload.
	LeaseTaskType TaskType
	LeasePayload  *types.SignalPayload
	// CommittedLeaseToken identifies the lease that produced the current
	// terminal state. It lets a retry after a lost commit response receive a
	// stable duplicate outcome without allowing a stale lease to advance the
	// graph.
	CommittedLeaseToken LeaseToken
	CommittedAttempt    int
	Output              map[string]any
	Port                string
	Error               string
}

// ExpiredLease describes a node whose lease has passed its deadline and is
// eligible for reclamation by the sweeper.
type ExpiredLease struct {
	ExecutionID  types.ExecutionID
	NodeName     string
	NodeIdx      int
	LeaseID      LeaseID
	LeaseToken   LeaseToken
	IssuedAt     time.Time
	TTL          time.Duration
	ActivationID int
	AutoDepth    int
	// Namespace is the namespace that owns the execution. The sweeper uses it to
	// reconstruct the namespace context for reclaim so keys are looked up in the
	// correct namespace.
	Namespace namespace.Namespace
	// TaskType and Payload reproduce the original queued task exactly when a
	// running or committing lease is reclaimed after a process crash.
	TaskType TaskType
	Payload  *types.SignalPayload
}

// SubExecution tracks a child execution spawned by a loop/split node.
type SubExecution struct {
	// JSON tags are lowercase because the Redis expansion Lua
	// (completeExpandedSubExecutionLua) addresses these fields by name
	// (child.status / child.result). Without the tags Go marshaled Status/Result
	// capitalized, so the Lua's status transition silently never fired. Result is
	// NOT omitempty so an empty-object batch result round-trips as {} rather than
	// being dropped.
	ParentExecID types.ExecutionID     `json:"parent_exec_id"`
	ParentNode   string                `json:"parent_node"`
	ChildExecID  types.ExecutionID     `json:"child_exec_id"`
	BatchIndex   int                   `json:"batch_index"`
	Status       types.ExecutionStatus `json:"status"`
	Result       map[string]any        `json:"result"`
}

// ExecutionEvent is emitted when an execution lifecycle state changes.
type ExecutionEvent struct {
	ExecutionID types.ExecutionID     `json:"execution_id"`
	Status      types.ExecutionStatus `json:"status,omitempty"`
	Error       string                `json:"error,omitempty"`
	Data        map[string]any        `json:"data,omitempty"`
}
