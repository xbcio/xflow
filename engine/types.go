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
	// 6 曾是 TaskTypeGroupResume（durable group suspend，已随该子系统一并移除）。
	// TaskType 是落进 outbox 的持久化数值，契约是 append-only：下一个新类型从 7
	// 起，不要回收 6。生产从未产生过值为 6 的任务，所以存量数据里没有它。
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

	// Port is the output port the source node committed on, carried forward on
	// TaskTypeNodeAdvance tasks so the advance does not have to read the node
	// back out of the store to learn a value the committing side already had.
	// Like the fields above it is internal metadata, not part of the public
	// runner JSON contract.
	//
	// It is a pointer because the empty port is a real, load-bearing value, not
	// an absence: a skipped node commits with no active port, and that is
	// exactly what tells downstreamArrivals to propagate the skip rather than
	// activate a branch. A plain string would make "no active port" and "this
	// task predates the field" the same value, and the advance would route a
	// legacy entry as though its source had been skipped. Same reason UnitIdx
	// goes over the wire as *int.
	//
	// nil means "not carried", and the advance branch falls back to reading the
	// node for that case. That is what makes a rolling deploy safe, since
	// advance entries already sitting in a durable outbox decode with it nil;
	// do not delete the fallback on the grounds that every task obviously
	// carries a port now.
	Port *string `json:"-"`

	// UnitIdx 是任务所属 durable unit 的下标。普通 node 任务恒等于其 node 下标
	// （无 group 时 unit 索引 == node 索引，退化等价）；group 任务指向 group unit。
	UnitIdx int `json:"-"`
}

// UnitIdxUnknown is the sentinel value for Task.UnitIdx meaning "the durable
// wire envelope this task was decoded from did not carry a _unit_idx field".
// Pure codecs (queue/outbox/dead-letter/assignment) set UnitIdx to this value
// instead of silently defaulting to 0 or NodeIdx when the field is absent —
// 0 is a legitimate unit index and NodeIdx is only equal to the unit index
// for graphs with no groups (see engine/graph/unit.go buildUnits). A
// graph-aware resolver layer (not the pure codec) is responsible for turning
// UnitIdxUnknown into a real unit index via Graph.UnitIndexForNode, or
// failing closed if it cannot load the authoritative graph.
const UnitIdxUnknown = -1

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
	// ExecutionDeadline is the absolute instant past which this node's single
	// execution is over. Stamped at lease build time from the compiled node
	// timeout. Zero means no limit.
	//
	// It is NOT the lease TTL. TTL is the renewal clock (60s by default); this
	// is the business deadline the renewal loop is not allowed to push past,
	// and the server refuses to renew beyond it (service/control renewLease).
	// Group leases already carry an equivalent absolute instant in
	// GroupLeasePayload -- this is the same idea for node leases.
	ExecutionDeadline time.Time `json:"execution_deadline,omitempty"`
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
	// GroupPayload carries the full group execution context for TaskTypeGroupExec
	// tasks. Nil for regular node tasks. The control plane populates this from
	// BuildGroupLease/RecoverGroupLease and serializes it on the wire so the
	// runner has the group package, entry input, and idempotency key.
	GroupPayload *GroupLeasePayload `json:"group_payload,omitempty"`
	// SubgraphPayload carries one map batch's execution context for
	// TaskTypeNodeBatch tasks. Nil for every other task type. Like
	// GroupPayload it is authoritative: Input is nil on a batch lease.
	SubgraphPayload *SubgraphLeasePayload `json:"subgraph_payload,omitempty"`
}

// TaskRouting is the side-effect-free routing metadata for a queued task. It is
// used by control-plane dispatchers to pick a capable runner before issuing a
// lease, so queue backpressure does not consume handler attempts.
type TaskRouting struct {
	NodeType       string                  `json:"node_type"`
	NodeVersion    int                     `json:"node_version,omitempty"`
	RunnerSelector *types.RunnerSelector   `json:"runner_selector,omitempty"`
	Requirements   []CapabilityRequirement `json:"requirements,omitempty"`
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
	// Scope holds expression roots that belong to the EXECUTION rather than to
	// any one node: every node of this execution sees them, whatever its
	// position in the graph. Today its only producer is a map body, which puts
	// $item/$index/$items here.
	//
	// Params cannot carry them. engine/input.go reads snap.Params only for a
	// node with zero in-edges, so roots shipped as submission params reached the
	// body's ENTRY member and nothing else -- a two-member body failed at
	// "unknown name $index" and took the whole map node down with it. The DSL
	// promises these roots "body 内", not "body 入口".
	Scope   map[string]any
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
	// Error is the execution-level failure reason recorded by
	// UpdateExecutionStatus. It is the ONLY carrier when no node holds the
	// reason: a cyclic execution that trips MaxAutoDepth fails with every node
	// at success, because scheduler.go rejects the downstream activation rather
	// than failing the node that tripped it. Without this field the reason was
	// write-only — both backends persisted it, but nothing read it back, so the
	// sole readback was the SQL audit row (executions.error_msg), invisible to
	// callers of the inspect API.
	Error string `json:"error,omitempty"`
}

// TerminalExecutionError picks the execution-level failure reason to persist
// for a terminal execution.
//
// Both backends must agree on this, so it lives here rather than in either one:
// a cyclic reason wins over a node reason because the cyclic terminal path can
// carry a reason no node holds (MaxAutoDepth trips with the tripping node at
// success), and a non-failed status carries no reason at all so a success can
// never inherit a stale one.
func TerminalExecutionError(status types.ExecutionStatus, nodeErr, cyclicErr string) string {
	if status != types.ExecutionStatusFailed {
		return ""
	}
	if cyclicErr != "" {
		return cyclicErr
	}
	return nodeErr
}

// NodeSnapshot is the engine's view of a single node's latest state stored in
// the backend.
type NodeSnapshot struct {
	ExecutionID types.ExecutionID
	Name        string
	NodeIdx     int
	UnitIdx     int
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
	UnitIdx      int
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
