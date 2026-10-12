package engine

import (
	"context"
	"errors"

	"github.com/xbcio/xflow/types"
)

// ErrRerunUnsupported reports a StateStore that has not implemented
// ExecutionSeeder. It is not a verdict about the execution being rerun: the
// store cannot create an execution around a set of already-resolved units, so
// no execution on it can be rerun.
var ErrRerunUnsupported = errors.New("state store does not support seeding an execution")

// SeedUnit is one unit of a seeded execution whose outcome is already decided.
// The caller resolved it — it completed in the execution this one derives from
// — and the store records it rather than running it.
type SeedUnit struct {
	// UnitIdx is the durable unit index the scheduling counters are keyed by.
	// It is the unit index, not the node index: the two coincide only for
	// definitions whose units are all single nodes.
	UnitIdx int
	// NodeName is the name the unit's state is stored under — the name a
	// downstream expression reads and Inspect reports.
	NodeName string
	// NodeIdx is the representative node index, as NodeSnapshot records it.
	NodeIdx int
	// Status is the resolution to record: NodeStatusSuccess for a unit that
	// completed with an output, NodeStatusSkipped for one that was resolved
	// without producing one (a branch the parent execution did not take).
	Status types.NodeStatus
	// Port is the output port the unit's data left through, empty for a skipped
	// unit. It is what a downstream read of the output resolves against.
	Port string
	// Output is the unit's stored output data, nil for a skipped unit.
	Output map[string]any
	// PrivateOutput marks an output the public projections must not expose. It
	// is derived by the caller from the compiled graph, never from stored data.
	PrivateOutput bool
	// Downstream are the arrivals this unit pushes to its successors, exactly as
	// downstreamArrivals computes them for the port the unit left through.
	//
	// The store applies them the way it applies a commit's arrivals: decrement
	// each successor's in-degree, add the active count, and write the execute or
	// skip intent the threshold decision lands on. A skipped unit therefore
	// carries ActiveCount 0 arrivals, which is how a skip propagates.
	Downstream []DownstreamArrival
}

// SeedExecutionRequest creates an execution around a set of already-resolved
// units.
//
// It is the submission path's counterpart for an execution that does not start
// at the graph's roots: rerun-from-node derives one from a terminal execution,
// and the units upstream of the rerun point arrive here as resolutions. The
// store must therefore do everything a submission does — admit against the
// workflow's concurrency policy, create the execution, seed the counters,
// deliver the ready work — plus record the resolutions in the same transition.
// A partial seed is not a valid state: an execution whose upstream units are
// not all recorded would sit with an in-degree that can never reach zero, so
// the transition is all-or-nothing.
type SeedExecutionRequest struct {
	// Execution is the execution to create. Its Status is the status it should
	// start at when no admission is consulted (running); the store decides the
	// real one, since an admission can park it in the queue instead.
	Execution *ExecutionSnapshot
	// Units are the resolutions to record, in any order. Every unit named here
	// is RESOLVED: the store must never schedule it, now or when an arrival from
	// another seeded unit later reaches its counters.
	Units []SeedUnit
	// Entries are intents to deliver outright, for units no arrival reaches: a
	// resolved unit's in-degree is never decremented into a schedule, so a unit
	// with no in-edges that has to run — a rerun's from-node when it is a root —
	// needs its intent written explicitly. A unit named here is scheduled by
	// this entry exactly once; its own arrivals still apply.
	Entries []OutboxEntry
	// Admission is the workflow's concurrency policy, or nil when the graph
	// declares none. A refused admission persists nothing and is reported as an
	// error, exactly as it is on the submission path.
	Admission *ConcurrencyAdmission
}

// SeedExecutionResponse reports how the seed landed.
type SeedExecutionResponse struct {
	// Queued is true when the admission parked the execution in its workflow's
	// QUEUE-mode line rather than starting it. The resolved units were recorded
	// either way; only the delivery of the ready work waits for the promotion.
	Queued bool
	// QueuePosition is the queued execution's 1-based place in line, 0 otherwise.
	QueuePosition int
	// Skipped reports the units the arrival application resolved as skip rather
	// than execute (see SkippedUnit). Seeding is a scheduling transition like any
	// other: the arrivals it applies pick execute or skip per successor.
	Skipped []SkippedUnit
}

// ExecutionSeeder is the optional StateStore capability that creates an
// execution around a set of already-resolved units. It backs
// Engine.Rerun; a backend that does not implement it reports
// ErrRerunUnsupported.
//
// The contract, in one transition:
//  1. admit the execution against the workflow's concurrency policy (charge a
//     slot, park it in the queue, or refuse with nothing persisted)
//  2. create the execution, with the parent and rerun origin its snapshot
//     carries
//  3. seed the scheduling counters from the graph, then record every unit in
//     Units as resolved — status, port and output — and decrement the remaining
//     counter for each
//  4. apply every resolved unit's Downstream arrivals, writing the execute or
//     skip intent each threshold decision lands on
//  5. write the entries in Entries
//  6. hold the ready work when the admission parked the execution
//
// Units are node units. A group unit's boundary outputs come from a runtime
// activation rather than a node result, so a caller with one to resolve is
// expected to refuse before reaching the store; the capability does not carry
// the exits to record it.
type ExecutionSeeder interface {
	SeedExecution(ctx context.Context, req SeedExecutionRequest) (SeedExecutionResponse, error)
}
