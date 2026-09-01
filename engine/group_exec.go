package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// GroupExit is one boundary output produced by a member node during group
// execution. Milestone A supplies a fake in test; Milestone B supplies the real
// runner-embedded engine.
type GroupExit struct {
	NodeName string
	Port     string
	Data     map[string]any
}

// GroupExecutor runs one pinned group unit locally and returns its boundary
// outputs. Milestone A supplies a fake in test; Milestone B supplies the real
// runner-embedded engine.
type GroupExecutor interface {
	ExecuteGroup(ctx context.Context, task *Task, meta graph.GroupMeta) (exits []GroupExit, fatal bool, err error)
}

// WithGroupExecutor sets the optional group executor capability.
func WithGroupExecutor(ge GroupExecutor) Option {
	return func(e *Engine) { e.groupExecutor = ge }
}

// executeGroup handles a TaskTypeGroupExec task: acquires a group lease, calls
// the GroupExecutor, and commits the result. commitGroup (Task 12) will
// implement the actual downstream propagation; for now it is a stub.
func (e *Engine) executeGroup(ctx context.Context, task *Task, flush bool) error {
	if e.groupExecutor == nil {
		return fmt.Errorf("no group executor configured for group %q", task.NodeName)
	}
	g, active, err := e.loadActiveGraph(ctx, task.ExecutionID)
	if err != nil {
		return err
	}
	if !active {
		return nil
	}
	if task.UnitIdx < 0 || task.UnitIdx >= g.UnitCount() || g.UnitKindAt(task.UnitIdx) != graph.UnitGroup {
		return fmt.Errorf("group exec unit %d out of range or not a group", task.UnitIdx)
	}
	meta := g.GroupMetaAt(task.UnitIdx)

	// Build group lease. Same ID/TTL generation as BuildTaskLease (lease.go).
	// Attempt is seeded as 1; AcquireGroupLease writes the real attempt count
	// back into lease.Attempt (both local and Redis backends do this: they
	// track the persisted attempt and bump it on every expiry cycle). After
	// AcquireGroupLease returns, lease.Attempt is the authoritative value used
	// to fence CommitGroup. What belongs to a future milestone is enforcement
	// of GroupMeta.Retry.MaxAttempts — scheduling a retry vs. failing the
	// execution when the group exhausts its budget.
	leaseID, leaseToken := newLeaseCredentials()
	lease := &GroupLease{
		ExecutionID:  task.ExecutionID,
		GroupUnitIdx: task.UnitIdx,
		LeaseID:      leaseID,
		LeaseToken:   leaseToken,
		Attempt:      1, // seed; overwritten by AcquireGroupLease with the real attempt
		IssuedAt:     time.Now().UTC(),
		TTL:          e.defaultLeaseTTL,
	}

	gs, ok := e.state.(GroupStateStore)
	if !ok {
		return fmt.Errorf("state store does not support group leases")
	}
	acquired, err := gs.AcquireGroupLease(ctx, lease)
	if err != nil {
		return err
	}
	if !acquired {
		return nil // already owned by another executor; safe to discard
	}
	e.notifyGroupLeaseAcquired(ctx)

	exits, fatal, execErr := e.groupExecutor.ExecuteGroup(ctx, task, meta)
	return e.commitGroup(ctx, g, lease, meta, exits, fatal, execErr, flush)
}

// commitGroup commits one group unit's terminal result, propagates downstream
// unit arrivals, and finalizes the execution when all units are done.
func (e *Engine) commitGroup(ctx context.Context, g *graph.Graph, lease *GroupLease, meta graph.GroupMeta, exits []GroupExit, fatal bool, execErr error, flush bool) error {
	gs := e.state.(GroupStateStore) // executeGroup already asserted
	outcome := GroupOutcomeSuccess
	errMsg := ""
	if execErr != nil {
		outcome = GroupOutcomeFailed
		errMsg = execErr.Error()
		// group-level OnError: reuse node OnError semantics to decide whether
		// to fail the entire execution.
		fatal = groupOnErrorFatal(meta.OnError)
	}

	// Downstream unit arrival descriptions. Arrival counting (in-degree DECR /
	// active / wait_all|wait_any threshold) is handled by CommitGroup in the
	// SAME atomic transition — not by a subsequent AdvanceNode call — because a
	// group has no per-node state, and AdvanceNode's source guard would
	// fail-closed.
	var downstream []DownstreamArrival
	if !fatal {
		downstream = e.downstreamUnitArrivals(g, meta.UnitIdx, exits)
	}

	// GroupExit (executor report) → GroupExitResult (commit request, Task 8).
	reqExits := make([]GroupExitResult, 0, len(exits))
	for _, ex := range exits {
		reqExits = append(reqExits, GroupExitResult{
			NodeIdx: nodeIdxOf(g, ex.NodeName), NodeName: ex.NodeName, Port: ex.Port, Data: ex.Data,
		})
	}

	res, err := gs.CommitGroup(ctx, GroupCommitRequest{
		ExecutionID:  lease.ExecutionID,
		GroupUnitIdx: lease.GroupUnitIdx,
		LeaseToken:   lease.LeaseToken,
		Attempt:      lease.Attempt,
		Outcome:      outcome,
		Fatal:        fatal,
		Error:        errMsg,
		Exits:        reqExits,
		Downstream:   downstream,
	})
	if err != nil {
		return fmt.Errorf("commit group %q: %w", meta.Name, err)
	}

	// commitObserver's outcome label space is deliberately narrower than
	// CommitOutcome. CommitGroupResult short-circuits to
	// CommitOutcomeExecutionInactive — a stale/late commit discarded before
	// ever reaching this function — so that case never appears here. Folding
	// "the group's business result" and "this commit was discarded as stale"
	// into one outcome label would corrupt the failure-rate ratio this metric
	// exists to expose, the same reason the package-cache metric (the
	// previous wiring pass) excludes its error branches. Observing discarded
	// commits is a new series, not a fourth value here.
	commitOutcome := "success"
	if execErr != nil {
		if fatal {
			commitOutcome = "failed_fatal"
		} else {
			commitOutcome = "failed_tolerated"
		}
	}
	// lease.IssuedAt is zero only if some construction site built a GroupLease
	// literal without setting it. Both existing sites (executeGroup above and
	// CommitGroupResult) do set it; this guard is structural defense against a
	// future third site making the same mistake, not a known live path. A
	// silent zero-value would otherwise report a duration of decades
	// (time.Since of the zero time), quietly wrecking the histogram rather
	// than failing loudly.
	if !lease.IssuedAt.IsZero() {
		e.notifyGroupCommit(ctx, commitOutcome, time.Since(lease.IssuedAt))
	}

	if res.ExecutionDone {
		e.notifyExecutionComplete(ctx, lease.ExecutionID, res.ExecutionStatus)
		e.EvictExecution(lease.ExecutionID)
		return nil
	}
	if flush {
		return e.FlushOutbox(ctx, lease.ExecutionID)
	}
	return nil
}

// nodeIdxOf resolves a member name to a node index. The name is always from
// the current graph's members/exits so it is guaranteed to exist.
func nodeIdxOf(g *graph.Graph, name string) int {
	idx, _ := g.NodeIndex(name)
	return idx
}

// groupOnErrorFatal maps the group's OnError strategy to whether a group
// failure fails the whole execution. Uses the node OnError constants
// (types/node.go): OnErrorContinue => non-fatal (execution continues);
// OnErrorStop or empty/default => fatal.
//
// error_output and main_output never reach here: validateGroupOnError
// (engine/graph/group_compile.go) rejects them at compile time, because routing
// a group-level failure requires a group error output port that does not exist
// — GroupMeta.BoundaryOutputs is derived solely from real member edges crossing
// the boundary, compileOneGroup never reads OnError to synthesize one, and
// CommitGroupResult rejects any exit whose (nodeIdx, port) is absent from
// BoundaryOutputs. Building it is a new mechanism, scoped in
// NODE-GROUP-COLOCATION.md §12.2.
//
// The catch-all is deliberate rather than a switch: an unknown value cannot
// arrive (compile rejects it), and fatal is the safe reading if one somehow did
// — a graph snapshot decoded from an older writer that predates the validation
// would fail the execution rather than silently continue past a failed group.
func groupOnErrorFatal(onErr string) bool {
	return onErr != string(types.OnErrorContinue)
}

// downstreamUnitArrivals maps a source unit's fired boundary exits to per-
// downstream-unit arrivals — the unit-graph analogue of downstreamArrivals
// (atomic.go). The active boundary ports select which downstream edges carry
// an active arrival (execute) vs an inactive one (skip propagation). Consumed
// by CommitGroup, which does the atomic in-degree/active/threshold counting.
func (e *Engine) downstreamUnitArrivals(g *graph.Graph, srcUnit int, exits []GroupExit) []DownstreamArrival {
	active := make(map[string]bool, len(exits))
	for _, ex := range exits {
		active[boundaryKey(nodeIdxOf(g, ex.NodeName), ex.Port)] = true
	}
	byDst := make(map[int]DownstreamArrival)
	for _, ue := range g.UnitOutEdges(srcUnit) {
		a, ok := byDst[ue.DstUnit]
		if !ok {
			execType := TaskTypeNodeExec
			target := g.UnitNodeIndex(ue.DstUnit)
			name := g.NodeAt(target).Name
			if g.UnitKindAt(ue.DstUnit) == graph.UnitGroup {
				gm := g.GroupMetaAt(ue.DstUnit)
				execType, target, name = TaskTypeGroupExec, gm.EntryIdx, gm.Name
			}
			a = DownstreamArrival{
				NodeName:     name,
				NodeIdx:      target,
				UnitIdx:      ue.DstUnit,
				MergeMode:    g.UnitMergeMode(ue.DstUnit),
				ExecTaskType: execType,
			}
		}
		a.ArrivalCount++
		if active[boundaryKey(ue.Src.NodeIdx, ue.Src.Port)] {
			a.ActiveCount++
		}
		byDst[ue.DstUnit] = a
	}
	out := make([]DownstreamArrival, 0, len(byDst))
	for _, a := range byDst {
		out = append(out, a)
	}
	return out
}

// boundaryKey builds a lookup key from a node index and port name for matching
// active boundary exits against unit out-edges.
func boundaryKey(nodeIdx int, port string) string {
	return fmt.Sprintf("%d\x00%s", nodeIdx, port)
}
