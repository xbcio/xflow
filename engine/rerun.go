package engine

import (
	"context"
	"errors"
	"fmt"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// ErrExecutionNotRerunnable reports an execution whose status this operation
// does not rerun from. Only the terminal statuses a caller can act on are
// rerunnable — failed, canceled and timeout — plus success when the caller asks
// for it explicitly; a running or queued execution is not rerun, it is allowed
// to finish (or paused and canceled).
var ErrExecutionNotRerunnable = errors.New("execution is not in a rerunnable status")

// ErrRerunNodeNotFound reports a from-node name that the execution's graph does
// not define. The graph is the one the execution ran, so a name that is not in
// it names a node that execution never had.
var ErrRerunNodeNotFound = errors.New("rerun node is not in the execution's graph")

// ErrRerunGraphUnsupported reports a graph shape rerun-from-node cannot seed.
//
// Two shapes are refused rather than approximated. A cyclic graph keeps its
// scheduling state in activation counters rather than in the per-unit in-degree
// the seed writes, so a seeded prefix would not mean what it means on an
// acyclic one. A GROUP unit's result is a runtime activation with boundary
// exits rather than a node result, and the seed records node units only — so a
// graph that has one outside the rerun subtree cannot be copied as resolved.
var ErrRerunGraphUnsupported = errors.New("graph shape does not support rerun from a node")

// ErrRerunUpstreamIncomplete reports a node upstream of the rerun point whose
// result cannot be copied: it never reached a terminal outcome in the execution
// being rerun (it was still running, pending, or failed), so the new execution
// has no input for it. The caller can rerun from that node instead, which is
// the point at which the missing result stops being an input.
var ErrRerunUpstreamIncomplete = errors.New("a node upstream of the rerun point has no result to copy")

// RerunRequest describes one rerun-from-node: which terminal execution to
// derive from, where its re-execution starts, and whether a successful parent
// is an acceptable source.
type RerunRequest struct {
	// ExecutionID is the execution to rerun. It is never modified.
	ExecutionID types.ExecutionID
	// FromNode is the node the new execution starts at. Everything upstream of
	// it is copied from the parent's result; it and everything downstream of it
	// run again.
	FromNode string
	// IncludeSuccess accepts a successful parent. Failed, canceled and timeout
	// parents are always accepted.
	IncludeSuccess bool
	// IdempotencyKey, when set, makes the rerun safe to retry: the new
	// execution's ID is derived from the parent and the key, and a retry with
	// the same key and the same request returns the execution the first attempt
	// created instead of creating another. The same key with a different
	// from-node or IncludeSuccess is refused with ErrIdempotencyConflict.
	IdempotencyKey string
}

// RerunResult reports the execution the rerun created.
type RerunResult struct {
	// ExecutionID is the NEW execution. The parent is untouched: its node
	// results, leases and audit history stay exactly as they were.
	ExecutionID types.ExecutionID
	// Queued is true when the workflow's concurrency policy parked the new
	// execution in its QUEUE-mode line instead of starting it. The copied
	// upstream results exist either way; only the delivery of the work waits.
	Queued bool
	// QueuePosition is the parked execution's 1-based place in line, 0
	// otherwise.
	QueuePosition int
	// Duplicate is true when the request's IdempotencyKey had already created
	// this execution: nothing was created by this call, and Queued and
	// QueuePosition are not reported.
	Duplicate bool
}

// Rerun derives a new execution from a terminal one, starting at FromNode.
//
// The new execution is a submission whose prefix is already decided: every unit
// that is not FromNode or downstream of it is recorded as resolved from the
// parent's own result — its stored output and port for a unit that ran, its
// skip for a unit the parent resolved without running — and FromNode and its
// downstream run again. The parent is never written to, which is the whole
// reason a rerun creates an execution instead of resetting nodes: a lease or a
// fence issued to the old execution cannot be confused with the new one, and the
// audit trail keeps both.
//
// The work is one store transition (ExecutionSeeder). What this method decides
// is the partition — which units are copied, which are re-run — and the
// admission the new execution contends for, which is its own graph's: a rerun
// is judged by the definition that ran, so a limit lowered since then does not
// retroactively refuse it and a limit raised does not retroactively admit it.
//
// A store without the capability reports ErrRerunUnsupported, and a graph that
// declares a concurrency limit on a store that cannot enforce one fails closed
// with ErrConcurrencyUnsupported, exactly as every other creation path does.
func (e *Engine) Rerun(ctx context.Context, req RerunRequest) (RerunResult, error) {
	seeder, ok := e.state.(ExecutionSeeder)
	if !ok {
		return RerunResult{}, fmt.Errorf("rerun %q: %w", req.ExecutionID, ErrRerunUnsupported)
	}

	parent, err := e.state.GetExecution(ctx, req.ExecutionID)
	if err != nil {
		return RerunResult{}, fmt.Errorf("rerun %q: %w", req.ExecutionID, err)
	}
	if parent == nil {
		return RerunResult{}, fmt.Errorf("rerun %q: %w", req.ExecutionID, ErrExecutionNotFound)
	}
	if !rerunnableStatus(parent.Status, req.IncludeSuccess) {
		return RerunResult{}, fmt.Errorf("rerun %q from %q: status %q: %w",
			req.ExecutionID, req.FromNode, parent.Status, ErrExecutionNotRerunnable)
	}

	// The parent's own graph, loaded from the store rather than taken from the
	// snapshot: GetExecution's graph is a process-local cache on the key-value
	// backend, and a rerun is typically issued on a replica that never ran the
	// original.
	g, err := e.loadGraph(ctx, req.ExecutionID)
	if err != nil {
		return RerunResult{}, fmt.Errorf("rerun %q: %w", req.ExecutionID, err)
	}
	if g == nil {
		// The execution's definition is gone with its keys, which is what an
		// expired retention looks like: whatever is left of the execution cannot
		// be rerun, and "not found" is the honest answer.
		return RerunResult{}, fmt.Errorf("rerun %q: %w", req.ExecutionID, ErrExecutionNotFound)
	}

	plan, err := e.rerunPlan(ctx, g, req)
	if err != nil {
		return RerunResult{}, err
	}

	// Fail closed, like every other creation path. A rerun copies a prefix, and
	// the copied nodes' results are all it reuses — but the plan's to-run set is
	// not inspected here for the same reason the concurrency gate below does not
	// inspect it: the graph declares what the deployment must be able to run,
	// and an engine that cannot run it at all should refuse before a claim is
	// taken rather than discover it one dispatch later.
	if err := e.admitSubWorkflows(g); err != nil {
		return RerunResult{}, fmt.Errorf("rerun %q: %w", req.ExecutionID, err)
	}

	admission := concurrencyAdmissionFor(ctx, g)
	if admission != nil {
		// Fail closed, like every other creation path: a store that cannot
		// enforce the limit would run the rerun unlimited, which is the one
		// outcome the definition declared the limit to prevent.
		if _, ok := e.state.(ConcurrencyLimitedStateStore); !ok {
			return RerunResult{}, fmt.Errorf("workflow %q: %w", admission.Workflow, ErrConcurrencyUnsupported)
		}
	}

	// The id is minted here rather than taken from the submission context's
	// pre-allocation slot, and the difference is load-bearing: that slot carries
	// the id the caller ALREADY addressed — for an execution-scoped route it is
	// the path's execution id, injected so Submit/Invoke can persist the id the
	// admission audit row recorded. A rerun's audit row names the execution
	// being rerun, so adopting that id would make this method write over the
	// very execution it exists to leave untouched.
	newID := NewExecutionID()
	var idem idempotentStart
	if req.IdempotencyKey != "" {
		fp, err := rerunFingerprint(req)
		if err != nil {
			return RerunResult{}, err
		}
		// The claim is taken once every refusal that depends only on the
		// request has been made, so a rerun the engine would refuse never holds
		// a key; and before anything is written, so a retry finds either the
		// execution or a claim it can take over.
		idem, err = e.claimIdempotency(ctx, g, rerunIdempotencyScope(req.ExecutionID), req.IdempotencyKey, fp)
		if err != nil {
			return RerunResult{}, fmt.Errorf("rerun %q: %w", req.ExecutionID, err)
		}
		if idem.duplicate {
			return RerunResult{ExecutionID: idem.id, Duplicate: true}, nil
		}
		newID = idem.id
	}
	res, err := e.seedRerun(ctx, seeder, req, parent, g, plan, admission, newID)
	e.settleIdempotency(ctx, idem, err)
	return res, err
}

// seedRerun creates the rerun's execution under newID: the snapshot, the
// seeded prefix and the start intent, in one store transition.
func (e *Engine) seedRerun(ctx context.Context, seeder ExecutionSeeder, req RerunRequest, parent *ExecutionSnapshot, g *graph.Graph, plan rerunPlan, admission *ConcurrencyAdmission, newID types.ExecutionID) (RerunResult, error) {
	snap := &ExecutionSnapshot{
		ID:      newID,
		Graph:   g,
		Status:  types.ExecutionStatusRunning,
		Params:  cloneMap(parent.Params),
		Runtime: cloneRuntime(parent.Runtime),
		// The link to what this execution derives from. It is what makes the
		// rerun traceable after the parent's keys expire.
		ParentID:  req.ExecutionID,
		RerunFrom: req.FromNode,
	}
	attachSubmissionMetadata(ctx, snap)
	// A rerun of a test run stays a test run, and a rerun issued with no scope
	// of its own keeps the parent's: the context is the newer authority (see
	// attachSubmissionMetadata), and the parent is the only other source. Losing
	// TestRun would serve a debugging rerun's pinned nodes for real.
	if !TestRunFromContext(ctx) {
		snap.TestRun = parent.TestRun
	}
	if len(snap.Scope) == 0 {
		snap.Scope = cloneMap(parent.Scope)
	}
	ctx = attachTransientHint(ctx, g)

	entries := []OutboxEntry(nil)
	if plan.startTask != nil {
		task := *plan.startTask
		task.ExecutionID = newID
		// The id is the submission path's, so a rerun's start intent is
		// addressable by the same rules as any other root intent.
		entries = append(entries, OutboxEntry{ID: initialOutboxID(newID, task.NodeName, 0), Task: task})
	}

	resp, err := seeder.SeedExecution(ctx, SeedExecutionRequest{
		Execution: snap,
		Units:     plan.units,
		Entries:   entries,
		Admission: admission,
	})
	if err != nil {
		// The refusals a caller acts on keep their sentinel and gain the
		// workflow they belong to, exactly as the submission path reports them:
		// a reached limit is a 429, not a failed rerun.
		switch {
		case errors.Is(err, ErrConcurrencyLimit):
			return RerunResult{}, fmt.Errorf("workflow %q: %w", admission.Workflow, ErrConcurrencyLimit)
		case errors.Is(err, ErrConcurrencyQueueUnsupported):
			return RerunResult{}, fmt.Errorf("workflow %q: %w", admission.Workflow, ErrConcurrencyQueueUnsupported)
		case errors.Is(err, ErrConcurrencyQueueFull):
			return RerunResult{}, fmt.Errorf("workflow %q: %w", admission.Workflow, ErrConcurrencyQueueFull)
		}
		return RerunResult{}, fmt.Errorf("rerun %q from %q: %w", req.ExecutionID, req.FromNode, err)
	}

	e.cacheExecutionGraph(newID, g)
	if !resp.Queued {
		// Best effort, like the submission path's: the state and the intents
		// were committed together, so a temporary queue outage is a deferred
		// delivery, not a failed rerun.
		e.flushInitialOutbox(ctx, newID)
	}
	e.notifySkip(ctx, flowRerun, resp.Skipped)

	return RerunResult{
		ExecutionID:   newID,
		Queued:        resp.Queued,
		QueuePosition: resp.QueuePosition,
	}, nil
}

// rerunPlan is the resolution of one rerun: the units to record as already
// decided, and — when no arrival can reach the rerun point — the task that has
// to be started outright.
//
// The task is carried without an execution ID because the plan is computed
// before the new execution exists: a refused plan must not have minted one, and
// an ID is only meaningful once the plan it belongs to is accepted.
type rerunPlan struct {
	units     []SeedUnit
	startTask *Task
}

// rerunPlan partitions the graph around the rerun point and reads the parent's
// results for the part that is copied.
//
// Three sets, and every unit is in exactly one of them:
//
//   - the RERUN subtree: the from-unit and everything reachable from it. These
//     run again, so nothing about them is seeded.
//   - the CLOSURE: the strict ancestors of the from-unit. Every one of them is
//     an input to the rerun, so each must have a result to copy; one that does
//     not is refused rather than approximated.
//   - the REST: units that are neither. They are copied too — that is what
//     makes the new execution a complete execution rather than a fragment —
//     but a unit the parent left unresolved is recorded as skipped rather than
//     refused: it is not an input to the rerun, and the branch it belongs to is
//     not part of it.
//
// A resolved unit's arrivals are computed exactly as its own advance would have
// computed them, so the seeded execution schedules what the parent's own
// scheduling would have: the units whose in-degree the copied prefix satisfies
// are queued, and the branches the prefix did not take are skipped.
func (e *Engine) rerunPlan(ctx context.Context, g *graph.Graph, req RerunRequest) (rerunPlan, error) {
	if g.AllowCycles() {
		return rerunPlan{}, fmt.Errorf("rerun %q: %w", req.ExecutionID, ErrRerunGraphUnsupported)
	}
	nodeIdx, ok := g.NodeIndex(req.FromNode)
	if !ok {
		return rerunPlan{}, fmt.Errorf("rerun %q: node %q: %w", req.ExecutionID, req.FromNode, ErrRerunNodeNotFound)
	}
	fromUnit := g.UnitIndexForNode(nodeIdx)
	if g.UnitKindAt(fromUnit) == graph.UnitGroup {
		return rerunPlan{}, fmt.Errorf("rerun %q: node %q is inside a group unit: %w",
			req.ExecutionID, req.FromNode, ErrRerunGraphUnsupported)
	}

	unitCount := g.UnitCount()
	predecessors := make([][]int, unitCount)
	for u := 0; u < unitCount; u++ {
		for _, edge := range g.UnitOutEdges(u) {
			predecessors[edge.DstUnit] = append(predecessors[edge.DstUnit], u)
		}
	}
	// The rerun subtree, by breadth-first walk over the from-unit's successors.
	rerunning := make([]bool, unitCount)
	rerunning[fromUnit] = true
	for queue := []int{fromUnit}; len(queue) > 0; {
		u := queue[0]
		queue = queue[1:]
		for _, edge := range g.UnitOutEdges(u) {
			if !rerunning[edge.DstUnit] {
				rerunning[edge.DstUnit] = true
				queue = append(queue, edge.DstUnit)
			}
		}
	}
	// The closure, walked the same way over the reversed edges. A unit is a
	// strict ancestor: the from-unit itself is never in it, which is why the
	// walk starts from the from-unit's predecessors rather than from it.
	closure := make([]bool, unitCount)
	for queue := append([]int(nil), predecessors[fromUnit]...); len(queue) > 0; {
		u := queue[0]
		queue = queue[1:]
		if closure[u] {
			continue
		}
		closure[u] = true
		queue = append(queue, predecessors[u]...)
	}

	plan := rerunPlan{}
	for u := 0; u < unitCount; u++ {
		if rerunning[u] {
			continue
		}
		if g.UnitKindAt(u) == graph.UnitGroup {
			return rerunPlan{}, fmt.Errorf("rerun %q from %q: group unit %q is outside the rerun subtree: %w",
				req.ExecutionID, req.FromNode, g.UnitDisplayName(u), ErrRerunGraphUnsupported)
		}
		unit, err := e.rerunResolution(ctx, req.ExecutionID, g, u, closure[u])
		if err != nil {
			return rerunPlan{}, err
		}
		plan.units = append(plan.units, unit)
	}

	// The from-unit is scheduled by an arrival from its predecessors — unless it
	// has none, in which case no arrival can reach it and its intent has to be
	// written outright.
	if g.UnitInDegreeAt(fromUnit) == 0 {
		plan.startTask = &Task{
			NodeName: req.FromNode,
			NodeIdx:  nodeIdx,
			UnitIdx:  fromUnit,
			Type:     execTaskTypeForNode(g.NodeAt(nodeIdx)),
		}
	}
	return plan, nil
}

// rerunResolution reads the parent's result for one unit and turns it into the
// resolution the new execution records.
//
// required marks a unit in the closure: it is an input to the rerun, so a
// status that carries no result is an error. For a unit outside the rerun's
// path the same status resolves as skipped instead — the branch it belongs to
// is not part of this rerun, and the new execution must not wait for it.
func (e *Engine) rerunResolution(ctx context.Context, parentID types.ExecutionID, g *graph.Graph, unitIdx int, required bool) (SeedUnit, error) {
	nodeIdx := g.UnitNodeIndex(unitIdx)
	node := g.NodeAt(nodeIdx)
	unit := SeedUnit{
		UnitIdx:  unitIdx,
		NodeName: node.Name,
		NodeIdx:  nodeIdx,
		// Derived from the compiled graph, never from stored data: privacy is
		// graph policy, and a seeded unit must be as private as the node it
		// copies (entryAdmissionExitPrivateOutput fails closed for a name the
		// graph cannot resolve).
		PrivateOutput: entryAdmissionExitPrivateOutput(g, node.Name),
	}

	current, err := e.state.GetNode(ctx, parentID, node.Name)
	if err != nil {
		return SeedUnit{}, fmt.Errorf("rerun %q: read node %q: %w", parentID, node.Name, err)
	}
	var status types.NodeStatus
	if current != nil {
		status = current.Status
	}
	switch status {
	case types.NodeStatusSuccess, types.NodeStatusPinned:
		output, err := e.state.GetOutput(ctx, parentID, node.Name)
		if err != nil {
			return SeedUnit{}, fmt.Errorf("rerun %q: read output %q: %w", parentID, node.Name, err)
		}
		unit.Status = types.NodeStatusSuccess
		unit.Port = current.Port
		unit.Output = cloneMap(output)
		unit.Downstream = downstreamArrivals(g, nodeIdx, current.Port)
		return unit, nil
	case types.NodeStatusSkipped:
		unit.Status = types.NodeStatusSkipped
		unit.Downstream = downstreamArrivals(g, nodeIdx, "")
		return unit, nil
	}
	if required {
		return SeedUnit{}, fmt.Errorf("rerun %q: node %q upstream of the rerun point is %q: %w",
			parentID, node.Name, status, ErrRerunUpstreamIncomplete)
	}
	unit.Status = types.NodeStatusSkipped
	unit.Downstream = downstreamArrivals(g, nodeIdx, "")
	return unit, nil
}

// rerunnableStatus is the status gate Rerun applies to its source.
func rerunnableStatus(status types.ExecutionStatus, includeSuccess bool) bool {
	switch status {
	case types.ExecutionStatusFailed, types.ExecutionStatusCanceled, types.ExecutionStatusTimeout:
		return true
	case types.ExecutionStatusSuccess:
		return includeSuccess
	}
	return false
}
