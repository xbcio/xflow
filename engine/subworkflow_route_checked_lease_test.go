package engine

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// ---------------------------------------------------------------------------
// stores
// ---------------------------------------------------------------------------

// subWorkflowCheckedLeaseState is the sub-workflow link store plus the
// RouteCheckedLeaseAcquirer capability, with a counter for each acquire entry
// point. The counters are the point: both paths return the same lease shape, so
// only the calls tell them apart.
type subWorkflowCheckedLeaseState struct {
	*subWorkflowLinkState
	checkedAcquires int
	legacyAcquires  int
	lastOptions     LeaseAcquireOptions
	// acquiredVerdict, when set, overrides the acquire's answer for the refusal
	// case without writing a lease — the shape of a route the checked acquire
	// refuses.
	acquiredVerdict *bool
}

var _ RouteCheckedLeaseAcquirer = (*subWorkflowCheckedLeaseState)(nil)

func (s *subWorkflowCheckedLeaseState) AcquireTaskLease(ctx context.Context, lease *TaskLease) (*NodeSnapshot, bool, error) {
	s.legacyAcquires++
	return s.subWorkflowLinkState.AcquireTaskLease(ctx, lease)
}

// AcquireTaskLeaseChecked mirrors the first-party backends: an absent or
// terminal execution refuses without mutating, a refused route reports the
// snapshot it saw, and only a granted acquire writes the node.
func (s *subWorkflowCheckedLeaseState) AcquireTaskLeaseChecked(ctx context.Context, lease *TaskLease, opts LeaseAcquireOptions) (LeaseAcquireResult, error) {
	s.checkedAcquires++
	s.lastOptions = opts
	res := LeaseAcquireResult{}
	snap, err := s.subWorkflowLinkState.GetExecution(ctx, lease.Task.ExecutionID)
	if err != nil {
		return LeaseAcquireResult{}, err
	}
	if snap == nil {
		return res, nil
	}
	res.ExecutionFound = true
	res.ExecutionStatus = snap.Status
	if types.IsTerminalExecutionStatus(snap.Status) {
		return res, nil
	}
	if s.acquiredVerdict != nil && !*s.acquiredVerdict {
		prev, err := s.subWorkflowLinkState.GetNode(ctx, lease.Task.ExecutionID, lease.Task.NodeName)
		if err != nil {
			return LeaseAcquireResult{}, err
		}
		res.Previous = prev
		return res, nil
	}
	prev, acquired, err := s.subWorkflowLinkState.AcquireTaskLease(ctx, lease)
	if err != nil {
		return LeaseAcquireResult{}, err
	}
	res.Previous, res.Acquired = prev, acquired
	return res, nil
}

// subWorkflowLegacyLeaseState is the link store WITHOUT the capability, with
// the legacy acquire counted. It pins the fallback the capability must not
// remove.
type subWorkflowLegacyLeaseState struct {
	*subWorkflowLinkState
	acquires int
}

func (s *subWorkflowLegacyLeaseState) AcquireTaskLease(ctx context.Context, lease *TaskLease) (*NodeSnapshot, bool, error) {
	s.acquires++
	return s.subWorkflowLinkState.AcquireTaskLease(ctx, lease)
}

// ---------------------------------------------------------------------------
// rig
// ---------------------------------------------------------------------------

// newSubWorkflowLeaseRig seeds one running parent execution of g on store and
// returns the engine plus the task the scheduler would have enqueued for its
// single sub-workflow node.
func newSubWorkflowLeaseRig(t *testing.T, store StateStore, g *graph.Graph) (*Engine, *Task) {
	t.Helper()
	parentID := types.ExecutionID("exec-parent")
	if err := store.CreateExecution(context.Background(), &ExecutionSnapshot{
		ID:      parentID,
		Graph:   g,
		Status:  types.ExecutionStatusRunning,
		TraceID: "trace-parent",
	}); err != nil {
		t.Fatalf("seed parent execution: %v", err)
	}
	eng := New(store, &fakeQueue{}, WithSubWorkflowResolver(subWorkflowStubResolver(t)))
	idx, ok := g.NodeIndex("sub")
	if !ok {
		t.Fatal(`the test graph has no node named "sub"`)
	}
	return eng, &Task{
		ExecutionID:  parentID,
		NodeName:     "sub",
		NodeIdx:      idx,
		UnitIdx:      g.UnitIndexForNode(idx),
		Type:         TaskTypeSubWorkflow,
		ActivationID: 1,
	}
}

// subWorkflowParentGraph compiles the acquire tests' parent: one sub-workflow
// node, plus a self-cycle when cyclic is set. A cyclic graph must name its
// xflow.start entry, which the test never dispatches — it delivers the sub
// node's task directly, as the scheduler would.
func subWorkflowParentGraph(t *testing.T, cyclic bool) *graph.Graph {
	t.Helper()
	def := subWorkflowParentDef()
	if cyclic {
		def = &types.WorkflowDef{
			Name:    "parent-cyclic",
			Options: &types.WorkflowOptions{AllowCycles: true, MaxAutoDepth: 3},
			Nodes: []types.NodeDef{
				{Name: "start", Type: "xflow.start"},
				{Name: "sub", Type: types.SubWorkflowNodeType, Parameters: map[string]any{"workflow": "child"}},
				{Name: "next", Type: "xflow.noop"},
			},
			Connections: types.Connections{
				"start": {"main": {Targets: []types.Connection{{Node: "sub", Input: "main"}}}},
				"sub":   {"main": {Targets: []types.Connection{{Node: "next", Input: "main"}}}},
				"next":  {"main": {Targets: []types.Connection{{Node: "start", Input: "main"}}}},
			},
		}
	}
	g, err := graph.CompileWithResolver(context.Background(), def, subWorkflowStubResolver(t))
	if err != nil {
		t.Fatalf("CompileWithResolver: %v", err)
	}
	return g
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

// TestSubWorkflowDispatchAcquireUsesRouteCheckedCapability pins the point of
// the change: on a store that declares the capability the dispatch acquire is
// the checked one — called exactly once, with the compiled graph's AllowCycles
// — and the legacy acquire is never called. The lease it writes is the one the
// node parks under.
func TestSubWorkflowDispatchAcquireUsesRouteCheckedCapability(t *testing.T) {
	for _, cyclic := range []bool{false, true} {
		name := "Acyclic"
		if cyclic {
			name = "Cyclic"
		}
		t.Run(name, func(t *testing.T) {
			g := subWorkflowParentGraph(t, cyclic)
			state := &subWorkflowCheckedLeaseState{subWorkflowLinkState: newSubWorkflowLinkState()}
			eng, task := newSubWorkflowLeaseRig(t, state, g)
			ctx := context.Background()

			handled, err := eng.handleSystemTask(ctx, task, true)
			if err != nil {
				t.Fatalf("handleSystemTask: %v", err)
			}
			if !handled {
				t.Fatal("the engine did not claim its own sub-workflow task")
			}
			if state.checkedAcquires != 1 {
				t.Fatalf("AcquireTaskLeaseChecked calls = %d, want exactly 1", state.checkedAcquires)
			}
			if state.legacyAcquires != 0 {
				t.Fatalf("legacy AcquireTaskLease calls = %d, want 0: the capability was declared", state.legacyAcquires)
			}
			if state.lastOptions.AllowCycles != cyclic {
				t.Fatalf("AllowCycles = %v, want %v: the option must come from the compiled graph",
					state.lastOptions.AllowCycles, cyclic)
			}

			node, err := state.GetNode(ctx, task.ExecutionID, task.NodeName)
			if err != nil {
				t.Fatalf("GetNode: %v", err)
			}
			if node == nil || node.Status != types.NodeStatusWaiting {
				t.Fatalf("node = %+v, want the parked wait: the checked acquire must write the lease the park fences on", node)
			}
			if node.LeaseID == "" || node.LeaseToken == "" || node.Attempt != 1 {
				t.Fatalf("parked node lease = (%q, %q, attempt %d), want a first-attempt lease identity",
					node.LeaseID, node.LeaseToken, node.Attempt)
			}
			link, ok, err := state.GetSubWorkflowLink(ctx, task.ExecutionID, task.NodeName)
			if err != nil || !ok {
				t.Fatalf("GetSubWorkflowLink: ok=%v err=%v", ok, err)
			}
			if link.LeaseToken != node.LeaseToken {
				t.Fatalf("link lease token = %q, want the parked node's %q", link.LeaseToken, node.LeaseToken)
			}
		})
	}
}

// TestSubWorkflowAcquireRouteCheckedBenignRefusals pins the two verdicts the
// acquire can report without granting a lease, both of which stay benign here:
// an execution that is gone or finished (reported by ExecutionFound/Status
// rather than by an error) and a refused route (Acquired=false). Neither may
// surface an error the caller does not expect, and neither may write a lease.
func TestSubWorkflowAcquireRouteCheckedBenignRefusals(t *testing.T) {
	ctx := context.Background()

	t.Run("TerminalExecution", func(t *testing.T) {
		g := subWorkflowParentGraph(t, false)
		state := &subWorkflowCheckedLeaseState{subWorkflowLinkState: newSubWorkflowLinkState()}
		eng, task := newSubWorkflowLeaseRig(t, state, g)
		// Warm the engine's graph cache BEFORE the execution terminalizes: the
		// acquire is what must observe the terminal status, not the loader.
		if _, err := eng.loadGraph(ctx, task.ExecutionID); err != nil {
			t.Fatalf("loadGraph: %v", err)
		}
		if err := state.UpdateExecutionStatus(ctx, task.ExecutionID, types.ExecutionStatusCanceled, "canceled by test"); err != nil {
			t.Fatalf("UpdateExecutionStatus: %v", err)
		}

		lease, err := eng.acquireSubWorkflowLease(ctx, task, g.NodeAt(task.NodeIdx))
		if err != nil || lease != nil {
			t.Fatalf("acquireSubWorkflowLease() on a canceled execution = (%v, %v), want the benign (nil, nil)", lease, err)
		}
		if state.checkedAcquires != 1 {
			t.Fatalf("AcquireTaskLeaseChecked calls = %d, want 1: the status verdict must come from the acquire", state.checkedAcquires)
		}
		if state.legacyAcquires != 0 {
			t.Fatalf("legacy AcquireTaskLease calls = %d, want 0", state.legacyAcquires)
		}
		if node, err := state.GetNode(ctx, task.ExecutionID, task.NodeName); err != nil || node != nil {
			t.Fatalf("node = (%+v, %v), want no lease written on an inactive execution", node, err)
		}
		eng.mu.RLock()
		_, cached := eng.graphs[task.ExecutionID]
		eng.mu.RUnlock()
		if cached {
			t.Fatalf("graph for %q is still cached after the acquire reported a terminal execution", task.ExecutionID)
		}
	})

	t.Run("MissingExecution", func(t *testing.T) {
		g := subWorkflowParentGraph(t, false)
		state := &subWorkflowCheckedLeaseState{subWorkflowLinkState: newSubWorkflowLinkState()}
		eng, task := newSubWorkflowLeaseRig(t, state, g)
		if _, err := eng.loadGraph(ctx, task.ExecutionID); err != nil {
			t.Fatalf("loadGraph: %v", err)
		}
		// The graph stays cached; the execution record goes away underneath it,
		// which is exactly what ExecutionFound=false reports.
		state.mu.Lock()
		delete(state.executions, task.ExecutionID)
		state.mu.Unlock()

		lease, err := eng.acquireSubWorkflowLease(ctx, task, g.NodeAt(task.NodeIdx))
		if err != nil || lease != nil {
			t.Fatalf("acquireSubWorkflowLease() for a vanished execution = (%v, %v), want the benign (nil, nil)", lease, err)
		}
		if state.checkedAcquires != 1 {
			t.Fatalf("AcquireTaskLeaseChecked calls = %d, want 1", state.checkedAcquires)
		}
		if node, err := state.GetNode(ctx, task.ExecutionID, task.NodeName); err != nil || node != nil {
			t.Fatalf("node = (%+v, %v), want no lease written for a vanished execution", node, err)
		}
	})

	t.Run("StaleRoute", func(t *testing.T) {
		g := subWorkflowParentGraph(t, false)
		state := &subWorkflowCheckedLeaseState{subWorkflowLinkState: newSubWorkflowLinkState()}
		eng, task := newSubWorkflowLeaseRig(t, state, g)
		refused := false
		state.acquiredVerdict = &refused

		lease, err := eng.acquireSubWorkflowLease(ctx, task, g.NodeAt(task.NodeIdx))
		if err != nil || lease != nil {
			t.Fatalf("acquireSubWorkflowLease() with a refused route = (%v, %v), want the benign (nil, nil)", lease, err)
		}
		if state.checkedAcquires != 1 {
			t.Fatalf("AcquireTaskLeaseChecked calls = %d, want 1", state.checkedAcquires)
		}
		if node, err := state.GetNode(ctx, task.ExecutionID, task.NodeName); err != nil || node != nil {
			t.Fatalf("node = (%+v, %v), want no lease written for a refused route", node, err)
		}
	})
}

// TestSubWorkflowAcquireMissingGraphIsBenign pins the one case the checked path
// cannot answer: no cached and no persisted graph. The legacy call leaves that
// to the backend, whose acquire refuses an execution it does not hold, so this
// path must report the same benign refusal without calling the acquire at all.
func TestSubWorkflowAcquireMissingGraphIsBenign(t *testing.T) {
	ctx := context.Background()
	g := subWorkflowParentGraph(t, false)
	state := &subWorkflowCheckedLeaseState{subWorkflowLinkState: newSubWorkflowLinkState()}
	_, task := newSubWorkflowLeaseRig(t, state, g)
	// Drop the execution before the engine ever caches its graph.
	state.mu.Lock()
	delete(state.executions, task.ExecutionID)
	state.mu.Unlock()

	state.checkedAcquires, state.legacyAcquires = 0, 0
	eng := New(state, &fakeQueue{}, WithSubWorkflowResolver(subWorkflowStubResolver(t)))
	lease, err := eng.acquireSubWorkflowLease(ctx, task, g.NodeAt(task.NodeIdx))
	if err != nil || lease != nil {
		t.Fatalf("acquireSubWorkflowLease() without a graph = (%v, %v), want the benign (nil, nil)", lease, err)
	}
	if state.checkedAcquires != 0 || state.legacyAcquires != 0 {
		t.Fatalf("acquire calls = (checked %d, legacy %d), want none: there was no route to check",
			state.checkedAcquires, state.legacyAcquires)
	}
}

// TestSubWorkflowAcquireWithoutCapabilityKeepsLegacyPath pins the fallback: a
// store without RouteCheckedLeaseAcquirer still acquires through
// AcquireTaskLease, and the attempt accounting is the legacy one — a reclaim of
// the same activation is the next attempt.
func TestSubWorkflowAcquireWithoutCapabilityKeepsLegacyPath(t *testing.T) {
	if _, ok := any(newSubWorkflowLinkState()).(RouteCheckedLeaseAcquirer); ok {
		t.Fatal("subWorkflowLinkState now implements RouteCheckedLeaseAcquirer: the fallback arm would stop covering the legacy path")
	}
	ctx := context.Background()
	g := subWorkflowParentGraph(t, false)
	state := &subWorkflowLegacyLeaseState{subWorkflowLinkState: newSubWorkflowLinkState()}
	eng, task := newSubWorkflowLeaseRig(t, state, g)

	handled, err := eng.handleSystemTask(ctx, task, true)
	if err != nil || !handled {
		t.Fatalf("first handleSystemTask = (%v, %v), want the task claimed", handled, err)
	}
	if state.acquires != 1 {
		t.Fatalf("legacy AcquireTaskLease calls = %d, want 1", state.acquires)
	}
	node, err := state.GetNode(ctx, task.ExecutionID, task.NodeName)
	if err != nil || node == nil || node.Status != types.NodeStatusWaiting {
		t.Fatalf("first dispatch node = (%+v, %v), want the parked wait", node, err)
	}
	if node.Attempt != 1 {
		t.Fatalf("first claim attempt = %d, want 1", node.Attempt)
	}

	// The sweeper's reclaim: revoke the wait and replay the same intent.
	revoked, err := state.RevokeLease(ctx, task.ExecutionID, task.NodeName, node.LeaseToken)
	if err != nil || !revoked {
		t.Fatalf("RevokeLease: revoked=%v err=%v", revoked, err)
	}
	handled, err = eng.handleSystemTask(ctx, task, true)
	if err != nil || !handled {
		t.Fatalf("second handleSystemTask = (%v, %v), want the task claimed", handled, err)
	}
	if state.acquires != 2 {
		t.Fatalf("legacy AcquireTaskLease calls = %d, want 2", state.acquires)
	}
	node, err = state.GetNode(ctx, task.ExecutionID, task.NodeName)
	if err != nil || node == nil || node.Status != types.NodeStatusWaiting {
		t.Fatalf("second dispatch node = (%+v, %v), want the re-parked wait", node, err)
	}
	if node.Attempt != 2 {
		t.Fatalf("reclaim attempt = %d, want 2: the legacy accounting counts retries within one activation", node.Attempt)
	}
}
