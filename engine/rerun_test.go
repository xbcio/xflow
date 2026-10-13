package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// seedRecorder is the ExecutionSeeder test double. It captures the transition
// the engine asked for instead of applying it: the engine's half of a rerun is
// the plan — which units are copied, which are re-run, what the new execution
// derives from — and the store's half is covered by the statestore contracts.
type seedRecorder struct {
	*fakeState
	requests []SeedExecutionRequest
	result   SeedExecutionResponse
	err      error
}

func (s *seedRecorder) SeedExecution(_ context.Context, req SeedExecutionRequest) (SeedExecutionResponse, error) {
	s.requests = append(s.requests, req)
	if s.err != nil {
		return SeedExecutionResponse{}, s.err
	}
	return s.result, nil
}

// lastRequest is the only request a well-behaved test drives, and failing here
// is clearer than an index panic three assertions into a case.
func (s *seedRecorder) lastRequest(t *testing.T) SeedExecutionRequest {
	t.Helper()
	if len(s.requests) != 1 {
		t.Fatalf("SeedExecution() called %d times, want 1", len(s.requests))
	}
	return s.requests[0]
}

// rerunState is the parent execution one case is derived from: a graph, the
// status the execution ended at, and the per-node result the parent recorded.
type rerunNode struct {
	status types.NodeStatus
	port   string
	output map[string]any
}

// rerunParent creates the parent execution a case reruns, with node snapshots
// and outputs exactly as the given results describe. A node the map does not
// name was never resolved by the parent (it has no snapshot at all), which is
// the shape of a node the execution never reached.
func rerunParent(t *testing.T, state *fakeState, g *graph.Graph, status types.ExecutionStatus, nodes map[string]rerunNode) types.ExecutionID {
	t.Helper()
	ctx := context.Background()
	id := NewExecutionID()
	if err := state.CreateExecution(ctx, &ExecutionSnapshot{
		ID:     id,
		Graph:  g,
		Status: types.ExecutionStatusRunning,
		Params: map[string]any{"input": "parent"},
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	for name, node := range nodes {
		if err := state.UpsertNode(ctx, &NodeSnapshot{
			ExecutionID: id,
			Name:        name,
			Status:      node.status,
			Port:        node.port,
		}); err != nil {
			t.Fatalf("UpsertNode(%q) error = %v", name, err)
		}
		if node.output != nil {
			if err := state.PutOutput(ctx, id, name, node.output); err != nil {
				t.Fatalf("PutOutput(%q) error = %v", name, err)
			}
		}
	}
	if err := state.UpdateExecutionStatus(ctx, id, status, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus(%q) error = %v", status, err)
	}
	return id
}

// rerunGraph compiles a workflow from a name and a connection list, with the
// node set derived from the connections so a case cannot declare a node it
// never wires.
func rerunGraph(t *testing.T, def *types.WorkflowDef) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("Compile(%s) error = %v", def.Name, err)
	}
	return g
}

// rerunDiamond is start -> {a, b} -> mid -> tail: the shape where the rerun
// point has a two-node closure and no branch outside it.
func rerunDiamond(t *testing.T) *graph.Graph {
	t.Helper()
	return rerunGraph(t, &types.WorkflowDef{
		Name: "rerun-diamond",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.echo"},
			{Name: "a", Type: "test.echo"},
			{Name: "b", Type: "test.echo"},
			{Name: "mid", Type: "test.echo"},
			{Name: "tail", Type: "test.echo"},
		},
		Connections: types.Connections{
			"start": {"main": {Targets: []types.Connection{
				{Node: "a", Input: "main"}, {Node: "b", Input: "main"},
			}}},
			"a":   {"main": {Targets: []types.Connection{{Node: "mid", Input: "main"}}}},
			"b":   {"main": {Targets: []types.Connection{{Node: "mid", Input: "main"}}}},
			"mid": {"main": {Targets: []types.Connection{{Node: "tail", Input: "main"}}}},
		},
	})
}

// diamondResolved is the parent result the diamond-shaped cases share: the
// three units upstream of mid ran, so a rerun from mid has a full closure to
// copy.
func diamondResolved() map[string]rerunNode {
	return map[string]rerunNode{
		"start": {status: types.NodeStatusSuccess, port: "main", output: map[string]any{"n": 1}},
		"a":     {status: types.NodeStatusSuccess, port: "main", output: map[string]any{"n": 2}},
		"b":     {status: types.NodeStatusSuccess, port: "main", output: map[string]any{"n": 3}},
		"mid":   {status: types.NodeStatusFailed, port: "error"},
	}
}

// resolvedNames maps the seeded resolutions by node name, so a case can assert
// on a set rather than on an order the plans are not obliged to promise.
func resolvedNames(units []SeedUnit) map[string]SeedUnit {
	out := make(map[string]SeedUnit, len(units))
	for _, unit := range units {
		out[unit.NodeName] = unit
	}
	return out
}

// TestRerunPlansTheAncestorClosure is the partition half of a rerun: everything
// upstream of the rerun point is copied from the parent's own result — status,
// port and output — and the rerun point itself is not seeded at all, because it
// is the node the new execution exists to run again.
func TestRerunPlansTheAncestorClosure(t *testing.T) {
	ctx := context.Background()
	g := rerunDiamond(t)
	state := newFakeState()
	parent := rerunParent(t, state, g, types.ExecutionStatusFailed, diamondResolved())
	seeder := &seedRecorder{fakeState: state}
	eng := New(seeder, &fakeQueue{})

	res, err := eng.Rerun(ctx, RerunRequest{ExecutionID: parent, FromNode: "mid"})
	if err != nil {
		t.Fatalf("Rerun() error = %v", err)
	}
	req := seeder.lastRequest(t)

	if res.ExecutionID == "" || res.ExecutionID == parent {
		t.Fatalf("Rerun() ExecutionID = %q, want a new execution derived from %q", res.ExecutionID, parent)
	}
	if req.Execution.ID != res.ExecutionID {
		t.Fatalf("seeded execution ID = %q, want the reported %q", req.Execution.ID, res.ExecutionID)
	}
	if req.Execution.ParentID != parent {
		t.Fatalf("seeded ParentID = %q, want %q", req.Execution.ParentID, parent)
	}
	if req.Execution.RerunFrom != "mid" {
		t.Fatalf("seeded RerunFrom = %q, want %q", req.Execution.RerunFrom, "mid")
	}
	if req.Execution.Graph != g {
		t.Fatal("the seeded execution does not carry the parent's graph: a rerun runs the definition the parent ran")
	}

	resolved := resolvedNames(req.Units)
	for _, name := range []string{"start", "a", "b"} {
		unit, ok := resolved[name]
		if !ok {
			t.Fatalf("resolved units = %v, want %q copied from the parent", namesOf(resolved), name)
		}
		if unit.Status != types.NodeStatusSuccess {
			t.Fatalf("resolved %q status = %q, want %q", name, unit.Status, types.NodeStatusSuccess)
		}
		if unit.Port != "main" {
			t.Fatalf("resolved %q port = %q, want %q", name, unit.Port, "main")
		}
		if unit.Output == nil {
			t.Fatalf("resolved %q has no output: a success resolution copies the parent's data", name)
		}
	}
	if unit, ok := resolved["mid"]; ok {
		t.Fatalf("the rerun point was seeded as %+v: it is the unit the new execution runs", unit)
	}
	if unit, ok := resolved["tail"]; ok {
		t.Fatalf("a downstream unit was seeded as %+v: everything from the rerun point on re-runs", unit)
	}
	// mid has two predecessors, so the copied arrivals satisfy its in-degree and
	// no intent has to be written for it.
	if len(req.Entries) != 0 {
		t.Fatalf("entries = %+v, want none: an arrival reaches the rerun point", req.Entries)
	}
}

// TestRerunStartsARootFromAnEntry pins the other way the rerun point reaches the
// new execution: a from-node with no predecessors is reached by no arrival, so
// its intent has to be carried by the seed itself.
func TestRerunStartsARootFromAnEntry(t *testing.T) {
	ctx := context.Background()
	g := rerunGraph(t, &types.WorkflowDef{
		Name: "rerun-root",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.echo"},
			{Name: "tail", Type: "test.echo"},
		},
		Connections: types.Connections{
			"start": {"main": {Targets: []types.Connection{{Node: "tail", Input: "main"}}}},
		},
	})
	state := newFakeState()
	parent := rerunParent(t, state, g, types.ExecutionStatusCanceled, map[string]rerunNode{
		"start": {status: types.NodeStatusSuccess, port: "main", output: map[string]any{"n": 1}},
	})
	seeder := &seedRecorder{fakeState: state}
	eng := New(seeder, &fakeQueue{})

	if _, err := eng.Rerun(ctx, RerunRequest{ExecutionID: parent, FromNode: "start"}); err != nil {
		t.Fatalf("Rerun() error = %v", err)
	}
	req := seeder.lastRequest(t)

	if len(req.Units) != 0 {
		t.Fatalf("resolved units = %v, want none: the rerun point is a root, so nothing precedes it",
			namesOf(resolvedNames(req.Units)))
	}
	if len(req.Entries) != 1 {
		t.Fatalf("entries = %+v, want the rerun point's own intent", req.Entries)
	}
	entry := req.Entries[0]
	if entry.Task.NodeName != "start" || entry.Task.ExecutionID != req.Execution.ID {
		t.Fatalf("entry = %+v, want an execute intent for start of the new execution", entry)
	}
	if entry.Task.Type != TaskTypeNodeExec || entry.Task.UnitIdx != g.UnitIndexForNode(0) {
		t.Fatalf("entry task = %+v, want an execute intent for the from-unit", entry.Task)
	}
	if entry.ID == "" {
		t.Fatal("entry has no id: an outbox intent is addressed by it")
	}
}

// TestRerunCopiesBranchesOutsideItsPath is the case the ancestor-only reading
// gets wrong: a unit that is neither upstream of the rerun point nor downstream
// of it is still a unit of the graph, and the new execution has to hold a
// decision about it. One that the parent resolved is copied; one the parent
// never reached is recorded as skipped rather than left to hold the execution
// open forever.
func TestRerunCopiesBranchesOutsideItsPath(t *testing.T) {
	ctx := context.Background()
	g := rerunGraph(t, &types.WorkflowDef{
		Name: "rerun-sibling",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.echo"},
			{Name: "a", Type: "test.echo"},
			{Name: "mid", Type: "test.echo"},
			{Name: "sibling", Type: "test.echo"},
			{Name: "orphan", Type: "test.echo"},
		},
		Connections: types.Connections{
			"start":   {"main": {Targets: []types.Connection{{Node: "a", Input: "main"}, {Node: "sibling", Input: "main"}}}},
			"a":       {"main": {Targets: []types.Connection{{Node: "mid", Input: "main"}}}},
			"sibling": {"main": {Targets: []types.Connection{{Node: "orphan", Input: "main"}}}},
		},
	})
	state := newFakeState()
	parent := rerunParent(t, state, g, types.ExecutionStatusFailed, map[string]rerunNode{
		"start":   {status: types.NodeStatusSuccess, port: "main", output: map[string]any{"n": 1}},
		"a":       {status: types.NodeStatusSuccess, port: "main", output: map[string]any{"n": 2}},
		"mid":     {status: types.NodeStatusFailed, port: "error"},
		"sibling": {status: types.NodeStatusSuccess, port: "main", output: map[string]any{"n": 3}},
		// "orphan" was never reached: the parent failed before its branch got there.
	})
	seeder := &seedRecorder{fakeState: state}
	eng := New(seeder, &fakeQueue{})

	if _, err := eng.Rerun(ctx, RerunRequest{ExecutionID: parent, FromNode: "mid"}); err != nil {
		t.Fatalf("Rerun() error = %v", err)
	}
	resolved := resolvedNames(seeder.lastRequest(t).Units)

	sibling, ok := resolved["sibling"]
	if !ok || sibling.Status != types.NodeStatusSuccess {
		t.Fatalf("sibling resolution = %+v, present=%v, want the parent's own success copied", sibling, ok)
	}
	if sibling.Output == nil {
		t.Fatal("sibling was copied without its output: a copied unit keeps what the parent produced")
	}
	orphan, ok := resolved["orphan"]
	if !ok {
		t.Fatalf("resolved units = %v, want orphan too: a unit outside the rerun path still needs a decision",
			namesOf(resolved))
	}
	if orphan.Status != types.NodeStatusSkipped {
		t.Fatalf("orphan resolution = %q, want %q: the parent never reached it and this rerun does not run it",
			orphan.Status, types.NodeStatusSkipped)
	}
	if orphan.Output != nil {
		t.Fatalf("orphan resolution carries output %#v, want none for a skipped unit", orphan.Output)
	}
}

// TestRerunRefusesWhatItCannotHonour is the refusal table. Every case here
// leaves the store untouched: a rerun either happens in full or writes nothing
// at all, so the assertion is both the sentinel and the absence of a call.
func TestRerunRefusesWhatItCannotHonour(t *testing.T) {
	ctx := context.Background()

	// A graph whose only unit upstream of the rerun point failed: its result
	// cannot be copied, and the caller has to rerun from that node instead.
	brokenUpstream := func(t *testing.T) (*fakeState, types.ExecutionID, *graph.Graph) {
		t.Helper()
		g := rerunGraph(t, &types.WorkflowDef{
			Name: "rerun-broken-upstream",
			Nodes: []types.NodeDef{
				{Name: "start", Type: "test.echo"},
				{Name: "tail", Type: "test.echo"},
			},
			Connections: types.Connections{
				"start": {"main": {Targets: []types.Connection{{Node: "tail", Input: "main"}}}},
			},
		})
		state := newFakeState()
		parent := rerunParent(t, state, g, types.ExecutionStatusFailed, map[string]rerunNode{
			"start": {status: types.NodeStatusFailed, port: "error"},
		})
		return state, parent, g
	}

	t.Run("StatusNotTerminal", func(t *testing.T) {
		g := rerunDiamond(t)
		state := newFakeState()
		parent := rerunParent(t, state, g, types.ExecutionStatusRunning, nil)
		seeder := &seedRecorder{fakeState: state}
		eng := New(seeder, &fakeQueue{})

		_, err := eng.Rerun(ctx, RerunRequest{ExecutionID: parent, FromNode: "mid"})
		if !errors.Is(err, ErrExecutionNotRerunnable) {
			t.Fatalf("Rerun() error = %v, want %v", err, ErrExecutionNotRerunnable)
		}
		if len(seeder.requests) != 0 {
			t.Fatal("a refused rerun seeded an execution")
		}
	})

	t.Run("SuccessWithoutInclude", func(t *testing.T) {
		g := rerunDiamond(t)
		state := newFakeState()
		parent := rerunParent(t, state, g, types.ExecutionStatusSuccess, diamondResolved())
		seeder := &seedRecorder{fakeState: state}
		eng := New(seeder, &fakeQueue{})

		if _, err := eng.Rerun(ctx, RerunRequest{ExecutionID: parent, FromNode: "mid"}); !errors.Is(err, ErrExecutionNotRerunnable) {
			t.Fatalf("Rerun(a success) error = %v, want %v", err, ErrExecutionNotRerunnable)
		}
		if _, err := eng.Rerun(ctx, RerunRequest{ExecutionID: parent, FromNode: "mid", IncludeSuccess: true}); err != nil {
			t.Fatalf("Rerun(a success, include_success) error = %v, want it accepted", err)
		}
	})

	t.Run("UnknownNode", func(t *testing.T) {
		g := rerunDiamond(t)
		state := newFakeState()
		parent := rerunParent(t, state, g, types.ExecutionStatusFailed, nil)
		seeder := &seedRecorder{fakeState: state}
		eng := New(seeder, &fakeQueue{})

		_, err := eng.Rerun(ctx, RerunRequest{ExecutionID: parent, FromNode: "nope"})
		if !errors.Is(err, ErrRerunNodeNotFound) {
			t.Fatalf("Rerun(unknown node) error = %v, want %v", err, ErrRerunNodeNotFound)
		}
	})

	t.Run("UpstreamIncomplete", func(t *testing.T) {
		state, parent, _ := brokenUpstream(t)
		seeder := &seedRecorder{fakeState: state}
		eng := New(seeder, &fakeQueue{})

		_, err := eng.Rerun(ctx, RerunRequest{ExecutionID: parent, FromNode: "tail"})
		if !errors.Is(err, ErrRerunUpstreamIncomplete) {
			t.Fatalf("Rerun(incomplete upstream) error = %v, want %v", err, ErrRerunUpstreamIncomplete)
		}
		if len(seeder.requests) != 0 {
			t.Fatal("a rerun with an incomplete upstream seeded an execution anyway")
		}
	})

	t.Run("CyclicGraph", func(t *testing.T) {
		g := rerunGraph(t, &types.WorkflowDef{
			Name:    "rerun-cyclic",
			Options: &types.WorkflowOptions{AllowCycles: true, MaxAutoDepth: 4},
			Nodes: []types.NodeDef{
				{Name: "start", Type: "xflow.start"},
				{Name: "loop", Type: "test.echo"},
			},
			Connections: types.Connections{
				"start": {"main": {Targets: []types.Connection{{Node: "loop", Input: "main"}}}},
				"loop":  {"main": {Targets: []types.Connection{{Node: "loop", Input: "main"}}}},
			},
		})
		state := newFakeState()
		parent := rerunParent(t, state, g, types.ExecutionStatusFailed, nil)
		seeder := &seedRecorder{fakeState: state}
		eng := New(seeder, &fakeQueue{})

		_, err := eng.Rerun(ctx, RerunRequest{ExecutionID: parent, FromNode: "loop"})
		if !errors.Is(err, ErrRerunGraphUnsupported) {
			t.Fatalf("Rerun(cyclic) error = %v, want %v", err, ErrRerunGraphUnsupported)
		}
	})

	t.Run("GroupUnitOutsideThePath", func(t *testing.T) {
		g := rerunGraph(t, &types.WorkflowDef{
			Name: "rerun-group",
			Nodes: []types.NodeDef{
				{Name: "start", Type: "test.echo"},
				{Name: "g.member", Type: "test.echo"},
				{Name: "g.sink", Type: "test.echo"},
			},
			Connections: types.Connections{
				"start":    {"main": {Targets: []types.Connection{{Node: "g.member", Input: "main"}}}},
				"g.member": {"main": {Targets: []types.Connection{{Node: "g.sink", Input: "main"}}}},
			},
			Groups: []types.GroupDef{{Name: "g", Members: []string{"g.member", "g.sink"}}},
		})
		state := newFakeState()
		parent := rerunParent(t, state, g, types.ExecutionStatusFailed, nil)
		seeder := &seedRecorder{fakeState: state}
		eng := New(seeder, &fakeQueue{})

		_, err := eng.Rerun(ctx, RerunRequest{ExecutionID: parent, FromNode: "g.member"})
		if !errors.Is(err, ErrRerunGraphUnsupported) {
			t.Fatalf("Rerun(a group member) error = %v, want %v", err, ErrRerunGraphUnsupported)
		}
	})

	t.Run("StoreWithoutTheCapability", func(t *testing.T) {
		g := rerunDiamond(t)
		state := newFakeState()
		parent := rerunParent(t, state, g, types.ExecutionStatusFailed, nil)
		eng := New(state, &fakeQueue{})

		_, err := eng.Rerun(ctx, RerunRequest{ExecutionID: parent, FromNode: "mid"})
		if !errors.Is(err, ErrRerunUnsupported) {
			t.Fatalf("Rerun(without the capability) error = %v, want %v", err, ErrRerunUnsupported)
		}
	})

	t.Run("AbsentExecution", func(t *testing.T) {
		state := newFakeState()
		seeder := &seedRecorder{fakeState: state}
		eng := New(seeder, &fakeQueue{})

		_, err := eng.Rerun(ctx, RerunRequest{ExecutionID: NewExecutionID(), FromNode: "mid"})
		if !errors.Is(err, ErrExecutionNotFound) {
			t.Fatalf("Rerun(absent) error = %v, want %v", err, ErrExecutionNotFound)
		}
	})
}

// TestRerunReportsTheAdmission pins the concurrency half: a rerun contends for
// its workflow's slot exactly as a submission does, so an over-limit refusal is
// the caller's to act on (a 429, not a failed rerun) and a queue-mode admission
// comes back with the place in line the new execution holds.
func TestRerunReportsTheAdmission(t *testing.T) {
	ctx := context.Background()

	t.Run("OverTheLimit", func(t *testing.T) {
		g := rerunGraph(t, &types.WorkflowDef{
			Name: "rerun-limited",
			Settings: &types.WorkflowSettings{Concurrency: &types.ConcurrencySettings{
				Limit: 1,
				Mode:  types.ConcurrencyModeReject,
			}},
			Nodes: []types.NodeDef{
				{Name: "start", Type: "test.echo"},
				{Name: "tail", Type: "test.echo"},
			},
			Connections: types.Connections{
				"start": {"main": {Targets: []types.Connection{{Node: "tail", Input: "main"}}}},
			},
		})
		state := newFakeState()
		parent := rerunParent(t, state, g, types.ExecutionStatusFailed, map[string]rerunNode{
			"start": {status: types.NodeStatusSuccess, port: "main", output: map[string]any{"n": 1}},
		})
		seeder := &seedRecorder{fakeState: state}
		eng := New(seeder, &fakeQueue{})

		// Hold the only slot with another execution of the same workflow.
		holder := NewExecutionID()
		if _, err := state.CreateExecutionWithConcurrency(ctx, &ExecutionSnapshot{
			ID: holder, Graph: g, Status: types.ExecutionStatusRunning,
		}, nil, ConcurrencyAdmission{
			Namespace: "", Workflow: g.Name(), Limit: 1, Mode: types.ConcurrencyModeReject,
		}); err != nil {
			t.Fatalf("CreateExecutionWithConcurrency(holder) error = %v", err)
		}

		seeder.err = ErrConcurrencyLimit
		_, err := eng.Rerun(ctx, RerunRequest{ExecutionID: parent, FromNode: "tail"})
		if !errors.Is(err, ErrConcurrencyLimit) {
			t.Fatalf("Rerun(over the limit) error = %v, want %v", err, ErrConcurrencyLimit)
		}
		// The admission the store was handed is the graph's own, so the refusal
		// is judged by the definition that ran.
		req := seeder.lastRequest(t)
		if req.Admission == nil || req.Admission.Limit != 1 || req.Admission.Workflow != g.Name() {
			t.Fatalf("seeded admission = %+v, want the parent graph's own limit", req.Admission)
		}
	})

	t.Run("Queued", func(t *testing.T) {
		g := rerunGraph(t, &types.WorkflowDef{
			Name: "rerun-queued",
			Settings: &types.WorkflowSettings{Concurrency: &types.ConcurrencySettings{
				Limit: 1,
				Mode:  types.ConcurrencyModeQueue,
			}},
			Nodes: []types.NodeDef{
				{Name: "start", Type: "test.echo"},
				{Name: "tail", Type: "test.echo"},
			},
			Connections: types.Connections{
				"start": {"main": {Targets: []types.Connection{{Node: "tail", Input: "main"}}}},
			},
		})
		state := newFakeState()
		parent := rerunParent(t, state, g, types.ExecutionStatusFailed, map[string]rerunNode{
			"start": {status: types.NodeStatusSuccess, port: "main", output: map[string]any{"n": 1}},
		})
		seeder := &seedRecorder{fakeState: state, result: SeedExecutionResponse{Queued: true, QueuePosition: 2}}
		eng := New(seeder, &fakeQueue{})

		res, err := eng.Rerun(ctx, RerunRequest{ExecutionID: parent, FromNode: "tail"})
		if err != nil {
			t.Fatalf("Rerun(queue mode) error = %v", err)
		}
		if !res.Queued || res.QueuePosition != 2 {
			t.Fatalf("Rerun(queue mode) = %+v, want Queued at position 2", res)
		}
		// A parked execution's intents are held by the store, so the rerun must
		// not try to deliver them itself.
		if queued := eng.queue.(*fakeQueue); len(queued.Drain()) != 0 {
			t.Fatal("a queued rerun delivered work: the store holds it until promotion")
		}
	})
}

// TestRerunLeavesTheParentAlone is the reason a rerun creates an execution
// instead of resetting nodes: the parent's results, status and history are not
// this operation's to rewrite, and a lease or a fence issued against the old
// execution must never be confusable with the new one.
func TestRerunLeavesTheParentAlone(t *testing.T) {
	ctx := context.Background()
	g := rerunDiamond(t)
	state := newFakeState()
	parent := rerunParent(t, state, g, types.ExecutionStatusFailed, diamondResolved())
	seeder := &seedRecorder{fakeState: state}
	eng := New(seeder, &fakeQueue{})

	res, err := eng.Rerun(ctx, RerunRequest{ExecutionID: parent, FromNode: "mid"})
	if err != nil {
		t.Fatalf("Rerun() error = %v", err)
	}

	snap, err := state.GetExecution(ctx, parent)
	if err != nil {
		t.Fatalf("GetExecution(parent) error = %v", err)
	}
	if snap.Status != types.ExecutionStatusFailed {
		t.Fatalf("parent status = %q, want it untouched at %q", snap.Status, types.ExecutionStatusFailed)
	}
	// The parent's own params are the new execution's: a rerun re-runs the same
	// work, not the same shape with different inputs.
	req := seeder.lastRequest(t)
	if req.Execution.Params["input"] != "parent" {
		t.Fatalf("seeded params = %#v, want the parent's own", req.Execution.Params)
	}
	// And the copy is a copy: mutating what the seed was handed must not reach
	// the parent's stored data.
	req.Units[0].Output["n"] = 99
	stored, err := state.GetOutput(ctx, parent, req.Units[0].NodeName)
	if err != nil {
		t.Fatalf("GetOutput(parent) error = %v", err)
	}
	if stored["n"] == 99 {
		t.Fatalf("the seeded resolution aliases the parent's stored output (%#v)", stored)
	}
	if res.ExecutionID == parent {
		t.Fatal("the rerun reported the parent as its own result")
	}
}

// TestRerunIgnoresAPreallocatedExecutionID pins the one context hook a rerun
// must NOT honour.
//
// WithExecutionID exists so a submission's audit row can name the id the
// execution will get. For an execution-scoped route the slot carries the id the
// CALLER ADDRESSED — the path's execution id — and a rerun shown that id would
// seed itself over the parent: it would rewrite the results it was asked to
// leave alone and the "new execution" it reports would be the old one. Minting
// the id here is what keeps the link a link instead of a self-reference.
func TestRerunIgnoresAPreallocatedExecutionID(t *testing.T) {
	ctx := WithExecutionID(context.Background(), "exec-addressed-by-the-caller")
	g := rerunDiamond(t)
	state := newFakeState()
	parent := rerunParent(t, state, g, types.ExecutionStatusFailed, diamondResolved())
	seeder := &seedRecorder{fakeState: state}
	eng := New(seeder, &fakeQueue{})

	res, err := eng.Rerun(ctx, RerunRequest{ExecutionID: parent, FromNode: "mid"})
	if err != nil {
		t.Fatalf("Rerun() error = %v", err)
	}
	if res.ExecutionID == "exec-addressed-by-the-caller" {
		t.Fatal("the rerun adopted the caller's addressed execution id; it would seed over the parent")
	}
	req := seeder.lastRequest(t)
	if req.Execution.ID != res.ExecutionID {
		t.Fatalf("seeded execution id = %q, reported %q", req.Execution.ID, res.ExecutionID)
	}
	if req.Execution.ParentID != parent {
		t.Fatalf("seeded parent_execution_id = %q, want %q", req.Execution.ParentID, parent)
	}
}

// namesOf renders a resolution map for failure messages.
func namesOf(units map[string]SeedUnit) []string {
	out := make([]string, 0, len(units))
	for name := range units {
		out = append(out, name)
	}
	return out
}
