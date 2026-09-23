package local

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/types"
)

// passThroughHandler succeeds without choosing a port, which leaves the node on
// "main".
type passThroughHandler struct{}

func (passThroughHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.pass_through"}
}

func (passThroughHandler) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	return &types.Output{Data: map[string]any{"ok": true}}, nil
}

// leftOnlyHandler always takes its "left" port, so "right" is the arm this
// workflow never takes and the skip cascade has something to consume.
type leftOnlyHandler struct{}

func (leftOnlyHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.left_only"}
}

func (leftOnlyHandler) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	return &types.Output{Data: map[string]any{"route": "left"}, Port: "left"}, nil
}

// TestSkipCascadeResolvesTheScheduleMarkerByUnit pins that a branch a definition
// does not take is consumed even when node and unit indexes have diverged.
//
// They diverge as soon as a declaration-only node exists: a supply node takes a
// node index and no unit index, so every node compiled after it has
// UnitIdx = NodeIdx - <supply nodes declared before it>. The system commit that
// consumes a skip intent used to resolve the intent's scheduling marker by
// NodeIdx, which then named a *neighbouring* unit. That unit's marker resolved
// to "execute" rather than "skip", so the backend refused the commit — and
// because a refused system commit is reported to the flusher as handled, the
// durable intent was acked and dropped. The skipped branch never terminalized,
// the remaining-unit counter never reached zero, and the execution stayed
// Running while every node that did run reported success. Nothing raised an
// error on any surface.
//
// The definition declares its supply node FIRST on purpose. That ordering is
// what makes the two indexes diverge; a fixture that declared the supply last
// (which is how a test written before the cause was known would naturally be
// arranged) leaves them equal and passes with or without the fix, which is why
// the precondition below is asserted rather than assumed.
func TestSkipCascadeResolvesTheScheduleMarkerByUnit(t *testing.T) {
	reg := execution.NewRegistry()
	reg.RegisterGlobal("xflow.start", passThroughHandler{})
	reg.RegisterGlobal("test.pass_through", passThroughHandler{})
	reg.RegisterGlobal("test.left_only", leftOnlyHandler{})

	def := &types.WorkflowDef{
		Name: "supply-first-skip",
		Nodes: []types.NodeDef{
			{Name: "rules", Type: "xflow.supply.external", Kind: types.NodeKindSupply},
			{Name: "start", Type: "xflow.start"},
			{Name: "route", Type: "test.left_only"},
			{Name: "taken", Type: "test.pass_through"},
			{Name: "untaken", Type: "test.pass_through"},
			{Name: "join", Type: "test.pass_through"},
			{Name: "done", Type: "test.pass_through"},
		},
		Connections: types.Connections{
			"start": {"main": {Targets: []types.Connection{{Node: "route", Input: "main"}}}},
			"route": {
				"left":  {Targets: []types.Connection{{Node: "taken", Input: "main"}}},
				"right": {Targets: []types.Connection{{Node: "untaken", Input: "main"}}},
			},
			"taken":   {"main": {Targets: []types.Connection{{Node: "join", Input: "main"}}}},
			"untaken": {"main": {Targets: []types.Connection{{Node: "join", Input: "main"}}}},
			"join":    {"main": {Targets: []types.Connection{{Node: "done", Input: "main"}}}},
		},
		DependencyEdges: []types.DependencyEdge{{Node: "join", Supply: "rules"}},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	untakenIdx, ok := g.NodeIndex("untaken")
	if !ok {
		t.Fatal("untaken not registered")
	}
	untakenUnit := g.UnitIndexForNode(untakenIdx)
	if untakenUnit == untakenIdx {
		t.Fatalf("untaken has unit index %d equal to its node index: this definition no "+
			"longer makes the two diverge, so the assertions below would hold even "+
			"without the fix and prove nothing", untakenUnit)
	}

	b := New(WithConcurrency(2), WithRegistry(reg))
	eng := engine.New(b.State(), b.Queue())
	stop := b.Bind(eng)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	id, err := eng.Submit(ctx, g, map[string]any{})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	res, err := b.WaitDone(ctx, id)
	if err != nil {
		t.Fatalf("wait: %v -- the execution never reached a terminal status; an untaken "+
			"branch's skip intent is the only thing outstanding", err)
	}
	if res.Status != types.ExecutionStatusSuccess {
		t.Fatalf("execution status = %v, want success (error %q)", res.Status, res.Error)
	}

	statusOf := func(name string) types.NodeStatus {
		t.Helper()
		snap, err := b.State().GetNode(ctx, id, name)
		if err != nil {
			t.Fatalf("GetNode(%q): %v", name, err)
		}
		if snap == nil {
			t.Fatalf("node %q has no snapshot", name)
		}
		return snap.Status
	}
	if got := statusOf("taken"); got != types.NodeStatusSuccess {
		t.Errorf("node taken status = %v, want success", got)
	}
	if got := statusOf("untaken"); got != types.NodeStatusSkipped {
		t.Errorf("node untaken status = %v, want skipped: the arm the router did not take "+
			"must be consumed by the cascade, not left unrun", got)
	}
}

// blockingHandler parks its node in Running until the test releases it, so the
// skip task below is applied to a live execution rather than one that already
// reached a terminal status and evicted its graph.
type blockingHandler struct{ release chan struct{} }

func (blockingHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.blocking"}
}

func (h blockingHandler) Execute(ctx context.Context, _ *types.Input) (*types.Output, error) {
	select {
	case <-h.release:
		return &types.Output{Data: map[string]any{"ok": true}}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// TestNodeSkipRejectsAnUnknownUnitIndex pins that a skip task with no unit
// identity fails loudly instead of being silently dropped.
//
// The commit that consumes a skip resolves its scheduling marker by UnitIdx.
// engine.UnitIdxUnknown (-1) is what a lease or queue payload that lost the
// field defaults to, and it names a key nothing ever wrote: the guard reads no
// marker, refuses the commit, and the refusal is reported as handled — so the
// intent is acked, the branch never terminalizes, and the remaining-unit
// counter never reaches zero. That is the same silent stall the unit-index fix
// addressed, reached from a different direction, and it is exactly what the
// NodeIdx check a few lines above already prevents for its own index. This test
// is the UnitIdx half of that guard.
func TestNodeSkipRejectsAnUnknownUnitIndex(t *testing.T) {
	release := make(chan struct{})

	reg := execution.NewRegistry()
	reg.RegisterGlobal("xflow.start", blockingHandler{release: release})
	reg.RegisterGlobal("test.pass_through", passThroughHandler{})

	def := &types.WorkflowDef{
		Name: "unknown-unit-index",
		Nodes: []types.NodeDef{
			{Name: "rules", Type: "xflow.supply.external", Kind: types.NodeKindSupply},
			{Name: "start", Type: "xflow.start"},
			{Name: "middle", Type: "test.pass_through"},
			{Name: "sink", Type: "test.pass_through"},
		},
		Connections: types.Connections{
			"start":  {"main": {Targets: []types.Connection{{Node: "middle", Input: "main"}}}},
			"middle": {"main": {Targets: []types.Connection{{Node: "sink", Input: "main"}}}},
		},
		DependencyEdges: []types.DependencyEdge{{Node: "sink", Supply: "rules"}},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	middleIdx, ok := g.NodeIndex("middle")
	if !ok {
		t.Fatal("middle not registered")
	}
	if unit := g.UnitIndexForNode(middleIdx); unit == middleIdx {
		t.Fatalf("middle has unit index %d equal to its node index: the fixture no longer "+
			"declares its supply node first, so the two indexes never diverge here", unit)
	}

	b := New(WithConcurrency(2), WithRegistry(reg))
	eng := engine.New(b.State(), b.Queue())
	stop := b.Bind(eng)
	// Release the parked worker before stopping the backend, or stop waits on a
	// worker the handler is still holding.
	defer func() {
		close(release)
		stop()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	id, err := eng.Submit(ctx, g, map[string]any{})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	handled, err := eng.HandleSystemTask(ctx, &engine.Task{
		ExecutionID: id,
		NodeName:    "middle",
		NodeIdx:     middleIdx,
		UnitIdx:     engine.UnitIdxUnknown,
		Type:        engine.TaskTypeNodeSkip,
	})
	if !handled {
		t.Fatal("HandleSystemTask reported the skip unhandled; the engine owns system tasks")
	}
	if err == nil {
		t.Fatalf("HandleSystemTask accepted a node-skip with UnitIdx = %d: its scheduling "+
			"marker resolves to a key nothing wrote, so the guard would refuse the commit and "+
			"report the refusal as handled, acking the intent and dropping the branch",
			engine.UnitIdxUnknown)
	}
}
