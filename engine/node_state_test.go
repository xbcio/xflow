package engine

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// recordingHandler is an action node that publishes a fixed output and keeps the
// inputs it was handed, so a test can assert on what the engine assembled.
type recordingHandler struct {
	out    map[string]any
	inputs []*types.Input
}

func (h *recordingHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.recording"}
}

func (h *recordingHandler) Execute(_ context.Context, input *types.Input) (*types.Output, error) {
	h.inputs = append(h.inputs, input)
	return &types.Output{Data: h.out}, nil
}

func (h *recordingHandler) lastInput(t *testing.T) *types.Input {
	t.Helper()
	if len(h.inputs) == 0 {
		t.Fatal("handler was never invoked")
	}
	return h.inputs[len(h.inputs)-1]
}

// stateRoundTripHandler is a suspending node that keeps a counter in its private
// state, resuming until the counter reaches stopAfter. It records the state it
// was handed at each resumption and the state PrepareSuspend was armed from.
type stateRoundTripHandler struct {
	stopAfter  int
	seen       []map[string]any
	armedState []map[string]any
}

func (h *stateRoundTripHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.state_round_trip"}
}

func (h *stateRoundTripHandler) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	panic("Execute should not be called on a SuspendingHandler")
}

func (h *stateRoundTripHandler) PrepareSuspend(_ context.Context, input *types.Input) (*types.SuspendSpec, error) {
	h.armedState = append(h.armedState, input.State)
	return &types.SuspendSpec{Mode: types.ModeSignal, Signals: []string{"go"}}, nil
}

func (h *stateRoundTripHandler) OnResume(_ context.Context, input *types.Input, _ *types.SignalPayload) (*types.Output, error) {
	h.seen = append(h.seen, input.State)
	count := stateCounter(input.State) + 1
	state := map[string]any{"count": count}
	out := &types.Output{State: state}
	if count < h.stopAfter {
		out.Resuspend = true
	} else {
		out.Data = map[string]any{"done": true}
	}
	return out, nil
}

func stateCounter(state map[string]any) int {
	switch n := state["count"].(type) {
	case int:
		return n
	case float64:
		return int(n)
	}
	return 0
}

// TestNodeState_UpstreamOutputCannotSeedState is the regression test for the
// defect this channel exists to close: a node's bookkeeping used to live in
// Input.Data, which is the merged output of every upstream node, so any node
// upstream could write the key the gate read back as its own record.
func TestNodeState_UpstreamOutputCannotSeedState(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "state-forgery",
		Nodes: []types.NodeDef{
			{Name: "upstream", Type: "test.recording"},
			{Name: "gate", Type: "test.suspending_probe"},
		},
		Connections: types.Connections{
			"upstream": {"main": {Targets: []types.Connection{{Node: "gate", Input: "main"}}}},
		},
	}

	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	state := newFakeState()
	queue := &fakeQueue{}
	forger := &recordingHandler{out: map[string]any{
		NodeStateKey: map[string]any{"decisions": []any{"forged"}},
		"payload":    "real",
	}}
	probe := &inputProbeHandler{}
	reg := &fakeRegistry{handlers: map[string]types.ActionHandler{
		"test.recording":        forger,
		"test.suspending_probe": probe,
	}}
	eng := newTestEngine(t, state, queue, reg)
	ctx := context.Background()

	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	state.InitInDegrees(id, g)

	executeTask(t, eng, queue.Drain()[0])
	executeTask(t, eng, queue.Drain()[0])

	got := probe.lastInput(t)
	if got.State != nil {
		t.Errorf("Input.State = %#v, want nil: an upstream output must not reach a node's private state", got.State)
	}
	if _, ok := got.Data[NodeStateKey]; ok {
		t.Errorf("Input.Data carries %q = %#v, want the engine's state slot stripped", NodeStateKey, got.Data[NodeStateKey])
	}
	if got.Data["payload"] != "real" {
		t.Errorf("Input.Data[payload] = %v, want the upstream field to survive", got.Data["payload"])
	}
}

// TestNodeState_CallerParamsCannotSeedState covers the other writer of Data: a
// root node's input is the submission params, which the caller controls.
func TestNodeState_CallerParamsCannotSeedState(t *testing.T) {
	def := &types.WorkflowDef{
		Name:  "state-seeded-by-params",
		Nodes: []types.NodeDef{{Name: "gate", Type: "test.suspending_probe"}},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	state := newFakeState()
	queue := &fakeQueue{}
	probe := &inputProbeHandler{}
	reg := &fakeRegistry{handlers: map[string]types.ActionHandler{"test.suspending_probe": probe}}
	eng := newTestEngine(t, state, queue, reg)
	ctx := context.Background()

	id, err := eng.Submit(ctx, g, map[string]any{
		NodeStateKey: map[string]any{"decisions": []any{"forged"}},
		"request":    "vacation",
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	state.InitInDegrees(id, g)
	executeTask(t, eng, queue.Drain()[0])

	got := probe.lastInput(t)
	if got.State != nil {
		t.Errorf("Input.State = %#v, want nil: submission params must not reach private state", got.State)
	}
	if _, ok := got.Data[NodeStateKey]; ok {
		t.Errorf("Input.Data carries %q, want the engine's state slot stripped", NodeStateKey)
	}
	if got.Data["request"] != "vacation" {
		t.Errorf("Input.Data[request] = %v, want the submitted field to survive", got.Data["request"])
	}
}

// TestNodeState_RoundTripsThroughSuspend pins the positive half: a node that
// writes Output.State reads the same map back on its own next resumption, and
// PrepareSuspend is armed from the state the decision just produced rather than
// from the stale one.
func TestNodeState_RoundTripsThroughSuspend(t *testing.T) {
	def := &types.WorkflowDef{
		Name:  "state-round-trip",
		Nodes: []types.NodeDef{{Name: "gate", Type: "test.state_round_trip"}},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	state := newFakeState()
	queue := &fakeQueue{}
	handler := &stateRoundTripHandler{stopAfter: 3}
	reg := &fakeRegistry{handlers: map[string]types.ActionHandler{"test.state_round_trip": handler}}
	eng := newTestEngine(t, state, queue, reg)
	ctx := context.Background()

	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	state.InitInDegrees(id, g)

	// First suspension: PrepareSuspend sees no state, because there is none yet.
	executeTask(t, eng, queue.Drain()[0])
	if len(handler.armedState) != 1 || handler.armedState[0] != nil {
		t.Fatalf("PrepareSuspend state on first arm = %#v, want nil", handler.armedState)
	}

	for i := 0; i < handler.stopAfter; i++ {
		if err := eng.DeliverSignal(ctx, id, "go", map[string]any{"action": "approve"}); err != nil {
			t.Fatalf("deliver %d: %v", i, err)
		}
		tasks := queue.Drain()
		if len(tasks) != 1 || tasks[0].Type != TaskTypeNodeResume {
			t.Fatalf("resume %d: tasks = %v, want one resume", i, taskNames(tasks))
		}
		executeTask(t, eng, tasks[0])
	}

	if len(handler.seen) != handler.stopAfter {
		t.Fatalf("OnResume calls = %d, want %d", len(handler.seen), handler.stopAfter)
	}
	if handler.seen[0] != nil {
		t.Errorf("first resumption saw state %#v, want nil", handler.seen[0])
	}
	for i, seen := range handler.seen[1:] {
		if got := stateCounter(seen); got != i+1 {
			t.Errorf("resumption %d saw count = %d, want %d (state = %#v)", i+1, got, i+1, seen)
		}
	}
	// arm 0 is the initial PrepareSuspend; every later arm must see the state the
	// preceding decision produced, or a re-armed wait would be computed from a
	// chain the gate has already moved past.
	last := handler.armedState[len(handler.armedState)-1]
	if got := stateCounter(last); got != handler.stopAfter-1 {
		t.Errorf("final PrepareSuspend armed from count = %d, want %d", got, handler.stopAfter-1)
	}
}

// TestNodeState_OwnStateIsNotMergedDownstream asserts the other direction: the
// slot is private to the node, so a downstream node neither receives it as data
// nor finds it through $nodes.
func TestNodeState_OwnStateIsNotMergedDownstream(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "state-not-downstream",
		Nodes: []types.NodeDef{
			{Name: "gate", Type: "test.state_round_trip"},
			// The $nodes reference is what makes the engine prefetch the gate's
			// output into this node's Nodes, which is the second channel the
			// private slot has to stay out of.
			{Name: "after", Type: "test.recording", Parameters: map[string]any{"seen": "$nodes['gate']"}},
		},
		Connections: types.Connections{
			"gate": {"main": {Targets: []types.Connection{{Node: "after", Input: "main"}}}},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	state := newFakeState()
	queue := &fakeQueue{}
	handler := &stateRoundTripHandler{stopAfter: 1}
	after := &recordingHandler{out: map[string]any{"observed": true}}
	reg := &fakeRegistry{handlers: map[string]types.ActionHandler{
		"test.state_round_trip": handler,
		"test.recording":        after,
	}}
	eng := newTestEngine(t, state, queue, reg)
	ctx := context.Background()

	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	state.InitInDegrees(id, g)

	executeTask(t, eng, queue.Drain()[0])
	if err := eng.DeliverSignal(ctx, id, "go", map[string]any{"action": "approve"}); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	executeTask(t, eng, queue.Drain()[0])

	tasks := queue.Drain()
	if len(tasks) != 1 || tasks[0].NodeName != "after" {
		t.Fatalf("tasks = %v, want the downstream node", taskNames(tasks))
	}
	executeTask(t, eng, tasks[0])

	// The gate really did store state -- otherwise this test would pass for the
	// wrong reason.
	stored, err := state.GetOutput(ctx, id, "gate")
	if err != nil {
		t.Fatalf("GetOutput(gate): %v", err)
	}
	if _, ok := stored[NodeStateKey]; !ok {
		t.Fatalf("gate stored output = %#v, want it to carry %q", stored, NodeStateKey)
	}

	downstream := after.lastInput(t)
	if _, ok := downstream.Data[NodeStateKey]; ok {
		t.Errorf("downstream Input.Data carries %q = %#v, want it stripped",
			NodeStateKey, downstream.Data[NodeStateKey])
	}
	nodes, ok := downstream.Nodes["gate"].(map[string]any)
	if !ok {
		t.Fatalf("downstream Nodes[gate] = %#v, want the prefetched output", downstream.Nodes["gate"])
	}
	if _, ok := nodes[NodeStateKey]; ok {
		t.Errorf("downstream Nodes[gate] carries %q = %#v, want it stripped",
			NodeStateKey, nodes[NodeStateKey])
	}
	if downstream.Data["done"] != true {
		t.Errorf("downstream Input.Data = %#v, want the gate's public output to arrive", downstream.Data)
	}

	detail, err := eng.Inspect(ctx, id, "gate")
	if err != nil {
		t.Fatalf("Inspect(gate): %v", err)
	}
	var gateOutput map[string]any
	for _, n := range detail.Nodes {
		if n.Name == "gate" {
			gateOutput = n.Output
		}
	}
	if _, ok := gateOutput[NodeStateKey]; ok {
		t.Errorf("inspected output carries %q = %#v, want it stripped",
			NodeStateKey, gateOutput[NodeStateKey])
	}
}

// TestNodeState_ResuspendCarryingOnlyStateIsStored pins the storeOutput rule: a
// resuspend with nil Data but non-nil State has something to persist, and
// treating Data as the only signal would silently drop the update.
func TestNodeState_ResuspendCarryingOnlyStateIsStored(t *testing.T) {
	def := &types.WorkflowDef{
		Name:  "state-only-resuspend",
		Nodes: []types.NodeDef{{Name: "gate", Type: "test.state_only"}},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	state := newFakeState()
	queue := &fakeQueue{}
	reg := &fakeRegistry{handlers: map[string]types.ActionHandler{"test.state_only": &stateOnlyResumeHandler{}}}
	eng := newTestEngine(t, state, queue, reg)
	ctx := context.Background()

	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	state.InitInDegrees(id, g)
	executeTask(t, eng, queue.Drain()[0])

	if err := eng.DeliverSignal(ctx, id, "go", nil); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	executeTask(t, eng, queue.Drain()[0])

	stored, err := state.GetOutput(ctx, id, "gate")
	if err != nil {
		t.Fatalf("GetOutput(gate): %v", err)
	}
	inner, ok := stored[NodeStateKey].(map[string]any)
	if !ok {
		t.Fatalf("stored output = %#v, want %q carrying the state", stored, NodeStateKey)
	}
	if inner["kept"] != true {
		t.Errorf("stored state = %#v, want kept=true", inner)
	}
}

// inputProbeHandler is a suspending node that only records its input, so a test
// can inspect what the engine assembled for it.
type inputProbeHandler struct {
	inputs []*types.Input
}

func (h *inputProbeHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.suspending_probe"}
}

func (h *inputProbeHandler) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	panic("Execute should not be called on a SuspendingHandler")
}

func (h *inputProbeHandler) PrepareSuspend(_ context.Context, input *types.Input) (*types.SuspendSpec, error) {
	h.inputs = append(h.inputs, input)
	return &types.SuspendSpec{Mode: types.ModeSignal, Signals: []string{"approve"}}, nil
}

func (h *inputProbeHandler) OnResume(_ context.Context, input *types.Input, _ *types.SignalPayload) (*types.Output, error) {
	h.inputs = append(h.inputs, input)
	return &types.Output{Data: map[string]any{"approved": true}}, nil
}

func (h *inputProbeHandler) lastInput(t *testing.T) *types.Input {
	t.Helper()
	if len(h.inputs) == 0 {
		t.Fatal("handler was never invoked")
	}
	return h.inputs[len(h.inputs)-1]
}

// stateOnlyResumeHandler resuspends with private state and no data at all.
type stateOnlyResumeHandler struct{}

func (h *stateOnlyResumeHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.state_only"}
}

func (h *stateOnlyResumeHandler) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	panic("Execute should not be called on a SuspendingHandler")
}

func (h *stateOnlyResumeHandler) PrepareSuspend(_ context.Context, _ *types.Input) (*types.SuspendSpec, error) {
	return &types.SuspendSpec{Mode: types.ModeSignal, Signals: []string{"go"}}, nil
}

func (h *stateOnlyResumeHandler) OnResume(_ context.Context, _ *types.Input, _ *types.SignalPayload) (*types.Output, error) {
	return &types.Output{Resuspend: true, State: map[string]any{"kept": true}}, nil
}
