package engine

import (
	"context"
	"sort"
	"sync"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// recordingOutputState sums up how buildInput reads node outputs: one batched
// GetOutputs call, or one GetOutput per name. The whole point of the batch
// capability is the number of store round trips, which no outcome assertion can
// see, so these tests count calls instead.
type recordingOutputState struct {
	*fakeState
	mu          sync.Mutex
	batchCalls  int
	batchNames  [][]string
	singleCalls []string
}

func newRecordingOutputState() *recordingOutputState {
	return &recordingOutputState{fakeState: newFakeState()}
}

func (s *recordingOutputState) GetOutput(ctx context.Context, id types.ExecutionID, name string) (map[string]any, error) {
	s.mu.Lock()
	s.singleCalls = append(s.singleCalls, name)
	s.mu.Unlock()
	return s.fakeState.GetOutput(ctx, id, name)
}

func (s *recordingOutputState) GetOutputs(ctx context.Context, id types.ExecutionID, names []string) (map[string]map[string]any, error) {
	s.mu.Lock()
	s.batchCalls++
	s.batchNames = append(s.batchNames, append([]string(nil), names...))
	s.mu.Unlock()
	outputs := make(map[string]map[string]any, len(names))
	for _, name := range names {
		if _, seen := outputs[name]; seen {
			continue
		}
		data, err := s.fakeState.GetOutput(ctx, id, name)
		if err != nil {
			return nil, err
		}
		// A name with no stored output is absent, matching engine.BatchOutputReader.
		if data != nil {
			outputs[name] = data
		}
	}
	return outputs, nil
}

func (s *recordingOutputState) batchSnapshot() (int, [][]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	calls := s.batchCalls
	names := make([][]string, len(s.batchNames))
	for i := range s.batchNames {
		names[i] = append([]string(nil), s.batchNames[i]...)
	}
	return calls, names
}

func (s *recordingOutputState) singleSnapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.singleCalls...)
}

func sortedStrings(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

// TestBuildInput_FanInReadsOutputsInOneBatch pins the fan-in read side: F
// inbound edges are read in one batch instead of F GetOutput round trips.
func TestBuildInput_FanInReadsOutputsInOneBatch(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "batch-fan-in",
		Nodes: []types.NodeDef{
			{Name: "A", Type: "test.noop"},
			{Name: "B", Type: "test.noop"},
			{Name: "M", Type: "test.noop"},
		},
		Connections: types.Connections{
			"A": {"main": {Targets: []types.Connection{{Node: "M", Input: "inventory"}}}},
			"B": {"main": {Targets: []types.Connection{{Node: "M", Input: "price"}}}},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	state := newRecordingOutputState()
	eng := New(state, &fakeQueue{})

	execID := types.ExecutionID("exec-batch-fan-in")
	if err := state.CreateExecution(context.Background(), &ExecutionSnapshot{
		ID: execID, Status: types.ExecutionStatusRunning, Graph: g,
	}); err != nil {
		t.Fatalf("CreateExecution: %v", err)
	}
	if err := state.PutOutput(context.Background(), execID, "A", map[string]any{"from": "a"}); err != nil {
		t.Fatalf("PutOutput(A): %v", err)
	}
	if err := state.PutOutput(context.Background(), execID, "B", map[string]any{"from": "b"}); err != nil {
		t.Fatalf("PutOutput(B): %v", err)
	}

	mIdx, ok := g.NodeIndex("M")
	if !ok {
		t.Fatal("node M is not in the compiled graph")
	}
	input, err := eng.buildInput(context.Background(), &Task{
		ExecutionID: execID, NodeName: "M", NodeIdx: mIdx, Type: TaskTypeNodeExec,
	}, g)
	if err != nil {
		t.Fatalf("buildInput: %v", err)
	}

	if got := portOf(t, input, "inventory")["from"]; got != "a" {
		t.Fatalf("$inputs.inventory.from = %v, want a", got)
	}
	if got := portOf(t, input, "price")["from"]; got != "b" {
		t.Fatalf("$inputs.price.from = %v, want b", got)
	}

	calls, names := state.batchSnapshot()
	if calls != 1 {
		t.Fatalf("GetOutputs called %d times for a 2-edge fan-in, want exactly 1", calls)
	}
	if got, want := sortedStrings(names[0]), []string{"A", "B"}; !equalStrings(got, want) {
		t.Fatalf("GetOutputs names = %v, want %v (one name per inbound edge)", got, want)
	}
	if singles := state.singleSnapshot(); len(singles) != 0 {
		t.Fatalf("GetOutput called %d times (%v) alongside the batch, want 0", len(singles), singles)
	}
}

// TestBuildInput_NodesRefsReadOutputsInOneBatch pins the $nodes read side. The
// node is a root (no in-edges), so the prefetch is the only output read and a
// per-name implementation would show up as GetOutput calls.
func TestBuildInput_NodesRefsReadOutputsInOneBatch(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "batch-nodes",
		Nodes: []types.NodeDef{
			{Name: "A", Type: "test.noop"},
			{Name: "B", Type: "test.noop"},
			{Name: "C", Type: "test.noop", Parameters: map[string]any{
				"a": "${{ $nodes['A'].from }}",
				"b": "${{ $nodes['B'].from }}",
			}},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	state := newRecordingOutputState()
	eng := New(state, &fakeQueue{})

	execID := types.ExecutionID("exec-batch-nodes")
	if err := state.CreateExecution(context.Background(), &ExecutionSnapshot{
		ID: execID, Status: types.ExecutionStatusRunning, Graph: g,
	}); err != nil {
		t.Fatalf("CreateExecution: %v", err)
	}
	if err := state.PutOutput(context.Background(), execID, "A", map[string]any{"from": "a"}); err != nil {
		t.Fatalf("PutOutput(A): %v", err)
	}
	if err := state.PutOutput(context.Background(), execID, "B", map[string]any{"from": "b"}); err != nil {
		t.Fatalf("PutOutput(B): %v", err)
	}

	cIdx, ok := g.NodeIndex("C")
	if !ok {
		t.Fatal("node C is not in the compiled graph")
	}
	input, err := eng.buildInput(context.Background(), &Task{
		ExecutionID: execID, NodeName: "C", NodeIdx: cIdx, Type: TaskTypeNodeExec,
	}, g)
	if err != nil {
		t.Fatalf("buildInput: %v", err)
	}

	a, ok := input.Nodes["A"].(map[string]any)
	if !ok || a["from"] != "a" {
		t.Fatalf("input.Nodes[A] = %#v, want from=a", input.Nodes["A"])
	}
	b, ok := input.Nodes["B"].(map[string]any)
	if !ok || b["from"] != "b" {
		t.Fatalf("input.Nodes[B] = %#v, want from=b", input.Nodes["B"])
	}

	calls, names := state.batchSnapshot()
	if calls != 1 {
		t.Fatalf("GetOutputs called %d times for 2 $nodes refs, want exactly 1", calls)
	}
	if got, want := sortedStrings(names[0]), []string{"A", "B"}; !equalStrings(got, want) {
		t.Fatalf("GetOutputs names = %v, want %v", got, want)
	}
	if singles := state.singleSnapshot(); len(singles) != 0 {
		t.Fatalf("GetOutput called %d times (%v) for a root node's $nodes prefetch, want 0", len(singles), singles)
	}
}

// TestBuildInput_ResumeMergesOwnOutputIntoTheBatch pins the resume path: the
// node's own stored output and its $nodes references are one batch, not one
// read plus a second fan-out.
func TestBuildInput_ResumeMergesOwnOutputIntoTheBatch(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "batch-resume",
		Nodes: []types.NodeDef{
			{Name: "A", Type: "test.noop"},
			{Name: "C", Type: "test.noop", Parameters: map[string]any{
				"a": "${{ $nodes['A'].from }}",
			}},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	state := newRecordingOutputState()
	eng := New(state, &fakeQueue{})

	execID := types.ExecutionID("exec-batch-resume")
	if err := state.CreateExecution(context.Background(), &ExecutionSnapshot{
		ID: execID, Status: types.ExecutionStatusRunning, Graph: g,
	}); err != nil {
		t.Fatalf("CreateExecution: %v", err)
	}
	if err := state.PutOutput(context.Background(), execID, "A", map[string]any{"from": "a"}); err != nil {
		t.Fatalf("PutOutput(A): %v", err)
	}
	if err := state.PutOutput(context.Background(), execID, "C", map[string]any{
		"count":      float64(1),
		NodeStateKey: map[string]any{"cursor": "c1"},
	}); err != nil {
		t.Fatalf("PutOutput(C): %v", err)
	}

	cIdx, ok := g.NodeIndex("C")
	if !ok {
		t.Fatal("node C is not in the compiled graph")
	}
	input, err := eng.buildInput(context.Background(), &Task{
		ExecutionID: execID, NodeName: "C", NodeIdx: cIdx, Type: TaskTypeNodeResume,
	}, g)
	if err != nil {
		t.Fatalf("buildInput: %v", err)
	}

	if input.State["cursor"] != "c1" {
		t.Fatalf("input.State = %#v, want the resumed node's own state (cursor=c1)", input.State)
	}
	if input.Data["count"] != float64(1) {
		t.Fatalf("input.Data = %#v, want the resumed node's own output (count=1)", input.Data)
	}
	if _, leaked := input.Data[NodeStateKey]; leaked {
		t.Fatal("input.Data still carries the engine's node-state slot")
	}
	a, ok := input.Nodes["A"].(map[string]any)
	if !ok || a["from"] != "a" {
		t.Fatalf("input.Nodes[A] = %#v, want from=a", input.Nodes["A"])
	}

	calls, names := state.batchSnapshot()
	if calls != 1 {
		t.Fatalf("GetOutputs called %d times for resume with one $nodes ref, want exactly 1", calls)
	}
	if got, want := sortedStrings(names[0]), []string{"A", "C"}; !equalStrings(got, want) {
		t.Fatalf("GetOutputs names = %v, want %v (the node's own output merged with its refs)", got, want)
	}
	if singles := state.singleSnapshot(); len(singles) != 0 {
		t.Fatalf("GetOutput called %d times (%v) on the resume path, want 0", len(singles), singles)
	}
}

// TestBuildInput_FanInAndNodesRefsShareOneBatch pins the combined read: a node
// that both fans in and references a peer reads everything in one batch, which
// is the F+R round trips the capability removes.
func TestBuildInput_FanInAndNodesRefsShareOneBatch(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "batch-fan-in-nodes",
		Nodes: []types.NodeDef{
			{Name: "A", Type: "test.noop"},
			{Name: "B", Type: "test.noop"},
			{Name: "X", Type: "test.noop"},
			{Name: "M", Type: "test.noop", Parameters: map[string]any{
				"x": "${{ $nodes['X'].from }}",
			}},
		},
		Connections: types.Connections{
			"A": {"main": {Targets: []types.Connection{{Node: "M", Input: "inventory"}}}},
			"B": {"main": {Targets: []types.Connection{{Node: "M", Input: "price"}}}},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	state := newRecordingOutputState()
	eng := New(state, &fakeQueue{})

	execID := types.ExecutionID("exec-batch-fan-in-nodes")
	if err := state.CreateExecution(context.Background(), &ExecutionSnapshot{
		ID: execID, Status: types.ExecutionStatusRunning, Graph: g,
	}); err != nil {
		t.Fatalf("CreateExecution: %v", err)
	}
	for name, from := range map[string]string{"A": "a", "B": "b", "X": "x"} {
		if err := state.PutOutput(context.Background(), execID, name, map[string]any{"from": from}); err != nil {
			t.Fatalf("PutOutput(%s): %v", name, err)
		}
	}

	mIdx, ok := g.NodeIndex("M")
	if !ok {
		t.Fatal("node M is not in the compiled graph")
	}
	input, err := eng.buildInput(context.Background(), &Task{
		ExecutionID: execID, NodeName: "M", NodeIdx: mIdx, Type: TaskTypeNodeExec,
	}, g)
	if err != nil {
		t.Fatalf("buildInput: %v", err)
	}

	if got := portOf(t, input, "inventory")["from"]; got != "a" {
		t.Fatalf("$inputs.inventory.from = %v, want a", got)
	}
	if got := portOf(t, input, "price")["from"]; got != "b" {
		t.Fatalf("$inputs.price.from = %v, want b", got)
	}
	x, ok := input.Nodes["X"].(map[string]any)
	if !ok || x["from"] != "x" {
		t.Fatalf("input.Nodes[X] = %#v, want from=x", input.Nodes["X"])
	}

	calls, names := state.batchSnapshot()
	if calls != 1 {
		t.Fatalf("GetOutputs called %d times for 2 edges + 1 reference, want exactly 1", calls)
	}
	if got, want := sortedStrings(names[0]), []string{"A", "B", "X"}; !equalStrings(got, want) {
		t.Fatalf("GetOutputs names = %v, want %v (edges and $nodes refs in one batch)", got, want)
	}
	if singles := state.singleSnapshot(); len(singles) != 0 {
		t.Fatalf("GetOutput called %d times (%v) alongside the combined batch, want 0", len(singles), singles)
	}
}

// TestBuildInput_SingleInEdgeStillReadsPerName keeps the single-upstream path
// on its existing GetOutput: there is no fan-in to amortize, and routing it
// through the batch would buy nothing while changing the read every one-edge
// node makes.
func TestBuildInput_SingleInEdgeStillReadsPerName(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "batch-single",
		Nodes: []types.NodeDef{
			{Name: "A", Type: "test.noop"},
			{Name: "B", Type: "test.noop"},
		},
		Connections: types.Connections{
			"A": {"main": {Targets: []types.Connection{{Node: "B", Input: "main"}}}},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	state := newRecordingOutputState()
	eng := New(state, &fakeQueue{})

	execID := types.ExecutionID("exec-batch-single")
	if err := state.CreateExecution(context.Background(), &ExecutionSnapshot{
		ID: execID, Status: types.ExecutionStatusRunning, Graph: g,
	}); err != nil {
		t.Fatalf("CreateExecution: %v", err)
	}
	if err := state.PutOutput(context.Background(), execID, "A", map[string]any{"from": "a"}); err != nil {
		t.Fatalf("PutOutput(A): %v", err)
	}

	bIdx, ok := g.NodeIndex("B")
	if !ok {
		t.Fatal("node B is not in the compiled graph")
	}
	input, err := eng.buildInput(context.Background(), &Task{
		ExecutionID: execID, NodeName: "B", NodeIdx: bIdx, Type: TaskTypeNodeExec,
	}, g)
	if err != nil {
		t.Fatalf("buildInput: %v", err)
	}
	if got := input.Data["from"]; got != "a" {
		t.Fatalf("input.Data.from = %v, want a", got)
	}

	if calls, _ := state.batchSnapshot(); calls != 0 {
		t.Fatalf("GetOutputs called %d times for a single in-edge, want 0 (the path stays per-name)", calls)
	}
	if singles := state.singleSnapshot(); len(singles) != 1 || singles[0] != "A" {
		t.Fatalf("GetOutput calls = %v, want exactly [A]", singles)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
