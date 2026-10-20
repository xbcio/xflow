package engine

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// recordingExecutionState wraps fakeState and stands in for a backend that has
// the two narrow execution-read capabilities. It counts which path the engine
// took — the narrow read or the full snapshot — because the whole point of the
// capabilities is the number of store calls, which no outcome assertion can
// see, and it can inject a read error to pin the failure semantics.
type recordingExecutionState struct {
	*fakeState

	mu           sync.Mutex
	fullReads    int
	narrowReads  int
	carrierReads int
	narrowErr    error
	carrierErr   error
}

func newRecordingExecutionState() *recordingExecutionState {
	return &recordingExecutionState{fakeState: newFakeState()}
}

func (s *recordingExecutionState) GetExecution(ctx context.Context, id types.ExecutionID) (*ExecutionSnapshot, error) {
	s.mu.Lock()
	s.fullReads++
	s.mu.Unlock()
	return s.fakeState.GetExecution(ctx, id)
}

func (s *recordingExecutionState) GetExecutionInputContext(ctx context.Context, id types.ExecutionID) (*ExecutionInputContext, bool, error) {
	s.mu.Lock()
	s.narrowReads++
	err := s.narrowErr
	s.mu.Unlock()
	if err != nil {
		return nil, false, err
	}
	snap, err := s.fakeState.GetExecution(ctx, id)
	if err != nil {
		return nil, false, err
	}
	if snap == nil {
		return nil, false, nil
	}
	return &ExecutionInputContext{
		Status:  snap.Status,
		Params:  snap.Params,
		Runtime: snap.Runtime,
		Scope:   snap.Scope,
		TraceID: snap.TraceID,
		SpanID:  snap.SpanID,
	}, true, nil
}

func (s *recordingExecutionState) GetExecutionTraceCarrier(ctx context.Context, id types.ExecutionID) (map[string]string, bool, error) {
	s.mu.Lock()
	s.carrierReads++
	err := s.carrierErr
	s.mu.Unlock()
	if err != nil {
		return nil, false, err
	}
	snap, err := s.fakeState.GetExecution(ctx, id)
	if err != nil {
		return nil, false, err
	}
	if snap == nil || len(snap.TraceCarrier) == 0 {
		return nil, false, nil
	}
	return snap.TraceCarrier, true, nil
}

func (s *recordingExecutionState) setNarrowError(err error) {
	s.mu.Lock()
	s.narrowErr = err
	s.mu.Unlock()
}

func (s *recordingExecutionState) setCarrierError(err error) {
	s.mu.Lock()
	s.carrierErr = err
	s.mu.Unlock()
}

func (s *recordingExecutionState) readCounts() (full, narrow, carrier int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fullReads, s.narrowReads, s.carrierReads
}

// snapshotOnlyState is a backend WITHOUT the narrow capabilities: it only
// exposes GetExecution, which is what the engine must fall back to.
type snapshotOnlyState struct {
	*fakeState

	mu        sync.Mutex
	fullReads int
}

func (s *snapshotOnlyState) GetExecution(ctx context.Context, id types.ExecutionID) (*ExecutionSnapshot, error) {
	s.mu.Lock()
	s.fullReads++
	s.mu.Unlock()
	return s.fakeState.GetExecution(ctx, id)
}

func (s *snapshotOnlyState) reads() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fullReads
}

// inputBranchCase is one buildInput shape whose input must be identical whether
// the execution state came from the narrow capability or the full snapshot.
type inputBranchCase struct {
	name     string
	def      *types.WorkflowDef
	node     string
	taskType TaskType
	snapshot ExecutionSnapshot
	outputs  map[string]map[string]any
}

// runInputBranchCase builds a node's input through both paths against two
// independently seeded stores, asserts the narrow path never touched
// GetExecution, and returns the input for branch-specific assertions.
func runInputBranchCase(t *testing.T, c inputBranchCase) *types.Input {
	t.Helper()
	ctx := context.Background()
	g, err := graph.Compile(c.def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	id := types.ExecutionID("exec-input-context-" + c.name)
	nodeIdx, ok := g.NodeIndex(c.node)
	if !ok {
		t.Fatalf("node %q is not in the compiled graph", c.node)
	}
	task := &Task{ExecutionID: id, NodeName: c.node, NodeIdx: nodeIdx, Type: c.taskType}

	seed := func(t *testing.T, st *fakeState) {
		t.Helper()
		snap := c.snapshot
		snap.ID = id
		snap.Graph = g
		if snap.Status == "" {
			snap.Status = types.ExecutionStatusRunning
		}
		if err := st.CreateExecution(ctx, &snap); err != nil {
			t.Fatalf("CreateExecution: %v", err)
		}
		for name, data := range c.outputs {
			if err := st.PutOutput(ctx, id, name, data); err != nil {
				t.Fatalf("PutOutput(%q): %v", name, err)
			}
		}
	}

	narrowState := newRecordingExecutionState()
	seed(t, narrowState.fakeState)
	narrowInput, err := New(narrowState, &fakeQueue{}).buildInput(ctx, task, g)
	if err != nil {
		t.Fatalf("buildInput (narrow): %v", err)
	}
	full, narrow, _ := narrowState.readCounts()
	if full != 0 {
		t.Fatalf("buildInput called GetExecution %d times on a backend with the narrow capability, want 0", full)
	}
	if narrow != 1 {
		t.Fatalf("buildInput called GetExecutionInputContext %d times, want exactly 1", narrow)
	}

	plainState := newFakeState()
	seed(t, plainState)
	fallbackInput, err := New(plainState, &fakeQueue{}).buildInput(ctx, task, g)
	if err != nil {
		t.Fatalf("buildInput (snapshot fallback): %v", err)
	}
	if !reflect.DeepEqual(narrowInput, fallbackInput) {
		t.Fatalf("narrow input differs from the snapshot-derived input:\nnarrow:  %#v\nsnapshot: %#v",
			narrowInput, fallbackInput)
	}
	return narrowInput
}

// TestBuildInput_NarrowReadMatchesSnapshot pins the four buildInput shapes the
// narrow read must serve unchanged: a root node, a fan-in, a $nodes prefetch,
// and a resume. Each case compares the narrow result with the snapshot-derived
// one field by field through reflect.DeepEqual, so a dropped Scope, Runtime or
// trace identity fails here rather than surfacing as a node evaluating against
// different data depending on its backend.
func TestBuildInput_NarrowReadMatchesSnapshot(t *testing.T) {
	t.Run("Root", func(t *testing.T) {
		input := runInputBranchCase(t, inputBranchCase{
			name: "root",
			def: &types.WorkflowDef{
				Name:  "input-context-root",
				Nodes: []types.NodeDef{{Name: "A", Type: "test.noop"}},
			},
			node: "A",
			snapshot: ExecutionSnapshot{
				Params:  map[string]any{"claim_id": "c-1", "retry_budget": float64(2)},
				Runtime: &types.Runtime{Vars: map[string]any{"tenant": "acme"}},
				Scope:   map[string]any{"$item": map[string]any{"sku": "s-1"}},
				TraceID: "trace-root",
				SpanID:  "span-root",
			},
		})
		if input.Data["claim_id"] != "c-1" {
			t.Errorf("input.Data[claim_id] = %v, want c-1 (root nodes take submission params)", input.Data["claim_id"])
		}
		if input.Data["$item"] == nil {
			t.Error("input.Data is missing the execution-scope root $item")
		}
		if input.Runtime == nil || input.Runtime.Vars["tenant"] != "acme" {
			t.Errorf("input.Runtime = %#v, want the execution runtime vars", input.Runtime)
		}
		if input.Vars["tenant"] != "acme" {
			t.Errorf("input.Vars[tenant] = %v, want acme from the runtime vars", input.Vars["tenant"])
		}
		if input.TraceID != "trace-root" || input.SpanID != "span-root" {
			t.Errorf("input trace identity = (%q, %q), want (trace-root, span-root)", input.TraceID, input.SpanID)
		}
	})

	t.Run("FanIn", func(t *testing.T) {
		input := runInputBranchCase(t, inputBranchCase{
			name: "fan-in",
			def: &types.WorkflowDef{
				Name: "input-context-fan-in",
				Nodes: []types.NodeDef{
					{Name: "A", Type: "test.noop"},
					{Name: "B", Type: "test.noop"},
					{Name: "M", Type: "test.noop"},
				},
				Connections: types.Connections{
					"A": {"main": {Targets: []types.Connection{{Node: "M", Input: "inventory"}}}},
					"B": {"main": {Targets: []types.Connection{{Node: "M", Input: "price"}}}},
				},
			},
			node: "M",
			snapshot: ExecutionSnapshot{
				Scope: map[string]any{"$index": float64(3)},
			},
			outputs: map[string]map[string]any{
				"A": {"from": "a"},
				"B": {"from": "b"},
			},
		})
		if got := input.Inputs["inventory"].(map[string]any)["from"]; got != "a" {
			t.Errorf("$inputs.inventory.from = %v, want a", got)
		}
		if got := input.Inputs["price"].(map[string]any)["from"]; got != "b" {
			t.Errorf("$inputs.price.from = %v, want b", got)
		}
		if input.Data["$index"] != float64(3) {
			t.Errorf("input.Data[$index] = %v, want the execution-scope root", input.Data["$index"])
		}
	})

	t.Run("NodesRef", func(t *testing.T) {
		input := runInputBranchCase(t, inputBranchCase{
			name: "nodes-ref",
			def: &types.WorkflowDef{
				Name: "input-context-nodes",
				Nodes: []types.NodeDef{
					{Name: "A", Type: "test.noop"},
					{Name: "B", Type: "test.noop", Parameters: map[string]any{
						"value": "${{ $nodes['A'].value }}",
					}},
				},
			},
			node: "B",
			outputs: map[string]map[string]any{
				"A": {"value": float64(42)},
			},
		})
		a, ok := input.Nodes["A"].(map[string]any)
		if !ok || a["value"] != float64(42) {
			t.Fatalf("input.Nodes[A] = %#v, want value=42", input.Nodes["A"])
		}
	})

	t.Run("Resume", func(t *testing.T) {
		input := runInputBranchCase(t, inputBranchCase{
			name:     "resume",
			taskType: TaskTypeNodeResume,
			def: &types.WorkflowDef{
				Name: "input-context-resume",
				Nodes: []types.NodeDef{
					{Name: "A", Type: "test.noop"},
					{Name: "C", Type: "test.noop", Parameters: map[string]any{
						"a": "${{ $nodes['A'].from }}",
					}},
				},
			},
			node: "C",
			outputs: map[string]map[string]any{
				"A": {"from": "a"},
				"C": {"count": float64(1), NodeStateKey: map[string]any{"cursor": "c1"}},
			},
		})
		if input.State["cursor"] != "c1" {
			t.Errorf("input.State = %#v, want the resumed node's own state (cursor=c1)", input.State)
		}
		if input.Data["count"] != float64(1) {
			t.Errorf("input.Data = %#v, want the resumed node's own output (count=1)", input.Data)
		}
		if _, leaked := input.Data[NodeStateKey]; leaked {
			t.Error("input.Data still carries the engine's node-state slot")
		}
		a, ok := input.Nodes["A"].(map[string]any)
		if !ok || a["from"] != "a" {
			t.Errorf("input.Nodes[A] = %#v, want from=a", input.Nodes["A"])
		}
		if input.TraceID != "" || input.SpanID != "" {
			t.Errorf("input trace identity = (%q, %q), want empty for a snapshot that carried none", input.TraceID, input.SpanID)
		}
	})
}

// TestBuildInput_NarrowReadFailureModes pins that the narrow read keeps the
// full snapshot's semantics: absence and terminal status are ErrExecutionInactive,
// and a store error is surfaced rather than silently retried through GetExecution
// (which would both hide the failure and pay for the very read this capability
// removes).
func TestBuildInput_NarrowReadFailureModes(t *testing.T) {
	ctx := context.Background()
	def := &types.WorkflowDef{
		Name:  "input-context-failures",
		Nodes: []types.NodeDef{{Name: "A", Type: "test.noop"}},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	aIdx, _ := g.NodeIndex("A")
	task := &Task{ExecutionID: "exec-input-context-failure", NodeName: "A", NodeIdx: aIdx, Type: TaskTypeNodeExec}

	t.Run("AbsentExecution", func(t *testing.T) {
		state := newRecordingExecutionState()
		_, err := New(state, &fakeQueue{}).buildInput(ctx, task, g)
		if !errors.Is(err, ErrExecutionInactive) {
			t.Fatalf("buildInput(absent) error = %v, want ErrExecutionInactive", err)
		}
		if full, narrow, _ := state.readCounts(); full != 0 || narrow != 1 {
			t.Fatalf("absent read used GetExecution %d times and the narrow read %d times, want 0 and 1", full, narrow)
		}
	})

	t.Run("TerminalExecution", func(t *testing.T) {
		state := newRecordingExecutionState()
		if err := state.CreateExecution(ctx, &ExecutionSnapshot{
			ID: task.ExecutionID, Graph: g, Status: types.ExecutionStatusSuccess,
		}); err != nil {
			t.Fatalf("CreateExecution: %v", err)
		}
		_, err := New(state, &fakeQueue{}).buildInput(ctx, task, g)
		if !errors.Is(err, ErrExecutionInactive) {
			t.Fatalf("buildInput(terminal) error = %v, want ErrExecutionInactive", err)
		}
	})

	t.Run("StoreErrorIsAuthoritative", func(t *testing.T) {
		state := newRecordingExecutionState()
		boom := errors.New("state unavailable")
		state.setNarrowError(boom)
		_, err := New(state, &fakeQueue{}).buildInput(ctx, task, g)
		if !errors.Is(err, boom) {
			t.Fatalf("buildInput(store error) = %v, want the store error to surface", err)
		}
		if !strings.Contains(err.Error(), "get execution") {
			t.Errorf("buildInput(store error) = %q, want the operation context in the message", err)
		}
		if full, narrow, _ := state.readCounts(); full != 0 || narrow != 1 {
			t.Fatalf("erroring read used GetExecution %d times and the narrow read %d times, want 0 and 1 "+
				"(no fallback on failure: it would hide the failure and pay for the full read)",
				full, narrow)
		}
	})
}
