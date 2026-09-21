package engine

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

func TestTransientOutputReclaimsForCommit_GroupBoundaryExitToSingleOrdinarySink(t *testing.T) {
	g := compileTransientOutputReclaimGraph(t, transientOutputReclaimDefinition(true))

	assertTransientOutputReclaimsForCommit(t, g, "sink", []string{"exit"})
}

func TestTransientOutputReclaimsForCommit_DurableGraphReturnsNil(t *testing.T) {
	g := compileTransientOutputReclaimGraph(t, transientOutputReclaimDefinition(false))

	assertTransientOutputReclaimsForCommit(t, g, "sink", nil)
}

func TestTransientOutputReclaimsForCommit_FanOutReturnsNil(t *testing.T) {
	def := transientOutputReclaimDefinition(true)
	def.Nodes = append(def.Nodes, types.NodeDef{Name: "other_sink", Type: "test.noop"})
	def.Connections["exit"]["main"] = types.PortConnections{Targets: []types.Connection{
		{Node: "sink", Input: "main"},
		{Node: "other_sink", Input: "main"},
	}}
	g := compileTransientOutputReclaimGraph(t, def)

	assertTransientOutputReclaimsForCommit(t, g, "sink", nil)
}

func TestTransientOutputReclaimsForCommit_GroupConsumerReturnsNil(t *testing.T) {
	def := transientOutputReclaimDefinition(true)
	def.Groups = append(def.Groups, types.GroupDef{Name: "sink_group", Members: []string{"sink"}})
	g := compileTransientOutputReclaimGraph(t, def)

	assertTransientOutputReclaimsForCommit(t, g, "sink", nil)
}

func TestTransientOutputReclaimsForCommit_ExternalStaticNodesObserverReturnsNil(t *testing.T) {
	def := transientOutputReclaimDefinition(true)
	def.Nodes = append(def.Nodes, types.NodeDef{
		Name: "observer",
		Type: "test.noop",
		Parameters: map[string]any{
			"value": "${{ $nodes['exit'].result }}",
		},
	})
	g := compileTransientOutputReclaimGraph(t, def)

	assertTransientOutputReclaimsForCommit(t, g, "sink", nil)
}

func TestTransientOutputReclaimsForCommit_ExternalDynamicNodesObserverReturnsNil(t *testing.T) {
	def := transientOutputReclaimDefinition(true)
	def.Context = &types.WorkflowContext{Vars: map[string]any{"which": "exit"}}
	def.Nodes = append(def.Nodes, types.NodeDef{
		Name: "observer",
		Type: "test.noop",
		Parameters: map[string]any{
			"value": "${{ $nodes[$vars.which].result }}",
		},
	})
	g := compileTransientOutputReclaimGraph(t, def)

	// A computed name is not represented by NodesRefsFor, but it can still name
	// the group exit at runtime. Reclamation must remain conservative until the
	// planner has ruled out dynamic $nodes observers as well.
	assertTransientOutputReclaimsForCommit(t, g, "sink", nil)
}

func TestTransientOutputReclaimsForCommit_ExternalMapBodyStaticNodesObserverReturnsNil(t *testing.T) {
	g := compileTransientOutputReclaimGraph(t, transientOutputReclaimMapBodyReaderDefinition("${{ $nodes['exit'].result }}"))
	mapIdx, ok := g.NodeIndex("map")
	if !ok {
		t.Fatal("compiled graph has no map node")
	}
	if g.NodeAt(mapIdx).GroupIdx >= 0 {
		t.Fatal("map body observer must remain outside the producing group")
	}
	if refs := g.BodyOuterRefsFor(mapIdx); len(refs) != 1 || refs[0].Member != "body_reader" || refs[0].Node != "exit" {
		t.Fatalf("BodyOuterRefsFor(map) = %#v, want [{body_reader exit}]", refs)
	}

	assertTransientOutputReclaimsForCommit(t, g, "sink", nil)
}

func TestTransientOutputReclaimsForCommit_ExternalMapBodyDynamicNodesObserverReturnsNil(t *testing.T) {
	def := transientOutputReclaimMapBodyReaderDefinition("${{ $nodes[$vars.which].result }}")
	def.Context = &types.WorkflowContext{Vars: map[string]any{"which": "exit"}}
	g := compileTransientOutputReclaimGraph(t, def)
	mapIdx, ok := g.NodeIndex("map")
	if !ok {
		t.Fatal("compiled graph has no map node")
	}
	if g.NodeAt(mapIdx).GroupIdx >= 0 || g.BodyAt(mapIdx) == nil {
		t.Fatal("dynamic map body observer must be an ungrouped projected body")
	}

	assertTransientOutputReclaimsForCommit(t, g, "sink", nil)
}

func TestTransientOutputReclaimsForCommit_CyclicGraphReturnsNil(t *testing.T) {
	g := compileTransientOutputReclaimGraph(t, &types.WorkflowDef{
		Name:    "transient-output-reclaim-cyclic",
		Options: &types.WorkflowOptions{AllowCycles: true, Transient: true},
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "sink", Type: "test.noop"},
		},
		Connections: types.Connections{
			"start": {"main": {Targets: []types.Connection{{Node: "sink", Input: "main"}}}},
			"sink":  {"main": {Targets: []types.Connection{{Node: "start", Input: "main"}}}},
		},
	})

	assertTransientOutputReclaimsForCommit(t, g, "sink", nil)
}

func TestTransientOutputReclaimCommitRequest_RequiresNormalHandlerSuccess(t *testing.T) {
	cases := []struct {
		name      string
		configure func(*types.WorkflowDef)
		result    TaskResult
		attempt   int
		want      []string
		status    types.NodeStatus
		port      string
	}{
		{
			name:    "normal handler success",
			result:  TaskResult{Output: &types.Output{Data: map[string]any{"ok": true}}},
			attempt: 1,
			want:    []string{"exit"},
			status:  types.NodeStatusSuccess,
			port:    "main",
		},
		{
			name: "error output policy",
			configure: func(def *types.WorkflowDef) {
				def.Nodes[2].OnError = string(types.OnErrorOutput)
			},
			result:  TaskResult{Error: errors.New("handler failed")},
			attempt: 1,
			status:  types.NodeStatusSuccess,
			port:    "error",
		},
		{
			name: "main output policy",
			configure: func(def *types.WorkflowDef) {
				def.Nodes[2].OnError = string(types.OnErrorMainOutput)
			},
			result:  TaskResult{Error: errors.New("handler failed")},
			attempt: 1,
			status:  types.NodeStatusSuccess,
			port:    "main",
		},
		{
			name: "continue policy",
			configure: func(def *types.WorkflowDef) {
				def.Nodes[2].OnError = string(types.OnErrorContinue)
			},
			result:  TaskResult{Error: errors.New("handler failed")},
			attempt: 1,
			status:  types.NodeStatusContinued,
			port:    "main",
		},
		{
			name: "exhausted error-port retry",
			configure: func(def *types.WorkflowDef) {
				def.Nodes[2].OnError = string(types.OnErrorOutput)
				def.Nodes[2].Retry = &types.RetrySettings{MaxAttempts: 1}
			},
			result: TaskResult{Output: &types.Output{
				Port: "error",
				Data: map[string]any{"error": "retry budget exhausted"},
			}},
			attempt: 1,
			status:  types.NodeStatusSuccess,
			port:    "error",
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			def := transientOutputReclaimDefinition(true)
			if tt.configure != nil {
				tt.configure(def)
			}
			g := compileTransientOutputReclaimGraph(t, def)
			sinkIdx, ok := g.NodeIndex("sink")
			if !ok {
				t.Fatal("compiled graph has no sink node")
			}

			state := &transientOutputReclaimCommitRecordingState{fakeState: newFakeState()}
			eng := New(state, &fakeQueue{})
			lease := &TaskLease{
				LeaseID:    LeaseID("lease-" + tt.name),
				LeaseToken: LeaseToken("token-" + tt.name),
				Attempt:    tt.attempt,
				Task: Task{
					ExecutionID:  types.ExecutionID("transient-output-reclaim-" + tt.name),
					NodeName:     "sink",
					NodeIdx:      sinkIdx,
					UnitIdx:      g.UnitIndexForNode(sinkIdx),
					Type:         TaskTypeNodeExec,
					ActivationID: 1,
				},
			}

			outcome, err := eng.commitAcyclicTaskResult(context.Background(), lease, g, tt.result)
			if err != nil {
				t.Fatalf("commitAcyclicTaskResult: %v", err)
			}
			if outcome != CommitOutcomeAccepted {
				t.Fatalf("outcome = %q, want %q", outcome, CommitOutcomeAccepted)
			}
			if len(state.requests) != 1 {
				t.Fatalf("CommitNode calls = %d, want 1", len(state.requests))
			}
			request := state.requests[0]
			if request.Status != tt.status {
				t.Fatalf("CommitNode status = %q, want %q", request.Status, tt.status)
			}
			if request.Port != tt.port {
				t.Fatalf("CommitNode port = %q, want %q", request.Port, tt.port)
			}
			if !reflect.DeepEqual(request.ReclaimOutputNames, tt.want) {
				t.Fatalf("CommitNode ReclaimOutputNames = %#v, want %#v", request.ReclaimOutputNames, tt.want)
			}
		})
	}
}

func TestTransientOutputReclaimCommitRequest_DoesNotReclaimServerTimeout(t *testing.T) {
	ctx := context.Background()
	def := transientOutputReclaimDefinition(true)
	def.Nodes[2].OnError = string(types.OnErrorOutput)
	g := compileTransientOutputReclaimGraph(t, def)
	sinkIdx, ok := g.NodeIndex("sink")
	if !ok {
		t.Fatal("compiled graph has no sink node")
	}

	state := &transientOutputReclaimCommitRecordingState{fakeState: newFakeState()}
	eng := New(state, &fakeQueue{})
	id := types.ExecutionID("transient-output-reclaim-timeout")
	if err := state.CreateExecution(ctx, &ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	lease := &TaskLease{
		LeaseID: "timeout-lease", LeaseToken: "timeout-token", IssuedAt: time.Now().UTC(), TTL: time.Minute,
		Task: Task{
			ExecutionID: id, NodeName: "sink", NodeIdx: sinkIdx,
			UnitIdx: g.UnitIndexForNode(sinkIdx), Type: TaskTypeNodeExec, ActivationID: 1,
		},
	}
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease() acquired=%v err=%v, want true/nil", acquired, err)
	}
	lease.Attempt = 1

	if err := eng.CommitTaskTimeout(ctx, lease, nil); err != nil {
		t.Fatalf("CommitTaskTimeout() error = %v", err)
	}
	if len(state.requests) != 1 {
		t.Fatalf("CommitNode calls = %d, want 1", len(state.requests))
	}
	request := state.requests[0]
	if request.Status != types.NodeStatusSuccess || request.Port != "error" {
		t.Fatalf("timeout commit = status %q port %q, want success/error", request.Status, request.Port)
	}
	if request.ReclaimOutputNames != nil {
		t.Fatalf("timeout commit reclaimed outputs %#v, want none", request.ReclaimOutputNames)
	}
}

// transientOutputReclaimCommitRecordingState captures the request at the
// AtomicStateStore boundary while retaining fakeState's outbox methods for
// post-commit processing.
type transientOutputReclaimCommitRecordingState struct {
	*fakeState
	requests []CommitNodeRequest
}

func (s *transientOutputReclaimCommitRecordingState) CommitNode(_ context.Context, req CommitNodeRequest) (CommitNodeResult, error) {
	captured := req
	if req.ReclaimOutputNames != nil {
		captured.ReclaimOutputNames = make([]string, len(req.ReclaimOutputNames))
		copy(captured.ReclaimOutputNames, req.ReclaimOutputNames)
	}
	s.requests = append(s.requests, captured)
	return CommitNodeResult{Outcome: CommitOutcomeAccepted, Applied: true}, nil
}

func transientOutputReclaimMapBodyReaderDefinition(bodyReference string) *types.WorkflowDef {
	def := transientOutputReclaimDefinition(true)
	def.Nodes = append(def.Nodes, types.NodeDef{
		Name: "map",
		Type: "xflow.map",
		Parameters: map[string]any{
			"items": "$input.items",
			"body": map[string]any{
				"type": "xflow.subgraph",
				"parameters": map[string]any{
					"nodes": []any{
						map[string]any{
							"name": "body_reader",
							"type": "test.noop",
							"parameters": map[string]any{
								"value": bodyReference,
							},
						},
					},
				},
			},
		},
	})
	def.Connections["sink"] = map[string]types.PortConnections{
		"main": {Targets: []types.Connection{{Node: "map", Input: "main"}}},
	}
	return def
}

func transientOutputReclaimDefinition(transient bool) *types.WorkflowDef {
	return &types.WorkflowDef{
		Name:    "transient-output-reclaim",
		Options: &types.WorkflowOptions{Transient: transient},
		Nodes: []types.NodeDef{
			{Name: "collect", Type: "test.noop"},
			{Name: "exit", Type: "test.noop"},
			{Name: "sink", Type: "test.noop"},
		},
		Connections: types.Connections{
			"collect": {"main": {Targets: []types.Connection{{Node: "exit", Input: "main"}}}},
			"exit":    {"main": {Targets: []types.Connection{{Node: "sink", Input: "main"}}}},
		},
		Groups: []types.GroupDef{{Name: "collect_group", Members: []string{"collect", "exit"}}},
	}
}

func compileTransientOutputReclaimGraph(t *testing.T, def *types.WorkflowDef) *graph.Graph {
	t.Helper()

	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile graph: %v", err)
	}
	return g
}

func assertTransientOutputReclaimsForCommit(t *testing.T, g *graph.Graph, consumer string, want []string) {
	t.Helper()

	consumerIdx, ok := g.NodeIndex(consumer)
	if !ok {
		t.Fatalf("compiled graph has no %q node", consumer)
	}
	if got := transientOutputReclaimsForCommit(g, consumerIdx); !reflect.DeepEqual(got, want) {
		t.Fatalf("transientOutputReclaimsForCommit(%q) = %#v, want %#v", consumer, got, want)
	}
}
