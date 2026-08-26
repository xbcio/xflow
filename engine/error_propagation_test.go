package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

type stateFaults struct {
	*fakeState

	putOutputErr          error
	upsertNodeErr         error
	getNodeErr            error
	legacyCommitErr       error
	listSuspendedNodesErr error
	updateStatusErr       map[types.ExecutionStatus]error
}

func newStateFaults() *stateFaults {
	return &stateFaults{fakeState: newFakeState()}
}

func (s *stateFaults) CommitLeasedNode(ctx context.Context, req CommitNodeRequest) (CommitNodeResult, error) {
	if s.legacyCommitErr != nil {
		return CommitNodeResult{}, s.legacyCommitErr
	}
	return s.fakeState.CommitLeasedNode(ctx, req)
}

func (s *stateFaults) PutOutput(ctx context.Context, id types.ExecutionID, name string, data map[string]any) error {
	if s.putOutputErr != nil {
		return s.putOutputErr
	}
	return s.fakeState.PutOutput(ctx, id, name, data)
}

func (s *stateFaults) UpsertNode(ctx context.Context, node *NodeSnapshot) error {
	if s.upsertNodeErr != nil {
		return s.upsertNodeErr
	}
	return s.fakeState.UpsertNode(ctx, node)
}

func (s *stateFaults) GetNode(ctx context.Context, id types.ExecutionID, name string) (*NodeSnapshot, error) {
	if s.getNodeErr != nil {
		return nil, s.getNodeErr
	}
	return s.fakeState.GetNode(ctx, id, name)
}

func (s *stateFaults) ListSuspendedNodes(ctx context.Context, id types.ExecutionID) ([]string, error) {
	if s.listSuspendedNodesErr != nil {
		return nil, s.listSuspendedNodesErr
	}
	return s.fakeState.ListSuspendedNodes(ctx, id)
}

func (s *stateFaults) UpdateExecutionStatus(ctx context.Context, id types.ExecutionID, status types.ExecutionStatus, errMsg string) error {
	if err := s.updateStatusErr[status]; err != nil {
		return err
	}
	return s.fakeState.UpdateExecutionStatus(ctx, id, status, errMsg)
}

type queueFaults struct {
	*fakeQueue

	enqueueErr        error
	enqueueDelayedErr error
}

func newQueueFaults() *queueFaults {
	return &queueFaults{fakeQueue: &fakeQueue{}}
}

func (q *queueFaults) Enqueue(ctx context.Context, task *Task) error {
	if q.enqueueErr != nil {
		return q.enqueueErr
	}
	return q.fakeQueue.Enqueue(ctx, task)
}

func (q *queueFaults) EnqueueDelayed(ctx context.Context, task *Task, delay time.Duration) error {
	if q.enqueueDelayedErr != nil {
		return q.enqueueDelayedErr
	}
	return q.fakeQueue.EnqueueDelayed(ctx, task, delay)
}

type completionHookRecorder struct {
	BaseHooks

	nodeCompletions      int
	executionCompletions int
}

func (h *completionHookRecorder) OnNodeComplete(context.Context, types.ExecutionID, string, types.NodeStatus) {
	h.nodeCompletions++
}

func (h *completionHookRecorder) OnExecutionComplete(context.Context, types.ExecutionID, types.ExecutionStatus) {
	h.executionCompletions++
}

func legacyResultLease(t *testing.T, state *stateFaults, queue *queueFaults, hooks Hooks, retry *types.RetrySettings) (*Engine, *TaskLease, *graph.Graph) {
	t.Helper()

	def := &types.WorkflowDef{
		Name:     "legacy-state-errors",
		Options:  &types.WorkflowOptions{AllowCycles: true},
		Settings: &types.WorkflowSettings{Retry: retry},
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	eng := New(state, queue, WithHooks(hooks))
	if _, err := eng.Submit(context.Background(), g, nil); err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	tasks := queue.Drain()
	if len(tasks) != 1 {
		t.Fatalf("queued tasks = %d, want 1", len(tasks))
	}
	lease, err := eng.BuildTaskLease(context.Background(), tasks[0])
	if err != nil {
		t.Fatalf("BuildTaskLease() error = %v", err)
	}
	return eng, lease, g
}

func singleNodeGraph(t *testing.T) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "state-error-single-node",
		Nodes: []types.NodeDef{
			{Name: "node", Type: "test.echo"},
		},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	return g
}

// TestEngineLegacyResultPropagatesStateErrors pins that a failed state write on
// the legacy commit path surfaces the error AND withholds the completion hook.
// A hook fired for a commit that did not land tells a downstream consumer the
// node reached a terminal status the store never recorded.
//
// This used to be three rows named for three fault-injection points -- "success
// output persistence", "success lease read", "error outcome node transition" --
// and all three set state.legacyCommitErr. The first two were the same case
// twice with a different error value. There is no third point to inject at: the
// legacy path reaches the store through CommitLeasedNode as one composite call,
// which is why putOutputErr and getNodeErr are declared on stateFaults and
// wired into overrides but never assigned anywhere in this file.
//
// The no-fault rows are what give "want 0 hooks" its meaning. Without them,
// every hook assertion in this file -- this one and the two below -- is green
// for an OnNodeComplete that is never called at all.
func TestEngineLegacyResultPropagatesStateErrors(t *testing.T) {
	commitErr := errors.New("commit leased node failed")

	tests := []struct {
		name            string
		result          TaskResult
		fault           error
		wantCompletions int
		wantStatus      types.NodeStatus
	}{
		{
			name:            "success outcome, commit fails",
			result:          TaskResult{Output: &types.Output{Data: map[string]any{"ok": true}}},
			fault:           commitErr,
			wantCompletions: 0,
		},
		{
			name:            "error outcome, commit fails",
			result:          TaskResult{Error: errors.New("handler failed")},
			fault:           commitErr,
			wantCompletions: 0,
		},
		{
			name:            "success outcome, no fault",
			result:          TaskResult{Output: &types.Output{Data: map[string]any{"ok": true}}},
			wantCompletions: 1,
			wantStatus:      types.NodeStatusSuccess,
		},
		{
			name:            "error outcome, no fault",
			result:          TaskResult{Error: errors.New("handler failed")},
			wantCompletions: 1,
			wantStatus:      types.NodeStatusFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			state := newStateFaults()
			queue := newQueueFaults()
			hooks := &completionHookRecorder{}
			eng, lease, _ := legacyResultLease(t, state, queue, hooks, nil)
			state.legacyCommitErr = tt.fault

			err := eng.CommitTaskResult(ctx, lease, tt.result)
			if tt.fault != nil {
				if !errors.Is(err, tt.fault) {
					t.Fatalf("CommitTaskResult() error = %v, want wrapped %v", err, tt.fault)
				}
			} else if err != nil {
				t.Fatalf("CommitTaskResult() error = %v, want nil", err)
			}
			if hooks.nodeCompletions != tt.wantCompletions {
				t.Fatalf("node completion hooks = %d, want %d",
					hooks.nodeCompletions, tt.wantCompletions)
			}
			if tt.wantStatus == "" {
				return
			}
			// The hook count only means "the commit landed" if the store agrees
			// it landed, and with the status the hook was reporting.
			node, err := state.GetNode(ctx, lease.Task.ExecutionID, "start")
			if err != nil || node == nil {
				t.Fatalf("GetNode() = %v, %v, want the committed node", node, err)
			}
			if node.Status != tt.wantStatus {
				t.Fatalf("committed node status = %s, want %s", node.Status, tt.wantStatus)
			}
		})
	}
}

func TestEngineLegacyRetryQueueFailureDoesNotFallThrough(t *testing.T) {
	queueErr := errors.New("enqueue retry failed")
	state := newStateFaults()
	queue := newQueueFaults()
	hooks := &completionHookRecorder{}
	eng, lease, _ := legacyResultLease(t, state, queue, hooks, &types.RetrySettings{
		MaxAttempts:     2,
		InitialInterval: 1,
	})

	if err := eng.CommitTaskResult(context.Background(), lease, TaskResult{Error: errors.New("temporary handler failure")}); err != nil {
		t.Fatalf("CommitTaskResult() error = %v, want accepted durable retry", err)
	}
	entries := listAtomicOutbox(t, state.fakeState, lease.Task.ExecutionID, time.Now().Add(time.Hour))
	if len(entries) != 1 {
		t.Fatalf("retry outbox entries = %+v, want one", entries)
	}

	makeFakeOutboxReady(state.fakeState, lease.Task.ExecutionID)
	queue.enqueueErr = queueErr
	err := eng.FlushOutbox(context.Background(), lease.Task.ExecutionID)
	if !errors.Is(err, queueErr) {
		t.Fatalf("FlushOutbox() error = %v, want wrapped %v", err, queueErr)
	}
	node, err := state.GetNode(context.Background(), lease.Task.ExecutionID, lease.Task.NodeName)
	if err != nil {
		t.Fatalf("GetNode() error = %v", err)
	}
	if node == nil || node.Status != types.NodeStatusPending {
		t.Fatalf("node after retry enqueue failure = %+v, want pending", node)
	}
	if entries := listAtomicOutbox(t, state.fakeState, lease.Task.ExecutionID, time.Now().Add(time.Second)); len(entries) != 1 {
		t.Fatalf("retry outbox was lost after queue failure: %+v", entries)
	}
	if hooks.nodeCompletions != 0 {
		t.Fatalf("node completion hooks = %d, want 0 after retry enqueue failure", hooks.nodeCompletions)
	}
}

func TestEngineTerminalHooksFollowPersistentState(t *testing.T) {
	t.Run("forced failure fenced-commit error does not publish completion and retains graph", func(t *testing.T) {
		// H1: a fatal cyclic node's terminal transition and the execution
		// finalization are one fenced commit (CyclicComplete). There is no longer
		// a separate execution-status write that can fail independently of the
		// node write — the whole fenced commit either applies or it does not. A
		// failed fenced commit therefore publishes NO node/execution completion
		// and retains the graph so recovery can retry.
		commitErr := errors.New("execution status write failed")
		state := newStateFaults()
		queue := newQueueFaults()
		hooks := &completionHookRecorder{}
		eng, lease, _ := legacyResultLease(t, state, queue, hooks, nil)
		state.legacyCommitErr = commitErr

		err := eng.CommitTaskFailure(context.Background(), lease, errors.New("runtime incompatible"))
		if !errors.Is(err, commitErr) {
			t.Fatalf("CommitTaskFailure() error = %v, want wrapped %v", err, commitErr)
		}
		if hooks.nodeCompletions != 0 {
			t.Fatalf("node completion hooks = %d, want 0 after failed fenced commit (atomic finalization)", hooks.nodeCompletions)
		}
		if hooks.executionCompletions != 0 {
			t.Fatalf("execution completion hooks = %d, want 0", hooks.executionCompletions)
		}
		if _, ok := eng.graphs[lease.Task.ExecutionID]; !ok {
			t.Fatal("execution graph was evicted despite failed fenced terminal commit")
		}
	})

	t.Run("scheduler completion retains graph when status write fails", func(t *testing.T) {
		statusErr := errors.New("complete execution write failed")
		state := newStateFaults()
		queue := newQueueFaults()
		hooks := &completionHookRecorder{}
		eng := New(state, queue, WithHooks(hooks))
		g := singleNodeGraph(t)
		id := types.ExecutionID("exec-complete-write-error")
		if err := state.CreateExecution(context.Background(), &ExecutionSnapshot{ID: id, Graph: g, Status: types.ExecutionStatusRunning}); err != nil {
			t.Fatalf("CreateExecution() error = %v", err)
		}
		eng.graphs[id] = g
		state.updateStatusErr = map[types.ExecutionStatus]error{
			types.ExecutionStatusSuccess: statusErr,
		}

		err := eng.completeExecution(context.Background(), id, types.ExecutionStatusSuccess, "")
		if !errors.Is(err, statusErr) {
			t.Fatalf("completeExecution() error = %v, want wrapped %v", err, statusErr)
		}
		if hooks.executionCompletions != 0 {
			t.Fatalf("execution completion hooks = %d, want 0", hooks.executionCompletions)
		}
		if _, ok := eng.graphs[id]; !ok {
			t.Fatal("execution graph was evicted despite failed terminal status write")
		}
	})
}

func TestEngineCancelPropagatesStateErrors(t *testing.T) {
	listErr := errors.New("list suspended nodes failed")
	nodeErr := errors.New("cancel node write failed")
	statusErr := errors.New("cancel status write failed")

	tests := []struct {
		name  string
		fault func(*stateFaults, types.ExecutionID)
		want  error
	}{
		{
			name: "list suspended nodes",
			fault: func(state *stateFaults, _ types.ExecutionID) {
				state.listSuspendedNodesErr = listErr
			},
			want: listErr,
		},
		{
			name: "suspended node transition",
			fault: func(state *stateFaults, id types.ExecutionID) {
				state.suspended[string(id)+"/wait"] = &types.SuspendSpec{Mode: types.ModeSignal, Signals: []string{"approval"}}
				state.upsertNodeErr = nodeErr
			},
			want: nodeErr,
		},
		{
			name: "terminal execution transition",
			fault: func(state *stateFaults, _ types.ExecutionID) {
				state.updateStatusErr = map[types.ExecutionStatus]error{
					types.ExecutionStatusCanceled: statusErr,
				}
			},
			want: statusErr,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := newStateFaults()
			queue := newQueueFaults()
			hooks := &completionHookRecorder{}
			eng := New(state, queue, WithHooks(hooks))
			g, err := graph.Compile(&types.WorkflowDef{
				Name: "cancel-state-errors",
				Nodes: []types.NodeDef{
					{Name: "wait", Type: "test.echo"},
				},
			})
			if err != nil {
				t.Fatalf("Compile() error = %v", err)
			}
			id := types.ExecutionID("exec-cancel-state-error")
			if err := state.CreateExecution(context.Background(), &ExecutionSnapshot{ID: id, Graph: g, Status: types.ExecutionStatusRunning}); err != nil {
				t.Fatalf("CreateExecution() error = %v", err)
			}
			eng.graphs[id] = g
			tt.fault(state, id)

			err = eng.Cancel(context.Background(), id)
			if !errors.Is(err, tt.want) {
				t.Fatalf("Cancel() error = %v, want wrapped %v", err, tt.want)
			}
			if hooks.executionCompletions != 0 {
				t.Fatalf("execution completion hooks = %d, want 0", hooks.executionCompletions)
			}
			if _, ok := eng.graphs[id]; !ok {
				t.Fatal("execution graph was evicted despite failed cancellation state write")
			}
		})
	}
}
