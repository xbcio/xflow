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

func TestEngineLegacyResultPropagatesStateErrors(t *testing.T) {
	outputErr := errors.New("put output failed")
	leaseErr := errors.New("get node failed")
	nodeErr := errors.New("upsert node failed")

	tests := []struct {
		name   string
		result TaskResult
		want   error
		fault  func(*stateFaults)
	}{
		{
			name:   "success output persistence",
			result: TaskResult{Output: &types.Output{Data: map[string]any{"ok": true}}},
			want:   outputErr,
			fault: func(state *stateFaults) {
				state.legacyCommitErr = outputErr
			},
		},
		{
			name:   "success lease read",
			result: TaskResult{Output: &types.Output{Data: map[string]any{"ok": true}}},
			want:   leaseErr,
			fault: func(state *stateFaults) {
				state.legacyCommitErr = leaseErr
			},
		},
		{
			name:   "error outcome node transition",
			result: TaskResult{Error: errors.New("handler failed")},
			want:   nodeErr,
			fault: func(state *stateFaults) {
				state.legacyCommitErr = nodeErr
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := newStateFaults()
			queue := newQueueFaults()
			hooks := &completionHookRecorder{}
			eng, lease, _ := legacyResultLease(t, state, queue, hooks, nil)
			tt.fault(state)

			err := eng.CommitTaskResult(context.Background(), lease, tt.result)
			if !errors.Is(err, tt.want) {
				t.Fatalf("CommitTaskResult() error = %v, want wrapped %v", err, tt.want)
			}
			if hooks.nodeCompletions != 0 {
				t.Fatalf("node completion hooks = %d, want 0 after failed state write", hooks.nodeCompletions)
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
	t.Run("forced failure does not publish execution completion when status write fails", func(t *testing.T) {
		statusErr := errors.New("execution status write failed")
		state := newStateFaults()
		queue := newQueueFaults()
		hooks := &completionHookRecorder{}
		eng, lease, _ := legacyResultLease(t, state, queue, hooks, nil)
		state.updateStatusErr = map[types.ExecutionStatus]error{
			types.ExecutionStatusFailed: statusErr,
		}

		err := eng.CommitTaskFailure(context.Background(), lease, errors.New("runtime incompatible"))
		if !errors.Is(err, statusErr) {
			t.Fatalf("CommitTaskFailure() error = %v, want wrapped %v", err, statusErr)
		}
		if hooks.nodeCompletions != 1 {
			t.Fatalf("node completion hooks = %d, want 1 after node state write", hooks.nodeCompletions)
		}
		if hooks.executionCompletions != 0 {
			t.Fatalf("execution completion hooks = %d, want 0", hooks.executionCompletions)
		}
		if _, ok := eng.graphs[lease.Task.ExecutionID]; !ok {
			t.Fatal("execution graph was evicted despite failed terminal status write")
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

	t.Run("skip does not publish a completion hook when its node write fails", func(t *testing.T) {
		nodeErr := errors.New("skip node write failed")
		state := newStateFaults()
		queue := newQueueFaults()
		hooks := &completionHookRecorder{}
		eng := New(state, queue, WithHooks(hooks))
		g := singleNodeGraph(t)
		state.upsertNodeErr = nodeErr

		err := eng.skipCascade(context.Background(), "exec-skip-write-error", g, 0)
		if !errors.Is(err, nodeErr) {
			t.Fatalf("skipCascade() error = %v, want wrapped %v", err, nodeErr)
		}
		if hooks.nodeCompletions != 0 {
			t.Fatalf("node completion hooks = %d, want 0", hooks.nodeCompletions)
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
