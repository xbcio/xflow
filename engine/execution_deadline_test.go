package engine

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// deadlineGraph compiles a one-node workflow whose settings.timeout is the
// given budget (0 leaves it unbounded).
func deadlineGraph(t *testing.T, timeout time.Duration) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:     "execution-deadline",
		Settings: &types.WorkflowSettings{Timezone: "UTC", Timeout: timeout},
		Nodes:    []types.NodeDef{{Name: "start", Type: "xflow.start"}},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return g
}

func TestStampExecutionDeadline(t *testing.T) {
	created := time.Date(2026, 10, 24, 8, 0, 0, 0, time.UTC)
	t.Run("a timeout anchors the deadline on CreatedAt", func(t *testing.T) {
		snap := &ExecutionSnapshot{Graph: deadlineGraph(t, 90*time.Second), CreatedAt: created}
		stampExecutionDeadline(snap)
		if want := created.Add(90 * time.Second); !snap.Deadline.Equal(want) {
			t.Fatalf("Deadline = %s, want CreatedAt + 90s = %s", snap.Deadline, want)
		}
		if !snap.CreatedAt.Equal(created) {
			t.Fatalf("CreatedAt = %s, want the caller's %s kept", snap.CreatedAt, created)
		}
	})
	t.Run("a zero CreatedAt is stamped from the clock the deadline uses", func(t *testing.T) {
		snap := &ExecutionSnapshot{Graph: deadlineGraph(t, time.Minute)}
		before := time.Now()
		stampExecutionDeadline(snap)
		after := time.Now()
		if snap.CreatedAt.Before(before) || snap.CreatedAt.After(after) {
			t.Fatalf("CreatedAt = %s, want within [%s, %s]", snap.CreatedAt, before, after)
		}
		if want := snap.CreatedAt.Add(time.Minute); !snap.Deadline.Equal(want) {
			t.Fatalf("Deadline = %s, want CreatedAt + 1m = %s", snap.Deadline, want)
		}
	})
	t.Run("no timeout leaves the snapshot untouched", func(t *testing.T) {
		snap := &ExecutionSnapshot{Graph: deadlineGraph(t, 0)}
		stampExecutionDeadline(snap)
		if !snap.Deadline.IsZero() {
			t.Fatalf("Deadline = %s, want zero for a graph without settings.timeout", snap.Deadline)
		}
		if !snap.CreatedAt.IsZero() {
			t.Fatalf("CreatedAt = %s, want zero: an unbounded graph must reach the backend unchanged", snap.CreatedAt)
		}
	})
	t.Run("no graph leaves the snapshot untouched", func(t *testing.T) {
		snap := &ExecutionSnapshot{}
		stampExecutionDeadline(snap)
		if !snap.Deadline.IsZero() || !snap.CreatedAt.IsZero() {
			t.Fatalf("snapshot = %+v, want it unchanged without a graph", snap)
		}
	})
}

// submittedSnapshot reads back the snapshot a Submit or Invoke stored.
func submittedSnapshot(t *testing.T, state *fakeState, id types.ExecutionID) *ExecutionSnapshot {
	t.Helper()
	snap, err := state.GetExecution(context.Background(), id)
	if err != nil || snap == nil {
		t.Fatalf("read execution %q: snap=%v err=%v", id, snap, err)
	}
	return snap
}

func TestSubmitStampsExecutionDeadline(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name    string
		timeout time.Duration
	}{
		{"bounded", 2 * time.Hour},
		{"unbounded", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := newFakeState()
			eng := New(state, &fakeQueue{})
			id, err := eng.Submit(ctx, deadlineGraph(t, tc.timeout), nil)
			if err != nil {
				t.Fatalf("Submit: %v", err)
			}
			snap := submittedSnapshot(t, state, id)
			if tc.timeout == 0 {
				if !snap.Deadline.IsZero() {
					t.Fatalf("Deadline = %s, want zero without settings.timeout", snap.Deadline)
				}
				return
			}
			if snap.CreatedAt.IsZero() {
				t.Fatal("CreatedAt is zero: the deadline has no anchor to be read against")
			}
			if want := snap.CreatedAt.Add(tc.timeout); !snap.Deadline.Equal(want) {
				t.Fatalf("Deadline = %s, want CreatedAt + %s = %s", snap.Deadline, tc.timeout, want)
			}
		})
	}
}

// queueingState is a *fakeState whose concurrency admission can park an
// execution: the first limit admissions take a slot, later ones are stored as
// queued with their intents withheld, which is what a queue-mode backend does.
// fakeState itself refuses queue mode, so it cannot reach this path.
type queueingState struct {
	*fakeState
	held int
}

func (q *queueingState) CreateExecutionWithConcurrency(_ context.Context, execution *ExecutionSnapshot, entries []OutboxEntry, admission ConcurrencyAdmission) (ConcurrencyAdmissionResult, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.held >= admission.Limit {
		execution.Status = types.ExecutionStatusQueued
		q.createExecutionLocked(execution)
		return ConcurrencyAdmissionResult{Queued: true, QueuePosition: 1}, nil
	}
	q.held++
	q.createExecutionLocked(execution)
	for _, entry := range entries {
		q.putAtomicOutboxLocked(execution.ID, entry.ID, entry.Task, entry.AvailableAt)
	}
	return ConcurrencyAdmissionResult{}, nil
}

// TestSubmitStampsExecutionDeadlineOnQueuedExecutions pins that the budget
// clock starts at admission: an execution parked behind a full concurrency
// limit already carries its deadline, so the time it waits counts against it.
func TestSubmitStampsExecutionDeadlineOnQueuedExecutions(t *testing.T) {
	ctx := context.Background()
	const timeout = 30 * time.Minute
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "queued-deadline",
		Settings: &types.WorkflowSettings{
			Timezone:    "UTC",
			Timeout:     timeout,
			Concurrency: &types.ConcurrencySettings{Limit: 1, Mode: types.ConcurrencyModeQueue},
		},
		Nodes: []types.NodeDef{{Name: "start", Type: "test.echo"}},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	state := &queueingState{fakeState: newFakeState()}
	eng := New(state, &fakeQueue{})

	if _, err := eng.Submit(ctx, g, nil); err != nil {
		t.Fatalf("Submit (takes the only slot): %v", err)
	}
	queued, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("Submit (queued): %v", err)
	}

	snap := submittedSnapshot(t, state.fakeState, queued)
	if snap.Status != types.ExecutionStatusQueued {
		t.Fatalf("Status = %q, want %q: the second submission must have been parked", snap.Status, types.ExecutionStatusQueued)
	}
	if snap.CreatedAt.IsZero() || snap.Deadline.IsZero() {
		t.Fatalf("queued snapshot has CreatedAt %s, Deadline %s, want both stamped", snap.CreatedAt, snap.Deadline)
	}
	if want := snap.CreatedAt.Add(timeout); !snap.Deadline.Equal(want) {
		t.Fatalf("Deadline = %s, want CreatedAt + %s = %s", snap.Deadline, timeout, want)
	}
}

func TestInvokeStampsExecutionDeadline(t *testing.T) {
	ctx := context.Background()
	state := newFakeState()
	eng := New(state, &fakeQueue{})
	id, err := eng.Invoke(ctx, deadlineGraph(t, 15*time.Minute), "start", nil)
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	snap := submittedSnapshot(t, state, id)
	if want := snap.CreatedAt.Add(15 * time.Minute); snap.CreatedAt.IsZero() || !snap.Deadline.Equal(want) {
		t.Fatalf("Deadline = %s with CreatedAt %s, want CreatedAt + 15m", snap.Deadline, snap.CreatedAt)
	}

	id, err = eng.Invoke(ctx, deadlineGraph(t, 0), "start", nil)
	if err != nil {
		t.Fatalf("Invoke unbounded: %v", err)
	}
	if got := submittedSnapshot(t, state, id).Deadline; !got.IsZero() {
		t.Fatalf("Deadline = %s, want zero without settings.timeout", got)
	}
}

// TestRerunStartsANewExecutionClock pins that a rerun does not inherit the
// parent's exhausted deadline: it is a new execution, so its budget is measured
// from its own admission.
func TestRerunStartsANewExecutionClock(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name    string
		timeout time.Duration
	}{
		{"bounded", 45 * time.Minute},
		{"unbounded", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := rerunGraph(t, &types.WorkflowDef{
				Name:     "rerun-deadline",
				Settings: &types.WorkflowSettings{Timezone: "UTC", Timeout: tc.timeout},
				Nodes: []types.NodeDef{
					{Name: "start", Type: "test.echo"},
					{Name: "tail", Type: "test.echo"},
				},
				Connections: types.Connections{
					"start": {"main": {Targets: []types.Connection{{Node: "tail", Input: "main"}}}},
				},
			})
			state := newFakeState()
			parent := rerunParent(t, state, g, types.ExecutionStatusTimeout, map[string]rerunNode{
				"start": {status: types.NodeStatusSuccess, port: "main", output: map[string]any{"n": 1}},
			})
			// The parent ran out its budget two hours ago.
			parentSnap := submittedSnapshot(t, state, parent)
			parentSnap.CreatedAt = time.Now().UTC().Add(-3 * time.Hour)
			if tc.timeout > 0 {
				parentSnap.Deadline = parentSnap.CreatedAt.Add(tc.timeout)
			}
			seeder := &seedRecorder{fakeState: state}
			eng := New(seeder, &fakeQueue{})

			if _, err := eng.Rerun(ctx, RerunRequest{ExecutionID: parent, FromNode: "start"}); err != nil {
				t.Fatalf("Rerun: %v", err)
			}
			rerun := seeder.lastRequest(t).Execution
			if tc.timeout == 0 {
				if !rerun.Deadline.IsZero() {
					t.Fatalf("rerun Deadline = %s, want zero without settings.timeout", rerun.Deadline)
				}
				return
			}
			if want := rerun.CreatedAt.Add(tc.timeout); rerun.CreatedAt.IsZero() || !rerun.Deadline.Equal(want) {
				t.Fatalf("rerun Deadline = %s with CreatedAt %s, want CreatedAt + %s", rerun.Deadline, rerun.CreatedAt, tc.timeout)
			}
			if !rerun.Deadline.After(time.Now()) {
				t.Fatalf("rerun Deadline = %s is already past: it inherited the parent's clock", rerun.Deadline)
			}
			if rerun.Deadline.Equal(parentSnap.Deadline) {
				t.Fatalf("rerun Deadline equals the parent's %s", parentSnap.Deadline)
			}
		})
	}
}

// TestSubWorkflowChildDeadlineIsStampedIndependently pins that a child is
// stamped from its own graph: the parent's budget is neither copied to it nor
// does a child without a timeout become bounded by the parent's.
func TestSubWorkflowChildDeadlineIsStampedIndependently(t *testing.T) {
	childWith := func(timeout time.Duration) *graph.Graph {
		g, err := graph.Compile(&types.WorkflowDef{
			Name:     "child",
			Version:  "v1",
			Settings: &types.WorkflowSettings{Timezone: "UTC", Timeout: timeout},
			Nodes:    []types.NodeDef{{Name: "start", Type: "xflow.start"}},
		})
		if err != nil {
			t.Fatalf("compile child: %v", err)
		}
		return g
	}
	for _, tc := range []struct {
		name  string
		child time.Duration
	}{
		{"child declares its own budget", 10 * time.Minute},
		{"child declares none", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resolver := stubSubWorkflowResolver{target: graph.SubWorkflowTarget{
				Version:        "v1",
				DefinitionHash: "sha256:child",
				Graph:          childWith(tc.child),
			}}
			def := subWorkflowParentDef()
			def.Settings = &types.WorkflowSettings{Timezone: "UTC", Timeout: time.Hour}
			parent, err := graph.CompileWithResolver(context.Background(), def, resolver)
			if err != nil {
				t.Fatalf("CompileWithResolver: %v", err)
			}
			rig := newSubWorkflowRig(t, parent, nil, resolver)

			rig.dispatch(t, rig.task)

			child := rig.child(t, rig.link(t).ChildExecutionID)
			if tc.child == 0 {
				if !child.Deadline.IsZero() {
					t.Fatalf("child Deadline = %s, want zero: the parent's budget must not leak into it", child.Deadline)
				}
				return
			}
			if want := child.CreatedAt.Add(tc.child); child.CreatedAt.IsZero() || !child.Deadline.Equal(want) {
				t.Fatalf("child Deadline = %s with CreatedAt %s, want CreatedAt + %s (its own timeout, not the parent's 1h)",
					child.Deadline, child.CreatedAt, tc.child)
			}
		})
	}
}

func TestExecutionSnapshotDeadlineJSON(t *testing.T) {
	t.Run("a zero deadline writes no key", func(t *testing.T) {
		raw, err := json.Marshal(&ExecutionSnapshot{ID: "exec-1", Status: types.ExecutionStatusRunning})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		var keys map[string]json.RawMessage
		if err := json.Unmarshal(raw, &keys); err != nil {
			t.Fatalf("unmarshal keys: %v", err)
		}
		if _, ok := keys["deadline"]; ok {
			t.Fatalf("snapshot JSON %s carries a deadline key for an unbounded execution", raw)
		}
	})
	t.Run("a deadline survives a round trip", func(t *testing.T) {
		deadline := time.Date(2026, 10, 24, 9, 30, 0, 123, time.UTC)
		raw, err := json.Marshal(&ExecutionSnapshot{ID: "exec-1", Status: types.ExecutionStatusRunning, Deadline: deadline})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		var got ExecutionSnapshot
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if !got.Deadline.Equal(deadline) {
			t.Fatalf("Deadline = %s after a round trip, want %s (JSON %s)", got.Deadline, deadline, raw)
		}
	})
	t.Run("a legacy snapshot decodes to no deadline", func(t *testing.T) {
		var got ExecutionSnapshot
		if err := json.Unmarshal([]byte(`{"ID":"exec-1"}`), &got); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if !got.Deadline.IsZero() {
			t.Fatalf("Deadline = %s, want zero for a snapshot written before the field existed", got.Deadline)
		}
	})
}
