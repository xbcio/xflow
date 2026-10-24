package engine

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// clampRig submits a one-node workflow whose graph declares settings.timeout
// (so the engine reads the execution deadline) and whose node carries nodeRaw as
// its own timeout, then overwrites the stored Deadline with an exact instant so
// each case can place the execution deadline precisely against the node's.
// A zero execDeadline leaves the snapshot without one.
func clampRig(t *testing.T, nodeRaw time.Duration, execDeadline time.Time) (*Engine, *fakeState, *Task) {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:     "clamp",
		Settings: &types.WorkflowSettings{Timezone: "UTC", Timeout: time.Hour},
		Nodes:    []types.NodeDef{{Name: "start", Type: "test.echo", Timeout: nodeRaw}},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	state := newFakeState()
	queue := &fakeQueue{}
	eng := New(state, queue)
	id, err := eng.Submit(context.Background(), g, nil)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	state.mu.Lock()
	state.executions[id].Deadline = execDeadline
	state.mu.Unlock()
	tasks := queue.Drain()
	if len(tasks) != 1 {
		t.Fatalf("root tasks = %d, want 1", len(tasks))
	}
	return eng, state, tasks[0]
}

func TestClampToExecutionDeadline(t *testing.T) {
	now := time.Date(2026, 10, 24, 8, 0, 0, 0, time.UTC)
	exec, later, earlier := now.Add(time.Minute), now.Add(time.Hour), now.Add(time.Second)
	for _, tc := range []struct {
		name    string
		exec, d time.Time
		want    time.Time
	}{
		{"no execution deadline leaves d alone", time.Time{}, later, later},
		{"no execution deadline and unbounded d stays unbounded", time.Time{}, time.Time{}, time.Time{}},
		{"earlier execution deadline wins", exec, later, exec},
		{"later execution deadline loses", exec, earlier, earlier},
		{"unbounded d takes the execution deadline", exec, time.Time{}, exec},
		{"equal instants agree", exec, exec, exec},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := clampToExecutionDeadline(tc.exec, tc.d); !got.Equal(tc.want) {
				t.Fatalf("clampToExecutionDeadline(%v, %v) = %v, want %v", tc.exec, tc.d, got, tc.want)
			}
		})
	}
}

func TestBuildTaskLeaseClampsToExecutionDeadline(t *testing.T) {
	ctx := context.Background()
	t.Run("an earlier execution deadline takes the smaller", func(t *testing.T) {
		deadline := time.Now().UTC().Add(10 * time.Second)
		eng, _, task := clampRig(t, 45*time.Second, deadline)
		lease, err := eng.BuildTaskLease(ctx, task)
		if err != nil {
			t.Fatalf("BuildTaskLease: %v", err)
		}
		if !lease.ExecutionDeadline.Equal(deadline) {
			t.Fatalf("ExecutionDeadline = %v, want the execution deadline %v", lease.ExecutionDeadline, deadline)
		}
	})
	t.Run("a later execution deadline keeps the node timeout", func(t *testing.T) {
		before := time.Now().UTC()
		eng, _, task := clampRig(t, 45*time.Second, before.Add(time.Hour))
		lease, err := eng.BuildTaskLease(ctx, task)
		if err != nil {
			t.Fatalf("BuildTaskLease: %v", err)
		}
		if want := lease.IssuedAt.Add(45 * time.Second); !lease.ExecutionDeadline.Equal(want) {
			t.Fatalf("ExecutionDeadline = %v, want issue time + node timeout %v", lease.ExecutionDeadline, want)
		}
	})
	t.Run("an unlimited node takes the execution deadline", func(t *testing.T) {
		deadline := time.Now().UTC().Add(10 * time.Second)
		eng, _, task := clampRig(t, -1, deadline)
		lease, err := eng.BuildTaskLease(ctx, task)
		if err != nil {
			t.Fatalf("BuildTaskLease: %v", err)
		}
		if !lease.ExecutionDeadline.Equal(deadline) {
			t.Fatalf("ExecutionDeadline = %v, want the execution deadline %v: an unlimited node must still end with its execution",
				lease.ExecutionDeadline, deadline)
		}
	})
	t.Run("no execution deadline matches the node-only behavior", func(t *testing.T) {
		eng, _, task := clampRig(t, 45*time.Second, time.Time{})
		lease, err := eng.BuildTaskLease(ctx, task)
		if err != nil {
			t.Fatalf("BuildTaskLease: %v", err)
		}
		if want := lease.IssuedAt.Add(45 * time.Second); !lease.ExecutionDeadline.Equal(want) {
			t.Fatalf("ExecutionDeadline = %v, want issue time + node timeout %v", lease.ExecutionDeadline, want)
		}
		eng, _, task = clampRig(t, -1, time.Time{})
		lease, err = eng.BuildTaskLease(ctx, task)
		if err != nil {
			t.Fatalf("BuildTaskLease unlimited: %v", err)
		}
		if !lease.ExecutionDeadline.IsZero() {
			t.Fatalf("ExecutionDeadline = %v, want zero for an unlimited node without an execution deadline", lease.ExecutionDeadline)
		}
	})
	t.Run("an already-passed deadline is stamped as is", func(t *testing.T) {
		deadline := time.Now().UTC().Add(-time.Minute)
		eng, _, task := clampRig(t, 45*time.Second, deadline)
		lease, err := eng.BuildTaskLease(ctx, task)
		if err != nil {
			t.Fatalf("BuildTaskLease refused a lease past the execution deadline: %v", err)
		}
		if !lease.ExecutionDeadline.Equal(deadline) {
			t.Fatalf("ExecutionDeadline = %v, want the passed execution deadline %v", lease.ExecutionDeadline, deadline)
		}
	})
	t.Run("only a graph with settings.timeout pays the extra snapshot read", func(t *testing.T) {
		// buildInput already reads the snapshot on a store without the narrow
		// input read, so the claim is about the DIFFERENCE: the deadline read
		// adds exactly one GetExecution for a bounded graph and none otherwise.
		eng, _, _, task := leaseTimeoutDef(t, 45*time.Second)
		unbounded := &countingStateReader{StateStore: eng.state}
		eng.state = unbounded
		if _, err := eng.BuildTaskLease(ctx, task); err != nil {
			t.Fatalf("BuildTaskLease: %v", err)
		}
		eng, _, task = clampRig(t, 45*time.Second, time.Time{})
		bounded := &countingStateReader{StateStore: eng.state}
		eng.state = bounded
		if _, err := eng.BuildTaskLease(ctx, task); err != nil {
			t.Fatalf("BuildTaskLease: %v", err)
		}
		if bounded.getExecution != unbounded.getExecution+1 {
			t.Fatalf("GetExecution calls: bounded graph %d, unbounded graph %d, want exactly one more for the bounded graph",
				bounded.getExecution, unbounded.getExecution)
		}
	})
}

// countingStateReader counts full-snapshot reads, the cost a graph without an
// execution deadline must not pay on the lease path.
type countingStateReader struct {
	StateStore
	getExecution int
}

func (c *countingStateReader) GetExecution(ctx context.Context, id types.ExecutionID) (*ExecutionSnapshot, error) {
	c.getExecution++
	return c.StateStore.GetExecution(ctx, id)
}

func TestRecoverTaskLeaseClampsToExecutionDeadline(t *testing.T) {
	ctx := context.Background()
	recoverWith := func(t *testing.T, nodeRaw time.Duration, deadline time.Time) (*TaskLease, time.Time) {
		t.Helper()
		eng, state, task := clampRig(t, nodeRaw, deadline)
		lease, err := eng.BuildTaskLease(ctx, task)
		if err != nil {
			t.Fatalf("BuildTaskLease: %v", err)
		}
		// A fresh engine over the same state is the control plane that took over
		// after a crash: it has no memo of its own, so recovery must read the
		// deadline back from the stored snapshot.
		restarted := New(state, &fakeQueue{})
		recovered, err := restarted.RecoverTaskLease(ctx, &lease.Task)
		if err != nil {
			t.Fatalf("RecoverTaskLease: %v", err)
		}
		return recovered, lease.IssuedAt
	}
	t.Run("an earlier execution deadline takes the smaller", func(t *testing.T) {
		deadline := time.Now().UTC().Add(10 * time.Second)
		recovered, _ := recoverWith(t, 45*time.Second, deadline)
		if !recovered.ExecutionDeadline.Equal(deadline) {
			t.Fatalf("ExecutionDeadline = %v, want the execution deadline %v", recovered.ExecutionDeadline, deadline)
		}
	})
	t.Run("a later execution deadline keeps the node timeout", func(t *testing.T) {
		recovered, issuedAt := recoverWith(t, 45*time.Second, time.Now().UTC().Add(time.Hour))
		if want := issuedAt.Add(45 * time.Second); !recovered.ExecutionDeadline.Equal(want) {
			t.Fatalf("ExecutionDeadline = %v, want issue time + node timeout %v", recovered.ExecutionDeadline, want)
		}
	})
	t.Run("an unlimited node takes the execution deadline", func(t *testing.T) {
		deadline := time.Now().UTC().Add(10 * time.Second)
		recovered, _ := recoverWith(t, -1, deadline)
		if !recovered.ExecutionDeadline.Equal(deadline) {
			t.Fatalf("ExecutionDeadline = %v, want the execution deadline %v", recovered.ExecutionDeadline, deadline)
		}
	})
	t.Run("no execution deadline matches the node-only behavior", func(t *testing.T) {
		recovered, issuedAt := recoverWith(t, 45*time.Second, time.Time{})
		if want := issuedAt.Add(45 * time.Second); !recovered.ExecutionDeadline.Equal(want) {
			t.Fatalf("ExecutionDeadline = %v, want issue time + node timeout %v", recovered.ExecutionDeadline, want)
		}
	})
}

// deadlineRenewer records the absolute deadline the engine asks the store to
// renew to, which is the one value RenewTaskLease decides.
type deadlineRenewer struct {
	*fakeState
	got   time.Time
	calls int
}

func (r *deadlineRenewer) RenewTaskLease(_ context.Context, _ types.ExecutionID, _ string, _ LeaseToken, deadline time.Time) (bool, error) {
	r.got = deadline
	r.calls++
	return true, nil
}

func TestRenewTaskLeaseClampsToExecutionDeadline(t *testing.T) {
	ctx := context.Background()
	renewWith := func(t *testing.T, execDeadline time.Time, extend time.Duration) (*deadlineRenewer, time.Time, time.Time) {
		t.Helper()
		eng, state, task := clampRig(t, 45*time.Second, execDeadline)
		lease, err := eng.BuildTaskLease(ctx, task)
		if err != nil {
			t.Fatalf("BuildTaskLease: %v", err)
		}
		rec := &deadlineRenewer{fakeState: state}
		eng.state = rec
		before := time.Now().UTC()
		renewed, err := eng.RenewTaskLease(ctx, lease, extend)
		after := time.Now().UTC()
		if err != nil || !renewed {
			t.Fatalf("RenewTaskLease = %v, %v", renewed, err)
		}
		if rec.calls != 1 {
			t.Fatalf("store renew calls = %d, want 1", rec.calls)
		}
		return rec, before.Add(extend), after.Add(extend)
	}
	t.Run("an execution deadline inside the extension caps the renewal", func(t *testing.T) {
		deadline := time.Now().UTC().Add(20 * time.Second)
		rec, _, _ := renewWith(t, deadline, 5*time.Minute)
		if !rec.got.Equal(deadline) {
			t.Fatalf("renewed to %v, want the execution deadline %v", rec.got, deadline)
		}
	})
	t.Run("an execution deadline past the extension leaves the renewal alone", func(t *testing.T) {
		rec, lo, hi := renewWith(t, time.Now().UTC().Add(time.Hour), time.Minute)
		if rec.got.Before(lo) || rec.got.After(hi) {
			t.Fatalf("renewed to %v, want now + extend within [%v, %v]", rec.got, lo, hi)
		}
	})
	t.Run("no execution deadline leaves the renewal alone", func(t *testing.T) {
		rec, lo, hi := renewWith(t, time.Time{}, 5*time.Minute)
		if rec.got.Before(lo) || rec.got.After(hi) {
			t.Fatalf("renewed to %v, want now + extend within [%v, %v]", rec.got, lo, hi)
		}
	})
	t.Run("a passed execution deadline is handed to the store as is", func(t *testing.T) {
		deadline := time.Now().UTC().Add(-time.Minute)
		rec, _, _ := renewWith(t, deadline, 5*time.Minute)
		if !rec.got.Equal(deadline) {
			t.Fatalf("renewed to %v, want the passed execution deadline %v", rec.got, deadline)
		}
	})
}
