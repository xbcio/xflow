package engine

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// groupDeadlineState wraps the group lease fake with the three observations the
// deadline tests need: how many full-snapshot reads the engine made, the
// absolute deadline a renewal asked the store for, and an IssuedAt on the lease
// read back for recovery (the plain fake returns a lease without one, which
// would anchor a group timeout at the year 1).
type groupDeadlineState struct {
	*fakeGroupLeaseState
	issuedAt     time.Time
	getExecution atomic.Int64
	renewedTo    time.Time
	renewCalls   int
}

func (s *groupDeadlineState) GetExecution(ctx context.Context, id types.ExecutionID) (*ExecutionSnapshot, error) {
	s.getExecution.Add(1)
	return s.fakeGroupLeaseState.GetExecution(ctx, id)
}

func (s *groupDeadlineState) GetGroupLease(ctx context.Context, id types.ExecutionID, unitIdx int) (*GroupLease, error) {
	lease, err := s.fakeGroupLeaseState.GetGroupLease(ctx, id, unitIdx)
	if lease != nil {
		lease.IssuedAt = s.issuedAt
	}
	return lease, err
}

func (s *groupDeadlineState) RenewGroupLease(ctx context.Context, id types.ExecutionID, unitIdx int, token LeaseToken, deadline time.Time) (bool, error) {
	s.renewedTo = deadline
	s.renewCalls++
	return s.fakeGroupLeaseState.RenewGroupLease(ctx, id, unitIdx, token, deadline)
}

// groupClampRig submits nothing: it stores one running execution of a graph
// with a single two-member group whose own timeout is groupTimeout (zero for
// none), stamps execDeadline on the snapshot as given, and returns an engine
// over it together with the queued group task. bounded decides whether the
// graph declares settings.timeout, which is what makes the engine read the
// execution deadline at all.
func groupClampRig(t *testing.T, bounded bool, groupTimeout time.Duration, execDeadline time.Time) (*Engine, *groupDeadlineState, *Task) {
	t.Helper()
	def := &types.WorkflowDef{
		Name:    "group-clamp",
		Version: "1",
		Nodes: []types.NodeDef{
			{Name: "A", Type: "http.request", Version: 1, Parameters: map[string]any{"url": "http://a"}},
			{Name: "B", Type: "code.python", Version: 2, Parameters: map[string]any{"script": "pass"}},
		},
		Groups: []types.GroupDef{
			{Name: "grp1", Members: []string{"A", "B"}, Timeout: groupTimeout},
		},
		Connections: types.Connections{
			"A": {"main": {Targets: []types.Connection{{Node: "B", Input: "main"}}}},
		},
	}
	if bounded {
		def.Settings = &types.WorkflowSettings{Timezone: "UTC", Timeout: time.Hour}
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	execID := types.ExecutionID("exec-group-clamp-1")
	state := &groupDeadlineState{fakeGroupLeaseState: &fakeGroupLeaseState{fakeState: newFakeState()}}
	if err := state.CreateExecution(context.Background(), &ExecutionSnapshot{
		ID:       execID,
		Graph:    g,
		Status:   types.ExecutionStatusRunning,
		Deadline: execDeadline,
	}); err != nil {
		t.Fatalf("CreateExecution: %v", err)
	}
	eng := New(state, &fakeQueue{})
	eng.cacheExecutionGraph(execID, g)
	gm := g.Groups()[0]
	return eng, state, &Task{
		ExecutionID: execID,
		NodeName:    gm.Name,
		NodeIdx:     gm.EntryIdx,
		UnitIdx:     gm.UnitIdx,
		Type:        TaskTypeGroupExec,
	}
}

// groupClampCase is one placement of the execution deadline against the
// group's own timeout. want derives the expected payload deadline from the
// lease's issue time, because the group timeout is anchored there.
type groupClampCase struct {
	name         string
	bounded      bool
	groupTimeout time.Duration
	execDeadline time.Time
	want         func(issuedAt time.Time) time.Time
}

func groupClampCases() []groupClampCase {
	now := time.Now().UTC()
	soon := now.Add(10 * time.Second)
	passed := now.Add(-time.Minute)
	atIssue := func(d time.Duration) func(time.Time) time.Time {
		return func(issuedAt time.Time) time.Time { return issuedAt.Add(d) }
	}
	exactly := func(at time.Time) func(time.Time) time.Time {
		return func(time.Time) time.Time { return at }
	}
	return []groupClampCase{
		{"an earlier execution deadline takes the smaller", true, 45 * time.Second, soon, exactly(soon)},
		{"a later execution deadline keeps the group timeout", true, 45 * time.Second, now.Add(time.Hour), atIssue(45 * time.Second)},
		{"a group without a timeout takes the execution deadline", true, 0, soon, exactly(soon)},
		{"no execution deadline matches the group-only behavior", true, 45 * time.Second, time.Time{}, atIssue(45 * time.Second)},
		{"no execution deadline and no group timeout stays unbounded", true, 0, time.Time{}, exactly(time.Time{})},
		{"a graph without settings.timeout matches the group-only behavior", false, 45 * time.Second, time.Time{}, atIssue(45 * time.Second)},
		{"a graph without settings.timeout and no group timeout stays unbounded", false, 0, time.Time{}, exactly(time.Time{})},
		{"an already-passed deadline is stamped as is", true, 45 * time.Second, passed, exactly(passed)},
	}
}

func TestBuildGroupLeaseClampsToExecutionDeadline(t *testing.T) {
	ctx := context.Background()
	for _, tc := range groupClampCases() {
		t.Run(tc.name, func(t *testing.T) {
			eng, _, task := groupClampRig(t, tc.bounded, tc.groupTimeout, tc.execDeadline)
			lease, payload, err := eng.BuildGroupLease(ctx, task)
			if err != nil {
				t.Fatalf("BuildGroupLease: %v", err)
			}
			if want := tc.want(lease.IssuedAt); !payload.Deadline.Equal(want) {
				t.Fatalf("payload deadline = %v, want %v", payload.Deadline, want)
			}
		})
	}
}

func TestRecoverGroupLeaseClampsToExecutionDeadline(t *testing.T) {
	ctx := context.Background()
	for _, tc := range groupClampCases() {
		t.Run(tc.name, func(t *testing.T) {
			eng, state, task := groupClampRig(t, tc.bounded, tc.groupTimeout, tc.execDeadline)
			lease, _, err := eng.BuildGroupLease(ctx, task)
			if err != nil {
				t.Fatalf("BuildGroupLease: %v", err)
			}
			state.issuedAt = lease.IssuedAt
			// A fresh engine over the same state is the control plane that took
			// over after a crash: it has no memo of its own, so recovery must read
			// the deadline back from the stored snapshot.
			restarted := New(state, &fakeQueue{})
			_, payload, err := restarted.RecoverGroupLease(ctx, task.ExecutionID, task.UnitIdx)
			if err != nil {
				t.Fatalf("RecoverGroupLease: %v", err)
			}
			if want := tc.want(lease.IssuedAt); !payload.Deadline.Equal(want) {
				t.Fatalf("recovered payload deadline = %v, want %v", payload.Deadline, want)
			}
		})
	}
}

func TestRenewGroupLeaseClampsToExecutionDeadline(t *testing.T) {
	ctx := context.Background()
	renewWith := func(t *testing.T, execDeadline time.Time, extend time.Duration) (*groupDeadlineState, time.Time, time.Time) {
		t.Helper()
		eng, state, task := groupClampRig(t, true, 45*time.Second, execDeadline)
		lease, _, err := eng.BuildGroupLease(ctx, task)
		if err != nil {
			t.Fatalf("BuildGroupLease: %v", err)
		}
		before := time.Now().UTC()
		renewed, err := eng.RenewGroupLease(ctx, lease, extend)
		after := time.Now().UTC()
		if err != nil || !renewed {
			t.Fatalf("RenewGroupLease = %v, %v", renewed, err)
		}
		if state.renewCalls != 1 {
			t.Fatalf("store renew calls = %d, want 1", state.renewCalls)
		}
		return state, before.Add(extend), after.Add(extend)
	}
	t.Run("an execution deadline inside the extension caps the renewal", func(t *testing.T) {
		deadline := time.Now().UTC().Add(20 * time.Second)
		state, _, _ := renewWith(t, deadline, 5*time.Minute)
		if !state.renewedTo.Equal(deadline) {
			t.Fatalf("renewed to %v, want the execution deadline %v", state.renewedTo, deadline)
		}
	})
	t.Run("an execution deadline past the extension leaves the renewal alone", func(t *testing.T) {
		state, lo, hi := renewWith(t, time.Now().UTC().Add(time.Hour), time.Minute)
		if state.renewedTo.Before(lo) || state.renewedTo.After(hi) {
			t.Fatalf("renewed to %v, want now + extend within [%v, %v]", state.renewedTo, lo, hi)
		}
	})
	t.Run("no execution deadline leaves the renewal alone", func(t *testing.T) {
		state, lo, hi := renewWith(t, time.Time{}, 5*time.Minute)
		if state.renewedTo.Before(lo) || state.renewedTo.After(hi) {
			t.Fatalf("renewed to %v, want now + extend within [%v, %v]", state.renewedTo, lo, hi)
		}
	})
	t.Run("a passed execution deadline is handed to the store as is", func(t *testing.T) {
		deadline := time.Now().UTC().Add(-time.Minute)
		state, _, _ := renewWith(t, deadline, 5*time.Minute)
		if !state.renewedTo.Equal(deadline) {
			t.Fatalf("renewed to %v, want the passed execution deadline %v", state.renewedTo, deadline)
		}
	})
}

// TestGroupLeaseDeadlineReads pins the cost of the clamp: a graph that declares
// settings.timeout pays exactly one GetExecution on each entry point (a fresh
// engine has no memo), and a graph without it pays none. The surrounding path
// already reads the snapshot on a store without the narrow status read, so the
// claim is about the DIFFERENCE between the two graphs.
func TestGroupLeaseDeadlineReads(t *testing.T) {
	ctx := context.Background()
	reads := func(t *testing.T, bounded bool, entry string) int64 {
		t.Helper()
		eng, state, task := groupClampRig(t, bounded, 45*time.Second, time.Time{})
		if entry == "build" {
			state.getExecution.Store(0)
			if _, _, err := eng.BuildGroupLease(ctx, task); err != nil {
				t.Fatalf("BuildGroupLease: %v", err)
			}
			return state.getExecution.Load()
		}
		lease, _, err := eng.BuildGroupLease(ctx, task)
		if err != nil {
			t.Fatalf("BuildGroupLease: %v", err)
		}
		state.issuedAt = lease.IssuedAt
		fresh := New(state, &fakeQueue{})
		state.getExecution.Store(0)
		switch entry {
		case "recover":
			if _, _, err := fresh.RecoverGroupLease(ctx, task.ExecutionID, task.UnitIdx); err != nil {
				t.Fatalf("RecoverGroupLease: %v", err)
			}
		case "renew":
			if _, err := fresh.RenewGroupLease(ctx, lease, time.Minute); err != nil {
				t.Fatalf("RenewGroupLease: %v", err)
			}
		}
		return state.getExecution.Load()
	}
	for _, entry := range []string{"build", "recover", "renew"} {
		t.Run(entry, func(t *testing.T) {
			unbounded := reads(t, false, entry)
			bounded := reads(t, true, entry)
			if entry == "renew" && unbounded != 0 {
				t.Fatalf("renewing a graph without settings.timeout read the snapshot %d times, want 0", unbounded)
			}
			if bounded != unbounded+1 {
				t.Fatalf("GetExecution calls: bounded graph %d, unbounded graph %d, want exactly one more for the bounded graph",
					bounded, unbounded)
			}
		})
	}
	t.Run("a graph without settings.timeout never asks the store for the deadline", func(t *testing.T) {
		eng, state, task := groupClampRig(t, false, 45*time.Second, time.Time{})
		g, err := eng.loadGraph(ctx, task.ExecutionID)
		if err != nil {
			t.Fatalf("loadGraph: %v", err)
		}
		state.getExecution.Store(0)
		if _, err := eng.executionDeadline(ctx, task.ExecutionID, g); err != nil {
			t.Fatalf("executionDeadline: %v", err)
		}
		if got := state.getExecution.Load(); got != 0 {
			t.Fatalf("GetExecution calls = %d, want 0", got)
		}
	})
}
