package statestoretest

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// GroupSuspendTestStore combines the interfaces needed to test the group
// suspend/resume contract.
type GroupSuspendTestStore interface {
	engine.StateStore
	engine.GroupStateStore
	engine.GroupSuspender
	engine.GroupResumer
	engine.GroupSuspendReader
	engine.GroupCanceler
}

// RunGroupSuspendContract exercises the GroupSuspender/GroupResumer/
// GroupSuspendReader/GroupCanceler contract against a concrete backend.
func RunGroupSuspendContract(t *testing.T, newStore func(t *testing.T) GroupSuspendTestStore) {
	ctx := context.Background()

	// suspendGraph returns a single-group graph suitable for suspend tests.
	suspendGraph := func(t *testing.T) *graph.Graph {
		t.Helper()
		return singleGroupGraph(t)
	}

	// seed creates an execution and returns the store, execution ID, and unit index.
	seed := func(t *testing.T) (GroupSuspendTestStore, types.ExecutionID, int) {
		t.Helper()
		s := newStore(t)
		g := suspendGraph(t)
		id := types.ExecutionID("e-" + t.Name())
		if err := s.CreateExecution(ctx, &engine.ExecutionSnapshot{
			ID: id, Graph: g, Status: types.ExecutionStatusRunning,
		}); err != nil {
			t.Fatalf("create execution: %v", err)
		}
		return s, id, g.Groups()[0].UnitIdx
	}

	// acquireRunning acquires a group lease and returns the token used.
	acquireRunning := func(t *testing.T, s GroupSuspendTestStore, id types.ExecutionID, gu int, token engine.LeaseToken) {
		t.Helper()
		l := &engine.GroupLease{
			LeaseID: engine.LeaseID("L-" + string(token)), LeaseToken: token,
			Attempt: 1, ExecutionID: id, GroupUnitIdx: gu, GroupID: "edge",
			IssuedAt: time.Now(), TTL: time.Minute,
		}
		ok, err := s.AcquireGroupLease(ctx, l)
		if err != nil || !ok {
			t.Fatalf("acquire group lease: ok=%v err=%v", ok, err)
		}
	}

	// doSuspend suspends a running group unit with the given spec and token.
	doSuspend := func(t *testing.T, s GroupSuspendTestStore, id types.ExecutionID, gu int, token engine.LeaseToken, spec engine.GroupSuspendSpec) engine.GroupSuspendResult {
		t.Helper()
		res, err := s.SuspendGroup(ctx, engine.GroupSuspendRequest{
			ExecutionID:  id,
			GroupUnitIdx: gu,
			GroupID:      "edge",
			LeaseID:      engine.LeaseID("L-" + string(token)),
			LeaseToken:   token,
			Attempt:      1,
			SuspendSpec:  spec,
			EntryInput:   map[string]any{"x": 1},
		})
		if err != nil {
			t.Fatalf("SuspendGroup: %v", err)
		}
		return res
	}

	// --- Sub-tests ---

	t.Run("HappyPath_SuspendAndResume", func(t *testing.T) {
		s, id, gu := seed(t)
		acquireRunning(t, s, id, gu, "T1")

		spec := engine.GroupSuspendSpec{
			NodeName:    "ingest",
			WaitSignals: []string{"done"},
			Quorum:      1,
		}
		res := doSuspend(t, s, id, gu, "T1", spec)
		if !res.Committed {
			t.Fatal("SuspendGroup must commit successfully")
		}

		// Verify suspend state is persisted.
		state, err := s.GetGroupSuspendState(ctx, id, gu)
		if err != nil {
			t.Fatalf("GetGroupSuspendState: %v", err)
		}
		if state == nil {
			t.Fatal("GetGroupSuspendState returned nil for suspended unit")
		}
		if state.Spec.NodeName != "ingest" {
			t.Errorf("spec.NodeName = %q, want ingest", state.Spec.NodeName)
		}
		if len(state.Spec.WaitSignals) != 1 || state.Spec.WaitSignals[0] != "done" {
			t.Errorf("spec.WaitSignals = %v, want [done]", state.Spec.WaitSignals)
		}
		// JSON round-trip may deserialize int as float64.
		if v, ok := state.EntryInput["x"]; !ok {
			t.Errorf("EntryInput missing key x, got %v", state.EntryInput)
		} else {
			switch n := v.(type) {
			case float64:
				if n != 1 {
					t.Errorf("EntryInput[x] = %v, want 1", n)
				}
			case int:
				if n != 1 {
					t.Errorf("EntryInput[x] = %v, want 1", n)
				}
			default:
				t.Errorf("EntryInput[x] = %v (%T), want numeric 1", v, v)
			}
		}

		// Resume with matching signal.
		rr, err := s.ResumeGroup(ctx, engine.GroupResumeRequest{
			ExecutionID:  id,
			GroupUnitIdx: gu,
			SignalName:   "done",
		})
		if err != nil {
			t.Fatalf("ResumeGroup: %v", err)
		}
		if !rr.Resumed {
			t.Fatal("ResumeGroup must resume when quorum=1 and signal delivered")
		}
	})

	t.Run("SuspendFenced_WrongToken", func(t *testing.T) {
		s, id, gu := seed(t)
		acquireRunning(t, s, id, gu, "T1")

		spec := engine.GroupSuspendSpec{
			NodeName:    "ingest",
			WaitSignals: []string{"done"},
		}
		res, err := s.SuspendGroup(ctx, engine.GroupSuspendRequest{
			ExecutionID:  id,
			GroupUnitIdx: gu,
			GroupID:      "edge",
			LeaseID:      "L-WRONG",
			LeaseToken:   "WRONG",
			Attempt:      1,
			SuspendSpec:  spec,
		})
		if err != nil {
			t.Fatalf("SuspendGroup: %v", err)
		}
		if res.Committed {
			t.Fatal("SuspendGroup with wrong token must not commit")
		}
	})

	t.Run("ResumeGroup_SingleSignal", func(t *testing.T) {
		s, id, gu := seed(t)
		acquireRunning(t, s, id, gu, "T1")

		spec := engine.GroupSuspendSpec{
			NodeName:    "ingest",
			WaitSignals: []string{"signal_a"},
			Quorum:      1,
		}
		doSuspend(t, s, id, gu, "T1", spec)

		rr, err := s.ResumeGroup(ctx, engine.GroupResumeRequest{
			ExecutionID:  id,
			GroupUnitIdx: gu,
			SignalName:   "signal_a",
			SignalData:   map[string]any{"payload": "hello"},
		})
		if err != nil {
			t.Fatalf("ResumeGroup: %v", err)
		}
		if !rr.Resumed {
			t.Fatal("single signal with quorum=1 must resume")
		}
	})

	t.Run("ResumeGroup_MultiSignalQuorum", func(t *testing.T) {
		s, id, gu := seed(t)
		acquireRunning(t, s, id, gu, "T1")

		spec := engine.GroupSuspendSpec{
			NodeName:    "ingest",
			WaitSignals: []string{"a", "b", "c"},
			Quorum:      2,
		}
		doSuspend(t, s, id, gu, "T1", spec)

		// First signal: quorum not yet met.
		rr1, err := s.ResumeGroup(ctx, engine.GroupResumeRequest{
			ExecutionID: id, GroupUnitIdx: gu, SignalName: "a",
		})
		if err != nil {
			t.Fatalf("ResumeGroup(a): %v", err)
		}
		if rr1.Resumed {
			t.Fatal("first signal must not resume (quorum=2)")
		}
		if rr1.Pending != 1 {
			t.Errorf("Pending after first signal = %d, want 1", rr1.Pending)
		}

		// Second signal: quorum satisfied.
		rr2, err := s.ResumeGroup(ctx, engine.GroupResumeRequest{
			ExecutionID: id, GroupUnitIdx: gu, SignalName: "b",
		})
		if err != nil {
			t.Fatalf("ResumeGroup(b): %v", err)
		}
		if !rr2.Resumed {
			t.Fatal("second signal must resume (quorum=2 satisfied)")
		}
	})

	t.Run("ResumeGroup_DuplicateSignal", func(t *testing.T) {
		s, id, gu := seed(t)
		acquireRunning(t, s, id, gu, "T1")

		spec := engine.GroupSuspendSpec{
			NodeName:    "ingest",
			WaitSignals: []string{"x"},
			Quorum:      1,
		}
		doSuspend(t, s, id, gu, "T1", spec)

		// First delivery resumes.
		rr1, err := s.ResumeGroup(ctx, engine.GroupResumeRequest{
			ExecutionID: id, GroupUnitIdx: gu, SignalName: "x",
		})
		if err != nil {
			t.Fatalf("ResumeGroup first: %v", err)
		}
		if !rr1.Resumed {
			t.Fatal("first delivery must resume")
		}

		// Second delivery: unit is no longer suspended, so it's a no-op.
		rr2, err := s.ResumeGroup(ctx, engine.GroupResumeRequest{
			ExecutionID: id, GroupUnitIdx: gu, SignalName: "x",
		})
		if err != nil {
			t.Fatalf("ResumeGroup duplicate: %v", err)
		}
		if rr2.Resumed {
			t.Fatal("duplicate signal after resume must be no-op")
		}
	})

	t.Run("ResumeGroup_UnknownSignal", func(t *testing.T) {
		s, id, gu := seed(t)
		acquireRunning(t, s, id, gu, "T1")

		spec := engine.GroupSuspendSpec{
			NodeName:    "ingest",
			WaitSignals: []string{"expected"},
			Quorum:      1,
		}
		doSuspend(t, s, id, gu, "T1", spec)

		// Deliver a signal not in WaitSignals.
		rr, err := s.ResumeGroup(ctx, engine.GroupResumeRequest{
			ExecutionID: id, GroupUnitIdx: gu, SignalName: "unknown",
		})
		if err != nil {
			t.Fatalf("ResumeGroup unknown: %v", err)
		}
		// Unknown signal must not satisfy quorum.
		if rr.Resumed {
			t.Fatal("unknown signal must not resume")
		}

		// Verify suspend state still intact.
		state, err := s.GetGroupSuspendState(ctx, id, gu)
		if err != nil {
			t.Fatalf("GetGroupSuspendState: %v", err)
		}
		if state == nil {
			t.Fatal("suspend state must still exist after unknown signal delivery")
		}
	})

	t.Run("CancelSuspendedGroup", func(t *testing.T) {
		s, id, gu := seed(t)
		acquireRunning(t, s, id, gu, "T1")

		spec := engine.GroupSuspendSpec{
			NodeName:    "ingest",
			WaitSignals: []string{"done"},
		}
		doSuspend(t, s, id, gu, "T1", spec)

		// Cancel the suspended group.
		if err := s.CancelSuspendedGroup(ctx, id, gu); err != nil {
			t.Fatalf("CancelSuspendedGroup: %v", err)
		}

		// Suspend state must be cleared.
		state, err := s.GetGroupSuspendState(ctx, id, gu)
		if err != nil {
			t.Fatalf("GetGroupSuspendState: %v", err)
		}
		if state != nil {
			t.Fatal("GetGroupSuspendState must return nil after cancel")
		}

		// Resume after cancel must be no-op.
		rr, err := s.ResumeGroup(ctx, engine.GroupResumeRequest{
			ExecutionID: id, GroupUnitIdx: gu, SignalName: "done",
		})
		if err != nil {
			t.Fatalf("ResumeGroup after cancel: %v", err)
		}
		if rr.Resumed {
			t.Fatal("resume after cancel must be no-op")
		}
	})

	t.Run("SuspendAfterDone_Rejected", func(t *testing.T) {
		s, id, gu := seed(t)
		acquireRunning(t, s, id, gu, "T1")

		// Commit group as done.
		commitReq := engine.GroupCommitRequest{
			ExecutionID:  id,
			GroupUnitIdx: gu,
			GroupID:      "edge",
			LeaseID:      "L-T1",
			LeaseToken:   "T1",
			Attempt:      1,
			Outcome:      engine.GroupOutcomeSuccess,
		}
		cr, err := s.CommitGroup(ctx, commitReq)
		if err != nil || !cr.Applied {
			t.Fatalf("CommitGroup: applied=%v err=%v", cr.Applied, err)
		}

		// Now try to suspend — unit is already done, must be rejected.
		spec := engine.GroupSuspendSpec{
			NodeName:    "ingest",
			WaitSignals: []string{"done"},
		}
		res, err := s.SuspendGroup(ctx, engine.GroupSuspendRequest{
			ExecutionID:  id,
			GroupUnitIdx: gu,
			GroupID:      "edge",
			LeaseID:      "L-T1",
			LeaseToken:   "T1",
			Attempt:      1,
			SuspendSpec:  spec,
		})
		if err != nil {
			t.Fatalf("SuspendGroup after done: %v", err)
		}
		if res.Committed {
			t.Fatal("SuspendGroup on a done unit must not commit")
		}
	})
}
