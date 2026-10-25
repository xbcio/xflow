package rstate

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// deadlineFixture is a millisecond-precision instant far in the future, so no
// retention in these tests can lapse while a case runs. The sub-second part is
// the point: the stored value is a millisecond epoch and must read back to the
// millisecond, not to the second.
var deadlineFixture = time.UnixMilli(4_102_444_800_123).UTC()

func requireDeadline(t *testing.T, state *Store, id types.ExecutionID, want time.Time) {
	t.Helper()
	snap, err := state.GetExecution(context.Background(), id)
	if err != nil || snap == nil {
		t.Fatalf("GetExecution(%q) = (%+v, %v), want the execution", id, snap, err)
	}
	if want.IsZero() {
		if !snap.Deadline.IsZero() {
			t.Fatalf("Deadline of %q = %v, want zero (no budget)", id, snap.Deadline)
		}
		return
	}
	if snap.Deadline.IsZero() {
		t.Fatalf("Deadline of %q read back zero, want %v: a lost deadline is a silently disabled timeout", id, want)
	}
	if !snap.Deadline.Equal(want) {
		t.Fatalf("Deadline of %q = %v (%d ms), want %v (%d ms)",
			id, snap.Deadline, snap.Deadline.UnixMilli(), want, want.UnixMilli())
	}
}

// TestExecutionDeadlineRoundTripsThroughEveryCreationPath pins the persistence
// of ExecutionSnapshot.Deadline for each way the store creates an execution
// from a snapshot the engine already stamped.
func TestExecutionDeadlineRoundTripsThroughEveryCreationPath(t *testing.T) {
	ctx := context.Background()

	t.Run("CreateExecution", func(t *testing.T) {
		_, _, state := newMiniredisStore(t)
		id := types.ExecutionID("deadline-create")
		if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
			ID: id, Status: types.ExecutionStatusRunning, Graph: testDurableGraph(), Deadline: deadlineFixture,
		}); err != nil {
			t.Fatalf("CreateExecution: %v", err)
		}
		requireDeadline(t, state, id, deadlineFixture)
	})

	t.Run("CreateExecutionWithOutbox", func(t *testing.T) {
		_, _, state := newMiniredisStore(t)
		id := types.ExecutionID("deadline-outbox")
		entries := []engine.OutboxEntry{{
			ID:   "root/" + string(id) + "/start/0",
			Task: engine.Task{ExecutionID: id, NodeName: "start", Type: engine.TaskTypeNodeExec},
		}}
		if err := state.CreateExecutionWithOutbox(ctx, &engine.ExecutionSnapshot{
			ID: id, Status: types.ExecutionStatusRunning, Graph: testDurableGraph(), Deadline: deadlineFixture,
		}, entries); err != nil {
			t.Fatalf("CreateExecutionWithOutbox: %v", err)
		}
		requireDeadline(t, state, id, deadlineFixture)
	})

	t.Run("CreateExecutionWithConcurrency", func(t *testing.T) {
		_, _, state := newMiniredisStore(t)
		g := sweepGraph(t, "deadline-conc")
		id := types.ExecutionID("deadline-conc-exec")
		if _, err := state.CreateExecutionWithConcurrency(ctx, &engine.ExecutionSnapshot{
			ID: id, Status: types.ExecutionStatusRunning, Graph: g, Deadline: deadlineFixture,
		}, nil, sweepAdmission(g, 1)); err != nil {
			t.Fatalf("CreateExecutionWithConcurrency: %v", err)
		}
		requireDeadline(t, state, id, deadlineFixture)
	})

	t.Run("SeedExecution", func(t *testing.T) {
		_, _, state := newMiniredisStore(t)
		id := types.ExecutionID("deadline-seed")
		if _, err := state.SeedExecution(ctx, engine.SeedExecutionRequest{
			Execution: &engine.ExecutionSnapshot{
				ID: id, Status: types.ExecutionStatusRunning, Graph: testDurableGraph(), Deadline: deadlineFixture,
			},
		}); err != nil {
			t.Fatalf("SeedExecution: %v", err)
		}
		requireDeadline(t, state, id, deadlineFixture)
	})
}

// TestExecutionDeadlineSurvivesRetentionRenewal pins the suspend path: status is
// renewed while a node waits, so the deadline has to be renewed with it. A
// deadline that lapsed under a live status reads back as "no budget" and the
// execution would never expire.
func TestExecutionDeadlineSurvivesRetentionRenewal(t *testing.T) {
	ctx := context.Background()
	srv, _, state := newMiniredisStore(t)
	id := types.ExecutionID("deadline-renewal")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Status: types.ExecutionStatusRunning, Graph: testDurableGraph(), Deadline: deadlineFixture,
	}); err != nil {
		t.Fatalf("CreateExecution: %v", err)
	}
	// New() was given a one-minute retention; renew to ten and let the original
	// window pass.
	if err := state.extendExecTTL(ctx, id, "start", nil, 10*time.Minute); err != nil {
		t.Fatalf("extendExecTTL: %v", err)
	}
	srv.FastForward(5 * time.Minute)
	requireDeadline(t, state, id, deadlineFixture)
}

// TestExecutionWithoutDeadlineWritesNoDeadlineKey keeps the absent key the one
// and only encoding of "no budget".
func TestExecutionWithoutDeadlineWritesNoDeadlineKey(t *testing.T) {
	ctx := context.Background()
	srv, _, state := newMiniredisStore(t)
	id := types.ExecutionID("deadline-none")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Status: types.ExecutionStatusRunning, Graph: testDurableGraph(),
	}); err != nil {
		t.Fatalf("CreateExecution: %v", err)
	}
	if srv.Exists(execKey(namespace.Default, id, "deadline_ms")) {
		t.Fatal("an execution without a Deadline wrote deadline_ms")
	}
	requireDeadline(t, state, id, time.Time{})

	seeded := types.ExecutionID("deadline-none-seed")
	if _, err := state.SeedExecution(ctx, engine.SeedExecutionRequest{
		Execution: &engine.ExecutionSnapshot{ID: seeded, Status: types.ExecutionStatusRunning, Graph: testDurableGraph()},
	}); err != nil {
		t.Fatalf("SeedExecution: %v", err)
	}
	if srv.Exists(execKey(namespace.Default, seeded, "deadline_ms")) {
		t.Fatal("a seeded execution without a Deadline wrote deadline_ms")
	}
	requireDeadline(t, state, seeded, time.Time{})
}
