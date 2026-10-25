package rstate

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// indexBase is an arbitrary fixed instant the index tests count from, so every
// deadline and every "before" is an explicit number rather than a wall-clock
// read. It is far in the past on purpose: nothing here may depend on now.
var indexBase = time.UnixMilli(1_700_000_000_000).UTC()

func createWithDeadline(t *testing.T, state *Store, ctx context.Context, id types.ExecutionID, deadline time.Time) {
	t.Helper()
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Status: types.ExecutionStatusRunning, Graph: testDurableGraph(), Deadline: deadline,
	}); err != nil {
		t.Fatalf("CreateExecution(%q): %v", id, err)
	}
}

func listDue(t *testing.T, state *Store, ctx context.Context, before time.Time, limit int) []engine.DeadlineExpiredExecution {
	t.Helper()
	got, err := state.ListDeadlineExceededExecutions(ctx, before, limit)
	if err != nil {
		t.Fatalf("ListDeadlineExceededExecutions: %v", err)
	}
	return got
}

func requireListed(t *testing.T, got []engine.DeadlineExpiredExecution, want ...types.ExecutionID) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("listed %d executions %+v, want %v", len(got), got, want)
	}
	for i := range want {
		if got[i].ExecutionID != want[i] {
			t.Fatalf("listed[%d] = %q, want %q (full list %+v)", i, got[i].ExecutionID, want[i], got)
		}
	}
}

func TestDeadlineIndexZeroDeadlineIsNeverRegistered(t *testing.T) {
	ctx := context.Background()
	srv, _, state := newMiniredisStore(t)
	createWithDeadline(t, state, ctx, "idx-zero", time.Time{})

	if srv.Exists(executionDeadlineIndexKey(namespace.Default)) {
		t.Fatal("an execution without a Deadline created the deadline index")
	}
	requireListed(t, listDue(t, state, ctx, indexBase.Add(100*365*24*time.Hour), 10))
}

func TestDeadlineIndexNotYetDueIsNotReturned(t *testing.T) {
	ctx := context.Background()
	_, _, state := newMiniredisStore(t)
	createWithDeadline(t, state, ctx, "idx-future", indexBase.Add(time.Second))

	requireListed(t, listDue(t, state, ctx, indexBase.Add(time.Second-time.Millisecond), 10))
}

func TestDeadlineIndexDueIsReturnedWithItsNamespace(t *testing.T) {
	ns := namespace.Namespace("tenant-due")
	ctx := namespace.WithNamespace(context.Background(), ns)
	_, _, state := newMiniredisStore(t)
	deadline := indexBase.Add(1500 * time.Millisecond)
	createWithDeadline(t, state, ctx, "idx-due", deadline)

	// Equal to the deadline counts as due, and the scan needs no namespace.
	got := listDue(t, state, context.Background(), deadline, 10)
	requireListed(t, got, "idx-due")
	if got[0].Namespace != ns {
		t.Fatalf("Namespace = %q, want %q", got[0].Namespace, ns)
	}
	if !got[0].Deadline.Equal(deadline) {
		t.Fatalf("Deadline = %v, want %v to the millisecond", got[0].Deadline, deadline)
	}
}

func TestDeadlineIndexOrdersByDeadlineAndBoundsThePage(t *testing.T) {
	ctx := context.Background()
	_, _, state := newMiniredisStore(t)
	// Created out of order, with a tie at +2s that only the ID can break.
	createWithDeadline(t, state, ctx, "idx-c", indexBase.Add(3*time.Second))
	createWithDeadline(t, state, ctx, "idx-b2", indexBase.Add(2*time.Second))
	createWithDeadline(t, state, ctx, "idx-a", indexBase.Add(1*time.Second))
	createWithDeadline(t, state, ctx, "idx-b1", indexBase.Add(2*time.Second))
	createWithDeadline(t, state, ctx, "idx-e", indexBase.Add(5*time.Second))
	before := indexBase.Add(time.Minute)

	for _, limit := range []int{0, -1} {
		requireListed(t, listDue(t, state, ctx, before, limit))
	}
	requireListed(t, listDue(t, state, ctx, before, 2), "idx-a", "idx-b1")
	requireListed(t, listDue(t, state, ctx, before, 5), "idx-a", "idx-b1", "idx-b2", "idx-c", "idx-e")
	requireListed(t, listDue(t, state, ctx, before, 50), "idx-a", "idx-b1", "idx-b2", "idx-c", "idx-e")
}

func TestDeadlineIndexSpansNamespaces(t *testing.T) {
	_, _, state := newMiniredisStore(t)
	nsA := namespace.WithNamespace(context.Background(), "tenant-a")
	nsB := namespace.WithNamespace(context.Background(), "tenant-b")
	createWithDeadline(t, state, nsB, "idx-in-b", indexBase.Add(1*time.Second))
	createWithDeadline(t, state, nsA, "idx-in-a", indexBase.Add(2*time.Second))

	got := listDue(t, state, context.Background(), indexBase.Add(time.Minute), 10)
	requireListed(t, got, "idx-in-b", "idx-in-a")
	if got[0].Namespace != "tenant-b" || got[1].Namespace != "tenant-a" {
		t.Fatalf("namespaces = (%q, %q), want (tenant-b, tenant-a)", got[0].Namespace, got[1].Namespace)
	}
	// A page smaller than the union keeps the globally earliest entry, not the
	// first namespace's.
	requireListed(t, listDue(t, state, context.Background(), indexBase.Add(time.Minute), 1), "idx-in-b")
}

func TestDeadlineIndexCancelingStaysListed(t *testing.T) {
	ctx := context.Background()
	_, _, state := newMiniredisStore(t)
	createWithDeadline(t, state, ctx, "idx-canceling", indexBase.Add(time.Second))
	if err := state.UpdateExecutionStatus(ctx, "idx-canceling", types.ExecutionStatusCanceling, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus(canceling): %v", err)
	}
	// ExpireExecution answers a canceling execution with ExpireOutcomeCanceling and
	// leaves it to the cancel; the entry must survive until that cancel concludes.
	requireListed(t, listDue(t, state, ctx, indexBase.Add(time.Minute), 10), "idx-canceling")
}

func TestDeadlineIndexTimeoutKeepsTheEntryUntilAck(t *testing.T) {
	ctx := context.Background()
	_, _, state := newMiniredisStore(t)
	createWithDeadline(t, state, ctx, "idx-timeout", indexBase.Add(time.Second))
	createWithDeadline(t, state, ctx, "idx-other", indexBase.Add(2*time.Second))
	before := indexBase.Add(time.Minute)

	if err := state.UpdateExecutionStatus(ctx, "idx-timeout", types.ExecutionStatusTimeout, engine.ExecutionDeadlineExceededError); err != nil {
		t.Fatalf("UpdateExecutionStatus(timeout): %v", err)
	}
	requireListed(t, listDue(t, state, ctx, before, 10), "idx-timeout", "idx-other")

	if err := state.AckDeadlineExpired(ctx, "idx-timeout"); err != nil {
		t.Fatalf("AckDeadlineExpired: %v", err)
	}
	requireListed(t, listDue(t, state, ctx, before, 10), "idx-other")
}

func TestDeadlineIndexAckIsIdempotent(t *testing.T) {
	ctx := context.Background()
	_, _, state := newMiniredisStore(t)
	createWithDeadline(t, state, ctx, "idx-ack", indexBase.Add(time.Second))
	createWithDeadline(t, state, ctx, "idx-keep", indexBase.Add(2*time.Second))

	for i := 0; i < 2; i++ {
		if err := state.AckDeadlineExpired(ctx, "idx-ack"); err != nil {
			t.Fatalf("AckDeadlineExpired #%d: %v", i+1, err)
		}
	}
	if err := state.AckDeadlineExpired(ctx, "never-existed"); err != nil {
		t.Fatalf("AckDeadlineExpired(unknown): %v", err)
	}
	// Only that execution's entry went.
	requireListed(t, listDue(t, state, ctx, indexBase.Add(time.Minute), 10), "idx-keep")
}
