package statestoretest

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// RunCancelingExecutionContract exercises engine.CancelingExecutionLister
// against a concrete backend.
//
// The capability exists because Cancel is resumable but nothing in the request
// path can resume it after the request is gone: an execution left in canceling
// by a crash or one failed step would keep its workflow concurrency slot with
// no actor able to finish it. The reaper that fixes that needs two guarantees
// from the store, and this suite pins both:
//
//   - VISIBILITY. Every execution that reached canceling is reported, with the
//     namespace its keys live under and an age the caller can compare against
//     its stuck threshold. The register-before-write ordering inside
//     UpdateExecutionStatus is what makes this hold even when the status write
//     and the index write cannot be atomic.
//   - STABILITY. The age belongs to the first canceling transition and must not
//     move when Cancel is retried or resumed; otherwise a repeatedly retried
//     cancel would never look stuck, which is exactly the case this exists for.
//
// Two things are deliberately NOT pinned here:
//
//   - The exact timestamp. It is a wall-clock instant from the backend, so the
//     suite only checks that it is real and monotone across a re-registration.
//   - The namespace value on a backend without a namespace dimension. The
//     in-memory backend keys executions by ID alone and reports the empty
//     namespace; a namespaced backend reports the context namespace. The
//     reaper handles both (it only scopes the cancel context when the value is
//     non-empty), so the suite accepts either.
func RunCancelingExecutionContract(t *testing.T, newStore ConcurrencyStoreFactory) {
	t.Helper()
	t.Run("ReportsACancelingExecution", func(t *testing.T) {
		cancelingContractReports(t, newStore(t))
	})
	t.Run("HonorsTheStuckThreshold", func(t *testing.T) {
		cancelingContractThreshold(t, newStore(t))
	})
	t.Run("DoesNotAgeOnARepeatedCancelingWrite", func(t *testing.T) {
		cancelingContractStableAge(t, newStore(t))
	})
	t.Run("DropsTheEntryWhenTheExecutionIsCanceled", func(t *testing.T) {
		cancelingContractDropsOnCanceled(t, newStore(t))
	})
	t.Run("BoundsThePage", func(t *testing.T) {
		cancelingContractBoundsPage(t, newStore(t))
	})
}

const cancelingContractNamespace namespace.Namespace = "tenant-canceling-contract"

func cancelingContractLister(t *testing.T, state engine.StateStore) engine.CancelingExecutionLister {
	t.Helper()
	lister, ok := state.(engine.CancelingExecutionLister)
	if !ok {
		t.Fatalf("%T does not implement engine.CancelingExecutionLister", state)
	}
	return lister
}

func cancelingContractContext() context.Context {
	return namespace.WithNamespace(context.Background(), cancelingContractNamespace)
}

func cancelingContractSeed(t *testing.T, state engine.StateStore, ctx context.Context, id types.ExecutionID) {
	t.Helper()
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID:     id,
		Graph:  ContractGraph(),
		Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution(%q) error = %v", id, err)
	}
}

func cancelingContractFind(candidates []engine.CancelingExecution, id types.ExecutionID) (engine.CancelingExecution, bool) {
	for _, candidate := range candidates {
		if candidate.ExecutionID == id {
			return candidate, true
		}
	}
	return engine.CancelingExecution{}, false
}

func cancelingContractReports(t *testing.T, state engine.StateStore) {
	ctx := cancelingContractContext()
	lister := cancelingContractLister(t, state)
	started := time.Now()
	cancelingContractSeed(t, state, ctx, "exec-canceling-running")
	cancelingContractSeed(t, state, ctx, "exec-canceling-reported")

	if err := state.UpdateExecutionStatus(ctx, "exec-canceling-reported", types.ExecutionStatusCanceling, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus(canceling) error = %v", err)
	}
	// A running execution must never be reported: the reaper would call Cancel
	// on work nobody asked to cancel.
	candidates, err := lister.ListCancelingExecutions(ctx, time.Now().Add(time.Hour), 16)
	if err != nil {
		t.Fatalf("ListCancelingExecutions() error = %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("ListCancelingExecutions() = %+v, want exactly the canceling execution", candidates)
	}
	candidate := candidates[0]
	if candidate.ExecutionID != "exec-canceling-reported" {
		t.Fatalf("candidate id = %q, want exec-canceling-reported", candidate.ExecutionID)
	}
	if candidate.Namespace != "" && candidate.Namespace != cancelingContractNamespace {
		t.Fatalf("candidate namespace = %q, want %q or empty for a namespace-less backend",
			candidate.Namespace, cancelingContractNamespace)
	}
	if candidate.Since.IsZero() {
		t.Fatal("candidate Since is zero: the reaper cannot compare it against a stuck threshold")
	}
	if candidate.Since.Before(started.Add(-time.Minute)) || candidate.Since.After(time.Now().Add(time.Minute)) {
		t.Fatalf("candidate Since = %v, want an instant around the test window (%v)", candidate.Since, started)
	}
}

func cancelingContractThreshold(t *testing.T, state engine.StateStore) {
	ctx := cancelingContractContext()
	lister := cancelingContractLister(t, state)
	cancelingContractSeed(t, state, ctx, "exec-canceling-threshold")
	if err := state.UpdateExecutionStatus(ctx, "exec-canceling-threshold", types.ExecutionStatusCanceling, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus(canceling) error = %v", err)
	}

	// A threshold older than the registration must not report it: a cancel that
	// is still inside the grace period is not stuck.
	candidates, err := lister.ListCancelingExecutions(ctx, time.Now().Add(-time.Hour), 16)
	if err != nil {
		t.Fatalf("ListCancelingExecutions(past) error = %v", err)
	}
	if len(candidates) != 0 {
		t.Fatalf("ListCancelingExecutions(past) = %+v, want none inside the grace period", candidates)
	}

	// A threshold after it must. One second of slack absorbs millisecond
	// truncation on the distributed backend.
	candidates, err = lister.ListCancelingExecutions(ctx, time.Now().Add(time.Second), 16)
	if err != nil {
		t.Fatalf("ListCancelingExecutions(future) error = %v", err)
	}
	if _, ok := cancelingContractFind(candidates, "exec-canceling-threshold"); !ok {
		t.Fatalf("ListCancelingExecutions(future) = %+v, want the canceling execution", candidates)
	}
}

func cancelingContractStableAge(t *testing.T, state engine.StateStore) {
	ctx := cancelingContractContext()
	lister := cancelingContractLister(t, state)
	cancelingContractSeed(t, state, ctx, "exec-canceling-stable")
	if err := state.UpdateExecutionStatus(ctx, "exec-canceling-stable", types.ExecutionStatusCanceling, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus(canceling) error = %v", err)
	}
	first, err := lister.ListCancelingExecutions(ctx, time.Now().Add(time.Hour), 16)
	if err != nil {
		t.Fatalf("ListCancelingExecutions() error = %v", err)
	}
	before, ok := cancelingContractFind(first, "exec-canceling-stable")
	if !ok {
		t.Fatalf("ListCancelingExecutions() = %+v, want the canceling execution", first)
	}

	// Leave a visible gap so a re-stamped score would differ even at the
	// distributed backend's millisecond precision.
	time.Sleep(10 * time.Millisecond)
	if err := state.UpdateExecutionStatus(ctx, "exec-canceling-stable", types.ExecutionStatusCanceling, ""); err != nil {
		t.Fatalf("repeated UpdateExecutionStatus(canceling) error = %v", err)
	}
	second, err := lister.ListCancelingExecutions(ctx, time.Now().Add(time.Hour), 16)
	if err != nil {
		t.Fatalf("ListCancelingExecutions() error = %v", err)
	}
	after, ok := cancelingContractFind(second, "exec-canceling-stable")
	if !ok {
		t.Fatalf("ListCancelingExecutions() after re-registration = %+v, want the canceling execution", second)
	}
	if !after.Since.Equal(before.Since) {
		t.Fatalf("Since moved on a repeated canceling write: before=%v after=%v. "+
			"A retried Cancel must not reset the stuck age, or the reaper never fires.", before.Since, after.Since)
	}
}

func cancelingContractDropsOnCanceled(t *testing.T, state engine.StateStore) {
	ctx := cancelingContractContext()
	lister := cancelingContractLister(t, state)
	cancelingContractSeed(t, state, ctx, "exec-canceling-finished")
	if err := state.UpdateExecutionStatus(ctx, "exec-canceling-finished", types.ExecutionStatusCanceling, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus(canceling) error = %v", err)
	}
	listed, err := lister.ListCancelingExecutions(ctx, time.Now().Add(time.Hour), 16)
	if err != nil {
		t.Fatalf("ListCancelingExecutions() error = %v", err)
	}
	if _, ok := cancelingContractFind(listed, "exec-canceling-finished"); !ok {
		t.Fatalf("ListCancelingExecutions() = %+v, want the canceling execution", listed)
	}

	if err := state.UpdateExecutionStatus(ctx, "exec-canceling-finished", types.ExecutionStatusCanceled, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus(canceled) error = %v", err)
	}
	listed, err = lister.ListCancelingExecutions(ctx, time.Now().Add(time.Hour), 16)
	if err != nil {
		t.Fatalf("ListCancelingExecutions() error = %v", err)
	}
	if _, ok := cancelingContractFind(listed, "exec-canceling-finished"); ok {
		t.Fatalf("ListCancelingExecutions() still reports a canceled execution: %+v", listed)
	}
}

func cancelingContractBoundsPage(t *testing.T, state engine.StateStore) {
	ctx := cancelingContractContext()
	lister := cancelingContractLister(t, state)
	ids := []types.ExecutionID{"exec-canceling-page-1", "exec-canceling-page-2", "exec-canceling-page-3"}
	for _, id := range ids {
		cancelingContractSeed(t, state, ctx, id)
		if err := state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusCanceling, ""); err != nil {
			t.Fatalf("UpdateExecutionStatus(canceling, %q) error = %v", id, err)
		}
	}

	page, err := lister.ListCancelingExecutions(ctx, time.Now().Add(time.Hour), 2)
	if err != nil {
		t.Fatalf("ListCancelingExecutions(limit=2) error = %v", err)
	}
	if len(page) != 2 {
		t.Fatalf("ListCancelingExecutions(limit=2) returned %d entries, want 2", len(page))
	}
	empty, err := lister.ListCancelingExecutions(ctx, time.Now().Add(time.Hour), 0)
	if err != nil {
		t.Fatalf("ListCancelingExecutions(limit=0) error = %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("ListCancelingExecutions(limit=0) returned %d entries, want 0", len(empty))
	}
}
