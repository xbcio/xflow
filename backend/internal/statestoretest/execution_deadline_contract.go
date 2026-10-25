package statestoretest

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// RunExecutionDeadlineContract exercises engine.ExecutionDeadlineExpirer
// against a concrete backend.
//
// The capability exists because a settings.timeout deadline is only noticed by
// a task holding a lease. An execution can sit past its deadline with no lease
// at all (every task finished and the next never scheduled, the only runner
// died, the execution is parked or queued), and a crash between
// ExpireExecution's timeout write and its cascade leaves a timed-out execution
// whose nodes are still active. The leader-driven sweep that repairs both needs
// four guarantees from the store, and this suite pins them:
//
//   - REGISTRATION. An execution created with a non-zero Deadline is listed once
//     the deadline is reached, whichever creation path made it; one created with
//     the zero Deadline (no settings.timeout) is never listed.
//   - THE PAGE. At most limit entries come back, the earliest deadline first and
//     ties by ID, across every namespace the backend serves, with the caller
//     choosing the instant. The store never reads its own clock, so no case here
//     sleeps.
//   - WITHDRAWAL. success, failed and canceled drop the entry in the write that
//     concludes the execution. timeout keeps it until AckDeadlineExpired, because
//     the cascade that stops the remaining nodes runs after the timeout write and
//     a sweep must keep seeing the execution until the caller says it finished.
//     canceling is not terminal and stays listed.
//   - ACKNOWLEDGEMENT. Ack is idempotent and withdraws only the execution it
//     names.
//
// Deliberately NOT pinned here:
//
//   - The instant's precision. Redis stores milliseconds and the in-memory
//     backend keeps nanoseconds, so every comparison is on UnixMilli.
//   - The namespace value on a backend without a namespace dimension. The
//     in-memory backend reports the empty namespace, a namespaced backend the
//     namespace the execution's keys live under; the sweep scopes its context
//     only when the value is non-empty, so either is accepted.
//   - The repair of stale entries (an entry whose execution is gone or already
//     concluded). That works through backend internals and is covered by each
//     backend's own tests.
func RunExecutionDeadlineContract(t *testing.T, manifest *Manifest, newStore ConcurrencyStoreFactory) {
	t.Helper()
	manifest.Report(t)
	manifest.RequireDeclared(t, CapabilityExecutionDeadlineExpirer)
	t.Run("ZeroDeadlineIsNeverRegistered", func(t *testing.T) {
		deadlineZeroIsNeverRegistered(t, newStore(t))
	})
	t.Run("NotYetDueIsNotReturned", func(t *testing.T) {
		deadlineNotYetDueIsNotReturned(t, newStore(t))
	})
	t.Run("DueIsReturnedWithItsNamespace", func(t *testing.T) {
		deadlineDueIsReturnedWithItsNamespace(t, newStore(t))
	})
	t.Run("OrdersByDeadlineAndBoundsThePage", func(t *testing.T) {
		deadlineOrdersAndBoundsThePage(t, newStore(t))
	})
	t.Run("SpansNamespaces", func(t *testing.T) {
		deadlineSpansNamespaces(t, newStore(t))
	})
	t.Run("TerminalStatusWithdrawsTheEntry", func(t *testing.T) {
		deadlineTerminalStatusWithdrawsTheEntry(t, newStore)
	})
	t.Run("CommitCompletionWithdrawsTheEntry", func(t *testing.T) {
		deadlineCommitCompletionWithdrawsTheEntry(t, manifest, newStore(t))
	})
	t.Run("TimeoutKeepsTheEntryUntilAck", func(t *testing.T) {
		deadlineTimeoutKeepsTheEntryUntilAck(t, newStore(t))
	})
	t.Run("AckIsIdempotent", func(t *testing.T) {
		deadlineAckIsIdempotent(t, newStore(t))
	})
	t.Run("AckOnlyWithdrawsThatExecution", func(t *testing.T) {
		deadlineAckOnlyWithdrawsThatExecution(t, newStore(t))
	})
	t.Run("CancelingStaysListed", func(t *testing.T) {
		deadlineCancelingStaysListed(t, newStore(t))
	})
	t.Run("ExpiresAQueuedExecution", func(t *testing.T) {
		deadlineExpiresAQueuedExecution(t, manifest, newStore(t))
	})
	t.Run("PersistsTheDeadlineAcrossCreateEntrypoints", func(t *testing.T) {
		deadlinePersistsAcrossCreateEntrypoints(t, manifest, newStore)
	})
}

// deadlineContractBase is the fixed instant the cases measure from. The store
// only compares against the before it is handed, so a deadline far in the
// future of the wall clock is as good as any and no case reads the clock.
var deadlineContractBase = time.Date(2030, 1, 1, 12, 0, 0, 0, time.UTC)

const (
	deadlineContractNamespaceA namespace.Namespace = "tenant-deadline-a"
	deadlineContractNamespaceB namespace.Namespace = "tenant-deadline-b"
)

func deadlineContractExpirer(t *testing.T, state engine.StateStore) engine.ExecutionDeadlineExpirer {
	t.Helper()
	expirer, ok := state.(engine.ExecutionDeadlineExpirer)
	if !ok {
		t.Fatalf("%T does not implement engine.ExecutionDeadlineExpirer", state)
	}
	return expirer
}

// deadlineContractCreate creates a running execution carrying the deadline. The
// engine stamps the deadline before the store sees the snapshot, so setting it
// on the snapshot is how a case creates an execution with a settings.timeout;
// the zero time is the workflow that declares none.
func deadlineContractCreate(t *testing.T, state engine.StateStore, ctx context.Context, id types.ExecutionID, deadline time.Time) {
	t.Helper()
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID:       id,
		Graph:    ContractGraph(),
		Status:   types.ExecutionStatusRunning,
		Deadline: deadline,
	}); err != nil {
		t.Fatalf("CreateExecution(%q) error = %v", id, err)
	}
}

// deadlineContractList asks for the entries due at before with a context that
// names no namespace: the sweep spans every namespace the backend serves and
// has none of its own.
func deadlineContractList(t *testing.T, state engine.StateStore, before time.Time, limit int) []engine.DeadlineExpiredExecution {
	t.Helper()
	got, err := deadlineContractExpirer(t, state).ListDeadlineExceededExecutions(context.Background(), before, limit)
	if err != nil {
		t.Fatalf("ListDeadlineExceededExecutions(%v, %d) error = %v", before, limit, err)
	}
	return got
}

func deadlineContractAck(t *testing.T, state engine.StateStore, ctx context.Context, id types.ExecutionID) {
	t.Helper()
	if err := deadlineContractExpirer(t, state).AckDeadlineExpired(ctx, id); err != nil {
		t.Fatalf("AckDeadlineExpired(%q) error = %v", id, err)
	}
}

func deadlineContractSetStatus(t *testing.T, state engine.StateStore, ctx context.Context, id types.ExecutionID, status types.ExecutionStatus) {
	t.Helper()
	reason := ""
	if status == types.ExecutionStatusTimeout {
		// A timeout always carries a reason; the deadline's own is what
		// ExpireExecution writes.
		reason = engine.ExecutionDeadlineExceededError
	}
	if err := state.UpdateExecutionStatus(ctx, id, status, reason); err != nil {
		t.Fatalf("UpdateExecutionStatus(%q, %s) error = %v", id, status, err)
	}
}

// deadlineContractRequireIDs fails unless the listing is exactly want, in
// order. An empty want means "nothing is due".
func deadlineContractRequireIDs(t *testing.T, got []engine.DeadlineExpiredExecution, want ...types.ExecutionID) {
	t.Helper()
	ids := make([]types.ExecutionID, 0, len(got))
	for _, item := range got {
		ids = append(ids, item.ExecutionID)
	}
	if len(ids) != len(want) {
		t.Fatalf("listed = %v, want %v", ids, want)
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Fatalf("listed = %v, want %v", ids, want)
		}
	}
}

// deadlineContractRequireNamespace accepts the empty namespace of a backend
// without a namespace dimension, or exactly the namespace the execution was
// created under.
func deadlineContractRequireNamespace(t *testing.T, item engine.DeadlineExpiredExecution, want namespace.Namespace) {
	t.Helper()
	if item.Namespace != "" && item.Namespace != want {
		t.Fatalf("%q namespace = %q, want %q or empty for a namespace-less backend",
			item.ExecutionID, item.Namespace, want)
	}
}

// deadlineContractRequireSameInstant compares two instants at the millisecond,
// the finest grain every backend keeps.
func deadlineContractRequireSameInstant(t *testing.T, what string, got, want time.Time) {
	t.Helper()
	if got.IsZero() {
		t.Fatalf("%s is the zero time, want %v", what, want)
	}
	if got.UnixMilli() != want.UnixMilli() {
		t.Fatalf("%s = %v, want %v (compared at the millisecond)", what, got, want)
	}
}

// deadlineZeroIsNeverRegistered pins the no-budget case: a workflow without
// settings.timeout stamps no deadline, and listing at an instant a century away
// must still not report it. A second execution with a deadline keeps the case
// from passing on a store that lists nothing at all.
func deadlineZeroIsNeverRegistered(t *testing.T, state engine.StateStore) {
	ctx := context.Background()
	deadlineContractCreate(t, state, ctx, "exec-deadline-zero", time.Time{})
	deadlineContractCreate(t, state, ctx, "exec-deadline-set", deadlineContractBase)

	deadlineContractRequireIDs(t,
		deadlineContractList(t, state, deadlineContractBase.AddDate(100, 0, 0), 8),
		"exec-deadline-set")
}

func deadlineNotYetDueIsNotReturned(t *testing.T, state engine.StateStore) {
	ctx := context.Background()
	deadlineContractCreate(t, state, ctx, "exec-deadline-future", deadlineContractBase.Add(time.Second))

	deadlineContractRequireIDs(t, deadlineContractList(t, state, deadlineContractBase, 8))
	// One millisecond short of the deadline is still not due.
	deadlineContractRequireIDs(t,
		deadlineContractList(t, state, deadlineContractBase.Add(time.Second-time.Millisecond), 8))
}

// deadlineDueIsReturnedWithItsNamespace pins the inclusive boundary (a deadline
// equal to before is due) and what a returned entry carries: the execution, the
// instant the backend registered, and the namespace the sweep scopes its call
// to.
func deadlineDueIsReturnedWithItsNamespace(t *testing.T, state engine.StateStore) {
	ctx := namespace.WithNamespace(context.Background(), deadlineContractNamespaceA)
	deadlineContractCreate(t, state, ctx, "exec-deadline-due", deadlineContractBase)

	got := deadlineContractList(t, state, deadlineContractBase, 8)
	deadlineContractRequireIDs(t, got, "exec-deadline-due")
	deadlineContractRequireSameInstant(t, "listed Deadline", got[0].Deadline, deadlineContractBase)
	deadlineContractRequireNamespace(t, got[0], deadlineContractNamespaceA)
}

// deadlineOrdersAndBoundsThePage pins the page: limit <= 0 is nothing, limit
// cuts the listing, and the order is the deadline with equal deadlines by ID,
// so the sweep always starts from the longest-overdue execution.
func deadlineOrdersAndBoundsThePage(t *testing.T, state engine.StateStore) {
	ctx := context.Background()
	deadlineContractCreate(t, state, ctx, "exec-c", deadlineContractBase.Add(2*time.Second))
	deadlineContractCreate(t, state, ctx, "exec-b", deadlineContractBase.Add(time.Second))
	deadlineContractCreate(t, state, ctx, "exec-a", deadlineContractBase.Add(time.Second))
	deadlineContractCreate(t, state, ctx, "exec-e", deadlineContractBase.Add(4*time.Second))
	deadlineContractCreate(t, state, ctx, "exec-d", deadlineContractBase.Add(3*time.Second))
	before := deadlineContractBase.Add(time.Hour)

	deadlineContractRequireIDs(t, deadlineContractList(t, state, before, 0))
	deadlineContractRequireIDs(t, deadlineContractList(t, state, before, -1))
	deadlineContractRequireIDs(t, deadlineContractList(t, state, before, 2), "exec-a", "exec-b")
	deadlineContractRequireIDs(t, deadlineContractList(t, state, before, 5),
		"exec-a", "exec-b", "exec-c", "exec-d", "exec-e")
	deadlineContractRequireIDs(t, deadlineContractList(t, state, before, 50),
		"exec-a", "exec-b", "exec-c", "exec-d", "exec-e")
}

// deadlineSpansNamespaces pins the cross-namespace scan with the page cut after
// the merge. The older entry sits in the namespace that sorts later, so a store
// that took its first namespace's entries before ordering would return the
// wrong one for limit 1.
func deadlineSpansNamespaces(t *testing.T, state engine.StateStore) {
	ctxA := namespace.WithNamespace(context.Background(), deadlineContractNamespaceA)
	ctxB := namespace.WithNamespace(context.Background(), deadlineContractNamespaceB)
	deadlineContractCreate(t, state, ctxA, "exec-deadline-ns-a", deadlineContractBase.Add(time.Second))
	deadlineContractCreate(t, state, ctxB, "exec-deadline-ns-b", deadlineContractBase)
	before := deadlineContractBase.Add(time.Hour)

	got := deadlineContractList(t, state, before, 8)
	deadlineContractRequireIDs(t, got, "exec-deadline-ns-b", "exec-deadline-ns-a")
	deadlineContractRequireNamespace(t, got[0], deadlineContractNamespaceB)
	deadlineContractRequireNamespace(t, got[1], deadlineContractNamespaceA)

	deadlineContractRequireIDs(t, deadlineContractList(t, state, before, 1), "exec-deadline-ns-b")
}

// deadlineContractAckListed acknowledges a listed entry the way the sweep does:
// under the namespace the listing reported, when it reported one. A namespaced
// backend withdraws from the index of the context's namespace, so the sweep
// scopes the call to the candidate and so does this.
func deadlineContractAckListed(t *testing.T, state engine.StateStore, item engine.DeadlineExpiredExecution) {
	t.Helper()
	ctx := context.Background()
	if item.Namespace != "" {
		ctx = namespace.WithNamespace(ctx, item.Namespace)
	}
	deadlineContractAck(t, state, ctx, item.ExecutionID)
}

// deadlineTerminalStatusWithdrawsTheEntry pins the three statuses that conclude
// an execution for good: the write that records one drops the entry, because
// there is nothing left to expire and a sweep would only inspect it again.
func deadlineTerminalStatusWithdrawsTheEntry(t *testing.T, newStore ConcurrencyStoreFactory) {
	for _, status := range []types.ExecutionStatus{
		types.ExecutionStatusSuccess,
		types.ExecutionStatusFailed,
		types.ExecutionStatusCanceled,
	} {
		t.Run(string(status), func(t *testing.T) {
			state := newStore(t)
			ctx := context.Background()
			deadlineContractCreate(t, state, ctx, "exec-deadline-terminal", deadlineContractBase)
			deadlineContractRequireIDs(t,
				deadlineContractList(t, state, deadlineContractBase, 8), "exec-deadline-terminal")

			deadlineContractSetStatus(t, state, ctx, "exec-deadline-terminal", status)

			deadlineContractRequireIDs(t, deadlineContractList(t, state, deadlineContractBase.Add(time.Hour), 8))
		})
	}
}

// deadlineCommitCompletionWithdrawsTheEntry drives the terminal write through
// the commit that concludes an execution instead of a status write. A store has
// more than one terminal writer, and the commit path is the one a healthy
// execution ends through, so a withdrawal wired only into the status write would
// leave every successful run listed until a sweep repaired it.
func deadlineCommitCompletionWithdrawsTheEntry(t *testing.T, manifest *Manifest, state engine.StateStore) {
	atomic, ok := OptionalCapability[engine.AtomicStateStore](t, manifest, CapabilityAtomicStateStore, state)
	if !ok {
		return
	}
	ctx := context.Background()
	g := concurrencyGraph(t, "deadline-commit")
	snap, entries := concurrencySnapshot(t, g)
	snap.Deadline = deadlineContractBase
	if err := atomic.CreateExecutionWithOutbox(ctx, snap, entries); err != nil {
		t.Fatalf("CreateExecutionWithOutbox() error = %v", err)
	}
	deadlineContractRequireIDs(t, deadlineContractList(t, state, deadlineContractBase, 8), snap.ID)

	lease := &engine.TaskLease{
		LeaseID:    engine.LeaseID("deadline-lease"),
		LeaseToken: engine.LeaseToken("deadline-token"),
		Attempt:    1,
		Task:       entries[0].Task,
		IssuedAt:   time.Now().UTC(),
		TTL:        time.Minute,
	}
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease() acquired=%v err=%v, want true/nil", acquired, err)
	}
	res, err := atomic.CommitNode(ctx, engine.CommitNodeRequest{
		ExecutionID: snap.ID,
		NodeName:    "start",
		NodeIdx:     0,
		UnitIdx:     0,
		LeaseID:     lease.LeaseID,
		LeaseToken:  lease.LeaseToken,
		Attempt:     1,
		Status:      types.NodeStatusSuccess,
	})
	if err != nil {
		t.Fatalf("CommitNode() error = %v", err)
	}
	if !res.ExecutionDone {
		t.Fatalf("CommitNode() = %+v, want the one-unit execution done", res)
	}
	deadlineContractRequireIDs(t, deadlineContractList(t, state, deadlineContractBase.Add(time.Hour), 8))
}

// deadlineTimeoutKeepsTheEntryUntilAck pins the one terminal status that does
// not withdraw. The cascade that stops the execution's nodes runs after the
// timeout write, so the entry must stay discoverable until the caller confirms
// the cascade finished; a listing is not that confirmation.
func deadlineTimeoutKeepsTheEntryUntilAck(t *testing.T, state engine.StateStore) {
	ctx := namespace.WithNamespace(context.Background(), deadlineContractNamespaceA)
	deadlineContractCreate(t, state, ctx, "exec-deadline-timeout", deadlineContractBase)

	deadlineContractSetStatus(t, state, ctx, "exec-deadline-timeout", types.ExecutionStatusTimeout)

	got := deadlineContractList(t, state, deadlineContractBase, 8)
	deadlineContractRequireIDs(t, got, "exec-deadline-timeout")
	again := deadlineContractList(t, state, deadlineContractBase, 8)
	deadlineContractRequireIDs(t, again, "exec-deadline-timeout")

	deadlineContractAckListed(t, state, got[0])
	deadlineContractRequireIDs(t, deadlineContractList(t, state, deadlineContractBase.Add(time.Hour), 8))
}

// deadlineAckIsIdempotent pins the acknowledgement the sweep sends after every
// cascade, including the one it repeats after a crash: a second ack, an ack for
// an execution that was never registered, and an ack for one whose entry a
// conclusion already withdrew all succeed.
func deadlineAckIsIdempotent(t *testing.T, state engine.StateStore) {
	ctx := context.Background()
	deadlineContractCreate(t, state, ctx, "exec-deadline-ack", deadlineContractBase)
	deadlineContractCreate(t, state, ctx, "exec-deadline-unregistered", time.Time{})
	deadlineContractCreate(t, state, ctx, "exec-deadline-concluded", deadlineContractBase)
	deadlineContractSetStatus(t, state, ctx, "exec-deadline-concluded", types.ExecutionStatusSuccess)

	deadlineContractAck(t, state, ctx, "exec-deadline-ack")
	deadlineContractAck(t, state, ctx, "exec-deadline-ack")
	deadlineContractAck(t, state, ctx, "exec-deadline-unregistered")
	deadlineContractAck(t, state, ctx, "exec-deadline-concluded")
	deadlineContractAck(t, state, ctx, "exec-deadline-unknown")

	deadlineContractRequireIDs(t, deadlineContractList(t, state, deadlineContractBase.Add(time.Hour), 8))
}

// deadlineAckOnlyWithdrawsThatExecution guards the acknowledgement against
// taking its neighbours with it: only the named execution leaves the listing.
func deadlineAckOnlyWithdrawsThatExecution(t *testing.T, state engine.StateStore) {
	ctx := context.Background()
	deadlineContractCreate(t, state, ctx, "exec-deadline-one", deadlineContractBase)
	deadlineContractCreate(t, state, ctx, "exec-deadline-two", deadlineContractBase.Add(time.Second))

	deadlineContractAck(t, state, ctx, "exec-deadline-one")

	deadlineContractRequireIDs(t,
		deadlineContractList(t, state, deadlineContractBase.Add(time.Hour), 8), "exec-deadline-two")
}

// deadlineCancelingStaysListed pins that canceling is not terminal. ExpireExecution
// answers a canceling execution with ExpireOutcomeCanceling and leaves it to the
// cancel, so the entry has to survive until the cancel's own terminal write
// withdraws it; a store that dropped it at canceling would leave an execution
// whose cancel then failed with no deadline left to enforce.
func deadlineCancelingStaysListed(t *testing.T, state engine.StateStore) {
	ctx := context.Background()
	deadlineContractCreate(t, state, ctx, "exec-deadline-canceling", deadlineContractBase)

	deadlineContractSetStatus(t, state, ctx, "exec-deadline-canceling", types.ExecutionStatusCanceling)
	deadlineContractRequireIDs(t,
		deadlineContractList(t, state, deadlineContractBase, 8), "exec-deadline-canceling")

	deadlineContractSetStatus(t, state, ctx, "exec-deadline-canceling", types.ExecutionStatusCanceled)
	deadlineContractRequireIDs(t, deadlineContractList(t, state, deadlineContractBase.Add(time.Hour), 8))
}

// deadlineExpiresAQueuedExecution pins the case the sweep exists for beyond a
// dead runner: an execution waiting for a concurrency slot has no lease and no
// task, so only the index can notice its deadline passing. It is reported when
// due; writing it timeout takes it out of the line without promoting it, and
// the entry stays until acknowledged, exactly as for a running execution.
func deadlineExpiresAQueuedExecution(t *testing.T, manifest *Manifest, state engine.StateStore) {
	if !manifest.SkipUnlessDeclared(t, CapabilityConcurrencyQueueMode) {
		return
	}
	ctx := context.Background()
	g := concurrencyGraph(t, "deadline-queued")
	admission := queueAdmissionFor(t, g, 1, 0, 0)

	holder, _ := queueSubmit(t, state, ctx, g, admission)
	limited := concurrencyCapability(t, state)
	queuedSnap, queuedEntries := concurrencySnapshot(t, g)
	queuedSnap.Deadline = deadlineContractBase
	res, err := limited.CreateExecutionWithConcurrency(ctx, queuedSnap, queuedEntries, admission)
	if err != nil {
		t.Fatalf("queued submission error = %v", err)
	}
	if !res.Queued {
		t.Fatalf("over-limit submission = %+v, want it parked", res)
	}
	queued := queuedSnap.ID
	if got := executionStatus(t, state, ctx, queued); got != types.ExecutionStatusQueued {
		t.Fatalf("parked execution status = %q, want %q", got, types.ExecutionStatusQueued)
	}

	// Due, though nothing is running for it. The holder carries no deadline and
	// is not listed.
	deadlineContractRequireIDs(t, deadlineContractList(t, state, deadlineContractBase, 8), queued)

	deadlineContractSetStatus(t, state, ctx, queued, types.ExecutionStatusTimeout)
	if inspector, ok := state.(engine.ConcurrencyQueueInspector); ok {
		if got := queuePosition(t, inspector, ctx, queued); got != 0 {
			t.Fatalf("position after the timeout = %d, want 0: the execution left the line", got)
		}
	}
	deadlineContractRequireIDs(t, deadlineContractList(t, state, deadlineContractBase, 8), queued)

	// The expired execution is out of the line, so releasing the holder
	// promotes nobody, and the entry is still waiting for its acknowledgement.
	if err := state.UpdateExecutionStatus(ctx, holder, types.ExecutionStatusSuccess, ""); err != nil {
		t.Fatalf("release holder error = %v", err)
	}
	if got := executionStatus(t, state, ctx, queued); got != types.ExecutionStatusTimeout {
		t.Fatalf("timed-out queued execution after the holder released = %q, want it still %q",
			got, types.ExecutionStatusTimeout)
	}
	deadlineContractRequireIDs(t, deadlineContractList(t, state, deadlineContractBase, 8), queued)

	deadlineContractAck(t, state, ctx, queued)
	deadlineContractRequireIDs(t, deadlineContractList(t, state, deadlineContractBase.Add(time.Hour), 8))
}

// deadlinePersistsAcrossCreateEntrypoints pins that every way of creating an
// execution carries its Deadline through to GetExecution and registers it. The
// failure it guards is silent: a path that drops the field reads back the zero
// time, which means "no budget", so the execution is never expired and nothing
// reports it. Redis stores milliseconds and the in-memory backend keeps
// nanoseconds, so the instants are compared at the millisecond.
//
// The entry-seed paths are covered where they build their own snapshot, in the
// entry-admission contract.
func deadlinePersistsAcrossCreateEntrypoints(t *testing.T, manifest *Manifest, newStore ConcurrencyStoreFactory) {
	// A deadline that is not a whole second, so truncating to seconds fails the
	// comparison instead of passing it.
	deadline := deadlineContractBase.Add(1500 * time.Millisecond)
	check := func(t *testing.T, state engine.StateStore, id types.ExecutionID) {
		t.Helper()
		loaded, err := state.GetExecution(context.Background(), id)
		if err != nil || loaded == nil {
			t.Fatalf("GetExecution(%q) = (%v, %v), want the created execution", id, loaded, err)
		}
		deadlineContractRequireSameInstant(t, "read-back Deadline", loaded.Deadline, deadline)
		deadlineContractRequireIDs(t, deadlineContractList(t, state, deadline, 8), id)
	}

	t.Run("CreateExecution", func(t *testing.T) {
		state := newStore(t)
		snap := &engine.ExecutionSnapshot{
			ID: engine.NewExecutionID(), Graph: ContractGraph(),
			Status: types.ExecutionStatusRunning, Deadline: deadline,
		}
		if err := state.CreateExecution(context.Background(), snap); err != nil {
			t.Fatalf("CreateExecution() error = %v", err)
		}
		check(t, state, snap.ID)
	})
	t.Run("CreateExecutionWithOutbox", func(t *testing.T) {
		state := newStore(t)
		atomic, ok := OptionalCapability[engine.AtomicStateStore](t, manifest, CapabilityAtomicStateStore, state)
		if !ok {
			return
		}
		snap, entries := concurrencySnapshot(t, concurrencyGraph(t, "deadline-outbox"))
		snap.Deadline = deadline
		if err := atomic.CreateExecutionWithOutbox(context.Background(), snap, entries); err != nil {
			t.Fatalf("CreateExecutionWithOutbox() error = %v", err)
		}
		check(t, state, snap.ID)
	})
	t.Run("CreateExecutionWithConcurrency", func(t *testing.T) {
		state := newStore(t)
		if !manifest.SkipUnlessDeclared(t, CapabilityConcurrencyLimitedStateStore) {
			return
		}
		g := concurrencyGraph(t, "deadline-concurrency")
		snap, entries := concurrencySnapshot(t, g)
		snap.Deadline = deadline
		res, err := concurrencyCapability(t, state).CreateExecutionWithConcurrency(
			context.Background(), snap, entries, concurrencyAdmissionFor(t, g, 1))
		if err != nil || res.Queued {
			t.Fatalf("CreateExecutionWithConcurrency() = (%+v, %v), want an admitted execution", res, err)
		}
		check(t, state, snap.ID)
	})
	t.Run("SeedExecution", func(t *testing.T) {
		state := newStore(t)
		if !manifest.SkipUnlessDeclared(t, CapabilityExecutionSeeder) {
			return
		}
		g := seedContractGraph(t, "deadline-seed")
		snap := seedSnapshot(t, g)
		snap.Deadline = deadline
		if _, err := seedCapability(t, state).SeedExecution(context.Background(), engine.SeedExecutionRequest{
			Execution: snap,
			Units:     []engine.SeedUnit{seedUnitResolving(t, g, "start", "main", map[string]any{"v": 1}, "mid")},
		}); err != nil {
			t.Fatalf("SeedExecution() error = %v", err)
		}
		check(t, state, snap.ID)
	})
}
