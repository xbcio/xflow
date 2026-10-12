package statestoretest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// RunExecutionPauseContract exercises engine.ExecutionPauser against a concrete
// backend.
//
// A pause is a DISPATCH HOLD, and the contract is about the two halves of that
// word.
//
// HOLD: while an execution is paused, none of its ready work may be delivered —
// not the intents that were already due when the pause landed, not the ones an
// in-flight commit appends afterwards, and not at any horizon a caller asks
// with. A backend that wrote the status and nothing else would leave a paused
// execution running at full speed, because the delivery path reads nothing else
// about the execution; the status is only load-bearing where delivery is
// granted.
//
// KEEP: the held work has to survive intact. A resume must make the same
// intents deliverable again, each with the deadline it had, because "paused"
// means suspended rather than discarded, and because a resume that released
// every intent to "now" would fire a timer an operator scheduled for later.
//
// The enforcement point is the dispatch transaction, which is why this is a
// store contract rather than an engine test: a check made before calling
// LeaseOutbox races the pause itself (see engine.ExecutionPauser), so the
// backend that grants the claim is the only one that can refuse it atomically
// with the pause. Every case here therefore drives the store directly.
//
// Three things are deliberately NOT pinned:
//
//   - Execution events. Neither backend publishes one for a pause or a resume,
//     and no reader treats a non-terminal event as a completion.
//   - Discovery (ListOutboxExecutions). A paused execution may or may not be
//     reported as having work: a caller that ignores it is correct, and a
//     discovery-driven flush that finds nothing is cheap. What must not happen
//     is work being DELIVERED, which the cases below pin instead.
//   - The status a pause writes beyond GetExecution. The workflow-slot case
//     pins the one read that matters to admission; nothing else reads "paused"
//     except the operator.
func RunExecutionPauseContract(t *testing.T, newStore ConcurrencyStoreFactory) {
	t.Helper()
	t.Run("HoldsTheReadySet", func(t *testing.T) {
		pauseHoldsTheReadySet(t, newStore(t))
	})
	t.Run("ReleasesTheReadySetOnResume", func(t *testing.T) {
		pauseReleasesTheReadySetOnResume(t, newStore(t))
	})
	t.Run("KeepsADelayedIntentAtItsOwnDeadline", func(t *testing.T) {
		pauseKeepsADelayedIntentAtItsOwnDeadline(t, newStore(t))
	})
	t.Run("IsIdempotentForARepeatedPause", func(t *testing.T) {
		pauseIsIdempotentForARepeatedPause(t, newStore(t))
	})
	t.Run("HoldsWorkAnInFlightCommitAppends", func(t *testing.T) {
		pauseHoldsWorkAnInFlightCommitAppends(t, newStore(t))
	})
	t.Run("KeepsTheWorkflowSlot", func(t *testing.T) {
		pauseKeepsTheWorkflowSlot(t, newStore(t))
	})
	t.Run("AcceptsATerminalWriteWhilePaused", func(t *testing.T) {
		pauseAcceptsATerminalWriteWhilePaused(t, newStore(t))
	})
	t.Run("RefusesAnExecutionThatIsNotRunning", func(t *testing.T) {
		pauseRefusesAnExecutionThatIsNotRunning(t, newStore(t))
	})
	t.Run("RefusesAnAbsentExecution", func(t *testing.T) {
		pauseRefusesAnAbsentExecution(t, newStore(t))
	})
	t.Run("RefusesToResumeWhatIsNotPaused", func(t *testing.T) {
		pauseRefusesToResumeWhatIsNotPaused(t, newStore(t))
	})
}

// pauseCapability is the store's pause capability. A missing one is a failure
// rather than a skip: a backend the engine can submit to but never pause is a
// backend whose long-running executions can only be cancelled, which is the
// asymmetry this capability exists to remove.
func pauseCapability(t *testing.T, state engine.StateStore) engine.ExecutionPauser {
	t.Helper()
	pauser, ok := state.(engine.ExecutionPauser)
	if !ok {
		t.Fatalf("state store %T does not implement engine.ExecutionPauser", state)
	}
	return pauser
}

// pauseIntents builds the intents a pause case contends over: a root delivery
// that is due immediately, and — when delayedAt is non-zero — a second intent
// that is not due until then. The pair is what separates "held" from "released
// early": a resume has to free the first and leave the second alone.
//
// The delayed one is spelled as a retry so the two are not the same intent
// twice. No store is expected to care — the case is about a deadline, not about
// what an intent means — but two identical intents would make a failure
// ambiguous between "lost one" and "double-counted one".
//
// delayedAt has to be millisecond-precision, because a distributed backend
// stores availability as a millisecond score and reads it back. A nanosecond
// deadline would come back truncated and the case would fail on a rounding the
// contract has no opinion about.
func pauseIntents(id types.ExecutionID, delayedAt time.Time) []engine.OutboxEntry {
	task := engine.Task{
		ExecutionID: id,
		NodeName:    "start",
		NodeIdx:     0,
		UnitIdx:     0,
		Type:        engine.TaskTypeNodeExec,
	}
	entries := []engine.OutboxEntry{{
		ID:   "root/" + string(id) + "/start/0",
		Task: task,
	}}
	if delayedAt.IsZero() {
		return entries
	}
	return append(entries, engine.OutboxEntry{
		ID:          "retry/" + string(id) + "/start/0",
		Task:        task,
		AvailableAt: delayedAt,
	})
}

// pauseSubmit persists one running execution carrying the given intents, the
// way the engine's submit path does: through the concurrency-aware create, so
// the execution is admitted to its workflow's slot in the same transition that
// writes its intents. A plain CreateExecution would leave it uncharged, and
// every case that paused one would then be exercising a slot state no
// production submission can reach.
func pauseSubmit(t *testing.T, state engine.StateStore, ctx context.Context, g *graph.Graph, id types.ExecutionID, entries []engine.OutboxEntry) {
	t.Helper()
	limited := concurrencyCapability(t, state)
	snap := &engine.ExecutionSnapshot{ID: id, Graph: g, Status: types.ExecutionStatusRunning}
	res, err := limited.CreateExecutionWithConcurrency(ctx, snap, entries, concurrencyAdmissionFor(t, g, 1))
	if err != nil {
		t.Fatalf("CreateExecutionWithConcurrency() error = %v", err)
	}
	if res.Queued {
		t.Fatalf("submission Queued = true against a free slot: %+v", res)
	}
}

// pauseLease takes the delivery lease the in-flight cases commit against, the
// way a runner does: claim the task, then commit with the lease's own identity.
// The commit fences on the lease, so a request built without it would be refused
// as a stale token and the case would assert nothing about a paused execution.
func pauseLease(t *testing.T, state engine.StateStore, ctx context.Context, entry engine.OutboxEntry) *engine.TaskLease {
	t.Helper()
	lease := &engine.TaskLease{
		LeaseID:    engine.LeaseID("pause-contract-lease"),
		LeaseToken: engine.LeaseToken("pause-contract-token"),
		Attempt:    1,
		Task:       entry.Task,
		IssuedAt:   time.Now().UTC(),
		TTL:        time.Minute,
	}
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease() acquired=%v err=%v, want true/nil", acquired, err)
	}
	return lease
}

// leasedOutbox is what a deliverer would CLAIM at the given instant — the
// dispatch decision, as opposed to readyOutbox's listing of it. The instant is
// a parameter because LeaseOutbox takes one, which is also what lets a case ask
// what happens to a delayed intent without waiting for its deadline.
//
// A store without the lease is a failure rather than a skip: both backends have
// it, and the claim is the path the engine actually takes.
func leasedOutbox(t *testing.T, state engine.StateStore, ctx context.Context, id types.ExecutionID, now time.Time) []engine.OutboxEntry {
	t.Helper()
	leaser, ok := state.(engine.OutboxLeaser)
	if !ok {
		t.Fatalf("%T does not implement engine.OutboxLeaser", state)
	}
	entries, err := leaser.LeaseOutbox(ctx, id, now, 32)
	if err != nil {
		t.Fatalf("LeaseOutbox(%q) error = %v", id, err)
	}
	return entries
}

// listedOutboxAt is readyOutbox at a caller-chosen instant, which is how a case
// asks what a delayed intent's deadline survived as without leasing anything —
// a claim would hide the entry behind its own lease and make the next question
// unanswerable.
func listedOutboxAt(t *testing.T, state engine.StateStore, ctx context.Context, id types.ExecutionID, at time.Time) []engine.OutboxEntry {
	t.Helper()
	atomic, ok := state.(engine.AtomicStateStore)
	if !ok {
		t.Fatalf("%T does not implement engine.AtomicStateStore", state)
	}
	entries, err := atomic.ListOutbox(ctx, id, at, 32)
	if err != nil {
		t.Fatalf("ListOutbox(%q) error = %v", id, err)
	}
	return entries
}

// outboxEntry finds one intent by ID. Cases assert on membership rather than on
// position: two backends may order a listing by ID or by score, and the
// contract is about which intents are deliverable, not about the order.
func outboxEntry(entries []engine.OutboxEntry, id string) (engine.OutboxEntry, bool) {
	for _, entry := range entries {
		if entry.ID == id {
			return entry, true
		}
	}
	return engine.OutboxEntry{}, false
}

// pauseHoldsTheReadySet is the core of the hold: an execution with work that is
// due right now stops being deliverable the moment it is paused, through both
// reads the delivery path can take and at every horizon.
//
// Both reads matter. claimOutbox prefers LeaseOutbox — what the engine actually
// calls on an atomic store — but falls back to ListOutbox for backends without
// the lease, so a hold honoured by only one of them would be a hold that
// depends on which capability a store happens to declare.
//
// The far horizon is the third arm: a hold implemented by making entries "not
// yet due" is a hold that a caller willing to ask about the future walks
// around, and the store cannot tell such a caller from a legitimate one.
func pauseHoldsTheReadySet(t *testing.T, state engine.StateStore) {
	ctx := context.Background()
	g := concurrencyGraph(t, "pause-holds-ready")
	id := engine.NewExecutionID()
	pauseSubmit(t, state, ctx, g, id, pauseIntents(id, time.Time{}))

	// The intent is deliverable before the pause, so the case cannot pass by
	// asserting an outbox that was empty to begin with.
	if entries := readyOutbox(t, state, ctx, id); len(entries) != 1 {
		t.Fatalf("ready intents before the pause = %d, want the root intent", len(entries))
	}

	pauser := pauseCapability(t, state)
	if err := pauser.PauseExecution(ctx, id); err != nil {
		t.Fatalf("PauseExecution() error = %v", err)
	}
	if got := executionStatus(t, state, ctx, id); got != types.ExecutionStatusPaused {
		t.Fatalf("status after the pause = %q, want %q", got, types.ExecutionStatusPaused)
	}
	if entries := leasedOutbox(t, state, ctx, id, time.Now().UTC()); len(entries) != 0 {
		t.Fatalf("claimable intents on a paused execution = %+v, want none: a claim granted "+
			"here delivers exactly the work the pause was asked to stop", entries)
	}
	if entries := readyOutbox(t, state, ctx, id); len(entries) != 0 {
		t.Fatalf("ready intents on a paused execution = %+v, want none", entries)
	}
	if entries := parkedOutbox(t, state, ctx, id); len(entries) != 0 {
		t.Fatalf("intents visible at a far horizon on a paused execution = %+v, want none: the "+
			"hold has to be a property of the execution, not of the instant asked about", entries)
	}
}

// pauseReleasesTheReadySetOnResume is the other half: a pause is a suspension,
// so the claim path has to work again afterwards. This asserts on the claim
// rather than the listing because the claim is the dispatch decision.
func pauseReleasesTheReadySetOnResume(t *testing.T, state engine.StateStore) {
	ctx := context.Background()
	g := concurrencyGraph(t, "pause-resume-releases")
	id := engine.NewExecutionID()
	entries := pauseIntents(id, time.Time{})
	pauseSubmit(t, state, ctx, g, id, entries)

	pauser := pauseCapability(t, state)
	if err := pauser.PauseExecution(ctx, id); err != nil {
		t.Fatalf("PauseExecution() error = %v", err)
	}
	if err := pauser.ResumeExecution(ctx, id); err != nil {
		t.Fatalf("ResumeExecution() error = %v", err)
	}
	if got := executionStatus(t, state, ctx, id); got != types.ExecutionStatusRunning {
		t.Fatalf("status after the resume = %q, want %q", got, types.ExecutionStatusRunning)
	}
	claimed := leasedOutbox(t, state, ctx, id, time.Now().UTC())
	if len(claimed) != 1 || claimed[0].ID != entries[0].ID {
		t.Fatalf("claimed intents after the resume = %+v, want exactly %q: the pause held the "+
			"ready set, so the resume has to hand the same intent back", claimed, entries[0].ID)
	}
}

// pauseKeepsADelayedIntentAtItsOwnDeadline is why the hold cannot be
// implemented by releasing everything to "now".
//
// A resume that did that would look correct against a ready set of due intents
// and be wrong for every delayed one: a retry or timer intent scheduled an hour
// out would fire the moment an operator resumed, which is work the pause was
// never asked to bring forward.
func pauseKeepsADelayedIntentAtItsOwnDeadline(t *testing.T, state engine.StateStore) {
	ctx := context.Background()
	g := concurrencyGraph(t, "pause-keeps-deadlines")
	id := engine.NewExecutionID()
	now := time.Now().UTC().Truncate(time.Millisecond)
	entries := pauseIntents(id, now.Add(time.Hour))
	pauseSubmit(t, state, ctx, g, id, entries)

	pauser := pauseCapability(t, state)
	if err := pauser.PauseExecution(ctx, id); err != nil {
		t.Fatalf("PauseExecution() error = %v", err)
	}
	if err := pauser.ResumeExecution(ctx, id); err != nil {
		t.Fatalf("ResumeExecution() error = %v", err)
	}

	if got := readyOutbox(t, state, ctx, id); len(got) != 1 || got[0].ID != entries[0].ID {
		t.Fatalf("intents due right after the resume = %+v, want only %q: the delayed intent has "+
			"to keep waiting for its own deadline", got, entries[0].ID)
	}
	later := listedOutboxAt(t, state, ctx, id, now.Add(2*time.Hour))
	if len(later) != 2 {
		t.Fatalf("intents due two hours on = %d, want both: the delayed intent has to survive "+
			"the pause with its deadline intact", len(later))
	}
	delayed, ok := outboxEntry(later, entries[1].ID)
	if !ok {
		t.Fatalf("intents due two hours on = %+v, want %q among them", later, entries[1].ID)
	}
	if !delayed.AvailableAt.Equal(entries[1].AvailableAt) {
		t.Fatalf("delayed intent available at %s, want %s: a resume must not pull a future "+
			"deadline forward", delayed.AvailableAt, entries[1].AvailableAt)
	}
}

// pauseIsIdempotentForARepeatedPause pins the shape a retried request takes. A
// pause whose response was lost arrives twice, and reporting the second as a
// conflict would make an ordinary retry indistinguishable from a real refusal —
// leaving the caller unable to tell whether the execution is paused at all.
func pauseIsIdempotentForARepeatedPause(t *testing.T, state engine.StateStore) {
	ctx := context.Background()
	g := concurrencyGraph(t, "pause-idempotent")
	id := engine.NewExecutionID()
	pauseSubmit(t, state, ctx, g, id, pauseIntents(id, time.Time{}))

	pauser := pauseCapability(t, state)
	if err := pauser.PauseExecution(ctx, id); err != nil {
		t.Fatalf("PauseExecution(first) error = %v", err)
	}
	if err := pauser.PauseExecution(ctx, id); err != nil {
		t.Fatalf("PauseExecution(second) error = %v, want the repeat to be a no-op success", err)
	}
	if got := executionStatus(t, state, ctx, id); got != types.ExecutionStatusPaused {
		t.Fatalf("status after the repeated pause = %q, want %q", got, types.ExecutionStatusPaused)
	}
	if entries := leasedOutbox(t, state, ctx, id, time.Now().UTC()); len(entries) != 0 {
		t.Fatalf("claimable intents after a repeated pause = %+v, want none", entries)
	}
}

// pauseHoldsWorkAnInFlightCommitAppends pins the half of the design a status
// write alone cannot deliver: a task leased before the pause runs to completion
// and its commit is accepted, but the downstream work that commit produces is
// born held and is only released by a resume.
//
// The dangerous failure is the opposite of a leak. A store that refused the
// commit would strand the runner holding the lease, and — because the refusal
// came from the pause — that intent would be redelivered after the resume to a
// runner that had already done the work. Accepting the commit while holding its
// downstream is what makes a pause safe to issue at any moment.
func pauseHoldsWorkAnInFlightCommitAppends(t *testing.T, state engine.StateStore) {
	ctx := context.Background()
	g := ContractGraph()
	atomic, ok := state.(engine.AtomicStateStore)
	if !ok {
		t.Fatalf("%T does not implement engine.AtomicStateStore", state)
	}
	id := engine.NewExecutionID()
	entries := pauseIntents(id, time.Time{})
	pauseSubmit(t, state, ctx, g, id, entries)
	lease := pauseLease(t, state, ctx, entries[0])

	pauser := pauseCapability(t, state)
	if err := pauser.PauseExecution(ctx, id); err != nil {
		t.Fatalf("PauseExecution() error = %v", err)
	}

	startIdx, _ := g.NodeIndex("start")
	unitIdx := g.UnitIndexForNode(startIdx)
	port := "main"
	res, err := atomic.CommitNode(ctx, engine.CommitNodeRequest{
		ExecutionID: id,
		NodeName:    "start",
		NodeIdx:     startIdx,
		UnitIdx:     unitIdx,
		LeaseID:     lease.LeaseID,
		LeaseToken:  lease.LeaseToken,
		Attempt:     lease.Attempt,
		Status:      types.NodeStatusSuccess,
		Port:        port,
		AdvanceTask: &engine.Task{
			ExecutionID: id,
			NodeName:    "start",
			NodeIdx:     startIdx,
			UnitIdx:     unitIdx,
			Type:        engine.TaskTypeNodeAdvance,
			Port:        &port,
		},
	})
	if err != nil {
		t.Fatalf("CommitNode(while paused) error = %v", err)
	}
	if !res.Applied || res.Outcome != engine.CommitOutcomeAccepted {
		t.Fatalf("CommitNode(while paused) = %+v, want accepted and applied: a lease issued before "+
			"the pause is work already in flight, and refusing its commit strands it", res)
	}
	if res.ExecutionDone {
		t.Fatalf("CommitNode(while paused).ExecutionDone = true, want the two-node graph to still "+
			"have its downstream unit to run: %+v", res)
	}
	if got := readyOutbox(t, state, ctx, id); len(got) != 0 {
		t.Fatalf("ready intents after a commit landed on a paused execution = %+v, want none: the "+
			"commit is accepted, but nothing it produced may be delivered", got)
	}

	if err := pauser.ResumeExecution(ctx, id); err != nil {
		t.Fatalf("ResumeExecution() error = %v", err)
	}
	after := readyOutbox(t, state, ctx, id)
	advances := 0
	for _, entry := range after {
		if entry.Task.Type == engine.TaskTypeNodeAdvance && entry.Task.NodeName == "start" {
			advances++
		}
	}
	if advances != 1 {
		t.Fatalf("intents after the resume = %+v, want the advance intent the in-flight commit "+
			"appended among them: the work was held, not dropped", after)
	}
}

// pauseKeepsTheWorkflowSlot pins the ruling that a paused execution holds its
// concurrency slot.
//
// It holds intermediate state — in-flight leases, counter positions, buffered
// signals — that only a resume can put back to work, so releasing the slot
// would admit a replacement competing with the work the resume restarts. The
// slot is given up the ordinary way instead, by reaching a terminal status,
// which is the last assertion here: a pause that quietly leaked the slot would
// stay invisible until a resumed execution was found running beside a
// submission that had taken its place.
func pauseKeepsTheWorkflowSlot(t *testing.T, state engine.StateStore) {
	ctx := context.Background()
	g := concurrencyGraph(t, "pause-keeps-slot")
	limited := concurrencyCapability(t, state)
	admission := concurrencyAdmissionFor(t, g, 1)

	holder := engine.NewExecutionID()
	pauseSubmit(t, state, ctx, g, holder, pauseIntents(holder, time.Time{}))
	pauser := pauseCapability(t, state)
	if err := pauser.PauseExecution(ctx, holder); err != nil {
		t.Fatalf("PauseExecution() error = %v", err)
	}

	rival := engine.NewExecutionID()
	rivalSnapshot := &engine.ExecutionSnapshot{ID: rival, Graph: g, Status: types.ExecutionStatusRunning}
	rivalEntries := pauseIntents(rival, time.Time{})
	if _, err := limited.CreateExecutionWithConcurrency(ctx, rivalSnapshot, rivalEntries, admission); !errors.Is(err, engine.ErrConcurrencyLimit) {
		t.Fatalf("submission beside a paused holder error = %v, want %v: a paused execution keeps "+
			"its slot", err, engine.ErrConcurrencyLimit)
	}

	if err := pauser.ResumeExecution(ctx, holder); err != nil {
		t.Fatalf("ResumeExecution() error = %v", err)
	}
	if _, err := limited.CreateExecutionWithConcurrency(ctx, rivalSnapshot, rivalEntries, admission); !errors.Is(err, engine.ErrConcurrencyLimit) {
		t.Fatalf("submission beside a resumed holder error = %v, want %v: the resume must not hand "+
			"the slot to anyone else either", err, engine.ErrConcurrencyLimit)
	}

	if err := state.UpdateExecutionStatus(ctx, holder, types.ExecutionStatusSuccess, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus(success) error = %v", err)
	}
	if _, err := limited.CreateExecutionWithConcurrency(ctx, rivalSnapshot, rivalEntries, admission); err != nil {
		t.Fatalf("submission after the holder finished error = %v, want the slot released by the "+
			"terminal write rather than leaked by the pause", err)
	}
}

// pauseAcceptsATerminalWriteWhilePaused pins the other end of the in-flight
// commit: when the task that was already running turns out to be the last one,
// the execution finishes while paused and the terminal status stands.
//
// A store that fenced terminal writes against a paused execution would leave it
// paused forever — the pause holds dispatch, so nothing is left that could
// commit again, and the operator would see a finished execution stuck in a
// state nothing can advance.
func pauseAcceptsATerminalWriteWhilePaused(t *testing.T, state engine.StateStore) {
	ctx := context.Background()
	g := concurrencyGraph(t, "pause-terminal-write")
	id := engine.NewExecutionID()
	pauseSubmit(t, state, ctx, g, id, pauseIntents(id, time.Time{}))

	pauser := pauseCapability(t, state)
	if err := pauser.PauseExecution(ctx, id); err != nil {
		t.Fatalf("PauseExecution() error = %v", err)
	}
	if err := state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusSuccess, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus(success while paused) error = %v", err)
	}
	if got := executionStatus(t, state, ctx, id); got != types.ExecutionStatusSuccess {
		t.Fatalf("status after the terminal write = %q, want %q", got, types.ExecutionStatusSuccess)
	}
	if err := pauser.ResumeExecution(ctx, id); !errors.Is(err, engine.ErrExecutionNotPaused) {
		t.Fatalf("ResumeExecution(finished while paused) error = %v, want %v: the execution is no "+
			"longer paused, and a resume reporting success would promise work that will never run",
			err, engine.ErrExecutionNotPaused)
	}
}

// pauseRefusesAnExecutionThatIsNotRunning covers the two live statuses a pause
// must refuse.
//
// The queued execution is the important one: it holds no intermediate state at
// all, so there is nothing to suspend, and its request is a refusal rather than
// a silently accepted no-op — an operator who paused a queued submission would
// believe its place in line was frozen when the promotion is what decides when
// it runs. Cancel is the operation that acts on it.
//
// The terminal execution is refused for the plain reason that "paused" would be
// a lie about it: it is not suspended, it is over.
func pauseRefusesAnExecutionThatIsNotRunning(t *testing.T, state engine.StateStore) {
	ctx := context.Background()
	g := concurrencyGraph(t, "pause-refuses-queued")
	pauser := pauseCapability(t, state)
	admission := queueAdmissionFor(t, g, 1, 0, 0)

	holder, res := queueSubmit(t, state, ctx, g, admission)
	if res.Queued {
		t.Fatalf("first submission Queued = true, want it admitted to the free slot")
	}
	waiting, res := queueSubmit(t, state, ctx, g, admission)
	if !res.Queued {
		t.Fatal("over-limit submission Queued = false, want it parked")
	}
	if err := pauser.PauseExecution(ctx, waiting); !errors.Is(err, engine.ErrExecutionNotPausable) {
		t.Fatalf("PauseExecution(queued) error = %v, want %v", err, engine.ErrExecutionNotPausable)
	}
	if got := executionStatus(t, state, ctx, waiting); got != types.ExecutionStatusQueued {
		t.Fatalf("status after the refused pause = %q, want %q: the refusal must change nothing",
			got, types.ExecutionStatusQueued)
	}

	if err := state.UpdateExecutionStatus(ctx, holder, types.ExecutionStatusSuccess, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus(success) error = %v", err)
	}
	if err := pauser.PauseExecution(ctx, holder); !errors.Is(err, engine.ErrExecutionNotPausable) {
		t.Fatalf("PauseExecution(terminal) error = %v, want %v", err, engine.ErrExecutionNotPausable)
	}
}

// pauseRefusesAnAbsentExecution pins the answer for an execution that is not
// there. It is reported as unpausable rather than as a not-found: the capability
// has one refusal, and a caller checking for it must not have to tell "no such
// execution" from "an execution that cannot be paused" — both mean the pause
// did not happen.
func pauseRefusesAnAbsentExecution(t *testing.T, state engine.StateStore) {
	ctx := context.Background()
	pauser := pauseCapability(t, state)
	if err := pauser.PauseExecution(ctx, types.ExecutionID("pause-contract-no-such-execution")); !errors.Is(err, engine.ErrExecutionNotPausable) {
		t.Fatalf("PauseExecution(absent) error = %v, want %v", err, engine.ErrExecutionNotPausable)
	}
}

// pauseRefusesToResumeWhatIsNotPaused is the mirror of the pause refusal, and
// the one an operator hits after a race: the execution finished, or was
// cancelled, between the request and the resume. Reporting success there would
// tell the caller that a paused execution is running again when there is none
// to run.
func pauseRefusesToResumeWhatIsNotPaused(t *testing.T, state engine.StateStore) {
	ctx := context.Background()
	g := concurrencyGraph(t, "pause-resume-refuses")
	id := engine.NewExecutionID()
	pauseSubmit(t, state, ctx, g, id, pauseIntents(id, time.Time{}))

	pauser := pauseCapability(t, state)
	if err := pauser.ResumeExecution(ctx, id); !errors.Is(err, engine.ErrExecutionNotPaused) {
		t.Fatalf("ResumeExecution(running) error = %v, want %v", err, engine.ErrExecutionNotPaused)
	}
	if err := pauser.ResumeExecution(ctx, types.ExecutionID("pause-contract-no-such-execution")); !errors.Is(err, engine.ErrExecutionNotPaused) {
		t.Fatalf("ResumeExecution(absent) error = %v, want %v", err, engine.ErrExecutionNotPaused)
	}
	if got := executionStatus(t, state, ctx, id); got != types.ExecutionStatusRunning {
		t.Fatalf("status after the refused resume = %q, want %q: the refusal must change nothing",
			got, types.ExecutionStatusRunning)
	}
}
