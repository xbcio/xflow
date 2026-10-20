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

// RunConcurrencyQueueContract exercises QUEUE mode against a concrete backend.
//
// It is a separate suite from the admission contract because it describes a
// second capability — a store can enforce a limit without being able to park
// submissions, and such a store refuses queue mode instead (see the admission
// contract's QueueModeRefusalIsHonest). A backend that implements queue mode
// calls this in addition.
//
// The cases are built around what a queued execution must and must not be able
// to do. The dangerous half is the "must not": a queued execution that is
// deliverable starts work while holding no slot, and a promotion that starts
// the wrong execution breaks the FIFO order the queue exists to provide. Both
// are invisible from the engine — it sees a status and an outbox, not a line.
func RunConcurrencyQueueContract(t *testing.T, manifest *Manifest, newStore ConcurrencyStoreFactory) {
	t.Helper()
	manifest.Report(t)
	manifest.RequireDeclared(t,
		CapabilityConcurrencyLimitedStateStore,
		CapabilityConcurrencyQueueExpirer,
	)
	t.Run("QueuesOverTheLimit", func(t *testing.T) {
		queueQueuesOverTheLimit(t, newStore(t))
	})
	t.Run("ParksTheIntentsUntilPromotion", func(t *testing.T) {
		queueParksTheIntentsUntilPromotion(t, newStore(t))
	})
	t.Run("PromotesTheHeadOnRelease", func(t *testing.T) {
		queuePromotesTheHeadOnRelease(t, newStore(t))
	})
	t.Run("PromotesInFifoOrder", func(t *testing.T) {
		queuePromotesInFifoOrder(t, newStore(t))
	})
	t.Run("RefusesWhenTheQueueIsFull", func(t *testing.T) {
		queueRefusesWhenTheQueueIsFull(t, newStore(t))
	})
	t.Run("CancelsAQueuedExecution", func(t *testing.T) {
		queueCancelsAQueuedExecution(t, newStore(t))
	})
	t.Run("ExpiresAQueuedExecutionPastItsTimeout", func(t *testing.T) {
		queueExpiresAQueuedExecutionPastItsTimeout(t, newStore(t))
	})
	t.Run("ReportsThePositionOfAQueuedExecution", func(t *testing.T) {
		queueReportsThePositionOfAQueuedExecution(t, manifest, newStore(t))
	})
	t.Run("BuffersSignalsUntilPromotion", func(t *testing.T) {
		queueBuffersSignalsUntilPromotion(t, manifest, newStore(t))
	})
}

// queueAdmissionFor builds the QUEUE-mode admission one submission contends
// for. limit is the workflow's capacity; maxQueued and queueTimeout are the
// backlog controls under test.
func queueAdmissionFor(t *testing.T, g *graph.Graph, limit, maxQueued int, queueTimeout time.Duration) engine.ConcurrencyAdmission {
	t.Helper()
	return engine.ConcurrencyAdmission{
		Namespace:    "default",
		Workflow:     g.Name(),
		Limit:        limit,
		Mode:         types.ConcurrencyModeQueue,
		MaxQueued:    maxQueued,
		QueueTimeout: queueTimeout,
	}
}

// queueSubmit submits one execution and returns its ID with the admission's
// verdict, so each case can assert on the position the store reported.
func queueSubmit(t *testing.T, state engine.StateStore, ctx context.Context, g *graph.Graph, admission engine.ConcurrencyAdmission) (types.ExecutionID, engine.ConcurrencyAdmissionResult) {
	t.Helper()
	limited := concurrencyCapability(t, state)
	snap, entries := concurrencySnapshot(t, g)
	res, err := limited.CreateExecutionWithConcurrency(ctx, snap, entries, admission)
	if err != nil {
		t.Fatalf("submission error = %v", err)
	}
	return snap.ID, res
}

// executionStatus reads the lifecycle status a queued execution is visible
// under. A queued execution has to be readable like any other: the status is
// how a caller, a listing and the cancel path find it.
func executionStatus(t *testing.T, state engine.StateStore, ctx context.Context, id types.ExecutionID) types.ExecutionStatus {
	t.Helper()
	snap, err := state.GetExecution(ctx, id)
	if err != nil {
		t.Fatalf("GetExecution(%q) error = %v", id, err)
	}
	if snap == nil {
		t.Fatalf("GetExecution(%q) returned no execution", id)
	}
	return snap.Status
}

// readyOutbox is what the dispatcher would deliver right now.
func readyOutbox(t *testing.T, state engine.StateStore, ctx context.Context, id types.ExecutionID) []engine.OutboxEntry {
	t.Helper()
	atomic, ok := state.(engine.AtomicStateStore)
	if !ok {
		// ConcurrencyLimitedStateStore embeds AtomicStateStore and this suite
		// requires it, so a store reaching here must have it. Not a skip: the
		// outbox read is the only way this suite can observe what was parked.
		t.Fatalf("%T implements ConcurrencyLimitedStateStore but not AtomicStateStore", state)
	}
	entries, err := atomic.ListOutbox(ctx, id, time.Now().UTC(), 32)
	if err != nil {
		t.Fatalf("ListOutbox(%q) error = %v", id, err)
	}
	return entries
}

// parkedOutbox is everything the execution has persisted, deliverable or not.
// Asking with a horizon far past any real deadline is how "the intents are
// there, they are just not due" is observed without reaching into a backend's
// internals.
func parkedOutbox(t *testing.T, state engine.StateStore, ctx context.Context, id types.ExecutionID) []engine.OutboxEntry {
	t.Helper()
	atomic, ok := state.(engine.AtomicStateStore)
	if !ok {
		// See readyOutbox: AtomicStateStore is implied by the suite's required
		// ConcurrencyLimitedStateStore, so this is a failure, not a skip.
		t.Fatalf("%T implements ConcurrencyLimitedStateStore but not AtomicStateStore", state)
	}
	horizon := time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
	entries, err := atomic.ListOutbox(ctx, id, horizon, 32)
	if err != nil {
		t.Fatalf("ListOutbox(%q, horizon) error = %v", id, err)
	}
	return entries
}

func queueQueuesOverTheLimit(t *testing.T, state engine.StateStore) {
	ctx := context.Background()
	g := concurrencyGraph(t, "conc-queue-over-limit")
	admission := queueAdmissionFor(t, g, 1, 0, 0)

	holder, res := queueSubmit(t, state, ctx, g, admission)
	if res.Queued {
		t.Fatalf("first submission Queued = true, want it admitted to the free slot")
	}
	waiting, res := queueSubmit(t, state, ctx, g, admission)
	if !res.Queued {
		t.Fatal("over-limit submission Queued = false, want it parked")
	}
	if res.QueuePosition != 1 {
		t.Fatalf("over-limit submission QueuePosition = %d, want 1 (the head)", res.QueuePosition)
	}
	if got := executionStatus(t, state, ctx, waiting); got != types.ExecutionStatusQueued {
		t.Fatalf("parked execution status = %q, want %q", got, types.ExecutionStatusQueued)
	}
	// The holder is untouched: queueing the newcomer must not disturb the
	// execution that owns the slot.
	if got := executionStatus(t, state, ctx, holder); got != types.ExecutionStatusRunning {
		t.Fatalf("holder status = %q, want it still running", got)
	}
}

// queueParksTheIntentsUntilPromotion is the safety half of queueing: a queued
// execution's root intents are durable (so a crash cannot lose them) and
// undeliverable (so nothing starts work that holds no slot). A store that got
// only the first half right would run the whole backlog over the limit.
func queueParksTheIntentsUntilPromotion(t *testing.T, state engine.StateStore) {
	ctx := context.Background()
	g := concurrencyGraph(t, "conc-queue-parked")
	admission := queueAdmissionFor(t, g, 1, 0, 0)

	queueSubmit(t, state, ctx, g, admission)
	waiting, res := queueSubmit(t, state, ctx, g, admission)
	if !res.Queued {
		t.Fatal("over-limit submission was not parked")
	}
	if entries := parkedOutbox(t, state, ctx, waiting); len(entries) != 1 {
		t.Fatalf("parked intents = %d, want the root intent persisted", len(entries))
	}
	if entries := readyOutbox(t, state, ctx, waiting); len(entries) != 0 {
		t.Fatalf("ready intents on a queued execution = %d, want none: a deliverable intent starts work that holds no slot", len(entries))
	}
}

func queuePromotesTheHeadOnRelease(t *testing.T, state engine.StateStore) {
	ctx := context.Background()
	g := concurrencyGraph(t, "conc-queue-promote")
	admission := queueAdmissionFor(t, g, 1, 0, 0)

	holder, _ := queueSubmit(t, state, ctx, g, admission)
	waiting, _ := queueSubmit(t, state, ctx, g, admission)
	queued, err := state.GetExecution(ctx, waiting)
	if err != nil || queued == nil {
		t.Fatalf("GetExecution(queued) = (%v, %v), want a snapshot", queued, err)
	}
	// Leaving the queue is an accepted status transition, so it must advance
	// the execution clock that inspect reports as updated_at. The sleep gives
	// a millisecond-resolution backend room to move.
	time.Sleep(5 * time.Millisecond)
	if err := state.UpdateExecutionStatus(ctx, holder, types.ExecutionStatusSuccess, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus(success) error = %v", err)
	}

	if got := executionStatus(t, state, ctx, waiting); got != types.ExecutionStatusRunning {
		t.Fatalf("promoted execution status = %q, want %q", got, types.ExecutionStatusRunning)
	}
	promoted, err := state.GetExecution(ctx, waiting)
	if err != nil || promoted == nil {
		t.Fatalf("GetExecution(promoted) = (%v, %v), want a snapshot", promoted, err)
	}
	if !promoted.UpdatedAt.After(queued.UpdatedAt) {
		t.Fatalf("UpdatedAt = %v after promotion, want after %v: the queued→running "+
			"transition was accepted and must move the execution clock",
			promoted.UpdatedAt, queued.UpdatedAt)
	}
	if entries := readyOutbox(t, state, ctx, waiting); len(entries) != 1 {
		t.Fatalf("promoted execution ready intents = %d, want its root intent delivered", len(entries))
	}
	// The promoted execution now occupies the slot, so a newcomer queues
	// behind it: promotion took the capacity, it did not add one.
	_, res := queueSubmit(t, state, ctx, g, admission)
	if !res.Queued || res.QueuePosition != 1 {
		t.Fatalf("submission after promotion = %+v, want it queued at position 1", res)
	}
}

// queuePromotesInFifoOrder is the property the queue exists for: capacity goes
// to the execution that has waited longest. LIFO or arbitrary promotion would
// starve the oldest submission indefinitely under steady load, which is exactly
// the situation a backlog forms in.
func queuePromotesInFifoOrder(t *testing.T, state engine.StateStore) {
	ctx := context.Background()
	g := concurrencyGraph(t, "conc-queue-fifo")
	admission := queueAdmissionFor(t, g, 1, 0, 0)

	first, _ := queueSubmit(t, state, ctx, g, admission)
	second, res := queueSubmit(t, state, ctx, g, admission)
	if !res.Queued || res.QueuePosition != 1 {
		t.Fatalf("second submission = %+v, want position 1", res)
	}
	third, res := queueSubmit(t, state, ctx, g, admission)
	if !res.Queued || res.QueuePosition != 2 {
		t.Fatalf("third submission = %+v, want position 2", res)
	}

	if err := state.UpdateExecutionStatus(ctx, first, types.ExecutionStatusSuccess, ""); err != nil {
		t.Fatalf("release first error = %v", err)
	}
	if got := executionStatus(t, state, ctx, second); got != types.ExecutionStatusRunning {
		t.Fatalf("after the first release, second status = %q, want it promoted", got)
	}
	if got := executionStatus(t, state, ctx, third); got != types.ExecutionStatusQueued {
		t.Fatalf("after the first release, third status = %q, want it still queued", got)
	}

	if err := state.UpdateExecutionStatus(ctx, second, types.ExecutionStatusSuccess, ""); err != nil {
		t.Fatalf("release second error = %v", err)
	}
	if got := executionStatus(t, state, ctx, third); got != types.ExecutionStatusRunning {
		t.Fatalf("after the second release, third status = %q, want it promoted", got)
	}
}

func queueRefusesWhenTheQueueIsFull(t *testing.T, state engine.StateStore) {
	ctx := context.Background()
	limited := concurrencyCapability(t, state)
	g := concurrencyGraph(t, "conc-queue-full")
	admission := queueAdmissionFor(t, g, 1, 1, 0)

	queueSubmit(t, state, ctx, g, admission)
	queueSubmit(t, state, ctx, g, admission)

	overflow, entries := concurrencySnapshot(t, g)
	if _, err := limited.CreateExecutionWithConcurrency(ctx, overflow, entries, admission); !errors.Is(err, engine.ErrConcurrencyQueueFull) {
		t.Fatalf("submission past max_queued error = %v, want ErrConcurrencyQueueFull", err)
	}
	loaded, err := state.GetExecution(ctx, overflow.ID)
	if err != nil {
		t.Fatalf("GetExecution(overflow) error = %v", err)
	}
	if loaded != nil {
		t.Fatalf("refused submission was persisted: %+v", loaded)
	}
}

// queueCancelsAQueuedExecution pins what giving up on a parked submission has
// to do: leave the line, carry nothing forward. Its intents were never
// deliverable, so dropping them is the only reading — a canceled execution that
// never ran has no work left to hand off.
func queueCancelsAQueuedExecution(t *testing.T, state engine.StateStore) {
	ctx := context.Background()
	g := concurrencyGraph(t, "conc-queue-cancel")
	admission := queueAdmissionFor(t, g, 1, 0, 0)

	holder, _ := queueSubmit(t, state, ctx, g, admission)
	waiting, _ := queueSubmit(t, state, ctx, g, admission)
	behind, _ := queueSubmit(t, state, ctx, g, admission)

	// The engine's cancel path walks canceling → canceled; the store's release
	// must fire on the terminal step, not on the request.
	if err := state.UpdateExecutionStatus(ctx, waiting, types.ExecutionStatusCanceling, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus(canceling) error = %v", err)
	}
	if err := state.UpdateExecutionStatus(ctx, waiting, types.ExecutionStatusCanceled, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus(canceled) error = %v", err)
	}
	if got := executionStatus(t, state, ctx, waiting); got != types.ExecutionStatusCanceled {
		t.Fatalf("canceled queued execution status = %q, want %q", got, types.ExecutionStatusCanceled)
	}
	if entries := parkedOutbox(t, state, ctx, waiting); len(entries) != 0 {
		t.Fatalf("canceled queued execution still holds %d intents, want them dropped", len(entries))
	}
	// Cancelling a queued execution frees no capacity — the holder still has it
	// — so the execution behind it stays queued, and it is the one promoted
	// when the holder finally finishes.
	if got := executionStatus(t, state, ctx, behind); got != types.ExecutionStatusQueued {
		t.Fatalf("execution behind the canceled one status = %q, want it still queued", got)
	}
	if err := state.UpdateExecutionStatus(ctx, holder, types.ExecutionStatusSuccess, ""); err != nil {
		t.Fatalf("release holder error = %v", err)
	}
	if got := executionStatus(t, state, ctx, behind); got != types.ExecutionStatusRunning {
		t.Fatalf("after the holder released, status = %q, want the surviving waiter promoted", got)
	}
}

// queueExpiresAQueuedExecutionPastItsTimeout pins queue_timeout: a submission
// that waited too long for a slot ends as a timeout with a reason that says
// why. It expires while queued, which is the only moment it can: a promoted
// execution is not waiting for anything.
func queueExpiresAQueuedExecutionPastItsTimeout(t *testing.T, state engine.StateStore) {
	expirer, ok := state.(engine.ConcurrencyQueueExpirer)
	if !ok {
		t.Fatalf("%T does not implement engine.ConcurrencyQueueExpirer", state)
	}
	ctx := context.Background()
	g := concurrencyGraph(t, "conc-queue-timeout")

	holder, _ := queueSubmit(t, state, ctx, g, queueAdmissionFor(t, g, 1, 0, 0))
	waiting, _ := queueSubmit(t, state, ctx, g, queueAdmissionFor(t, g, 1, 0, time.Millisecond))

	time.Sleep(5 * time.Millisecond)
	expired, err := expirer.ExpireQueuedExecutions(ctx, 0)
	if err != nil {
		t.Fatalf("ExpireQueuedExecutions() error = %v", err)
	}
	if expired != 1 {
		t.Fatalf("ExpireQueuedExecutions() expired = %d, want 1", expired)
	}

	snap, err := state.GetExecution(ctx, waiting)
	if err != nil {
		t.Fatalf("GetExecution(expired) error = %v", err)
	}
	if snap == nil || snap.Status != types.ExecutionStatusTimeout {
		t.Fatalf("expired queued execution = %+v, want status %q", snap, types.ExecutionStatusTimeout)
	}
	if snap.Error != engine.ConcurrencyQueueTimeoutError {
		t.Fatalf("expired queued execution reason = %q, want %q", snap.Error, engine.ConcurrencyQueueTimeoutError)
	}
	if entries := parkedOutbox(t, state, ctx, waiting); len(entries) != 0 {
		t.Fatalf("expired queued execution still holds %d intents, want them dropped", len(entries))
	}

	// The holder's slot is untouched and the expired execution is gone from the
	// line: releasing the holder promotes nobody.
	if err := state.UpdateExecutionStatus(ctx, holder, types.ExecutionStatusSuccess, ""); err != nil {
		t.Fatalf("release holder error = %v", err)
	}
	if expired, err := expirer.ExpireQueuedExecutions(ctx, 0); err != nil || expired != 0 {
		t.Fatalf("second ExpireQueuedExecutions() = %d (err %v), want 0: nothing is queued any more", expired, err)
	}
}

// queueReportsThePositionOfAQueuedExecution pins the read path a waiting caller
// has: the admission result says where a submission entered the line, and only
// the store can say where it stands now. The position has to be the line's own
// order — the execution the next release will promote is 1 — and it has to be
// 0, not an error and not 1, for an execution that is not waiting at all.
func queueReportsThePositionOfAQueuedExecution(t *testing.T, manifest *Manifest, state engine.StateStore) {
	inspector, ok := OptionalCapability[engine.ConcurrencyQueueInspector](t, manifest, CapabilityConcurrencyQueueInspector, state)
	if !ok {
		return
	}
	ctx := context.Background()
	g := concurrencyGraph(t, "conc-queue-position")
	admission := queueAdmissionFor(t, g, 1, 0, 0)

	holder, _ := queueSubmit(t, state, ctx, g, admission)
	head, headRes := queueSubmit(t, state, ctx, g, admission)
	behind, behindRes := queueSubmit(t, state, ctx, g, admission)
	if !headRes.Queued || headRes.QueuePosition != 1 {
		t.Fatalf("head admission = %+v, want position 1", headRes)
	}
	if !behindRes.Queued || behindRes.QueuePosition != 2 {
		t.Fatalf("second admission = %+v, want position 2", behindRes)
	}

	if got := queuePosition(t, inspector, ctx, head); got != 1 {
		t.Fatalf("position of the head = %d, want 1", got)
	}
	if got := queuePosition(t, inspector, ctx, behind); got != 2 {
		t.Fatalf("position of the second in line = %d, want 2", got)
	}
	// An execution that holds a slot is not waiting, and 1 would be a lie the
	// caller could act on — it would read as "next to be promoted".
	if got := queuePosition(t, inspector, ctx, holder); got != 0 {
		t.Fatalf("position of the running holder = %d, want 0", got)
	}

	// The line moves, and the position with it: canceling the head promotes
	// nobody (the holder still holds the only slot) but does advance whoever was
	// behind it.
	if err := state.UpdateExecutionStatus(ctx, head, types.ExecutionStatusCanceling, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus(canceling) error = %v", err)
	}
	if err := state.UpdateExecutionStatus(ctx, head, types.ExecutionStatusCanceled, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus(canceled) error = %v", err)
	}
	if got := queuePosition(t, inspector, ctx, behind); got != 1 {
		t.Fatalf("position after the head left = %d, want 1", got)
	}
	if got := queuePosition(t, inspector, ctx, head); got != 0 {
		t.Fatalf("position of a canceled queued execution = %d, want 0", got)
	}

	// Promotion is the other departure, and it also ends the wait.
	if err := state.UpdateExecutionStatus(ctx, holder, types.ExecutionStatusSuccess, ""); err != nil {
		t.Fatalf("release holder error = %v", err)
	}
	if got := queuePosition(t, inspector, ctx, behind); got != 0 {
		t.Fatalf("position after promotion = %d, want 0: it is running now", got)
	}
}

// queueBuffersSignalsUntilPromotion pins decision 4's signal half: a signal
// delivered to an execution that is still waiting for a slot is stored, not
// dropped, and is consumed — not parked — once promotion lets the execution run.
//
// Both directions of the window are dangerous. Dropping the signal loses an
// approval the caller was told was accepted, and nothing would ever retry it.
// Consuming it instead would start a node of an execution that holds no slot,
// which is the invariant the whole queue exists to keep. The queued execution
// here has not run a single unit, so no waiter exists and "stored" is the only
// correct verdict.
func queueBuffersSignalsUntilPromotion(t *testing.T, manifest *Manifest, state engine.StateStore) {
	durable, ok := OptionalCapability[engine.DurableSignalDeliverer](t, manifest, CapabilityDurableSignalDeliverer, state)
	if !ok {
		return
	}
	ctx := context.Background()
	g := concurrencyGraph(t, "conc-queue-signal")
	admission := queueAdmissionFor(t, g, 1, 0, 0)

	holder, _ := queueSubmit(t, state, ctx, g, admission)
	waiting, res := queueSubmit(t, state, ctx, g, admission)
	if !res.Queued {
		t.Fatal("over-limit submission was not parked")
	}

	data := map[string]any{"approved_by": "ops"}
	node, _, committed, err := durable.DeliverSignalWithOutbox(ctx, waiting, "approval", data, engine.ResumeIntent{})
	if err != nil {
		t.Fatalf("DeliverSignalWithOutbox(queued) error = %v", err)
	}
	if node != "" || committed {
		t.Fatalf("delivery to a queued execution = (node %q, committed %v), want it stored: the execution has not run, so there is no waiter to resume", node, committed)
	}
	if got := executionStatus(t, state, ctx, waiting); got != types.ExecutionStatusQueued {
		t.Fatalf("status after the delivery = %q, want it still %q", got, types.ExecutionStatusQueued)
	}
	// Storing a signal is not starting work: the queued execution's intents stay
	// held, so nothing can run while its slot is unavailable.
	if entries := readyOutbox(t, state, ctx, waiting); len(entries) != 0 {
		t.Fatalf("ready intents after a stored signal = %d, want none", len(entries))
	}

	// Promotion is what makes the buffered signal deliverable.
	if err := state.UpdateExecutionStatus(ctx, holder, types.ExecutionStatusSuccess, ""); err != nil {
		t.Fatalf("release holder error = %v", err)
	}
	if got := executionStatus(t, state, ctx, waiting); got != types.ExecutionStatusRunning {
		t.Fatalf("promoted status = %q, want %q", got, types.ExecutionStatusRunning)
	}

	// The promoted execution reaches the node that waits for "approval" and
	// finds the payload already there, so it consumes instead of parking.
	spec := &types.SuspendSpec{Signals: []string{"approval"}}
	payload, err := state.SuspendOrConsume(ctx, waiting, "wait-approval", spec)
	if err != nil {
		t.Fatalf("SuspendOrConsume() error = %v", err)
	}
	if payload == nil {
		t.Fatal("the suspension parked: the signal buffered while queued was not delivered on promotion")
	}
	if payload.Triggered != types.SignalReceived || payload.Name != "approval" {
		t.Fatalf("payload = %+v, want the buffered %q signal", payload, "approval")
	}
	if got := payload.Data["approved_by"]; got != "ops" {
		t.Fatalf("payload data = %v, want the delivered value", payload.Data)
	}
	// Consuming the signal leaves no waiter behind for that node.
	suspended, err := state.ListSuspendedNodes(ctx, waiting)
	if err != nil {
		t.Fatalf("ListSuspendedNodes() error = %v", err)
	}
	if len(suspended) != 0 {
		t.Fatalf("suspended nodes after the consume = %v, want none", suspended)
	}
}

// queuePosition reads one execution's place in line, failing the test on a real
// read error so a store that cannot answer is distinguished from one that
// answers 0.
func queuePosition(t *testing.T, inspector engine.ConcurrencyQueueInspector, ctx context.Context, id types.ExecutionID) int {
	t.Helper()
	position, err := inspector.ConcurrencyQueuePosition(ctx, id)
	if err != nil {
		t.Fatalf("ConcurrencyQueuePosition(%q) error = %v", id, err)
	}
	return position
}
