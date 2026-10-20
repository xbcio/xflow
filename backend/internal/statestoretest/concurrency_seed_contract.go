package statestoretest

import (
	"context"
	"errors"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// RunConcurrencySeedContract exercises the concurrency policy on the entry-seed
// path (engine.ConcurrencyLimitedEntryAdmissionStore) against a concrete
// backend.
//
// It is a separate suite from the submission contract because the seed path
// reaches the same slot set by a different route: it charges and persists in two
// transitions instead of one, it runs no Submit/Invoke, and its execution ID is
// deterministic from the admission key rather than freshly generated. Every one
// of those differences is a way for the limit to be enforced on one path and
// not the other, which is the failure this suite is written against: a Kafka
// trigger group admitted over the limit looks exactly like a healthy workflow
// from everywhere except the workflow it is overrunning.
//
// The two halves that must agree are the admission (a refusal persists nothing;
// a queued seed is parked with its intents held) and the release (whatever a
// seeded execution took is given back on the same paths a submitted one's is,
// including the retry that never created anything).
func RunConcurrencySeedContract(t *testing.T, manifest *Manifest, newStore ConcurrencyStoreFactory) {
	t.Helper()
	manifest.Report(t)
	manifest.RequireDeclared(t, CapabilityConcurrencyLimitedEntryAdmissionStore)
	t.Run("RefusesWithoutPersisting", func(t *testing.T) {
		seedRefusesWithoutPersisting(t, newStore(t))
	})
	t.Run("QueuesOverTheLimit", func(t *testing.T) {
		seedQueuesOverTheLimit(t, newStore(t))
	})
	t.Run("SharesTheLineWithSubmissions", func(t *testing.T) {
		seedSharesTheLineWithSubmissions(t, newStore(t))
	})
	t.Run("DuplicateRetryKeepsItsPlace", func(t *testing.T) {
		seedDuplicateRetryKeepsItsPlace(t, newStore(t))
	})
	t.Run("RefusesWhenTheQueueIsFull", func(t *testing.T) {
		seedRefusesWhenTheQueueIsFull(t, newStore(t))
	})
	t.Run("TerminalSeedLeavesTheLine", func(t *testing.T) {
		seedTerminalSeedLeavesTheLine(t, manifest, newStore(t))
	})
	t.Run("DuplicateAfterCompletionReleasesItsCharge", func(t *testing.T) {
		seedDuplicateAfterCompletionReleasesItsCharge(t, newStore(t))
	})
}

// seedLimited runs one entry seed under a concurrency policy, failing the test
// on a transport error so a store that cannot enforce the policy at all is told
// apart from one that refused this particular seed.
func seedLimited(t *testing.T, state engine.StateStore, ctx context.Context, req engine.SeedExecutionFromEntryRequest, admission engine.ConcurrencyAdmission) engine.SeedExecutionFromEntryResponse {
	t.Helper()
	limited, ok := state.(engine.ConcurrencyLimitedEntryAdmissionStore)
	if !ok {
		t.Fatalf("state store %T does not implement engine.ConcurrencyLimitedEntryAdmissionStore", state)
	}
	resp, err := limited.SeedExecutionFromEntryWithConcurrency(ctx, req, admission)
	if err != nil {
		t.Fatalf("SeedExecutionFromEntryWithConcurrency(%q) error = %v", req.AdmissionKey, err)
	}
	return resp
}

// entrySeedRequest builds the seed one Kafka entry-unit runner submits: the
// trigger group's own result, with its out-edges to "store" as the arrivals the
// admission has to schedule. The arrivals are what keep a two-unit execution
// alive after the seed, so "still running" and "still queued" are states a case
// can observe rather than states the admission immediately answers.
func entrySeedRequest(t *testing.T, g *graph.Graph, key engine.AdmissionKey) engine.SeedExecutionFromEntryRequest {
	t.Helper()
	exits := []engine.BoundaryExit{{NodeName: "body", Port: "main", Data: map[string]any{"r": 1}}}
	req := buildAdmissionRequest(t, g, key, engine.GroupOutcomeSuccess, exits, nil)
	if idx, ok := g.NodeIndex("store"); ok {
		req.Downstream = []engine.DownstreamArrival{{
			NodeName:     "store",
			NodeIdx:      idx,
			UnitIdx:      g.UnitIndexForNode(idx),
			ArrivalCount: 1,
			ActiveCount:  1,
			MergeMode:    "wait_all",
			ExecTaskType: engine.TaskTypeNodeExec,
		}}
	}
	return req
}

// seedExecution reads the execution a seed names, failing the test on a real
// read error and answering nil for an execution that does not exist.
func seedExecution(t *testing.T, state engine.StateStore, ctx context.Context, id types.ExecutionID) *engine.ExecutionSnapshot {
	t.Helper()
	snap, err := state.GetExecution(ctx, id)
	if err != nil {
		t.Fatalf("GetExecution(%q) error = %v", id, err)
	}
	return snap
}

// seedRefusesWithoutPersisting is the reject-mode half of decision 5: a seed
// over the limit leaves nothing behind, so the entry-unit result is still the
// caller's to retry — which is what lets a Kafka trigger withhold the offset.
//
// "Nothing" is asserted on both sides of the refusal: no execution exists under
// the key's deterministic ID, and the admission key is still free — a retry
// after the slot frees is an admission, not a duplicate of one that was
// silently recorded while being refused.
func seedRefusesWithoutPersisting(t *testing.T, state engine.StateStore) {
	ctx := context.Background()
	g := triggerGroupWithDownstreamGraph(t)
	admission := concurrencyAdmissionFor(t, g, 1)

	holder, res := queueSubmit(t, state, ctx, g, admission)
	if res.Queued {
		t.Fatalf("holder admission = %+v, want it admitted to the free slot", res)
	}

	req := entrySeedRequest(t, g, "k-seed-refuse")
	resp := seedLimited(t, state, ctx, req, admission)
	if resp.State != engine.AdmissionStateOverLimit {
		t.Fatalf("over-limit seed state = %q, want %q", resp.State, engine.AdmissionStateOverLimit)
	}
	if resp.ExecutionID != "" {
		t.Fatalf("refused seed named execution %q, want none: nothing was persisted for it", resp.ExecutionID)
	}
	execID := engine.DeterministicExecutionID(req.AdmissionKey)
	if snap := seedExecution(t, state, ctx, execID); snap != nil {
		t.Fatalf("refused seed persisted %+v, want nothing", snap)
	}

	// The refusal changed no accounting: the slot the holder owns is still the
	// only one, so another submission is refused too.
	limited := concurrencyCapability(t, state)
	overflow, entries := concurrencySnapshot(t, g)
	if _, err := limited.CreateExecutionWithConcurrency(ctx, overflow, entries, admission); !errors.Is(err, engine.ErrConcurrencyLimit) {
		t.Fatalf("submission alongside a refused seed error = %v, want ErrConcurrencyLimit", err)
	}

	// With the slot free, the very same request is admitted — and creates its
	// execution under the deterministic ID, not as a duplicate.
	if err := state.UpdateExecutionStatus(ctx, holder, types.ExecutionStatusSuccess, ""); err != nil {
		t.Fatalf("release holder error = %v", err)
	}
	retried := seedLimited(t, state, ctx, req, admission)
	if retried.State != engine.AdmissionStateAccepted || retried.Duplicate {
		t.Fatalf("retry after the refusal = %+v, want a first-time acceptance", retried)
	}
	if retried.ExecutionID != execID {
		t.Fatalf("retry created execution %q, want the deterministic %q", retried.ExecutionID, execID)
	}
	snap := seedExecution(t, state, ctx, execID)
	if snap == nil || snap.Status != types.ExecutionStatusRunning {
		t.Fatalf("admitted seed execution = %+v, want it created and running", snap)
	}
}

// seedQueuesOverTheLimit is the queue-mode half of decision 5: a seeded
// execution that would exceed the limit enters the line exactly as a submitted
// one does. Its intents are durable and held, and a release promotes it into a
// running execution whose work becomes deliverable — the same two-step every
// other queued execution goes through.
func seedQueuesOverTheLimit(t *testing.T, state engine.StateStore) {
	ctx := context.Background()
	g := triggerGroupWithDownstreamGraph(t)
	admission := queueAdmissionFor(t, g, 1, 0, 0)

	holder, _ := queueSubmit(t, state, ctx, g, admission)
	req := entrySeedRequest(t, g, "k-seed-queue")
	resp := seedLimited(t, state, ctx, req, admission)
	if resp.State != engine.AdmissionStateAccepted {
		t.Fatalf("over-limit queued seed state = %q, want %q: queue mode holds the seed, it does not refuse it", resp.State, engine.AdmissionStateAccepted)
	}
	if !resp.Queued || resp.QueuePosition != 1 {
		t.Fatalf("over-limit queued seed = %+v, want it parked at position 1", resp)
	}
	if got := executionStatus(t, state, ctx, resp.ExecutionID); got != types.ExecutionStatusQueued {
		t.Fatalf("seeded execution status = %q, want %q", got, types.ExecutionStatusQueued)
	}
	if entries := parkedOutbox(t, state, ctx, resp.ExecutionID); len(entries) == 0 {
		t.Fatal("queued seeded execution holds no intents: a crash would lose the work its admission promised to run")
	}
	if entries := readyOutbox(t, state, ctx, resp.ExecutionID); len(entries) != 0 {
		t.Fatalf("ready intents on a queued seeded execution = %d, want none: a deliverable intent starts work that holds no slot", len(entries))
	}

	if err := state.UpdateExecutionStatus(ctx, holder, types.ExecutionStatusSuccess, ""); err != nil {
		t.Fatalf("release holder error = %v", err)
	}
	if got := executionStatus(t, state, ctx, resp.ExecutionID); got != types.ExecutionStatusRunning {
		t.Fatalf("promoted seeded execution status = %q, want %q", got, types.ExecutionStatusRunning)
	}
	if entries := readyOutbox(t, state, ctx, resp.ExecutionID); len(entries) == 0 {
		t.Fatal("promoted seeded execution has no deliverable intents: its parked work was lost instead of released")
	}
}

// seedSharesTheLineWithSubmissions pins that a seed and a submission contend for
// the same capacity and wait in the same order. Two separate lines, or an
// admission that bypasses the line for seeds, would each let a workflow run
// over its limit while every individual admission looked correct.
func seedSharesTheLineWithSubmissions(t *testing.T, state engine.StateStore) {
	ctx := context.Background()
	g := triggerGroupWithDownstreamGraph(t)
	admission := queueAdmissionFor(t, g, 1, 0, 0)

	holder, _ := queueSubmit(t, state, ctx, g, admission)
	submitted, subRes := queueSubmit(t, state, ctx, g, admission)
	if !subRes.Queued || subRes.QueuePosition != 1 {
		t.Fatalf("submission behind the holder = %+v, want position 1", subRes)
	}
	seeded := seedLimited(t, state, ctx, entrySeedRequest(t, g, "k-seed-line"), admission)
	if !seeded.Queued || seeded.QueuePosition != 2 {
		t.Fatalf("seed behind a queued submission = %+v, want position 2: the seed waits in the same line, in arrival order", seeded)
	}

	// First release promotes the submission — it arrived first, and promoting the
	// seed would be the starvation the FIFO order exists to prevent.
	if err := state.UpdateExecutionStatus(ctx, holder, types.ExecutionStatusSuccess, ""); err != nil {
		t.Fatalf("release holder error = %v", err)
	}
	if got := executionStatus(t, state, ctx, submitted); got != types.ExecutionStatusRunning {
		t.Fatalf("after the holder released, submission status = %q, want it promoted", got)
	}
	if got := executionStatus(t, state, ctx, seeded.ExecutionID); got != types.ExecutionStatusQueued {
		t.Fatalf("after the holder released, seeded status = %q, want it still queued", got)
	}

	// Second release promotes the seed, and its held work becomes deliverable.
	if err := state.UpdateExecutionStatus(ctx, submitted, types.ExecutionStatusSuccess, ""); err != nil {
		t.Fatalf("release submission error = %v", err)
	}
	if got := executionStatus(t, state, ctx, seeded.ExecutionID); got != types.ExecutionStatusRunning {
		t.Fatalf("after the second release, seeded status = %q, want it promoted", got)
	}
	if entries := readyOutbox(t, state, ctx, seeded.ExecutionID); len(entries) == 0 {
		t.Fatal("promoted seeded execution has no deliverable intents")
	}
}

// seedDuplicateRetryKeepsItsPlace pins the retry a broker redelivery produces on
// a seed that is still waiting. The retry created nothing, so it must neither
// take a second place in line nor lose the place its first attempt took — both
// of which an admission that charged before checking occupancy would do.
func seedDuplicateRetryKeepsItsPlace(t *testing.T, state engine.StateStore) {
	ctx := context.Background()
	g := triggerGroupWithDownstreamGraph(t)
	admission := queueAdmissionFor(t, g, 1, 0, 0)

	holder, _ := queueSubmit(t, state, ctx, g, admission)
	req := entrySeedRequest(t, g, "k-seed-dup")
	first := seedLimited(t, state, ctx, req, admission)
	if !first.Queued || first.QueuePosition != 1 {
		t.Fatalf("first seed = %+v, want position 1", first)
	}
	retry := seedLimited(t, state, ctx, req, admission)
	if !retry.Duplicate || retry.State != engine.AdmissionStateAccepted {
		t.Fatalf("redelivered seed = %+v, want an accepted duplicate", retry)
	}
	if retry.ExecutionID != first.ExecutionID {
		t.Fatalf("redelivered seed named execution %q, want the original %q", retry.ExecutionID, first.ExecutionID)
	}
	if !retry.Queued || retry.QueuePosition != 1 {
		t.Fatalf("redelivered seed = %+v, want the original place in line", retry)
	}

	// The proof that no second place was taken: the next arrival is position 2,
	// not 3.
	behind := seedLimited(t, state, ctx, entrySeedRequest(t, g, "k-seed-dup-2"), admission)
	if !behind.Queued || behind.QueuePosition != 2 {
		t.Fatalf("seed behind the retried one = %+v, want position 2", behind)
	}

	// And the promotion shows the line still holds one member per admission.
	if err := state.UpdateExecutionStatus(ctx, holder, types.ExecutionStatusSuccess, ""); err != nil {
		t.Fatalf("release holder error = %v", err)
	}
	if got := executionStatus(t, state, ctx, first.ExecutionID); got != types.ExecutionStatusRunning {
		t.Fatalf("after the holder released, original seed status = %q, want it promoted", got)
	}
	if got := executionStatus(t, state, ctx, behind.ExecutionID); got != types.ExecutionStatusQueued {
		t.Fatalf("after the holder released, later seed status = %q, want it still queued", got)
	}
}

// seedRefusesWhenTheQueueIsFull pins max_queued on the seed path. A full backlog
// is refused like a reached limit — nothing persisted, nothing queued — because
// the caller's only correct response to both is to retry later, and a seed
// parked in a line that is not admitting anyone would hold the entry-unit result
// forever instead.
func seedRefusesWhenTheQueueIsFull(t *testing.T, state engine.StateStore) {
	ctx := context.Background()
	g := triggerGroupWithDownstreamGraph(t)
	admission := queueAdmissionFor(t, g, 1, 1, 0)

	holder, _ := queueSubmit(t, state, ctx, g, admission)
	waiting := seedLimited(t, state, ctx, entrySeedRequest(t, g, "k-seed-full-1"), admission)
	if !waiting.Queued || waiting.QueuePosition != 1 {
		t.Fatalf("first queued seed = %+v, want position 1", waiting)
	}

	req := entrySeedRequest(t, g, "k-seed-full-2")
	overflow := seedLimited(t, state, ctx, req, admission)
	if overflow.State != engine.AdmissionStateQueueFull {
		t.Fatalf("seed past max_queued state = %q, want %q", overflow.State, engine.AdmissionStateQueueFull)
	}
	if overflow.ExecutionID != "" {
		t.Fatalf("refused seed named execution %q, want none", overflow.ExecutionID)
	}
	if snap := seedExecution(t, state, ctx, engine.DeterministicExecutionID(req.AdmissionKey)); snap != nil {
		t.Fatalf("refused seed persisted %+v, want nothing", snap)
	}

	// The refusal did not take a place: the holder's release promotes the one
	// seed that was actually waiting, and the line is then empty.
	if err := state.UpdateExecutionStatus(ctx, holder, types.ExecutionStatusSuccess, ""); err != nil {
		t.Fatalf("release holder error = %v", err)
	}
	if got := executionStatus(t, state, ctx, waiting.ExecutionID); got != types.ExecutionStatusRunning {
		t.Fatalf("after the holder released, waiting seed status = %q, want it promoted", got)
	}
	next := seedLimited(t, state, ctx, entrySeedRequest(t, g, "k-seed-full-3"), admission)
	if !next.Queued || next.QueuePosition != 1 {
		t.Fatalf("seed after the queue drained = %+v, want position 1", next)
	}
}

// seedTerminalSeedLeavesTheLine pins the admission that resolves its own
// execution. A single-unit trigger group is complete the moment it is seeded, so
// a queued admission of one has nothing left to wait for: it must give its place
// back rather than hold capacity no one will ever release, and it must not report
// a wait it is not in.
func seedTerminalSeedLeavesTheLine(t *testing.T, manifest *Manifest, state engine.StateStore) {
	ctx := context.Background()
	// UnitCount=1: the trigger group is the whole workflow, so the admission
	// itself finalizes the execution.
	g := triggerGroupOnlyGraph(t)
	admission := queueAdmissionFor(t, g, 1, 0, 0)

	// The holder is never released: the seeded execution has to leave the line on
	// its own, or this case would pass on a reservation that only a release could
	// undo.
	holder, _ := queueSubmit(t, state, ctx, g, admission)
	exits := []engine.BoundaryExit{{NodeName: "body", Port: "main", Data: map[string]any{"r": 1}}}
	req := buildAdmissionRequest(t, g, "k-seed-terminal", engine.GroupOutcomeSuccess, exits, nil)
	resp := seedLimited(t, state, ctx, req, admission)
	if resp.State != engine.AdmissionStateAccepted {
		t.Fatalf("seed state = %q, want %q", resp.State, engine.AdmissionStateAccepted)
	}
	if resp.Queued {
		t.Fatalf("seed = %+v, want it not reported as queued: its execution finished inside the admission", resp)
	}
	snap := seedExecution(t, state, ctx, resp.ExecutionID)
	if snap == nil || snap.Status != types.ExecutionStatusSuccess {
		t.Fatalf("seeded execution = %+v, want a finished %q", snap, types.ExecutionStatusSuccess)
	}
	// The position read is a separate capability and a separate subtest: a
	// backend that queues without an inspector must not lose the release
	// assertions below, but the omitted assertion must still be visible as a
	// CAPABILITY-SKIP rather than folded into a passing parent.
	t.Run("InspectorReportsNoPositionForTheFinishedSeed", func(t *testing.T) {
		inspector, ok := OptionalCapability[engine.ConcurrencyQueueInspector](t, manifest, CapabilityConcurrencyQueueInspector, state)
		if !ok {
			return
		}
		if got := queuePosition(t, inspector, ctx, resp.ExecutionID); got != 0 {
			t.Fatalf("position of the finished seeded execution = %d, want 0: it is not waiting for anything", got)
		}
	})

	// The place was given back rather than held: an arrival behind it is the head
	// of the line. A finished execution still occupying its place would make this
	// one position 2 (or, worse, would be promoted and never start).
	behind, res := queueSubmit(t, state, ctx, g, admission)
	if !res.Queued || res.QueuePosition != 1 {
		t.Fatalf("submission after the terminal seed = %+v, want position 1", res)
	}

	// And the slot is still the holder's, so the release promotes the newcomer —
	// not the execution that already finished.
	if err := state.UpdateExecutionStatus(ctx, holder, types.ExecutionStatusSuccess, ""); err != nil {
		t.Fatalf("release holder error = %v", err)
	}
	if got := executionStatus(t, state, ctx, behind); got != types.ExecutionStatusRunning {
		t.Fatalf("after the holder released, submission status = %q, want it promoted", got)
	}
	if got := executionStatus(t, state, ctx, resp.ExecutionID); got != types.ExecutionStatusSuccess {
		t.Fatalf("finished seeded execution status = %q, want %q", got, types.ExecutionStatusSuccess)
	}
}

// seedDuplicateAfterCompletionReleasesItsCharge pins the one duplicate a
// deterministic execution ID makes possible: a redelivery of a batch whose
// execution has already finished. Its slot was released when it terminated, so
// the retry finds nothing to short-circuit on and charges again on the way in —
// and that charge has to be given back, or every redelivered batch costs the
// workflow one slot of capacity until the reconciler sweeps it.
func seedDuplicateAfterCompletionReleasesItsCharge(t *testing.T, state engine.StateStore) {
	ctx := context.Background()
	g := triggerGroupWithDownstreamGraph(t)
	admission := concurrencyAdmissionFor(t, g, 1)

	holder, _ := queueSubmit(t, state, ctx, g, admission)
	req := entrySeedRequest(t, g, "k-seed-after-done")
	if resp := seedLimited(t, state, ctx, req, admission); resp.State != engine.AdmissionStateOverLimit {
		t.Fatalf("seed while the holder runs = %+v, want a refusal", resp)
	}

	// Admitted once the slot frees, then finished by its own terminal write.
	if err := state.UpdateExecutionStatus(ctx, holder, types.ExecutionStatusSuccess, ""); err != nil {
		t.Fatalf("release holder error = %v", err)
	}
	first := seedLimited(t, state, ctx, req, admission)
	if first.State != engine.AdmissionStateAccepted || first.Duplicate {
		t.Fatalf("seed after the release = %+v, want a first-time acceptance", first)
	}
	if err := state.UpdateExecutionStatus(ctx, first.ExecutionID, types.ExecutionStatusSuccess, ""); err != nil {
		t.Fatalf("finish seeded execution error = %v", err)
	}

	dup := seedLimited(t, state, ctx, req, admission)
	if !dup.Duplicate || dup.State != engine.AdmissionStateAccepted {
		t.Fatalf("redelivered finished batch = %+v, want an accepted duplicate", dup)
	}

	// The retry must have released whatever it charged: a submission finds the
	// slot free, exactly as it was before the redelivery.
	limited := concurrencyCapability(t, state)
	next, entries := concurrencySnapshot(t, g)
	if _, err := limited.CreateExecutionWithConcurrency(ctx, next, entries, admission); err != nil {
		t.Fatalf("submission after the redelivery error = %v, want the retry to have given its charge back", err)
	}
}
