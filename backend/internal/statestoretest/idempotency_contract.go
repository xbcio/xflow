package statestoretest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// IdempotencyHarness is one fresh backend for one idempotency sub-test.
//
// Elapse lets time pass for the store. A store whose clock is real (the
// in-memory one, a real Redis) leaves it nil and the suite sleeps; miniredis
// expires keys only when told to, so its harness sleeps AND fast-forwards —
// a claim's lease is judged against the caller's wall clock while its TTL is
// judged by the server.
type IdempotencyHarness struct {
	State  engine.StateStore
	Elapse func(d time.Duration)
}

func (h IdempotencyHarness) elapse(d time.Duration) {
	if h.Elapse != nil {
		h.Elapse(d)
		return
	}
	time.Sleep(d)
}

// IdempotencyStoreFactory returns a fresh harness for one sub-test; the
// factory registers its own cleanup.
type IdempotencyStoreFactory func(t *testing.T) IdempotencyHarness

// RunIdempotencyContract exercises engine.IdempotentStateStore against a
// concrete backend, both directly and through the engine's keyed Submit,
// Invoke and Rerun.
//
// The cases are organised around the three things a keyed request must never
// do: run twice (a retry, a concurrent duplicate, a retry after a crash
// between claim and creation), answer for work it was not asked to do (a
// conflicting reuse of a key), or answer for work that never happened (a
// claim left behind by a refusal that created nothing).
func RunIdempotencyContract(t *testing.T, manifest *Manifest, newStore IdempotencyStoreFactory) {
	t.Helper()
	manifest.Report(t)
	manifest.RequireDeclared(t, CapabilityIdempotentStateStore)
	t.Run("RetryReturnsTheOriginalExecution", func(t *testing.T) {
		idempotencyRetryReturnsTheOriginal(t, newStore(t))
	})
	t.Run("DifferentRequestIsAConflict", func(t *testing.T) {
		idempotencyDifferentRequestConflicts(t, newStore(t))
	})
	t.Run("ExpiredClaimIsFree", func(t *testing.T) {
		idempotencyExpiredClaimIsFree(t, newStore(t))
	})
	t.Run("ConcurrencyRefusalReleasesTheClaim", func(t *testing.T) {
		idempotencyRefusalReleasesTheClaim(t, newStore(t))
	})
	t.Run("AbandonedClaimIsTakenOver", func(t *testing.T) {
		idempotencyAbandonedClaimIsTakenOver(t, newStore(t))
	})
	t.Run("LiveClaimIsInProgress", func(t *testing.T) {
		idempotencyLiveClaimIsInProgress(t, newStore(t))
	})
	t.Run("QueuedDuplicateReportsQueued", func(t *testing.T) {
		idempotencyQueuedDuplicateReportsQueued(t, manifest, newStore(t))
	})
	t.Run("ExecutionWithoutClaimIsADuplicate", func(t *testing.T) {
		idempotencyExecutionWithoutClaimIsADuplicate(t, newStore(t))
	})
	t.Run("StaleTokenCannotSettle", func(t *testing.T) {
		idempotencyStaleTokenCannotSettle(t, newStore(t))
	})
	t.Run("ConfirmedClaimIsNotReleased", func(t *testing.T) {
		idempotencyConfirmedClaimIsNotReleased(t, newStore(t))
	})
	t.Run("ConcurrentRequestsCreateOne", func(t *testing.T) {
		idempotencyConcurrentRequestsCreateOne(t, newStore(t))
	})
	t.Run("RerunRetryReturnsTheOriginalRerun", func(t *testing.T) {
		idempotencyRerunRetryReturnsTheOriginal(t, manifest, newStore(t))
	})
	t.Run("RerunFromNodeIsPartOfTheFingerprint", func(t *testing.T) {
		idempotencyRerunFromNodeIsPartOfTheFingerprint(t, manifest, newStore(t))
	})
}

// discardQueue accepts every task and delivers none: the cases are about what
// the store persisted, and no runner is consuming.
type discardQueue struct{}

func (discardQueue) Enqueue(context.Context, *engine.Task) error { return nil }
func (discardQueue) EnqueueDelayed(context.Context, *engine.Task, time.Duration) error {
	return nil
}

func idempotencyCapability(t *testing.T, state engine.StateStore) engine.IdempotentStateStore {
	t.Helper()
	store, ok := state.(engine.IdempotentStateStore)
	if !ok {
		t.Fatalf("state store %T does not implement engine.IdempotentStateStore", state)
	}
	return store
}

// idempotencyGraph compiles a one-root workflow, optionally with a
// concurrency setting.
func idempotencyGraph(t *testing.T, name string, conc *types.ConcurrencySettings) *graph.Graph {
	t.Helper()
	def := &types.WorkflowDef{
		Name:    name,
		Version: "1",
		// xflow.start makes "start" an entry, so Invoke can address it.
		Nodes: []types.NodeDef{{Name: "start", Type: "xflow.start"}},
	}
	if conc != nil {
		def.Settings = &types.WorkflowSettings{Concurrency: conc}
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("Compile(%s) error = %v", name, err)
	}
	return g
}

// idempotencyTwoRootGraph compiles a workflow with two entry roots, so one
// parent can be rerun from either of them: the shape the from_node fingerprint
// case needs, two requests that are individually valid reruns of the same
// parent and differ only in where they start.
func idempotencyTwoRootGraph(t *testing.T, name string) *graph.Graph {
	t.Helper()
	def := &types.WorkflowDef{
		Name:    name,
		Version: "1",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "other", Type: "xflow.start"},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("Compile(%s) error = %v", name, err)
	}
	return g
}

func keyedContext(scope, key string) (context.Context, *engine.Idempotency) {
	idem := &engine.Idempotency{Key: key, Scope: scope}
	return engine.WithIdempotency(context.Background(), idem), idem
}

func mustExecution(t *testing.T, state engine.StateStore, id types.ExecutionID) *engine.ExecutionSnapshot {
	t.Helper()
	snap, err := state.GetExecution(context.Background(), id)
	if err != nil {
		t.Fatalf("GetExecution(%q) error = %v", id, err)
	}
	if snap == nil {
		t.Fatalf("execution %q was not persisted", id)
	}
	return snap
}

func idempotencyRetryReturnsTheOriginal(t *testing.T, h IdempotencyHarness) {
	eng := engine.New(h.State, discardQueue{})
	g := idempotencyGraph(t, "idem-retry", nil)
	params := map[string]any{"order": "o-1", "qty": 2}

	ctx, first := keyedContext("workflow:wf-retry", "order-1")
	id, err := eng.Submit(ctx, g, params)
	if err != nil {
		t.Fatalf("first keyed Submit error = %v", err)
	}
	if first.Duplicate {
		t.Fatal("first keyed Submit reported Duplicate")
	}
	if want := engine.IdempotentExecutionID(namespace.Default, "workflow:wf-retry", "order-1"); id != want {
		t.Fatalf("keyed execution id = %q, want the deterministic %q", id, want)
	}
	mustExecution(t, h.State, id)

	// The same request built in a different map order, and as the numbers a
	// JSON body would decode to, is the same request.
	retryCtx, retry := keyedContext("workflow:wf-retry", "order-1")
	again, err := eng.Submit(retryCtx, g, map[string]any{"qty": float64(2), "order": "o-1"})
	if err != nil {
		t.Fatalf("retried keyed Submit error = %v", err)
	}
	if again != id || !retry.Duplicate {
		t.Fatalf("retry = (%q, duplicate=%v), want (%q, true)", again, retry.Duplicate, id)
	}

	// Invoke shares the mechanism; its entry is part of the request.
	invokeCtx, invoked := keyedContext("workflow:wf-retry", "order-2")
	invID, err := eng.Invoke(invokeCtx, g, "start", params)
	if err != nil {
		t.Fatalf("keyed Invoke error = %v", err)
	}
	invokeAgainCtx, invokedAgain := keyedContext("workflow:wf-retry", "order-2")
	invAgain, err := eng.Invoke(invokeAgainCtx, g, "start", params)
	if err != nil {
		t.Fatalf("retried keyed Invoke error = %v", err)
	}
	if invoked.Duplicate || !invokedAgain.Duplicate || invAgain != invID {
		t.Fatalf("invoke retry = (%q, duplicate=%v), want (%q, true)", invAgain, invokedAgain.Duplicate, invID)
	}
}

func idempotencyDifferentRequestConflicts(t *testing.T, h IdempotencyHarness) {
	eng := engine.New(h.State, discardQueue{})
	g := idempotencyGraph(t, "idem-conflict", nil)

	ctx, _ := keyedContext("workflow:wf-conflict", "k")
	id, err := eng.Submit(ctx, g, map[string]any{"order": "o-1"})
	if err != nil {
		t.Fatalf("first keyed Submit error = %v", err)
	}

	cases := map[string]func(context.Context) (types.ExecutionID, error){
		"params": func(ctx context.Context) (types.ExecutionID, error) {
			return eng.Submit(ctx, g, map[string]any{"order": "o-2"})
		},
		"entry": func(ctx context.Context) (types.ExecutionID, error) {
			return eng.Invoke(ctx, g, "start", map[string]any{"order": "o-1"})
		},
		"test flag": func(ctx context.Context) (types.ExecutionID, error) {
			return eng.Submit(engine.WithTestRun(ctx, true), g, map[string]any{"order": "o-1"})
		},
	}
	for name, run := range cases {
		ctx, idem := keyedContext("workflow:wf-conflict", "k")
		got, err := run(ctx)
		if !errors.Is(err, engine.ErrIdempotencyConflict) {
			t.Fatalf("%s: reused key error = %v (id %q), want ErrIdempotencyConflict", name, err, got)
		}
		if idem.Duplicate {
			t.Fatalf("%s: a conflict reported Duplicate", name)
		}
	}
	// The original is untouched by the refused requests.
	if snap := mustExecution(t, h.State, id); snap.Params["order"] != "o-1" {
		t.Fatalf("original params = %v, want order=o-1", snap.Params)
	}
}

func idempotencyExpiredClaimIsFree(t *testing.T, h IdempotencyHarness) {
	store := idempotencyCapability(t, h.State)
	ctx := context.Background()
	id := engine.IdempotentExecutionID(namespace.Default, "workflow:wf-expire", "k")

	claim, err := store.ClaimIdempotency(ctx, engine.IdempotencyClaimRequest{
		ExecutionID: id, Fingerprint: "fp-1", TTL: 200 * time.Millisecond, Lease: time.Minute, Token: "t-1",
	})
	if err != nil || claim.State != engine.IdempotencyClaimAccepted {
		t.Fatalf("first claim = (%+v, %v), want accepted", claim, err)
	}
	h.elapse(400 * time.Millisecond)

	// A different fingerprint proves the key is free rather than taken over.
	claim, err = store.ClaimIdempotency(ctx, engine.IdempotencyClaimRequest{
		ExecutionID: id, Fingerprint: "fp-2", TTL: time.Minute, Lease: time.Minute, Token: "t-2",
	})
	if err != nil || claim.State != engine.IdempotencyClaimAccepted {
		t.Fatalf("claim after the TTL = (%+v, %v), want accepted", claim, err)
	}
}

func idempotencyRefusalReleasesTheClaim(t *testing.T, h IdempotencyHarness) {
	eng := engine.New(h.State, discardQueue{})
	g := idempotencyGraph(t, "idem-refused", &types.ConcurrencySettings{Limit: 1, Mode: types.ConcurrencyModeReject})

	holder, err := eng.Submit(context.Background(), g, nil)
	if err != nil {
		t.Fatalf("slot holder Submit error = %v", err)
	}
	ctx, _ := keyedContext("workflow:wf-refused", "k")
	if _, err := eng.Submit(ctx, g, nil); !errors.Is(err, engine.ErrConcurrencyLimit) {
		t.Fatalf("keyed over-limit Submit error = %v, want ErrConcurrencyLimit", err)
	}
	id := engine.IdempotentExecutionID(namespace.Default, "workflow:wf-refused", "k")
	if snap, err := h.State.GetExecution(context.Background(), id); err != nil || snap != nil {
		t.Fatalf("refused keyed submission persisted (%+v, %v)", snap, err)
	}

	if err := h.State.UpdateExecutionStatus(context.Background(), holder, types.ExecutionStatusSuccess, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus(holder) error = %v", err)
	}
	// The retry the 429 invited is judged afresh: a claim the refusal left
	// behind would answer it as a duplicate of an execution that never existed.
	retryCtx, retry := keyedContext("workflow:wf-refused", "k")
	got, err := eng.Submit(retryCtx, g, nil)
	if err != nil {
		t.Fatalf("keyed retry after the slot freed error = %v", err)
	}
	if retry.Duplicate || got != id {
		t.Fatalf("retry = (%q, duplicate=%v), want (%q, false)", got, retry.Duplicate, id)
	}
	mustExecution(t, h.State, id)
}

func idempotencyAbandonedClaimIsTakenOver(t *testing.T, h IdempotencyHarness) {
	store := idempotencyCapability(t, h.State)
	eng := engine.New(h.State, discardQueue{}, engine.WithIdempotencyClaimLease(100*time.Millisecond))
	g := idempotencyGraph(t, "idem-takeover", nil)
	fp, err := engine.SubmissionFingerprint(g, "", nil, false, "")
	if err != nil {
		t.Fatalf("SubmissionFingerprint error = %v", err)
	}
	id := engine.IdempotentExecutionID(namespace.Default, "workflow:wf-takeover", "k")

	// The request that took this claim crashed before it created anything.
	claim, err := store.ClaimIdempotency(context.Background(), engine.IdempotencyClaimRequest{
		ExecutionID: id, Fingerprint: fp, TTL: time.Minute, Lease: 100 * time.Millisecond, Token: "crashed",
	})
	if err != nil || claim.State != engine.IdempotencyClaimAccepted {
		t.Fatalf("crashed request's claim = (%+v, %v), want accepted", claim, err)
	}
	h.elapse(300 * time.Millisecond)

	ctx, redo := keyedContext("workflow:wf-takeover", "k")
	got, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("Submit over an abandoned claim error = %v", err)
	}
	if got != id || redo.Duplicate {
		t.Fatalf("redo = (%q, duplicate=%v), want (%q, false)", got, redo.Duplicate, id)
	}
	mustExecution(t, h.State, id)

	againCtx, again := keyedContext("workflow:wf-takeover", "k")
	if got, err := eng.Submit(againCtx, g, nil); err != nil || got != id || !again.Duplicate {
		t.Fatalf("retry after the redo = (%q, duplicate=%v, %v), want (%q, true, nil)", got, again.Duplicate, err, id)
	}
}

func idempotencyLiveClaimIsInProgress(t *testing.T, h IdempotencyHarness) {
	store := idempotencyCapability(t, h.State)
	eng := engine.New(h.State, discardQueue{})
	g := idempotencyGraph(t, "idem-live", nil)
	fp, err := engine.SubmissionFingerprint(g, "", nil, false, "")
	if err != nil {
		t.Fatalf("SubmissionFingerprint error = %v", err)
	}
	id := engine.IdempotentExecutionID(namespace.Default, "workflow:wf-live", "k")
	if _, err := store.ClaimIdempotency(context.Background(), engine.IdempotencyClaimRequest{
		ExecutionID: id, Fingerprint: fp, TTL: time.Minute, Lease: time.Minute, Token: "working",
	}); err != nil {
		t.Fatalf("ClaimIdempotency error = %v", err)
	}

	ctx, _ := keyedContext("workflow:wf-live", "k")
	if _, err := eng.Submit(ctx, g, nil); !errors.Is(err, engine.ErrIdempotencyInProgress) {
		t.Fatalf("Submit against a live claim error = %v, want ErrIdempotencyInProgress", err)
	}
	if snap, err := h.State.GetExecution(context.Background(), id); err != nil || snap != nil {
		t.Fatalf("in-progress request created the execution (%+v, %v)", snap, err)
	}
}

func idempotencyQueuedDuplicateReportsQueued(t *testing.T, manifest *Manifest, h IdempotencyHarness) {
	store := idempotencyCapability(t, h.State)
	if _, ok := OptionalCapability[engine.ConcurrencyLimitedStateStore](t, manifest, CapabilityConcurrencyLimitedStateStore, h.State); !ok {
		return
	}
	if !manifest.SkipUnlessDeclared(t, CapabilityConcurrencyQueueMode) {
		return
	}
	eng := engine.New(h.State, discardQueue{})
	g := idempotencyGraph(t, "idem-queued", &types.ConcurrencySettings{Limit: 1, Mode: types.ConcurrencyModeQueue})

	if _, err := eng.Submit(context.Background(), g, nil); err != nil {
		t.Fatalf("slot holder Submit error = %v", err)
	}
	ctx, _ := keyedContext("workflow:wf-queued", "k")
	id, err := eng.Submit(ctx, g, nil)
	if errors.Is(err, engine.ErrConcurrencyQueueUnsupported) {
		// The manifest declares queue mode, and the queue suite is what proves
		// the parking/promotion behaviour. A refusal here contradicts the
		// declaration, so it is a failure rather than a quiet skip.
		t.Fatalf("backend %q declares %s but keyed Submit refused a queue-mode graph with "+
			"ErrConcurrencyQueueUnsupported", manifest.Backend(), CapabilityConcurrencyQueueMode)
	}
	if err != nil {
		t.Fatalf("keyed queued Submit error = %v", err)
	}
	if snap := mustExecution(t, h.State, id); snap.Status != types.ExecutionStatusQueued {
		t.Fatalf("keyed submission status = %q, want queued", snap.Status)
	}

	retryCtx, retry := keyedContext("workflow:wf-queued", "k")
	if got, err := eng.Submit(retryCtx, g, nil); err != nil || got != id || !retry.Duplicate {
		t.Fatalf("retry of a queued submission = (%q, duplicate=%v, %v), want (%q, true, nil)", got, retry.Duplicate, err, id)
	}
	fp, err := engine.SubmissionFingerprint(g, "", nil, false, "")
	if err != nil {
		t.Fatalf("SubmissionFingerprint error = %v", err)
	}
	claim, err := store.ClaimIdempotency(context.Background(), engine.IdempotencyClaimRequest{
		ExecutionID: id, Fingerprint: fp, TTL: time.Minute, Lease: time.Minute, Token: "probe",
	})
	if err != nil || claim.State != engine.IdempotencyClaimDuplicate || claim.Status != types.ExecutionStatusQueued {
		t.Fatalf("claim on a queued execution = (%+v, %v), want duplicate with status queued", claim, err)
	}
	// Still exactly one execution in line: the retry did not park a second.
	if snap := mustExecution(t, h.State, id); snap.Status != types.ExecutionStatusQueued {
		t.Fatalf("status after the retry = %q, want queued", snap.Status)
	}
}

func idempotencyExecutionWithoutClaimIsADuplicate(t *testing.T, h IdempotencyHarness) {
	store := idempotencyCapability(t, h.State)
	g := idempotencyGraph(t, "idem-outlived", nil)
	id := engine.IdempotentExecutionID(namespace.Default, "workflow:wf-outlived", "k")
	if err := h.State.CreateExecution(context.Background(), &engine.ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution error = %v", err)
	}
	claim, err := store.ClaimIdempotency(context.Background(), engine.IdempotencyClaimRequest{
		ExecutionID: id, Fingerprint: "any", TTL: time.Minute, Lease: time.Minute, Token: "t",
	})
	if err != nil || claim.State != engine.IdempotencyClaimDuplicate || claim.Status != types.ExecutionStatusRunning {
		t.Fatalf("claim on an execution with no claim = (%+v, %v), want duplicate with status running", claim, err)
	}
}

func idempotencyStaleTokenCannotSettle(t *testing.T, h IdempotencyHarness) {
	store := idempotencyCapability(t, h.State)
	ctx := context.Background()
	id := engine.IdempotentExecutionID(namespace.Default, "workflow:wf-stale", "k")
	req := engine.IdempotencyClaimRequest{ExecutionID: id, Fingerprint: "fp", TTL: time.Minute, Lease: 100 * time.Millisecond, Token: "slow"}
	if claim, err := store.ClaimIdempotency(ctx, req); err != nil || claim.State != engine.IdempotencyClaimAccepted {
		t.Fatalf("first claim = (%+v, %v), want accepted", claim, err)
	}
	h.elapse(300 * time.Millisecond)
	req.Token, req.Lease = "fresh", time.Minute
	if claim, err := store.ClaimIdempotency(ctx, req); err != nil || claim.State != engine.IdempotencyClaimTakeover {
		t.Fatalf("takeover claim = (%+v, %v), want claimed_pending", claim, err)
	}

	// The overtaken request wakes up and tries to settle a claim it no longer
	// holds; neither write may touch the request that took it over.
	if err := store.ReleaseIdempotency(ctx, id, "slow"); err != nil {
		t.Fatalf("stale ReleaseIdempotency error = %v", err)
	}
	if err := store.ConfirmIdempotency(ctx, id, "slow"); err != nil {
		t.Fatalf("stale ConfirmIdempotency error = %v", err)
	}
	req.Token = "probe"
	if claim, err := store.ClaimIdempotency(ctx, req); err != nil || claim.State != engine.IdempotencyClaimInProgress {
		t.Fatalf("claim after stale settles = (%+v, %v), want in_progress (the takeover still holds it)", claim, err)
	}
}

func idempotencyConfirmedClaimIsNotReleased(t *testing.T, h IdempotencyHarness) {
	store := idempotencyCapability(t, h.State)
	ctx := context.Background()
	id := engine.IdempotentExecutionID(namespace.Default, "workflow:wf-confirmed", "k")
	req := engine.IdempotencyClaimRequest{ExecutionID: id, Fingerprint: "fp", TTL: time.Minute, Lease: 100 * time.Millisecond, Token: "t"}
	if _, err := store.ClaimIdempotency(ctx, req); err != nil {
		t.Fatalf("ClaimIdempotency error = %v", err)
	}
	if err := store.ConfirmIdempotency(ctx, id, "t"); err != nil {
		t.Fatalf("ConfirmIdempotency error = %v", err)
	}
	if err := store.ReleaseIdempotency(ctx, id, "t"); err != nil {
		t.Fatalf("ReleaseIdempotency error = %v", err)
	}
	h.elapse(300 * time.Millisecond)

	// Confirmed means the execution was created. With no execution visible (it
	// expired before its claim did) the claim still answers duplicate: the
	// lease is only ever a reason to redo a creation that never happened.
	req.Token = "retry"
	if claim, err := store.ClaimIdempotency(ctx, req); err != nil || claim.State != engine.IdempotencyClaimDuplicate {
		t.Fatalf("claim after confirm = (%+v, %v), want duplicate", claim, err)
	}
}

func idempotencyConcurrentRequestsCreateOne(t *testing.T, h IdempotencyHarness) {
	eng := engine.New(h.State, discardQueue{})
	g := idempotencyGraph(t, "idem-concurrent", nil)
	const n = 16

	type outcome struct {
		id        types.ExecutionID
		duplicate bool
		err       error
	}
	results := make([]outcome, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, idem := keyedContext("workflow:wf-concurrent", "k")
			id, err := eng.Submit(ctx, g, nil)
			results[i] = outcome{id: id, duplicate: idem.Duplicate, err: err}
		}(i)
	}
	wg.Wait()

	created := 0
	want := engine.IdempotentExecutionID(namespace.Default, "workflow:wf-concurrent", "k")
	for i, res := range results {
		switch {
		case res.err == nil && !res.duplicate:
			created++
			fallthrough
		case res.err == nil:
			if res.id != want {
				t.Fatalf("request %d id = %q, want %q", i, res.id, want)
			}
		case errors.Is(res.err, engine.ErrIdempotencyInProgress):
		default:
			t.Fatalf("request %d error = %v", i, res.err)
		}
	}
	if created != 1 {
		t.Fatalf("%d of %d concurrent keyed requests created the execution, want exactly 1", created, n)
	}
	mustExecution(t, h.State, want)
}

func idempotencyRerunRetryReturnsTheOriginal(t *testing.T, manifest *Manifest, h IdempotencyHarness) {
	if _, ok := OptionalCapability[engine.ExecutionSeeder](t, manifest, CapabilityExecutionSeeder, h.State); !ok {
		return
	}
	eng := engine.New(h.State, discardQueue{})
	g := idempotencyGraph(t, "idem-rerun", nil)
	parent, err := eng.Submit(context.Background(), g, nil)
	if err != nil {
		t.Fatalf("parent Submit error = %v", err)
	}
	if err := h.State.UpdateExecutionStatus(context.Background(), parent, types.ExecutionStatusFailed, "boom"); err != nil {
		t.Fatalf("UpdateExecutionStatus(parent) error = %v", err)
	}

	req := engine.RerunRequest{ExecutionID: parent, FromNode: "start", IdempotencyKey: "rerun-1"}
	first, err := eng.Rerun(context.Background(), req)
	if err != nil {
		t.Fatalf("keyed Rerun error = %v", err)
	}
	if first.Duplicate || first.ExecutionID == parent {
		t.Fatalf("first keyed rerun = %+v, want a new execution", first)
	}
	snap := mustExecution(t, h.State, first.ExecutionID)
	if snap.ParentID != parent {
		t.Fatalf("rerun parent = %q, want %q", snap.ParentID, parent)
	}

	again, err := eng.Rerun(context.Background(), req)
	if err != nil {
		t.Fatalf("retried keyed Rerun error = %v", err)
	}
	if !again.Duplicate || again.ExecutionID != first.ExecutionID {
		t.Fatalf("retried rerun = %+v, want duplicate of %q", again, first.ExecutionID)
	}

	req.IncludeSuccess = true
	if _, err := eng.Rerun(context.Background(), req); !errors.Is(err, engine.ErrIdempotencyConflict) {
		t.Fatalf("reused rerun key with a different request error = %v, want ErrIdempotencyConflict", err)
	}

	// The key is scoped to its parent: the same key on another parent is a
	// different rerun.
	other, err := eng.Submit(context.Background(), g, nil)
	if err != nil {
		t.Fatalf("second parent Submit error = %v", err)
	}
	if err := h.State.UpdateExecutionStatus(context.Background(), other, types.ExecutionStatusFailed, "boom"); err != nil {
		t.Fatalf("UpdateExecutionStatus(other) error = %v", err)
	}
	res, err := eng.Rerun(context.Background(), engine.RerunRequest{ExecutionID: other, FromNode: "start", IdempotencyKey: "rerun-1"})
	if err != nil || res.Duplicate || res.ExecutionID == first.ExecutionID {
		t.Fatalf("same key on another parent = (%+v, %v), want a new execution", res, err)
	}
}

// idempotencyRerunFromNodeIsPartOfTheFingerprint: FromNode picks which work a
// rerun re-does, so two keyed reruns of one parent starting at different nodes
// are different requests, not a retry of one. The second must be refused as a
// conflict — answering duplicate would hand the caller a rerun that starts
// somewhere it did not ask for.
func idempotencyRerunFromNodeIsPartOfTheFingerprint(t *testing.T, manifest *Manifest, h IdempotencyHarness) {
	if _, ok := OptionalCapability[engine.ExecutionSeeder](t, manifest, CapabilityExecutionSeeder, h.State); !ok {
		return
	}
	eng := engine.New(h.State, discardQueue{})
	g := idempotencyTwoRootGraph(t, "idem-rerun-from")
	parent, err := eng.Submit(context.Background(), g, nil)
	if err != nil {
		t.Fatalf("parent Submit error = %v", err)
	}
	if err := h.State.UpdateExecutionStatus(context.Background(), parent, types.ExecutionStatusFailed, "boom"); err != nil {
		t.Fatalf("UpdateExecutionStatus(parent) error = %v", err)
	}

	first, err := eng.Rerun(context.Background(), engine.RerunRequest{ExecutionID: parent, FromNode: "start", IdempotencyKey: "rerun-from-1"})
	if err != nil {
		t.Fatalf("keyed Rerun error = %v", err)
	}
	if first.Duplicate || first.ExecutionID == parent {
		t.Fatalf("first keyed rerun = %+v, want a new execution", first)
	}
	if snap := mustExecution(t, h.State, first.ExecutionID); snap.RerunFrom != "start" {
		t.Fatalf("rerun starts at %q, want %q", snap.RerunFrom, "start")
	}

	// Same key, same parent, a different starting node: a different rerun.
	if _, err := eng.Rerun(context.Background(), engine.RerunRequest{ExecutionID: parent, FromNode: "other", IdempotencyKey: "rerun-from-1"}); !errors.Is(err, engine.ErrIdempotencyConflict) {
		t.Fatalf("same key from another node error = %v, want ErrIdempotencyConflict", err)
	}

	// The original request still answers duplicate with the rerun it started: the
	// refusal above left the claim as it was.
	again, err := eng.Rerun(context.Background(), engine.RerunRequest{ExecutionID: parent, FromNode: "start", IdempotencyKey: "rerun-from-1"})
	if err != nil || !again.Duplicate || again.ExecutionID != first.ExecutionID {
		t.Fatalf("retried rerun = (%+v, %v), want duplicate of %q", again, err, first.ExecutionID)
	}
}
