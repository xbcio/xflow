package rstate

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// newOutboxIndexTestStore returns a miniredis-backed store with a clean
// keyspace. The miniredis handle is returned because one test has to write a
// key from inside a command hook, where issuing another command on the same
// client would recurse.
func newOutboxIndexTestStore(t *testing.T) (*Store, *redis.Client, *miniredis.Miniredis) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return New(rdb, nil, time.Hour), rdb, mr
}

// seedReadyOutbox creates one execution with one ready outbox entry, exactly the
// way the production create path does — so the readiness index is populated by
// the code under test rather than by the test.
func seedReadyOutbox(t *testing.T, state *Store, ctx context.Context, id types.ExecutionID, availableAt time.Time) string {
	t.Helper()
	entryID := string(id) + "/start/0"
	if err := state.CreateExecutionWithOutbox(ctx, &engine.ExecutionSnapshot{
		ID:     id,
		Status: types.ExecutionStatusRunning,
		Graph:  testGraphTwoNode(),
	}, []engine.OutboxEntry{{
		ID:          entryID,
		Task:        engine.Task{ExecutionID: id, NodeName: "start", Type: engine.TaskTypeNodeExec},
		AvailableAt: availableAt,
	}}); err != nil {
		t.Fatalf("seed %s: %v", id, err)
	}
	return entryID
}

func indexScore(t *testing.T, ctx context.Context, rdb *redis.Client, ns namespace.Namespace, id types.ExecutionID) (float64, bool) {
	t.Helper()
	score, err := rdb.ZScore(ctx, outboxReadyIndexKey(ns), string(id)).Result()
	if errors.Is(err, redis.Nil) {
		return 0, false
	}
	if err != nil {
		t.Fatalf("ZScore(index, %q) error = %v", id, err)
	}
	return score, true
}

func containsExecution(ids []types.ExecutionID, want types.ExecutionID) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}

// TestOutboxReadyIndexRegistersOnAppendAndRemovesWhenDrained pins the index's
// whole life cycle against the two transitions that define it: an atomic
// transition that appended a ready entry registers the execution, and the claim
// that finds nothing once the flush is done removes it.
//
// The removal is placed on the terminating empty claim rather than on the ack
// on purpose. FlushOutbox loops until a claim comes back empty, so that call is
// the first moment the execution's readiness is fully determined — ack, release
// and dead-letter have all happened, and any advance or skip intent the flush
// appended is already in the ready set. Removing on the ack instead would
// delete the member for work the same flush had just appended.
func TestOutboxReadyIndexRegistersOnAppendAndRemovesWhenDrained(t *testing.T) {
	state, rdb, _ := newOutboxIndexTestStore(t)
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)
	const id = types.ExecutionID("exec-index-0")

	entryID := seedReadyOutbox(t, state, ctx, id, time.Now().Add(-time.Second))

	score, present := indexScore(t, ctx, rdb, namespace.Default, id)
	if !present {
		t.Fatal("no readiness-index member after a create that appended a ready entry; " +
			"the execution is only discoverable by the keyspace sweep, which is the " +
			"defect the index exists to remove")
	}
	if score > float64(time.Now().UTC().UnixMilli()) {
		t.Fatalf("index score = %v, want a due instant — registration writes a lower "+
			"bound at the append, so it can never be in the future", score)
	}

	discovered, err := state.ListOutboxExecutions(ctx, 16)
	if err != nil {
		t.Fatalf("ListOutboxExecutions() error = %v", err)
	}
	if !containsExecution(discovered, id) {
		t.Fatalf("discovered %v, want it to contain %q — the index the create wrote is "+
			"what discovery must read", discovered, id)
	}

	// Drain it the way the engine does: lease, ack, then the claim that finds
	// nothing.
	leased, err := state.LeaseOutbox(ctx, id, time.Now().UTC(), 8)
	if err != nil {
		t.Fatalf("LeaseOutbox() error = %v", err)
	}
	if len(leased) != 1 || leased[0].ID != entryID {
		t.Fatalf("LeaseOutbox() = %v, want the seeded entry", outboxEntryIDs(leased))
	}
	if err := state.AckOutbox(ctx, id, entryID); err != nil {
		t.Fatalf("AckOutbox() error = %v", err)
	}
	rest, err := state.LeaseOutbox(ctx, id, time.Now().UTC(), 8)
	if err != nil {
		t.Fatalf("terminating LeaseOutbox() error = %v", err)
	}
	if len(rest) != 0 {
		t.Fatalf("terminating LeaseOutbox() returned %d entries, want 0", len(rest))
	}

	if _, present := indexScore(t, ctx, rdb, namespace.Default, id); present {
		t.Fatal("the readiness-index member survived a fully drained execution; a member " +
			"with nothing behind it is returned by every discovery read forever, and a " +
			"page of them is what starves the entries that do have work")
	}
	discovered, err = state.ListOutboxExecutions(ctx, 16)
	if err != nil {
		t.Fatalf("ListOutboxExecutions() after drain error = %v", err)
	}
	if containsExecution(discovered, id) {
		t.Fatalf("discovered %v after the execution drained, want it to be gone", discovered)
	}
}

// TestOutboxReadyIndexMissedRegistrationIsFoundByTheSweepWithinItsInterval
// makes the accelerator's central claim concrete: a registration that never
// happened costs DELAY, bounded by the sweep interval, and never delivery.
//
// The registration is removed directly rather than by failing a ZADD, because
// the outcome is identical — a ready execution the index does not know about —
// and removing it is deterministic. The bound asserted here is the one the file
// note states: at most outboxIndexSweepEveryCalls discoveries, plus one full
// cursor round where the keyspace is larger than one scan page (one page covers
// this fixture, so the round is free).
func TestOutboxReadyIndexMissedRegistrationIsFoundByTheSweepWithinItsInterval(t *testing.T) {
	state, rdb, _ := newOutboxIndexTestStore(t)
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)
	const id = types.ExecutionID("exec-missed-0")

	// The entry is deferred, which is what makes the second half of this test
	// meaningful: a claim on it finds nothing, which is the transition that
	// re-registers. Registration itself marks the execution due at the append
	// regardless of the entry's score, because the score is a lower bound.
	deferred := time.Now().Add(time.Hour)
	seedReadyOutbox(t, state, ctx, id, deferred)

	// One discovery call proves the index and starts the sweep cadence. It does
	// not sweep: proving happens before the cadence is consulted.
	if ids, err := state.ListOutboxExecutions(ctx, 16); err != nil {
		t.Fatalf("ListOutboxExecutions() error = %v", err)
	} else if !containsExecution(ids, id) {
		t.Fatalf("discovered %v, want %q", ids, id)
	}

	if err := rdb.ZRem(ctx, outboxReadyIndexKey(namespace.Default), string(id)).Err(); err != nil {
		t.Fatalf("ZRem() error = %v", err)
	}

	foundAt := 0
	for call := 2; call <= 4*outboxIndexSweepEveryCalls; call++ {
		ids, err := state.ListOutboxExecutions(ctx, 16)
		if err != nil {
			t.Fatalf("ListOutboxExecutions() call %d error = %v", call, err)
		}
		if containsExecution(ids, id) {
			foundAt = call
			break
		}
	}
	if foundAt == 0 {
		t.Fatalf("the key with a dropped registration was never rediscovered across %d "+
			"calls; the sweep is the staleness bound and it is not running",
			4*outboxIndexSweepEveryCalls-1)
	}
	if want := outboxIndexSweepEveryCalls; foundAt != want {
		t.Fatalf("rediscovered on call %d, want %d — the sweep must run once every %d "+
			"discovery calls while the index is proven, so that is exactly how long a "+
			"missed registration may stay invisible (%d calls of delay after the one that "+
			"proved the index)",
			foundAt, want, outboxIndexSweepEveryCalls, want-1)
	}

	// The sweep finds it; the flush's terminating claim re-registers it. This is
	// what makes the recovery permanent rather than a one-off rediscovery.
	if score, present := indexScore(t, ctx, rdb, namespace.Default, id); present {
		t.Fatalf("index member score = %v reappeared without a drain; discovery alone "+
			"must not write the index, or a sweep of the whole keyspace would register "+
			"everything it walked past", score)
	}
	if claimed, err := state.LeaseOutbox(ctx, id, time.Now().UTC(), 8); err != nil {
		t.Fatalf("LeaseOutbox() error = %v", err)
	} else if len(claimed) != 0 {
		t.Fatalf("LeaseOutbox() = %v, want nothing — the entry is deferred until %v",
			outboxEntryIDs(claimed), deferred)
	}
	score, present := indexScore(t, ctx, rdb, namespace.Default, id)
	if !present {
		t.Fatal("the claim that found nothing did not re-register the execution; the sweep " +
			"would then have to rediscover it on every cycle instead of once")
	}
	if int64(score) != deferred.UnixMilli() {
		t.Fatalf("re-registered score = %v, want %v — the claim must record when the "+
			"deferred work actually becomes deliverable", int64(score), deferred.UnixMilli())
	}
}

// TestOutboxReadyIndexStaleEntryCostsOneNoOpClaimAndRepairsItself pins the
// consequence of scoring registration as a lower bound: the score can be stale
// EARLY, i.e. it can say "due" while the entry is not yet deliverable.
//
// That is not a false execution — it is a hint the drain verifies — and the
// price is exactly one claim that yields nothing, after which the member is
// re-armed to the instant the work actually becomes deliverable. The test
// asserts all three: the stale member is still discovered, the claim is a
// no-op, and the next read no longer returns it.
func TestOutboxReadyIndexStaleEntryCostsOneNoOpClaimAndRepairsItself(t *testing.T) {
	state, rdb, _ := newOutboxIndexTestStore(t)
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)
	const id = types.ExecutionID("exec-stale-0")

	entryID := seedReadyOutbox(t, state, ctx, id, time.Now().Add(-time.Second))

	// Another deliverer leases the entry: the ready score moves into the future
	// while the index still says "due". This is exactly the state a lease
	// creates, and exactly the state OutboxMetrics separates from a real
	// backlog.
	deadline := time.Now().UTC().Add(30 * time.Second)
	if err := rdb.ZAdd(ctx, outboxReadyKey(namespace.Default, id), redis.Z{
		Score:  float64(deadline.UnixMilli()),
		Member: entryID,
	}).Err(); err != nil {
		t.Fatalf("ZAdd() error = %v", err)
	}

	ids, err := state.ListOutboxExecutions(ctx, 16)
	if err != nil {
		t.Fatalf("ListOutboxExecutions() error = %v", err)
	}
	if !containsExecution(ids, id) {
		t.Fatalf("discovered %v, want %q — a stale-early member must still be surfaced; "+
			"hiding it would turn an imprecise score into lost work", ids, id)
	}

	claimed, err := state.LeaseOutbox(ctx, id, time.Now().UTC(), 8)
	if err != nil {
		t.Fatalf("LeaseOutbox() error = %v", err)
	}
	if len(claimed) != 0 {
		t.Fatalf("LeaseOutbox() returned %d entries (%v), want 0 — nothing is due until %v",
			len(claimed), outboxEntryIDs(claimed), deadline)
	}

	score, present := indexScore(t, ctx, rdb, namespace.Default, id)
	if !present {
		t.Fatal("the no-op claim removed the member although the execution still has a " +
			"ready entry; the sweep would then have to rediscover it")
	}
	if int64(score) != deadline.UnixMilli() {
		t.Fatalf("re-armed index score = %v, want %d — a claim that found nothing must "+
			"record when the work actually becomes deliverable, or the member is returned "+
			"by every read until then", int64(score), deadline.UnixMilli())
	}

	ids, err = state.ListOutboxExecutions(ctx, 16)
	if err != nil {
		t.Fatalf("ListOutboxExecutions() after repair error = %v", err)
	}
	if containsExecution(ids, id) {
		t.Fatalf("discovered %v after the repair, want %q to be out of the due window — a "+
			"stale member that survives its own repair costs a no-op claim on every tick, "+
			"which is the discovery budget it was supposed to free", ids, id)
	}
}

// indexZRemRacingProducerHook injects the producer side of the prune race at the
// one instant it matters: between the prune's read of the ready set (which saw
// it empty) and the prune's own ZREM.
//
// It writes straight to miniredis rather than through the client, because
// issuing a command from inside a command hook would recurse.
type indexZRemRacingProducerHook struct {
	mr       *miniredis.Miniredis
	indexKey string
	readyKey string
	execID   string
	entryID  string
	// readyScore is the score the racing producer's append gives the new entry;
	// markScore is the score its registration writes. They differ so the score
	// the member ends up with identifies which write survived.
	readyScore float64
	markScore  float64
	fired      bool
	zrems      int
}

func (h *indexZRemRacingProducerHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *indexZRemRacingProducerHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		// ZRem is an ordinary *redis.IntCmd, so the command is identified by
		// name rather than by type.
		args := cmd.Args()
		if cmd.Name() != "zrem" || len(args) < 2 || fmt.Sprint(args[1]) != h.indexKey {
			return next(ctx, cmd)
		}
		h.zrems++
		if h.fired {
			return next(ctx, cmd)
		}
		h.fired = true
		// The racing producer: the atomic transition appends the entry, then the
		// non-atomic registration marks the index. Both land before the ZREM
		// below executes, which is the interleaving that would lose the
		// registration if the prune did not verify.
		_, _ = h.mr.ZAdd(h.readyKey, h.readyScore, h.entryID)
		_, _ = h.mr.ZAdd(h.indexKey, h.markScore, h.execID)
		return next(ctx, cmd)
	}
}

func (h *indexZRemRacingProducerHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// TestOutboxReadyIndexPruneDoesNotLoseARegistrationItRaces is the proof for the
// prune race, asserted on the outcome rather than argued.
//
// The prune cannot be atomic with the ready set it reads: the ready ZSET is
// execution-scoped and the index is not, so a Redis Cluster script cannot touch
// both. That leaves the interleaving where the prune reads "empty", a producer
// appends and registers, and the prune's ZREM then deletes the registration the
// producer just wrote. Left open, this loses a REGISTRATION — never work, since
// the entry is durable in the ready set and the sweep is the backstop — at the
// cost of one sweep interval of delay.
//
// The prune closes it by reading the ready set AGAIN after its own ZREM. The
// hook below produces the interleaving deterministically; the assertion is that
// the member is present afterwards, carrying the score read from the READY SET
// (1) rather than the score the racing producer wrote (7). That second part is
// what makes the test about the repair and not about the injection: the member
// is present because the verify saw the appended entry, not because the
// producer's ZADD was left alone.
func TestOutboxReadyIndexPruneDoesNotLoseARegistrationItRaces(t *testing.T) {
	state, rdb, mr := newOutboxIndexTestStore(t)
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)
	const id = types.ExecutionID("exec-race-0")

	// The execution is created, drained and acked, so its ready set is empty and
	// the member's ZREM is what the refresh is about to issue. The new entry the
	// racing producer appends is the work that must not be lost.
	seedReadyOutbox(t, state, ctx, id, time.Now().Add(-time.Second))
	state.ConfigureOutboxReadyIndex(false)
	leased, err := state.LeaseOutbox(ctx, id, time.Now().UTC(), 8)
	if err != nil {
		t.Fatalf("LeaseOutbox() error = %v", err)
	}
	if len(leased) != 1 {
		t.Fatalf("LeaseOutbox() = %v, want the seeded entry", outboxEntryIDs(leased))
	}
	if err := state.AckOutbox(ctx, id, leased[0].ID); err != nil {
		t.Fatalf("AckOutbox() error = %v", err)
	}
	state.ConfigureOutboxReadyIndex(true)
	// Re-mark so the member is present with a score the repair cannot confuse
	// with the racing producer's.
	if err := rdb.ZAdd(ctx, outboxReadyIndexKey(namespace.Default), redis.Z{
		Score:  99,
		Member: string(id),
	}).Err(); err != nil {
		t.Fatalf("ZAdd() error = %v", err)
	}

	hook := &indexZRemRacingProducerHook{
		mr:         mr,
		indexKey:   outboxReadyIndexKey(namespace.Default),
		readyKey:   outboxReadyKey(namespace.Default, id),
		execID:     string(id),
		entryID:    "advance/" + string(id) + "/start/1",
		readyScore: 1,
		markScore:  7,
	}
	rdb.AddHook(hook)

	state.refreshOutboxReadyIndex(ctx, namespace.Default, id)

	if hook.zrems == 0 {
		t.Fatal("the refresh never issued the ZREM this test injects into; the prune " +
			"branch was not reached and the race was not exercised")
	}
	if !hook.fired {
		t.Fatal("the racing producer was never injected; this test proves nothing")
	}
	score, present := indexScore(t, ctx, rdb, namespace.Default, id)
	if !present {
		t.Fatal("the registration the racing producer wrote is GONE: the prune's ZREM " +
			"deleted it and nothing restored it. The execution's ready entry is still " +
			"durable, so this is not lost work — but discovery now depends on the sweep, " +
			"which is the unbounded-in-practice case the verify read exists to prevent")
	}
	if int64(score) != int64(hook.readyScore) {
		t.Fatalf("index score = %v, want %v — the member survived with the racing "+
			"producer's score, which means it was never removed and this test is not "+
			"exercising the race; the repair writes the score it reads back from the "+
			"ready set", int64(score), int64(hook.readyScore))
	}
}

// TestOutboxReadyIndexAgreesWithReadySetsAfterConcurrentTraffic is the same
// no-loss claim as the race test above, but under real concurrency rather than
// an injected interleaving. The hook test proves the specific interleaving is
// closed; this one is the reason to believe the protocol converges in general.
//
// The invariant is the index's whole contract: at rest, an execution has an
// index member exactly when its ready set is non-empty. A producer here is an
// append followed by its non-atomic registration; a drain's readiness
// observation is a refresh. Both run against each execution at once, so the
// prune branch really does race real appends rather than a staged one.
func TestOutboxReadyIndexAgreesWithReadySetsAfterConcurrentTraffic(t *testing.T) {
	state, rdb, _ := newOutboxIndexTestStore(t)
	ns := namespace.Default
	ctx := namespace.WithNamespace(context.Background(), ns)

	const execs = 6
	const workers = 4
	const rounds = 40
	ids := make([]types.ExecutionID, execs)
	for i := range ids {
		ids[i] = types.ExecutionID(fmt.Sprintf("exec-concurrent-%d", i))
	}

	var wg sync.WaitGroup
	// A discovery reader runs throughout: the index must stay readable while it
	// is being written, and the sweep underneath it must not be perturbed.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for round := 0; round < rounds; round++ {
			if _, err := state.ListOutboxExecutions(ctx, 64); err != nil {
				t.Errorf("ListOutboxExecutions() error = %v", err)
				return
			}
		}
	}()

	for _, id := range ids {
		id := id
		readyKey := outboxReadyKey(ns, id)
		for w := 0; w < workers; w++ {
			w := w
			wg.Add(1)
			go func() {
				defer wg.Done()
				for round := 0; round < rounds; round++ {
					member := fmt.Sprintf("w%d/r%d", w, round)
					if err := rdb.ZAdd(ctx, readyKey, redis.Z{Score: float64(round), Member: member}).Err(); err != nil {
						t.Errorf("ZAdd(ready) error = %v", err)
						return
					}
					state.markOutboxReadyIndex(ctx, ns, id)
				}
			}()
			wg.Add(1)
			go func() {
				defer wg.Done()
				for round := 0; round < rounds; round++ {
					state.refreshOutboxReadyIndex(ctx, ns, id)
				}
			}()
		}
	}
	wg.Wait()

	for _, id := range ids {
		ready, err := rdb.ZCard(ctx, outboxReadyKey(ns, id)).Result()
		if err != nil {
			t.Fatalf("ZCard(ready, %q) error = %v", id, err)
		}
		_, present := indexScore(t, ctx, rdb, ns, id)
		if (ready > 0) != present {
			t.Fatalf("%q: %d ready entries, index member present = %v — at rest a member "+
				"must exist exactly when there is work behind it. Absent-while-ready is a "+
				"lost registration (recovered only by the sweep, which is the case the "+
				"verify read exists to prevent); present-while-empty is a member returned "+
				"by every discovery read until something happens to repair it",
				id, ready, present)
		}
	}
}

// TestOutboxReadyIndexDisabledLeavesTheSweepInCharge covers the operator escape
// hatch and, with it, every backend that has no index at all: with the
// accelerator off, nothing is written to Redis for it and discovery is exactly
// the keyspace sweep it was before.
func TestOutboxReadyIndexDisabledLeavesTheSweepInCharge(t *testing.T) {
	state, rdb, _ := newOutboxIndexTestStore(t)
	state.ConfigureOutboxReadyIndex(false)
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)

	ids := []types.ExecutionID{"exec-off-0", "exec-off-1", "exec-off-2"}
	for _, id := range ids {
		seedReadyOutbox(t, state, ctx, id, time.Now().Add(-time.Second))
	}

	exists, err := rdb.Exists(ctx, outboxReadyIndexKey(namespace.Default)).Result()
	if err != nil {
		t.Fatalf("Exists() error = %v", err)
	}
	if exists != 0 {
		t.Fatalf("the index key exists although the index is disabled (%d keys); a "+
			"disabled accelerator must not write anything", exists)
	}

	discovered, err := state.ListOutboxExecutions(ctx, 16)
	if err != nil {
		t.Fatalf("ListOutboxExecutions() error = %v", err)
	}
	for _, id := range ids {
		if !containsExecution(discovered, id) {
			t.Fatalf("discovered %v with the index disabled, want it to contain %q — the "+
				"sweep has to be sufficient on its own, or disabling the accelerator "+
				"loses work", discovered, id)
		}
	}
}

// TestOutboxReadyIndexThrottlesTheSweepOnlyOnceItHasProvenItself pins the
// degradation rule, which is the reason no real cluster is needed for this to be
// safe: the sweep is throttled only by evidence that the index is delivering.
//
// A store that never sees the index carry work — disabled, never written,
// failing, or belonging to a backend with no index — sweeps on every discovery
// call, which is the pre-index behaviour. A store that has seen it carry work
// sweeps once every outboxIndexSweepEveryCalls calls.
//
// A warm-up window is discarded before measuring, because a cold store
// legitimately sweeps AND reads the index until the index proves itself: that
// overlap is exactly what lets a store with a working index escape the cold
// state at all, and mixing it into the steady-state count would make the
// measurement say nothing.
func TestOutboxReadyIndexThrottlesTheSweepOnlyOnceItHasProvenItself(t *testing.T) {
	const calls = outboxIndexSweepEveryCalls

	measure := func(t *testing.T, indexEnabled bool) int {
		t.Helper()
		state, rdb, _ := newOutboxIndexTestStore(t)
		state.ConfigureOutboxReadyIndex(indexEnabled)
		ctx := namespace.WithNamespace(context.Background(), namespace.Default)
		seedReadyOutbox(t, state, ctx, "exec-throttle-0", time.Now().Add(-time.Second))

		hook := &pagedScanHook{}
		rdb.AddHook(hook)
		discover := func(count int) {
			for call := 0; call < count; call++ {
				if _, err := state.ListOutboxExecutions(ctx, 64); err != nil {
					t.Fatalf("ListOutboxExecutions() error = %v", err)
				}
			}
		}
		discover(calls)
		before := len(hook.cursorsFor(namespace.Default, "outbox:ready"))
		discover(calls)
		return len(hook.cursorsFor(namespace.Default, "outbox:ready")) - before
	}

	withoutIndex := measure(t, false)
	if withoutIndex != calls {
		t.Fatalf("sweeps over %d discovery calls with the index disabled = %d, want %d — a "+
			"store whose index is off must keep the pre-index behaviour exactly, not "+
			"discover less", calls, withoutIndex, calls)
	}

	withIndex := measure(t, true)
	if withIndex != 1 {
		t.Fatalf("steady-state sweeps over %d discovery calls with a proven index = %d, "+
			"want 1 — the sweep is the staleness bound, not the discovery path, and "+
			"running it on every call is the keyspace cost the index exists to remove",
			calls, withIndex)
	}
	if withoutIndex <= withIndex {
		t.Fatalf("sweeps: %d without the index, %d with it — throttling only pays when it "+
			"actually reduces the scan", withoutIndex, withIndex)
	}
	t.Logf("steady-state keyspace sweeps over %d discovery calls of one ready execution: "+
		"%d with the index disabled, %d with it proven (throttled to one call in %d)",
		calls, withoutIndex, withIndex, outboxIndexSweepEveryCalls)
}

// TestOutboxReadyIndexDiscoveryDoesNotTrackTheKeyspace is the cost claim, in the
// style of TestOutboxDiscoveryCostTracksTheKeyspaceNotTheBacklog and with the
// same numbers, so the two are directly comparable: the same 24 ready
// executions and the same 4000 unrelated keys.
//
// The assertion is not merely that discovery is faster; it is that discovery
// issues NO keyspace scan at all. The scan is what makes the cost track the
// keyspace, so a discovery that does not run one cannot track it. The comparison
// against the same store with the index disabled is what keeps this honest:
// without the accelerator the identical keyspace still costs nine calls.
func TestOutboxReadyIndexDiscoveryDoesNotTrackTheKeyspace(t *testing.T) {
	const backlog = 24
	const noise = 4000
	const page = 512

	available := make(map[types.ExecutionID]time.Time, backlog)
	for i := 0; i < backlog; i++ {
		available[types.ExecutionID(fmt.Sprintf("exec-indexed-%03d", i))] = time.Time{}
	}

	state, rdb := newOutboxMetricsTestStore(t, available)
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)
	hook := &keyspaceScanHook{}
	rdb.AddHook(hook)

	// Prove the index with the first call, which is also the only call in this
	// test that is allowed to sweep (a fresh store has not proven itself yet).
	if ids, err := state.ListOutboxExecutions(ctx, page); err != nil {
		t.Fatalf("ListOutboxExecutions() error = %v", err)
	} else if len(ids) != backlog {
		t.Fatalf("first discovery returned %d ids, want the whole backlog of %d", len(ids), backlog)
	}
	scansAfterWarmup := hook.scanCount()

	for i := 0; i < noise; i++ {
		if err := rdb.Set(ctx, fmt.Sprintf("%s%05d", noiseKeyPrefix, i), "x", time.Hour).Err(); err != nil {
			t.Fatal(err)
		}
	}
	keyspace, err := rdb.DBSize(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}

	ids, err := state.ListOutboxExecutions(ctx, page)
	if err != nil {
		t.Fatalf("ListOutboxExecutions() with %d keys error = %v", keyspace, err)
	}
	if len(ids) != backlog {
		t.Fatalf("one discovery call over a %d-key keyspace returned %d of %d ready "+
			"executions; with the index in place a call must return the whole due backlog "+
			"regardless of how large the keyspace is", keyspace, len(ids), backlog)
	}
	if got := hook.scanCount() - scansAfterWarmup; got != 0 {
		t.Fatalf("keyspace SCANs issued by one index-served discovery over a %d-key "+
			"keyspace = %d, want 0 — any scan at all is a cost that tracks the keyspace, "+
			"which is the dependency the index removes", keyspace, got)
	}

	// The comparison: the identical keyspace, the identical backlog, the same
	// page — with the accelerator off.
	withoutIndex := sameBacklogWithoutIndex(t, available, page, noise)
	if withoutIndex <= 1 {
		t.Fatalf("the same %d-execution backlog over the same %d-key keyspace took %d "+
			"discovery calls without the index, want more than one — if the sweep were "+
			"already keyspace-independent this test would be measuring nothing",
			backlog, keyspace, withoutIndex)
	}

	t.Logf("discovery calls to cover %d ready executions at page %d over a %d-key "+
		"keyspace: %d with the readiness index (0 keyspace SCANs), %d without it "+
		"(index disabled)",
		backlog, page, keyspace, 1, withoutIndex)
}

// sameBacklogWithoutIndex measures the same fixture with the accelerator off, so
// the comparison in the test above is against the store's own fallback rather
// than against a remembered number.
func sameBacklogWithoutIndex(t *testing.T, available map[types.ExecutionID]time.Time, page, noise int) int {
	t.Helper()
	state, rdb := newOutboxMetricsTestStore(t, available)
	state.ConfigureOutboxReadyIndex(false)
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)
	for i := 0; i < noise; i++ {
		if err := rdb.Set(ctx, fmt.Sprintf("%s%05d", noiseKeyPrefix, i), "x", time.Hour).Err(); err != nil {
			t.Fatal(err)
		}
	}
	rdb.AddHook(&keyspaceScanHook{})
	return discoveryCalls(t, ctx, state, page, len(available))
}
