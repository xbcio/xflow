package rstate

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// This file pins the orphan defect: an index member whose execution has no
// outbox left. The index key has no TTL of its own and outlives the ready set
// and body hash it points at, so a member can survive its own execution, and
// discovery used to return it as work. Two consequences, both measured on a
// live permanent stall:
//
//   - Orphans spend the page budget the index exists to spend on work, so a due
//     head of orphans HIDES the live backlog behind it: discovery answers with
//     dead ids and nothing is dispatched.
//   - `added > 0` is what proves the index to ListOutboxExecutions, so a page of
//     orphans latched the index as carrying work and throttled away the keyspace
//     sweep — the only mechanism left that would have found the work they hid.
//     The latch is per-process and the orphans are in Redis, which is why a
//     restart restored service while the orphans stayed to block again.
//
// The repair the design relied on is real but reaches neither case: it lives on
// the flush's terminating claim (LeaseOutbox → refreshOutboxReadyIndex → ZREM),
// so it needs the flush to run, needs it to run for that execution, and needs
// the refresh's own read to succeed — that read comes first, and a failed or
// canceled one returns before the ZREM. A member nothing flushed is never
// removed by it.

// seedOrphanIndexMembers writes index members for executions that have no
// outbox at all — no ready set and no body hash — scored earliest, which is the
// shape the defective head had in production (5928 members, 100% of the
// earliest 120 with no outbox key, the oldest 59 minutes old and still present).
//
// It writes the index directly rather than through the store because the state
// it reproduces cannot be produced by the store's own transitions: every
// transition that registers a member has just written the outbox it registers.
func seedOrphanIndexMembers(t *testing.T, ctx context.Context, rdb *redis.Client, ns namespace.Namespace, count int, scoredAt time.Time) []types.ExecutionID {
	t.Helper()
	indexKey := outboxReadyIndexKey(ns)
	pipe := rdb.Pipeline()
	ids := make([]types.ExecutionID, count)
	for i := range ids {
		ids[i] = types.ExecutionID(fmt.Sprintf("exec-orphan-%05d", i))
		pipe.ZAdd(ctx, indexKey, redis.Z{
			Score:  float64(scoredAt.UnixMilli() + int64(i)),
			Member: string(ids[i]),
		})
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("seed %d orphan index members: %v", count, err)
	}
	return ids
}

// indexMembers describes the index for a failure message: how many members it
// holds and whether the ones we care about are among them.
func indexMembers(mr *miniredis.Miniredis, ns namespace.Namespace) []string {
	members, err := mr.ZMembers(outboxReadyIndexKey(ns))
	if err != nil {
		return nil
	}
	return members
}

// TestOutboxReadyIndexStaleHeadDoesNotStarveLiveWork is the regression the
// outage would have failed.
//
// It reproduces the deployment's sequence rather than a synthetic one. Tick 1
// sees an index whose whole due head is orphans. Live work then arrives — as it
// does continuously in production — and tick 2 is the dispatcher's own
// discovery-and-flush loop. The assertion is that the live execution is
// discovered and its task reaches the queue on tick 2.
//
// Tick 2 is the tick that matters, and it is why this test drives two of them: a
// store that has never proven its index sweeps alongside the index read on tick
// 1, so tick 1 finds the work whatever the index says. From tick 2 the sweep is
// throttled away if the index was latched — and it was latched by orphans alone,
// which is exactly the state the deployment was stuck in: "sink not invoked for
// 40+ minutes", every tick reading dead ids over a live backlog no one swept.
func TestOutboxReadyIndexStaleHeadDoesNotStarveLiveWork(t *testing.T) {
	state, rdb, _ := newOutboxIndexTestStore(t)
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)

	// Two full pages of dead head, so anything behind them is out of reach of a
	// single paged read.
	const page = 256
	const orphans = 2 * page
	seedOrphanIndexMembers(t, ctx, rdb, namespace.Default, orphans, time.Now().Add(-90*time.Minute))

	queue := &StoreTestQueue{}
	eng := engine.New(state, queue)

	// Tick 1: the dead head, and nothing else due yet.
	if _, err := state.ListOutboxExecutions(ctx, page); err != nil {
		t.Fatalf("tick 1 ListOutboxExecutions() error = %v", err)
	}

	// Live work arrives after the head was established — the production case.
	const live = types.ExecutionID("exec-live-behind-orphans")
	entryID := seedReadyOutbox(t, state, ctx, live, time.Now().Add(-time.Second))

	// Tick 2: discover, then flush every id the way OutboxDispatcher.drain does.
	ids, err := state.ListOutboxExecutions(ctx, page)
	if err != nil {
		t.Fatalf("tick 2 ListOutboxExecutions() error = %v", err)
	}
	for _, id := range ids {
		if err := eng.FlushOutbox(ctx, id); err != nil {
			t.Fatalf("tick 2 FlushOutbox(%q) error = %v", id, err)
		}
	}
	if !containsExecution(ids, live) {
		t.Fatalf("tick 2 discovered %d ids and %q was not one of them: a due head of %d "+
			"stale members consumed the whole page, so the live execution behind them is "+
			"invisible for as long as the head stays dead", len(ids), live, orphans)
	}
	delivered := false
	for _, task := range queue.tasks {
		if task.ExecutionID == live {
			delivered = true
		}
	}
	if !delivered {
		t.Fatalf("discovered %q but no task for it reached the queue (%d tasks): the "+
			"stale head starved delivery itself, not just discovery", live, len(queue.tasks))
	}
	if len(queue.tasks) != 1 || queue.tasks[0].ExecutionID != live {
		t.Fatalf("queue received %d tasks with execution %v, want exactly the live entry %q — "+
			"a stale member must not be dispatched as work", len(queue.tasks), queueExecIDs(queue), entryID)
	}
}

// queueExecIDs lists the executions a StoreTestQueue received, for failure
// messages.
func queueExecIDs(q *StoreTestQueue) []types.ExecutionID {
	out := make([]types.ExecutionID, 0, len(q.tasks))
	for _, task := range q.tasks {
		out = append(out, task.ExecutionID)
	}
	return out
}

// TestOutboxReadyIndexReadReachesLiveWorkBehindAStaleHead pins the fix at the
// layer it was made, with the sweep out of the picture: one index read, a due
// head of stale members several times the page, and live work behind it.
//
// The read must come back with the live execution — not with a page of dead ids
// and not with nothing — because a read that answers with the dead head is the
// defect, whether or not some other mechanism later rescues the tick.
func TestOutboxReadyIndexReadReachesLiveWorkBehindAStaleHead(t *testing.T) {
	state, rdb, _ := newOutboxIndexTestStore(t)
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)

	const limit = 64
	orphans := seedOrphanIndexMembers(t, ctx, rdb, namespace.Default, 3*limit, time.Now().Add(-90*time.Minute))
	const live = types.ExecutionID("exec-live-read-behind-orphans")
	seedReadyOutbox(t, state, ctx, live, time.Now().Add(-time.Second))

	ids := make(map[types.ExecutionID]struct{})
	added, err := state.readOutboxReadyIndex(ctx, namespace.Default, time.Now().UTC(), limit, ids)
	if err != nil {
		t.Fatalf("readOutboxReadyIndex() error = %v", err)
	}
	if !containsExecution(mapKeys(ids), live) {
		t.Fatalf("read returned %d ids and %q was not one of them; %d stale members "+
			"precede it in score order and the read stopped at the head", added, live, len(orphans))
	}
	if added != 1 {
		t.Fatalf("added = %d, want 1 — only the live member is work, and the %d stale ones "+
			"must not be counted against the budget", added, len(orphans))
	}
	for _, orphan := range orphans {
		if _, stale := ids[orphan]; stale {
			t.Fatalf("stale member %q was returned as work", orphan)
		}
	}
}

// TestOutboxReadyIndexOrphansAreRepairedNotAccumulated pins the second half of
// the fix: the read does not merely ignore the dead head, it removes it, so the
// index does not accumulate members faster than the flush path can shed them.
//
// The removal is what the design said the flush would do and production showed
// it did not (5928 members and growing). Doing it at the read is what makes it
// independent of a flush running at all.
func TestOutboxReadyIndexOrphansAreRepairedNotAccumulated(t *testing.T) {
	state, rdb, mr := newOutboxIndexTestStore(t)
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)

	const limit = 32
	orphans := seedOrphanIndexMembers(t, ctx, rdb, namespace.Default, 3*limit, time.Now().Add(-90*time.Minute))
	const live = types.ExecutionID("exec-live-survives-repair")
	seedReadyOutbox(t, state, ctx, live, time.Now().Add(-time.Second))
	before := len(indexMembers(mr, namespace.Default))

	ids := make(map[types.ExecutionID]struct{})
	if _, err := state.readOutboxReadyIndex(ctx, namespace.Default, time.Now().UTC(), limit, ids); err != nil {
		t.Fatalf("readOutboxReadyIndex() error = %v", err)
	}

	after := indexMembers(mr, namespace.Default)
	for _, orphan := range orphans {
		for _, member := range after {
			if types.ExecutionID(member) == orphan {
				t.Fatalf("stale member %q survived the read that found it: the index was %d "+
					"members before and %d after, so nothing is being repaired and the head "+
					"stays dead forever", orphan, before, len(after))
			}
		}
	}
	// The live member is work and must not be repaired away with them.
	if _, present := indexScore(t, ctx, rdb, namespace.Default, live); !present {
		t.Fatalf("the repair removed %q, whose outbox is present and ready: index = %v",
			live, after)
	}
	// A second read has nothing stale left to walk and still answers with the work.
	ids = make(map[types.ExecutionID]struct{})
	added, err := state.readOutboxReadyIndex(ctx, namespace.Default, time.Now().UTC(), limit, ids)
	if err != nil {
		t.Fatalf("second readOutboxReadyIndex() error = %v", err)
	}
	if added != 1 || !containsExecution(mapKeys(ids), live) {
		t.Fatalf("second read added %d ids %v, want just %q — the orphans are still "+
			"occupying the index", added, mapKeys(ids), live)
	}
}

// TestOutboxReadyIndexPureOrphanReadDoesNotProveTheIndex pins the amplifier. A
// read that returned only members whose execution has no outbox has delivered no
// work, and must not latch the index as healthy — because that latch is what
// throttles the keyspace sweep, and the sweep is the only thing left that finds
// work the index is hiding.
//
// The second half is the consequence, asserted on behaviour rather than on the
// flag: work whose registration never reached the index is still discovered on
// the following call. It can only be found by the sweep, so discovering it
// proves the sweep ran.
func TestOutboxReadyIndexPureOrphanReadDoesNotProveTheIndex(t *testing.T) {
	state, rdb, _ := newOutboxIndexTestStore(t)
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)

	const page = 256
	seedOrphanIndexMembers(t, ctx, rdb, namespace.Default, page, time.Now().Add(-90*time.Minute))

	if _, err := state.ListOutboxExecutions(ctx, page); err != nil {
		t.Fatalf("ListOutboxExecutions() error = %v", err)
	}
	if state.outboxIndexProven.Load() {
		t.Fatal("a read that returned nothing but orphaned index members latched the index " +
			"as carrying work; that latch throttles the keyspace sweep to one call in ten, " +
			"which is what let a dead head block live work permanently")
	}

	// A registration the index never received, as in the live deployment's
	// "readiness index refresh failed" path. Only the sweep can find it.
	const unregistered = types.ExecutionID("exec-live-unregistered")
	seedReadyOutbox(t, state, ctx, unregistered, time.Now().Add(-time.Second))
	if err := rdb.ZRem(ctx, outboxReadyIndexKey(namespace.Default), string(unregistered)).Err(); err != nil {
		t.Fatalf("drop the registration: %v", err)
	}

	ids, err := state.ListOutboxExecutions(ctx, page)
	if err != nil {
		t.Fatalf("second ListOutboxExecutions() error = %v", err)
	}
	if !containsExecution(ids, unregistered) {
		t.Fatalf("discovered %v on the second call, want %q: the sweep did not run, so a "+
			"registration missing from the index is unreachable while the index holds "+
			"stale members", ids, unregistered)
	}
}

// TestOutboxReadyIndexMemberThatReappearsDuringRepairIsKept is the atomicity
// proof for the removal.
//
// The removal cannot be atomic with the outbox keys it judges — the index is
// namespace-global, those keys are execution-scoped, and a Redis Cluster script
// cannot touch both — so one interleaving is open: the execution appends its
// next entry and re-registers between this removal's probe and its ZREM. It is
// not exotic, because Redis drops an empty ZSET and an empty HASH, so a fully
// drained execution has no outbox keys at all while it is still alive and about
// to append.
//
// The hook produces that interleaving deterministically. The assertions are that
// the member survives — the repair probes again after its own removal, sees the
// outbox the producer just wrote, and re-registers — and that it is then
// dispatched, because it is work again.
func TestOutboxReadyIndexMemberThatReappearsDuringRepairIsKept(t *testing.T) {
	state, rdb, mr := newOutboxIndexTestStore(t)
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)
	ns := namespace.Default

	const stale = types.ExecutionID("exec-reappears")
	if err := rdb.ZAdd(ctx, outboxReadyIndexKey(ns), redis.Z{
		Score: float64(time.Now().Add(-90 * time.Minute).UnixMilli()), Member: string(stale),
	}).Err(); err != nil {
		t.Fatalf("seed the stale member: %v", err)
	}
	if mr.Exists(outboxReadyKey(ns, stale)) || mr.Exists(outboxBodyKey(ns, stale)) {
		t.Fatal("the seeded member must have no outbox, or it is not the state under test")
	}

	hook := &outboxIndexRepairRaceHook{
		mr:       mr,
		indexKey: outboxReadyIndexKey(ns),
		readyKey: outboxReadyKey(ns, stale),
		bodyKey:  outboxBodyKey(ns, stale),
		entryID:  string(stale) + "/start/1",
		execID:   string(stale),
	}
	rdb.AddHook(hook)

	ids := make(map[types.ExecutionID]struct{})
	if _, err := state.readOutboxReadyIndex(ctx, ns, time.Now().UTC(), 16, ids); err != nil {
		t.Fatalf("readOutboxReadyIndex() error = %v", err)
	}
	if hook.removals == 0 {
		t.Fatal("the repair never removed the stale member, so the race under test was not exercised")
	}
	if _, present := indexScore(t, ctx, rdb, ns, stale); !present {
		t.Fatal("the repair removed an index member whose execution had appended and " +
			"re-registered concurrently; the removal is not verified, so a registration " +
			"that races it is lost and the execution waits for a sweep it may not get")
	}
	if !containsExecution(mapKeys(ids), stale) {
		t.Fatalf("read returned %v, want %q: its outbox came back, so the member is work "+
			"again and must be dispatched rather than left for a sweep", mapKeys(ids), stale)
	}
}

// outboxIndexRepairRaceHook injects the producer side of the repair race at the
// one instant it matters: before the repair's ZREM of a member it just judged
// stale. It writes straight to miniredis rather than through the client, because
// issuing a command from inside a command hook would recurse.
type outboxIndexRepairRaceHook struct {
	mr       *miniredis.Miniredis
	indexKey string
	readyKey string
	bodyKey  string
	entryID  string
	execID   string
	fired    bool
	removals int
}

func (h *outboxIndexRepairRaceHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *outboxIndexRepairRaceHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.raceTheRemoval(cmd)
		return next(ctx, cmd)
	}
}

// ProcessPipelineHook covers the form the repair actually issues, which is a
// pipelined ZREM. The injection happens before the pipeline executes, so the
// producer's writes still land ahead of the removal.
func (h *outboxIndexRepairRaceHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, cmd := range cmds {
			h.raceTheRemoval(cmd)
		}
		return next(ctx, cmds)
	}
}

// raceTheRemoval injects the producer side ahead of the first removal of the
// index key: the execution appends its next entry (which creates both outbox
// keys again) and registers.
func (h *outboxIndexRepairRaceHook) raceTheRemoval(cmd redis.Cmder) {
	args := cmd.Args()
	if cmd.Name() != "zrem" || len(args) < 2 || fmt.Sprint(args[1]) != h.indexKey {
		return
	}
	h.removals++
	if h.fired {
		return
	}
	h.fired = true
	_, _ = h.mr.ZAdd(h.readyKey, float64(time.Now().UTC().UnixMilli()), h.entryID)
	h.mr.HSet(h.bodyKey, h.entryID, `{"id":"`+h.entryID+`"}`)
	_, _ = h.mr.ZAdd(h.indexKey, float64(time.Now().UTC().UnixMilli()), h.execID)
}

// TestOutboxReadyIndexLiveMembersAreDiscoveredAndKept is the healthy path, and
// it is here to pin that the fix costs it nothing: a due member whose execution
// still has its outbox is discovered, counted as work, left in the index for the
// flush's terminating claim to re-arm, and leasable.
func TestOutboxReadyIndexLiveMembersAreDiscoveredAndKept(t *testing.T) {
	state, rdb, _ := newOutboxIndexTestStore(t)
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)
	ns := namespace.Default

	const live = types.ExecutionID("exec-live-healthy")
	entryID := seedReadyOutbox(t, state, ctx, live, time.Now().Add(-time.Second))

	ids, err := state.ListOutboxExecutions(ctx, 16)
	if err != nil {
		t.Fatalf("ListOutboxExecutions() error = %v", err)
	}
	if !containsExecution(ids, live) {
		t.Fatalf("discovered %v, want %q", ids, live)
	}
	if !state.outboxIndexProven.Load() {
		t.Fatal("a read that returned live work did not prove the index, so the sweep stays " +
			"on every call and the accelerator buys nothing")
	}
	if _, present := indexScore(t, ctx, rdb, ns, live); !present {
		t.Fatal("the live member was removed by the read; it is work, and removing it would " +
			"cost the sweep a rediscovery that the terminating claim already handles")
	}
	leased, err := state.LeaseOutbox(ctx, live, time.Now().UTC(), 8)
	if err != nil {
		t.Fatalf("LeaseOutbox() error = %v", err)
	}
	if len(leased) != 1 || leased[0].ID != entryID {
		t.Fatalf("LeaseOutbox() = %v, want the one seeded entry %q", outboxEntryIDs(leased), entryID)
	}
}

// mapKeys is the sorted slice form of a discovery result, for failure messages.
func mapKeys(ids map[types.ExecutionID]struct{}) []types.ExecutionID {
	out := make([]types.ExecutionID, 0, len(ids))
	for id := range ids {
		out = append(out, id)
	}
	return out
}

// TestOutboxReadyIndexMemberWithReadySetButNoBodyIsNotOrphaned pins the SCOPE of
// the repair, because the boundary is what keeps it from over-reaching.
//
// The member is judged by its ready set, which is the index's own membership
// contract ("a member exists exactly when there is work behind it") and the same
// predicate the flush's terminating claim removes by. A member whose ready set
// survives but whose body hash is gone is therefore NOT an orphan, and it is
// removed by a different mechanism that already works: its first claim pops the
// ready member, finds no body to deliver, and the terminating empty claim drops
// the member with it.
//
// So this case repairs itself in one no-op claim and must not be removed at the
// read; removing it would be a lost registration in the direction this fix is
// careful to avoid.
func TestOutboxReadyIndexMemberWithReadySetButNoBodyIsNotOrphaned(t *testing.T) {
	state, rdb, mr := newOutboxIndexTestStore(t)
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)
	ns := namespace.Default

	const id = types.ExecutionID("exec-ready-without-body")
	seedReadyOutbox(t, state, ctx, id, time.Now().Add(-time.Second))
	mr.Del(outboxBodyKey(ns, id))
	if !mr.Exists(outboxReadyKey(ns, id)) {
		t.Fatal("the ready set must survive; this test is about the body hash being gone")
	}

	ids := make(map[types.ExecutionID]struct{})
	added, err := state.readOutboxReadyIndex(ctx, ns, time.Now().UTC(), 16, ids)
	if err != nil {
		t.Fatalf("readOutboxReadyIndex() error = %v", err)
	}
	if added != 1 || !containsExecution(mapKeys(ids), id) {
		t.Fatalf("read added %d ids %v, want %q: its ready set is present, so it is not an "+
			"orphan and the read must not judge it by the body hash", added, mapKeys(ids), id)
	}
	if _, present := indexScore(t, ctx, rdb, ns, id); !present {
		t.Fatal("the read removed a member its ready set still justifies — a lost " +
			"registration, not a repair")
	}

	// The no-op claim, and the terminating claim after it, are what repair this:
	// nothing is leasable without a body.
	leased, err := state.LeaseOutbox(ctx, id, time.Now().UTC(), 8)
	if err != nil {
		t.Fatalf("LeaseOutbox() error = %v", err)
	}
	if len(leased) != 0 {
		t.Fatalf("LeaseOutbox() = %v, want nothing — the body is gone", outboxEntryIDs(leased))
	}
	if _, present := indexScore(t, ctx, rdb, ns, id); present {
		t.Fatal("the terminating empty claim did not remove the member, so the body-gone " +
			"case is NOT self-healing and the read's ready-set-only predicate leaves a real " +
			"orphan behind")
	}
}
