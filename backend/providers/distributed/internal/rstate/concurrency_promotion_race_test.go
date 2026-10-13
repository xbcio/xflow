package rstate

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// queueWaiter parks one more execution behind a workflow's taken slot, through
// the real admission path, and returns its ID.
//
// It is separate from queueTTLTwoExecutions because the tests here need more
// than one waiter in the same line: the race below turns on what happens to the
// execution BEHIND the one being promoted.
func queueWaiter(t *testing.T, ctx context.Context, state *Store, g *graph.Graph, wait time.Duration, maxQueued int) types.ExecutionID {
	t.Helper()
	id := engine.NewExecutionID()
	res, err := state.CreateExecutionWithConcurrency(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusRunning,
	}, []engine.OutboxEntry{{
		ID: "root/" + string(id) + "/start/0",
		Task: engine.Task{
			ExecutionID: id, NodeName: "start", NodeIdx: 0, Type: engine.TaskTypeNodeExec,
		},
	}}, engine.ConcurrencyAdmission{
		Namespace: "default", Workflow: g.Name(), Limit: 1,
		Mode: types.ConcurrencyModeQueue, QueueTimeout: wait, MaxQueued: maxQueued,
	})
	if err != nil {
		t.Fatalf("queued submission error = %v", err)
	}
	if !res.Queued {
		t.Fatalf("queued submission result = %+v, want it parked", res)
	}
	return id
}

// TestCancelRacingPromotionDropsTheParkedOutbox pins the one parking leak that
// neither end of the race can see on its own.
//
// A release promotes by POPPING the head of the line and charging it a slot.
// The promoted execution's own keys live in another hash slot, so its status
// change and the release of its parked intents are a SECOND call that follows.
// A cancel landing in that window writes the terminal status while the
// execution still reads queued — and by then the promotion has already taken it
// out of the line, so the cancel's own release sees a HOLDER rather than a
// waiter: wasQueued is 0 and it drops nothing.
//
// A cancel that runs all the way to its terminal write does clear those keys,
// through cleanupOnCancel. But the window before that write lands has nobody
// else watching them: the execution is out of the line, the release has already
// passed, and the terminal write may still fail or never arrive — the parked
// intents then sit at their held sentinel for the whole retention, in an outbox
// nothing will ever read. The promotion that lost its CAS is the last place
// that can still tell, and this test drives that interleaving directly: the
// release script, then the cancel's first write, then the promotion — with the
// assertion made while the execution is still in that window. It also checks
// what the cancel then did with the slot the promotion charged: the freed
// capacity has to reach the waiter behind it, exactly once.
func TestCancelRacingPromotionDropsTheParkedOutbox(t *testing.T) {
	ctx := context.Background()
	_, rdb, state := newMiniredisStore(t)
	g := queueTTLGraph(t, "conc-promote-cancel", time.Hour)
	tag := workflowDigestTag(namespace.Default, g.Name())

	holder := admitForSweep(t, ctx, state, g, 1)
	promoted := queueWaiter(t, ctx, state, g, time.Hour, 2)
	behind := queueWaiter(t, ctx, state, g, time.Hour, 2)

	// The state the release script is about to act on: two waiters in line,
	// both with their intents parked at the held sentinel.
	readyKey := outboxReadyKey(namespace.Default, promoted)
	bodyKey := outboxBodyKey(namespace.Default, promoted)
	if n, err := rdb.ZCount(ctx, readyKey, "-inf", "+inf").Result(); err != nil || n == 0 {
		t.Fatalf("parked intents before the release = %d (%v), want a non-empty parked outbox", n, err)
	}
	if held, err := rdb.ZCount(ctx, readyKey,
		strconv.FormatInt(queuedOutboxHold.UnixMilli(), 10), strconv.FormatInt(queuedOutboxHold.UnixMilli(), 10)).Result(); err != nil || held == 0 {
		t.Fatalf("held intents before the release = %d (%v), want them still parked", held, err)
	}

	// The release that promotes: it pops the head of the line and charges it the
	// freed slot. The promotion call that normally follows is deliberately not
	// made here — that gap is the window the cancel wins.
	reply, err := workflowReleaseLua.Run(ctx, rdb, concurrencyReleaseKeys(namespace.Default, tag),
		concurrencySlotMember(holder), time.Now().UTC().Unix(), int(state.concurrencySlotTTL().Seconds())).Slice()
	if err != nil {
		t.Fatalf("release script error = %v", err)
	}
	if got := luaReplyStrings(reply, 2); len(got) != 1 || got[0] != string(promoted) {
		t.Fatalf("release promoted %v, want exactly [%s]", got, promoted)
	}

	// The cancel lands in the window, as engine.Cancel drives it: the first
	// write leaves the execution past queued, before the terminal write that
	// would clear its outbox. Its release frees the slot the promotion charged,
	// hands that capacity to the waiter behind, and — because the promoted
	// execution is no longer in the line — drops nothing.
	if err := state.UpdateExecutionStatus(ctx, promoted, types.ExecutionStatusCanceling, ""); err != nil {
		t.Fatalf("mark %q canceling error = %v", promoted, err)
	}

	// The promotion catches up and finds the execution gone, while the cancel is
	// still in flight. This is the last moment at which anyone can recognise the
	// intents as unreachable.
	if err := state.promoteExecution(ctx, namespace.Default, promoted); err != nil {
		t.Fatalf("late promotion of %q error = %v", promoted, err)
	}

	if n, err := rdb.ZCard(ctx, readyKey).Result(); err != nil || n != 0 {
		t.Fatalf("parked ready ZSET size after the lost promotion = %d (%v), want 0: "+
			"intents of an execution that will never run are storage nothing reads", n, err)
	}
	if n, err := rdb.Exists(ctx, bodyKey).Result(); err != nil || n != 0 {
		t.Fatalf("parked outbox body exists = %d (%v), want 0", n, err)
	}

	// The cancel completes. Its terminal write clears the same keys again — and
	// that is exactly what this test cannot lean on: the window above is where
	// the leak lives, not the world after the write landed.
	if err := state.UpdateExecutionStatus(ctx, promoted, types.ExecutionStatusCanceled, ""); err != nil {
		t.Fatalf("mark %q canceled error = %v", promoted, err)
	}

	// Slot accounting for the same race: the charge the promotion made was the
	// cancel's to give back, it gave it back once, and the capacity reached the
	// waiter behind — so the cancelled execution neither holds a slot nor costs
	// the workflow one.
	slotKey := concurrencySlotKey(namespace.Default, g.Name())
	if n, err := rdb.ZCard(ctx, slotKey).Result(); err != nil || n != 1 {
		t.Fatalf("workflow slot set size = %d (%v), want 1 (the waiter behind)", n, err)
	}
	if _, err := rdb.ZScore(ctx, slotKey, concurrencySlotMember(behind)).Result(); err != nil {
		t.Fatalf("waiter behind holds no slot (%v); the cancelled execution's capacity was "+
			"not handed on", err)
	}
	if _, err := rdb.ZScore(ctx, slotKey, concurrencySlotMember(promoted)).Result(); err != redis.Nil {
		t.Fatalf("cancelled execution still holds a slot (err = %v), want it released exactly once", err)
	}
	if n, err := rdb.ZCard(ctx, concurrencyQueueKeyForTag(namespace.Default, tag)).Result(); err != nil || n != 0 {
		t.Fatalf("line size = %d (%v), want 0", n, err)
	}
	if n, err := rdb.ZCount(ctx, outboxReadyKey(namespace.Default, behind),
		strconv.FormatInt(queuedOutboxHold.UnixMilli(), 10), strconv.FormatInt(queuedOutboxHold.UnixMilli(), 10)).Result(); err != nil || n != 0 {
		t.Fatalf("waiter behind still has held intents = %d (%v), want them released by its "+
			"own promotion", n, err)
	}
	snap, err := state.GetExecution(ctx, behind)
	if err != nil {
		t.Fatalf("GetExecution(%q) error = %v", behind, err)
	}
	if snap.Status != types.ExecutionStatusRunning {
		t.Fatalf("waiter behind status = %q, want %q: the freed capacity has to start it",
			snap.Status, types.ExecutionStatusRunning)
	}
}
