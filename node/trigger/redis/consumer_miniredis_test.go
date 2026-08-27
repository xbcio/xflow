package redis

import (
	"context"
	"encoding/json"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/node/trigger/triggertest"
	"github.com/xbcio/xflow/types"
)

// These exercise the real consumer against miniredis, which speaks the stream
// and pub/sub commands this trigger uses (XGROUP/XREADGROUP/XACK/XAUTOCLAIM,
// SUBSCRIBE). Everything else in this package injects a scripted consumer, so
// without these the entire redis-facing half — the group bootstrap, the read
// loop, the ID/timestamp decoding, and the acknowledgement — would never run.

func startMiniredis(t *testing.T) (*miniredis.Miniredis, *goredis.Client) {
	t.Helper()

	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return server, client
}

// TestRedisStreamTriggerEmitsAndAcknowledges is the end-to-end shape: entries
// added to a stream must reach the engine, and what reached the engine must
// stop being pending. The pending check is the load-bearing half — an emit that
// is never acknowledged looks identical from the engine's side, and only shows
// up later as the same entry being redelivered forever.
func TestRedisStreamTriggerEmitsAndAcknowledges(t *testing.T) {
	server, client := startMiniredis(t)
	ctx := context.Background()

	for _, body := range []string{"one", "two", "three"} {
		if _, err := client.XAdd(ctx, &goredis.XAddArgs{
			Stream: "orders",
			Values: map[string]any{"body": body},
		}).Result(); err != nil {
			t.Fatalf("XAdd(%q) error = %v", body, err)
		}
	}

	rt := triggertest.NewFakeRuntime()
	// start_id "0" so the group sees the entries added above; the production
	// default of "$" would skip everything that already exists, which is right
	// for a live stream and useless for a test that seeds it first.
	node := New().Addr(server.Addr()).Stream("orders").Group("workers").
		Tuning(Tuning{StartID: "0", Consumer: "runner-a"})
	sub, err := node.Activate(ctx, &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "redis",
		Params:     node.RawParams().(map[string]any),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatalf("Activate() error = %v", err)
	}
	defer func() { _ = sub.Close(ctx) }()

	if !rt.WaitForEmitCount(3, 5*time.Second) {
		t.Fatalf("emit count = %d, want 3: the stream read loop did not deliver "+
			"the entries already in the stream", rt.EmitCount())
	}

	// The emit goroutines run concurrently, so the recorded order is arrival
	// order, not stream order. What must hold is that every entry arrived
	// exactly once.
	seen := map[string]int{}
	byBody := map[string]*types.TriggerEvent{}
	for _, event := range rt.Events() {
		// The payload is the whole entry, JSON-encoded, because no
		// payload_field was configured.
		var payload map[string]string
		if err := json.Unmarshal(event.Raw, &payload); err != nil {
			t.Fatalf("event payload %q is not a JSON object: %v", event.Raw, err)
		}
		seen[payload["body"]]++
		byBody[payload["body"]] = event
	}
	for _, body := range []string{"one", "two", "three"} {
		if seen[body] != 1 {
			t.Fatalf("entry body=%q emitted %d times, want exactly 1 (all: %v)", body, seen[body], seen)
		}
	}

	first := byBody["one"]
	if first.ID != "orders/"+decodeEntryID(t, first) {
		t.Fatalf("event ID = %q, want orders/<entry-id>", first.ID)
	}
	// The timestamp must come from the entry ID Redis assigned, not from the
	// moment this process happened to read it.
	wantMillis := entryMillis(t, decodeEntryID(t, first))
	if got := first.Time.UnixMilli(); got != wantMillis {
		t.Fatalf("event time = %d ms, want the entry ID's %d ms", got, wantMillis)
	}

	waitPendingZero(t, client, "orders", "workers")
}

// TestRedisStreamTriggerLeavesUnemittedEntriesPending pins the other direction:
// when the emit fails, the entry must stay in the pending list so some later
// consumer gets it again. Acknowledging unconditionally would pass every
// assertion in the test above while silently dropping every failed message.
func TestRedisStreamTriggerLeavesUnemittedEntriesPending(t *testing.T) {
	server, client := startMiniredis(t)
	ctx := context.Background()

	if _, err := client.XAdd(ctx, &goredis.XAddArgs{
		Stream: "orders",
		Values: map[string]any{"body": "one"},
	}).Result(); err != nil {
		t.Fatalf("XAdd() error = %v", err)
	}

	rt := triggertest.NewFakeRuntime()
	rt.SetEmitFunc(func(context.Context, types.WorkflowID, string, *types.TriggerEvent) (types.ExecutionID, error) {
		return "", context.DeadlineExceeded
	})
	node := New().Addr(server.Addr()).Stream("orders").Group("workers").
		Tuning(Tuning{StartID: "0", Consumer: "runner-a"})
	sub, err := node.Activate(ctx, &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "redis",
		Params:     node.RawParams().(map[string]any),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatalf("Activate() error = %v", err)
	}
	defer func() { _ = sub.Close(ctx) }()

	if !rt.WaitForEmitCount(1, 5*time.Second) {
		t.Fatal("the entry was never delivered, so nothing about acknowledgement was exercised")
	}
	// Give the emit goroutine room to acknowledge if it were going to. Asserting
	// the pending count the instant after the emit returns would pass even for
	// an implementation that acks a moment later.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		pending, err := client.XPending(ctx, "orders", "workers").Result()
		if err != nil {
			t.Fatalf("XPending() error = %v", err)
		}
		if pending.Count != 1 {
			t.Fatalf("pending count = %d, want 1: the entry was acknowledged even "+
				"though the emit failed, so the message is lost", pending.Count)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestRedisStreamTriggerReclaimsAbandonedEntries covers XAUTOCLAIM. An entry
// delivered to a runner that then died stays pending forever: XREADGROUP with
// ">" only ever returns entries nobody has seen. Without the reclaim, that
// message is simply gone, and the trigger's at-least-once claim is untrue.
func TestRedisStreamTriggerReclaimsAbandonedEntries(t *testing.T) {
	server, client := startMiniredis(t)
	ctx := context.Background()

	if err := client.XGroupCreateMkStream(ctx, "orders", "workers", "0").Err(); err != nil {
		t.Fatalf("XGroupCreateMkStream() error = %v", err)
	}
	if _, err := client.XAdd(ctx, &goredis.XAddArgs{
		Stream: "orders",
		Values: map[string]any{"body": "abandoned"},
	}).Result(); err != nil {
		t.Fatalf("XAdd() error = %v", err)
	}
	// Deliver it to a consumer that never comes back, exactly as a crashed
	// runner would leave it.
	if _, err := client.XReadGroup(ctx, &goredis.XReadGroupArgs{
		Group:    "workers",
		Consumer: "runner-that-died",
		Streams:  []string{"orders", ">"},
		Count:    10,
	}).Result(); err != nil {
		t.Fatalf("XReadGroup() as the dead runner error = %v", err)
	}

	rt := triggertest.NewFakeRuntime()
	node := New().Addr(server.Addr()).Stream("orders").Group("workers").
		Tuning(Tuning{StartID: "0", Consumer: "runner-b", ClaimMinIdle: time.Millisecond})
	sub, err := node.Activate(ctx, &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "redis",
		Params:     node.RawParams().(map[string]any),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatalf("Activate() error = %v", err)
	}
	defer func() { _ = sub.Close(ctx) }()

	if !rt.WaitForEmitCount(1, 5*time.Second) {
		t.Fatal("the abandoned entry was never reclaimed: an entry left pending by " +
			"a dead runner would never be delivered again")
	}
	waitPendingZero(t, client, "orders", "workers")
}

// TestRedisPubSubConsumerDelivers drives the pub/sub loop directly rather than
// through Activate, which would first take a lock this test has nothing to say
// about.
func TestRedisPubSubConsumerDelivers(t *testing.T) {
	server, _ := startMiniredis(t)

	consumer, err := newRedisConsumer(ConsumerConfig{
		Addr:        server.Addr(),
		Mode:        "pubsub",
		Channel:     "orders",
		MaxInflight: 4,
		DialTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("newRedisConsumer() error = %v", err)
	}
	defer func() { _ = consumer.Close() }()

	// Publishing before SUBSCRIBE lands is a real race — pub/sub has no
	// backlog — so keep publishing until it arrives.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatal("no pub/sub message arrived within 5s")
		}
		server.Publish("orders", "hello")
		select {
		case msg := <-consumer.Messages():
			if msg.Channel != "orders" || string(msg.Payload) != "hello" {
				t.Fatalf("message = %+v, want channel=orders payload=hello", msg)
			}
			if msg.Time.IsZero() {
				t.Fatal("pub/sub message carries no time; the emitted event would " +
					"be stamped at emit time instead of arrival time")
			}
			return
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// TestRedisConsumerCloseStopsItsGoroutine pins that Close actually shuts down
// rather than merely asking to. Close returning proves nothing on its own: a
// version that forgot to wait would return just as promptly and leave the read
// loop running against a stream nobody is listening to.
func TestRedisConsumerCloseStopsItsGoroutine(t *testing.T) {
	server, _ := startMiniredis(t)

	const frame = "node/trigger/redis.(*redisConsumer).runStream"
	if before := countGoroutineFrames(frame); before != 0 {
		t.Fatalf("%d runStream goroutines before the test; a previous test leaked one", before)
	}

	consumer, err := newRedisConsumer(ConsumerConfig{
		Addr:         server.Addr(),
		Mode:         "stream",
		Stream:       "orders",
		Group:        "workers",
		Consumer:     "runner-a",
		StartID:      "0",
		ClaimMinIdle: time.Minute,
		MaxInflight:  4,
		DialTimeout:  time.Second,
	})
	if err != nil {
		t.Fatalf("newRedisConsumer() error = %v", err)
	}

	// Assert the goroutine exists first: otherwise "gone after Close" would be
	// satisfied by a factory that never started one.
	if !waitForGoroutineFrames(frame, 1, 5*time.Second) {
		_ = consumer.Close()
		t.Fatalf("runStream goroutine count = %d, want 1", countGoroutineFrames(frame))
	}
	if err := consumer.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if got := countGoroutineFrames(frame); got != 0 {
		t.Fatalf("runStream goroutine count = %d after Close, want 0: Close returned "+
			"before the read loop had stopped", got)
	}
	// Idempotent, and a second Close must not block on an already-closed done.
	if err := consumer.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
}

func decodeEntryID(t *testing.T, event *types.TriggerEvent) string {
	t.Helper()

	_, id, found := strings.Cut(event.ID, "/")
	if !found {
		t.Fatalf("event ID %q has no stream prefix", event.ID)
	}
	return id
}

func entryMillis(t *testing.T, id string) int64 {
	t.Helper()

	stamp := streamEntryTime(id)
	if stamp.IsZero() {
		t.Fatalf("entry ID %q carries no timestamp", id)
	}
	return stamp.UnixMilli()
}

func waitPendingZero(t *testing.T, client *goredis.Client, stream, group string) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	var last int64 = -1
	for time.Now().Before(deadline) {
		pending, err := client.XPending(context.Background(), stream, group).Result()
		if err != nil {
			t.Fatalf("XPending() error = %v", err)
		}
		last = pending.Count
		if last == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("pending count = %d after 5s, want 0: the entries were delivered but "+
		"never acknowledged, so they would be redelivered forever", last)
}

func countGoroutineFrames(frame string) int {
	buf := make([]byte, 1<<20)
	buf = buf[:runtime.Stack(buf, true)]
	return strings.Count(string(buf), frame)
}

func waitForGoroutineFrames(frame string, want int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if countGoroutineFrames(frame) >= want {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}
