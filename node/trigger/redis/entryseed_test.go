package redis

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/trigger/internal/seedadmit"
	"github.com/xbcio/xflow/node/trigger/triggertest"
	"github.com/xbcio/xflow/types"
)

// seedParams is the param set a runner delivers for a remote-hosted redis
// trigger: the node's own configuration plus the entry-seed selectors.
func seedParams(tr *Node) map[string]any {
	params := tr.RawParams().(map[string]any)
	params["entry_seed"] = true
	params["entry_unit_id"] = "entry"
	params["workflow_version"] = "v1"
	return params
}

// activatingConsumer is a scripted consumer that records which messages it was
// asked to acknowledge, so a test can assert the trigger only acknowledges what
// the control plane admitted.
type ackingConsumer struct {
	*scriptedConsumer
	mu   sync.Mutex
	acks []Message
	ack  chan struct{}
}

func newAckingConsumer(messages []Message) *ackingConsumer {
	return &ackingConsumer{scriptedConsumer: newScriptedConsumer(messages), ack: make(chan struct{}, len(messages))}
}

func (c *ackingConsumer) Ack(_ context.Context, msg Message) error {
	c.mu.Lock()
	c.acks = append(c.acks, msg)
	c.mu.Unlock()
	select {
	case c.ack <- struct{}{}:
	default:
	}
	return nil
}

func (c *ackingConsumer) Acked() []Message {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]Message(nil), c.acks...)
}

func (c *ackingConsumer) WaitAck(timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-c.ack:
		return true
	case <-timer.C:
		return false
	}
}

// TestRedisStreamEntrySeedAcksAfterAdmission asserts the acknowledgement rule
// of the stream path: the entry is acknowledged only once the control plane
// admitted it, and the fire key is derived from the entry's own identity so a
// reclaimed redelivery of the same entry is a duplicate rather than a second
// execution.
func TestRedisStreamEntrySeedAcksAfterAdmission(t *testing.T) {
	restoreConsumer(t, newAckingConsumer([]Message{{ID: "1-0", Stream: "orders", Payload: []byte("one")}}))
	consumer := installedConsumer.(*ackingConsumer)

	rt := triggertest.NewFakeEntrySeedRuntime()
	tr := New().Addr("127.0.0.1:6379").Mode("stream").Stream("orders").Group("workers")
	sub, err := tr.Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "entry",
		Params:     seedParams(tr),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}
	defer func() { _ = sub.Close(context.Background()) }()

	if !rt.WaitForSeedCount(1, 2*time.Second) {
		t.Fatal("redis trigger did not admit the entry")
	}
	calls := rt.Seeds()
	want := "trigger:wf-1:entry:redis:stream/orders/1-0"
	if got := calls[0].Request.AdmissionKey; got != want {
		t.Fatalf("fire key = %q, want %q", got, want)
	}
	if !consumer.WaitAck(2 * time.Second) {
		t.Fatal("the admitted entry was never acknowledged")
	}
	acked := consumer.Acked()
	if len(acked) != 1 || acked[0].ID != "1-0" {
		t.Fatalf("acknowledged %d entries (%v), want exactly 1-0", len(acked), acked)
	}
	if emits, dedups, locks := rt.LegacyCalls(); emits != 0 || dedups != 0 || locks != 0 {
		t.Fatalf("legacy calls = emit:%d dedup:%d trylock:%d, want none on the entry-seed path",
			emits, dedups, locks)
	}
}

// TestRedisStreamEntrySeedRefusalLeavesEntryUnacked asserts the row that keeps
// a throttled workflow from losing messages: a refused admission acknowledges
// nothing, so the entry stays pending and the consumer group's reclaim hands it
// back. The refusal is not retried in place — the redelivery is the retry.
func TestRedisStreamEntrySeedRefusalLeavesEntryUnacked(t *testing.T) {
	restoreConsumer(t, newAckingConsumer([]Message{{ID: "2-0", Stream: "orders", Payload: []byte("two")}}))
	consumer := installedConsumer.(*ackingConsumer)

	rt := triggertest.NewFakeEntrySeedRuntime()
	rt.SetResponder(func(types.EntrySeedRequest, int) (types.EntrySeedResponse, error) {
		return types.EntrySeedResponse{Refused: true}, nil
	})
	tr := New().Addr("127.0.0.1:6379").Mode("stream").Stream("orders").Group("workers")
	sub, err := tr.Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "entry",
		Params:     seedParams(tr),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}
	defer func() { _ = sub.Close(context.Background()) }()

	if !rt.WaitForSeedCount(1, 2*time.Second) {
		t.Fatal("redis trigger did not attempt an admission")
	}
	if consumer.WaitAck(300 * time.Millisecond) {
		t.Fatal("a refused entry was acknowledged; it must stay pending for redelivery")
	}
	if got := rt.SeedCount(); got != 1 {
		t.Fatalf("attempts = %d, want 1: a refusal waits for redelivery rather than retrying in place", got)
	}
}

// TestRedisStreamEntrySeedTransportFailureLeavesEntryUnacked pins the last row:
// a transport failure is retried within the bound and then the entry is left
// unacknowledged, which is what makes the reclaim path the recovery.
func TestRedisStreamEntrySeedTransportFailureLeavesEntryUnacked(t *testing.T) {
	restoreConsumer(t, newAckingConsumer([]Message{{ID: "3-0", Stream: "orders", Payload: []byte("three")}}))
	consumer := installedConsumer.(*ackingConsumer)

	previous := admitRetry
	admitRetry = seedadmit.Retry{MaxRetries: 3, Delay: time.Millisecond}
	t.Cleanup(func() { admitRetry = previous })

	rt := triggertest.NewFakeEntrySeedRuntime()
	rt.SetResponder(func(types.EntrySeedRequest, int) (types.EntrySeedResponse, error) {
		return types.EntrySeedResponse{}, context.DeadlineExceeded
	})
	tr := New().Addr("127.0.0.1:6379").Mode("stream").Stream("orders").Group("workers")
	sub, err := tr.Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "entry",
		Params:     seedParams(tr),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}
	defer func() { _ = sub.Close(context.Background()) }()

	if !rt.WaitForSeedCount(1, 2*time.Second) {
		t.Fatal("redis trigger did not attempt an admission")
	}
	if consumer.WaitAck(300 * time.Millisecond) {
		t.Fatal("an entry whose admission failed was acknowledged; it must stay pending")
	}
	if got := rt.SeedCount(); got != 4 {
		t.Fatalf("attempts = %d, want 4 (one attempt plus three retries)", got)
	}
}

// TestRedisStreamEntrySeedRedeliveryIsDuplicate asserts the reason the fire key
// is the entry's own identity: the same entry delivered twice — a reclaim after
// a crash between admission and acknowledgement — must produce one execution.
func TestRedisStreamEntrySeedRedeliveryIsDuplicate(t *testing.T) {
	restoreConsumer(t, newAckingConsumer([]Message{
		{ID: "4-0", Stream: "orders", Payload: []byte("four")},
		{ID: "4-0", Stream: "orders", Payload: []byte("four")},
	}))

	rt := triggertest.NewFakeEntrySeedRuntime()
	tr := New().Addr("127.0.0.1:6379").Mode("stream").Stream("orders").Group("workers").MaxInflight(1)
	sub, err := tr.Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "entry",
		Params:     seedParams(tr),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}
	defer func() { _ = sub.Close(context.Background()) }()

	if !rt.WaitForSeedCount(2, 2*time.Second) {
		t.Fatalf("attempts = %d, want 2", rt.SeedCount())
	}
	if got := rt.AcceptedCount(); got != 1 {
		t.Fatalf("accepted = %d, want 1 (a redelivered entry is a duplicate)", got)
	}
}

// TestRedisPubSubEntrySeedSkipsTheLock asserts the pub/sub lock is not taken on
// the entry-seed path. The fake's TryLock fails closed, so a successful
// Activate is itself the proof: a trigger that still called TryLock would have
// failed to activate. The admission key is the weak one — a hash of channel and
// payload bucketed to the second — which is why the replica count is held to
// one at compile time instead of relying on it.
func TestRedisPubSubEntrySeedSkipsTheLock(t *testing.T) {
	restoreConsumer(t, newScriptedConsumer([]Message{{Channel: "events", Payload: []byte("one"), Time: time.Now()}}))

	rt := triggertest.NewFakeEntrySeedRuntime()
	tr := New().Addr("127.0.0.1:6379").Mode("pubsub").Channel("events")
	sub, err := tr.Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "entry",
		Params:     seedParams(tr),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatalf("Activate: %v (the pub/sub lock must not be attempted on the entry-seed path)", err)
	}
	defer func() { _ = sub.Close(context.Background()) }()

	if !rt.WaitForSeedCount(1, 2*time.Second) {
		t.Fatal("redis pubsub trigger did not admit the message")
	}
	got := rt.Seeds()[0].Request.AdmissionKey
	if !strings.HasPrefix(got, "trigger:wf-1:entry:redis:pubsub/") {
		t.Fatalf("fire key = %q, want the weak pubsub shape", got)
	}
	// The key is the hash of channel+payload, bucketed to the message's own
	// second: the same publish arriving at another replica derives this exact
	// key, which is what makes the two collide into one execution.
	sameSecond := time.Unix(time.Now().Unix(), 0)
	want := "trigger:wf-1:entry:redis:" + redisFireID("pubsub",
		Message{Channel: "events", Payload: []byte("one")}, &types.TriggerEvent{Time: sameSecond})
	if !strings.HasPrefix(want, "trigger:wf-1:entry:redis:pubsub/") {
		t.Fatalf("test fixture produced %q, which is not the pubsub shape", want)
	}
	if emits, dedups, locks := rt.LegacyCalls(); emits != 0 || dedups != 0 || locks != 0 {
		t.Fatalf("legacy calls = emit:%d dedup:%d trylock:%d, want none", emits, dedups, locks)
	}
}

// TestRedisPubSubEntrySeedMergesSameSecondIdenticalPublishes pins what the weak
// key does and does not do. It merges deliveries a subscriber cannot itself
// tell apart — same channel, same payload, same second — which is the property
// that lets one replica stand in for several. It is deliberately NOT a
// substitute for a single replica, because it also merges two genuinely
// distinct publishes of the same payload in the same second.
func TestRedisPubSubEntrySeedMergesSameSecondIdenticalPublishes(t *testing.T) {
	published := time.Now().Truncate(time.Second).Add(500 * time.Millisecond)
	restoreConsumer(t, newScriptedConsumer([]Message{
		{Channel: "events", Payload: []byte("same"), Time: published},
		{Channel: "events", Payload: []byte("same"), Time: published},
	}))

	rt := triggertest.NewFakeEntrySeedRuntime()
	tr := New().Addr("127.0.0.1:6379").Mode("pubsub").Channel("events").MaxInflight(1)
	sub, err := tr.Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "entry",
		Params:     seedParams(tr),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}
	defer func() { _ = sub.Close(context.Background()) }()

	if !rt.WaitForSeedCount(2, 2*time.Second) {
		t.Fatalf("attempts = %d, want 2", rt.SeedCount())
	}
	if got := rt.AcceptedCount(); got != 1 {
		t.Fatalf("accepted = %d, want 1 (the second delivery is the weak key's duplicate)", got)
	}
}

// TestRedisPubSubEntrySeedRefusalDropsTheMessage pins the pub/sub disposition
// of a refusal: there is no acknowledgement and no redelivery behind it, so the
// message is gone. That is why the log line is part of the contract rather than
// decoration.
func TestRedisPubSubEntrySeedRefusalDropsTheMessage(t *testing.T) {
	restoreConsumer(t, newScriptedConsumer([]Message{{Channel: "events", Payload: []byte("two"), Time: time.Now()}}))

	rt := triggertest.NewFakeEntrySeedRuntime()
	rt.SetResponder(func(types.EntrySeedRequest, int) (types.EntrySeedResponse, error) {
		return types.EntrySeedResponse{Refused: true}, nil
	})
	tr := New().Addr("127.0.0.1:6379").Mode("pubsub").Channel("events")
	sub, err := tr.Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "entry",
		Params:     seedParams(tr),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}
	defer func() { _ = sub.Close(context.Background()) }()

	if !rt.WaitForSeedCount(1, 2*time.Second) {
		t.Fatal("redis pubsub trigger did not attempt an admission")
	}
	if got := rt.SeedCount(); got != 1 {
		t.Fatalf("attempts = %d, want 1: a refusal is not retried in place", got)
	}
	if got := rt.AcceptedCount(); got != 0 {
		t.Fatalf("accepted = %d, want 0 after a refusal", got)
	}
}

// TestRedisStreamEntrySeedKeepsLegacyPathForAnEngineRuntime asserts the
// capability gate on this trigger too: entry_seed against the embedded engine's
// runtime keeps the dedup+emit path.
func TestRedisStreamEntrySeedKeepsLegacyPathForAnEngineRuntime(t *testing.T) {
	restoreConsumer(t, newScriptedConsumer([]Message{{ID: "5-0", Stream: "orders", Payload: []byte("five")}}))

	rt := triggertest.NewFakeRuntime()
	tr := New().Addr("127.0.0.1:6379").Mode("stream").Stream("orders").Group("workers")
	sub, err := tr.Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "entry",
		Params:     seedParams(tr),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}
	defer func() { _ = sub.Close(context.Background()) }()

	if !rt.WaitEmit(2 * time.Second) {
		t.Fatal("redis trigger did not emit through the legacy path")
	}
}

// installedConsumer is the consumer the last restoreConsumer call installed.
// The factory seam takes no arguments a test could read back, and the trigger
// owns the instance it receives, so the test keeps its own reference here.
var installedConsumer Consumer

// restoreConsumer installs a scripted consumer for one test and restores the
// production factory afterwards.
func restoreConsumer(t *testing.T, consumer Consumer) {
	t.Helper()
	orig := newConsumer
	installedConsumer = consumer
	newConsumer = func(ConsumerConfig) (Consumer, error) { return consumer, nil }
	t.Cleanup(func() {
		newConsumer = orig
		installedConsumer = nil
	})
}
