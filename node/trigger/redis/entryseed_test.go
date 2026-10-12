package redis

import (
	"context"
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
