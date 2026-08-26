package redishub

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/trigger/triggertest"
	"github.com/xbcio/xflow/types"
)

// The redishub tests that reach emitMessage assert EmitCount and nothing else —
// they never read back the event they caused. That leaves the event identity
// unpinned, and for this trigger the identity is doing two jobs at once: it is
// the deduplication key and it is the `<stream>/<entry-id>` string downstream
// consumers match on. Reversing the two halves keeps every existing test green.
//
// The TTL is unpinned for the same reason it was in cron, timer and webhook: no
// fake ever looked at it.

func TestEmitMessageStreamEventIDIsStreamThenEntry(t *testing.T) {
	rt := triggertest.NewFakeRuntime()
	in := &types.TriggerActivateInput{WorkflowID: "wf-1", NodeName: "hub", Runtime: rt}

	emitMessage(context.Background(), in, "stream", Message{
		Stream:  "orders",
		ID:      "1700000000000-0",
		Payload: []byte(`{"id":1}`),
		Time:    time.Unix(1700000000, 0),
	})

	events := rt.Events()
	if len(events) != 1 {
		t.Fatalf("emitted %d events, want 1", len(events))
	}
	const want = "orders/1700000000000-0"
	if got := events[0].ID; got != want {
		t.Fatalf("stream event ID = %q, want %q. The two halves are not "+
			"interchangeable: the entry ID is unique only within its stream, so "+
			"putting it first makes the deduplication key collide across streams, "+
			"and anything matching on the documented <stream>/<entry> shape stops "+
			"matching", got, want)
	}
	if got := events[0].Data["stream"]; got != "orders" {
		t.Fatalf("event stream = %v, want orders", got)
	}
	if got := events[0].Data["mode"]; got != "stream" {
		t.Fatalf("event mode = %v, want stream", got)
	}
}

func TestEmitMessagePubSubEventIDDoesNotBorrowTheStreamPrefix(t *testing.T) {
	// The prefix is stream-mode only. Applying it in pub/sub mode would prepend
	// an empty stream name and every event ID would start with a bare slash.
	rt := triggertest.NewFakeRuntime()
	in := &types.TriggerActivateInput{WorkflowID: "wf-1", NodeName: "hub", Runtime: rt}

	emitMessage(context.Background(), in, "pubsub", Message{
		Channel: "orders",
		ID:      "msg-7",
		Payload: []byte(`{"id":1}`),
		Time:    time.Unix(1700000000, 0),
	})

	events := rt.Events()
	if len(events) != 1 {
		t.Fatalf("emitted %d events, want 1", len(events))
	}
	if got := events[0].ID; got != "msg-7" {
		t.Fatalf("pubsub event ID = %q, want msg-7", got)
	}
}

func TestEmitMessageDedupWindowCoversRedelivery(t *testing.T) {
	rt := triggertest.NewFakeRuntime()
	in := &types.TriggerActivateInput{WorkflowID: "wf-1", NodeName: "hub", Runtime: rt}

	emitMessage(context.Background(), in, "stream", Message{
		Stream: "orders", ID: "1700000000000-0", Time: time.Unix(1700000000, 0),
	})

	calls := rt.DedupCalls()
	if len(calls) != 1 {
		t.Fatalf("dedup calls = %d, want 1", len(calls))
	}
	if got := calls[0].TTL; got != 24*time.Hour {
		t.Fatalf("dedup TTL = %v, want 24h. A consumer group redelivers an entry "+
			"whose ack never landed, which can be long after the original read; a "+
			"shorter window lets that redelivery start a second execution", got)
	}
	if got, want := calls[0].Key, "trigger:wf-1:hub:orders/1700000000000-0"; got != want {
		t.Fatalf("dedup key = %q, want %q: the key must be namespaced by workflow "+
			"and node, or two workflows reading the same stream deduplicate against "+
			"each other and only one of them ever runs", got, want)
	}
}
