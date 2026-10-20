package kafka

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/node/trigger/triggertest"
	"github.com/xbcio/xflow/types"
)

// TestEmitMessageRefusalIsLogged pins the legacy per-message path's only trace
// of a refused emit.
//
// The offset is deliberately left uncommitted when the emit fails, which is the
// correct at-least-once behaviour and also a silent one: the worker moves on to
// the next message, and a workflow refusing its trigger traffic at a
// REJECT-mode concurrency limit then looks exactly like a topic with nothing on
// it. A QUEUE-mode workflow never refuses here — its over-limit firings are
// parked and admitted later — so this line is what an operator reads when the
// limit, not the topic, is the reason nothing runs.
func TestEmitMessageRefusalIsLogged(t *testing.T) {
	buf := captureAdmissionLog(t)
	rt := triggertest.NewFakeRuntime()
	rt.SetEmitFunc(func(context.Context, types.WorkflowID, string, *types.TriggerEvent) (types.ExecutionID, error) {
		return "", engine.ErrConcurrencyLimit
	})
	in := &types.TriggerActivateInput{WorkflowID: "wf-1", NodeName: "kafka", Runtime: rt}

	if err := emitMessage(context.Background(), in, Message{Topic: "orders", Partition: 3, Offset: 42}); err == nil {
		t.Fatal("emitMessage() = nil for a refused emit, want an error: the caller commits only on nil")
	}

	logged := buf.String()
	if !strings.Contains(logged, "kafka trigger emit did not succeed") {
		t.Fatalf("a refused emit left no log line; captured:\n%s", logged)
	}
	// The identifiers are the actionable part: they bound the refused record to
	// a topic, a partition and an offset that can be inspected and, if needed,
	// replayed by hand.
	for _, want := range []string{"orders", "partition=3", "offset=42", "workflow concurrency limit reached"} {
		if !strings.Contains(logged, want) {
			t.Fatalf("log line is missing %q; captured:\n%s", want, logged)
		}
	}
}

// TestKafkaRefusedMessageIsNotCommitted is decision 5's kafka half end to end on
// the legacy path: a refused emit must not advance the committed offset, so the
// record is redelivered rather than dropped.
func TestKafkaRefusedMessageIsNotCommitted(t *testing.T) {
	msgs := []Message{{Topic: "t", Partition: 0, Offset: 7, Value: []byte("a")}}
	consumer := newReplayableKafkaConsumer(msgs, nil)
	orig := newConsumer
	newConsumer = func(ConsumerConfig) (Consumer, error) { return consumer, nil }
	t.Cleanup(func() { newConsumer = orig })

	rt := triggertest.NewFakeRuntime()
	rt.SetEmitFunc(func(context.Context, types.WorkflowID, string, *types.TriggerEvent) (types.ExecutionID, error) {
		return "", engine.ErrConcurrencyLimit
	})
	// No AggregateByPartition: this is the per-message runtime, the one a
	// deployment without batching uses.
	tr := New().Brokers("localhost:9092").Topic("t").Group("g")
	sub, err := tr.Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "kafka",
		Params:     tr.RawParams().(map[string]any),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatal(err)
	}

	if !rt.WaitForEmitCount(1, 2*time.Second) {
		t.Fatalf("emit count = %d, want >= 1", rt.EmitCount())
	}
	// A brief window for any erroneous commit to land before the assertion.
	time.Sleep(30 * time.Millisecond)
	_ = sub.Close(context.Background())

	if got := consumer.commitCount(); got != 0 {
		t.Fatalf("commit count = %d, want 0: a refused emit must not advance the offset", got)
	}
}
