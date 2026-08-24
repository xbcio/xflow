package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/trigger/triggertest"
	"github.com/xbcio/xflow/types"
)

func BenchmarkKafkaTriggerAggregate4000Messages(b *testing.B) {
	orig := newConsumer
	defer func() { newConsumer = orig }()

	messages := make([]Message, 4000)
	for i := range messages {
		messages[i] = Message{
			Topic:     "orders",
			Partition: 0,
			Offset:    int64(i + 1),
			Value:     []byte("value"),
		}
	}

	trigger := New().
		Brokers("localhost:9092").
		Topic("orders").
		Group("workers").
		AggregateByPartition(100, time.Hour)
	params := trigger.RawParams().(map[string]any)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		consumer := newScriptedKafkaConsumer(messages)
		newConsumer = func(ConsumerConfig) (Consumer, error) { return consumer, nil }

		rt := triggertest.NewFakeRuntime()
		sub, err := trigger.Activate(context.Background(), &types.TriggerActivateInput{
			WorkflowID: "wf-1",
			NodeName:   "kafka",
			Params:     params,
			Runtime:    rt,
		})
		if err != nil {
			b.Fatal(err)
		}
		closed := false
		defer func() {
			if !closed {
				_ = sub.Close(context.Background())
			}
		}()

		if !rt.WaitForEmitCount(40, time.Second) {
			b.Fatalf("emit count = %d, want 40", rt.EmitCount())
		}
		if err := sub.Close(context.Background()); err != nil {
			b.Fatal(err)
		}
		closed = true
		if got := rt.EmitCount(); got != 40 {
			b.Fatalf("emit count = %d, want 40", got)
		}

		events := rt.Events()
		if len(events) != 40 {
			b.Fatalf("events len = %d, want 40", len(events))
		}
		for _, event := range events {
			if event.Kind != "kafka.batch" {
				b.Fatalf("event kind = %q, want kafka.batch", event.Kind)
			}
			raw, ok := event.Data["messages"].([]map[string]any)
			if !ok {
				b.Fatalf("event data messages = %#v, want []map[string]any", event.Data["messages"])
			}
			if len(raw) != 100 {
				b.Fatalf("batch size = %d, want 100", len(raw))
			}
		}
	}
}

// BenchmarkKafkaTriggerAggregateDelayedEmit measures the property ordered
// concurrency is meant to change: one partition whose downstream latency is
// much larger than the time needed to build its next batch. Framework overhead
// benchmarks alone cannot distinguish a serial flush slot from a reorder
// window because an instantaneous fake never overlaps work.
func BenchmarkKafkaTriggerAggregateDelayedEmit(b *testing.B) {
	orig := newConsumer
	defer func() { newConsumer = orig }()

	const (
		messageCount = 400
		batchSize    = 100
		emitLatency  = 5 * time.Millisecond
	)
	messages := make([]Message, messageCount)
	for i := range messages {
		messages[i] = Message{
			Topic:     "orders",
			Partition: 0,
			Offset:    int64(i),
			Value:     []byte("value"),
		}
	}
	trigger := New().
		Brokers("localhost:9092").
		Topic("orders").
		Group("workers").
		MaxInflight(4).
		AggregateByPartition(batchSize, time.Hour)
	params := trigger.RawParams().(map[string]any)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		consumer := newReplayableKafkaConsumer(messages, nil)
		newConsumer = func(ConsumerConfig) (Consumer, error) { return consumer, nil }
		rt := triggertest.NewFakeRuntime()
		rt.SetEmitFunc(func(context.Context, types.WorkflowID, string, *types.TriggerEvent) (types.ExecutionID, error) {
			time.Sleep(emitLatency)
			return "exec", nil
		})
		sub, err := trigger.Activate(context.Background(), &types.TriggerActivateInput{
			WorkflowID: "wf-1",
			NodeName:   "kafka",
			Params:     params,
			Runtime:    rt,
		})
		if err != nil {
			b.Fatal(err)
		}
		if !consumer.waitForCommitCount(messageCount, 5*time.Second) {
			_ = sub.Close(context.Background())
			b.Fatalf("committed %d/%d messages", consumer.commitCount(), messageCount)
		}
		if err := sub.Close(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
}
