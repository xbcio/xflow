package node

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/types"
)

func TestKafkaTriggerBackpressureAndClose(t *testing.T) {
	orig := newKafkaConsumer
	consumer := newFakeKafkaConsumer(1000)
	newKafkaConsumer = func(KafkaConsumerConfig) (KafkaConsumer, error) { return consumer, nil }
	t.Cleanup(func() { newKafkaConsumer = orig })

	rt := &slowEmitRuntime{}
	tr := KafkaTrigger().Brokers("localhost:9092").Topic("orders").Group("workers").MaxInflight(4)

	started := time.Now()
	sub, err := tr.Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "kafka",
		Params:     tr.RawParams().(map[string]any),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("Activate took %s, want non-blocking", elapsed)
	}

	closeStarted := time.Now()
	if err := sub.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(closeStarted); elapsed > time.Second {
		t.Fatalf("Close took %s, want under 1s", elapsed)
	}
	if got := rt.maxActive.Load(); got > 4 {
		t.Fatalf("max active emits = %d, want <= 4", got)
	}
}

type fakeKafkaConsumer struct {
	ch chan KafkaMessage
}

func newFakeKafkaConsumer(n int) *fakeKafkaConsumer {
	ch := make(chan KafkaMessage, n)
	for i := 0; i < n; i++ {
		ch <- KafkaMessage{Topic: "orders", Partition: 0, Offset: int64(i), Value: []byte("payload")}
	}
	return &fakeKafkaConsumer{ch: ch}
}

func (c *fakeKafkaConsumer) Messages() <-chan KafkaMessage { return c.ch }
func (c *fakeKafkaConsumer) Close() error {
	close(c.ch)
	return nil
}

type slowEmitRuntime struct {
	active    atomic.Int64
	maxActive atomic.Int64
}

func (r *slowEmitRuntime) Emit(context.Context, types.WorkflowID, string, *types.TriggerEvent) (types.ExecutionID, error) {
	active := r.active.Add(1)
	for {
		max := r.maxActive.Load()
		if active <= max || r.maxActive.CompareAndSwap(max, active) {
			break
		}
	}
	time.Sleep(20 * time.Millisecond)
	r.active.Add(-1)
	return "exec-1", nil
}

func (r *slowEmitRuntime) Dedup(context.Context, string, time.Duration) (bool, error) {
	return true, nil
}

func (r *slowEmitRuntime) TryLock(context.Context, string, time.Duration) (types.TriggerLock, bool, error) {
	return fakeTriggerLock{}, true, nil
}

func (r *slowEmitRuntime) State(context.Context, string) types.TriggerState { return nil }
