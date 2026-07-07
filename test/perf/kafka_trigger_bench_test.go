//go:build perf

package perf

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/xbcio/xflow/nodes/node"
	"github.com/xbcio/xflow/types"
)

// perfTriggerRuntime is a minimal types.TriggerRuntime that counts individual
// messages delivered via Emit callbacks.  It handles both "kafka" (single) and
// "kafka.batch" (aggregated) event kinds so that the counter tracks raw message
// throughput regardless of the MaxSize setting.
type perfTriggerRuntime struct {
	msgCount int64
	signal   chan struct{}
}

func newPerfTriggerRuntime() *perfTriggerRuntime {
	return &perfTriggerRuntime{signal: make(chan struct{}, 4096)}
}

func (r *perfTriggerRuntime) Emit(_ context.Context, _ types.WorkflowID, _ string, event *types.TriggerEvent) (types.ExecutionID, error) {
	n := int64(1)
	if event.Kind == "kafka.batch" {
		if msgs, ok := event.Data["messages"].([]map[string]any); ok && len(msgs) > 0 {
			n = int64(len(msgs))
		} else if c, ok := event.Data["count"]; ok {
			if ci, err := strconv.ParseInt(fmt.Sprintf("%v", c), 10, 64); err == nil && ci > 0 {
				n = ci
			}
		}
	}
	atomic.AddInt64(&r.msgCount, n)
	select {
	case r.signal <- struct{}{}:
	default:
	}
	return "exec-perf", nil
}

func (r *perfTriggerRuntime) Dedup(_ context.Context, _ string, _ time.Duration) (bool, error) {
	return true, nil
}

func (r *perfTriggerRuntime) TryLock(_ context.Context, _ string, _ time.Duration) (types.TriggerLock, bool, error) {
	return perfTriggerLock{}, true, nil
}

func (r *perfTriggerRuntime) State(_ context.Context, _ string) types.TriggerState { return nil }

type perfTriggerLock struct{}

func (perfTriggerLock) Release(context.Context) error { return nil }

// BenchmarkKafkaTriggerAggregateReal benchmarks KafkaTrigger with
// AggregateByPartition at three batch sizes (MaxSize=1/10/100).
// It writes b.N messages to Kafka and measures the time until the last
// message is delivered via the runtime Emit callback.
func BenchmarkKafkaTriggerAggregateReal(b *testing.B) {
	brokers := realKafkaBrokers(b)
	baseTopic := fmt.Sprintf("xflow-perf-kafka-%d", time.Now().UnixNano())

	for _, size := range []int{1, 10, 100} {
		size := size
		b.Run(fmt.Sprintf("MaxSize=%d", size), func(b *testing.B) {
			// Each sub-bench gets its own topic and consumer group to avoid
			// cross-contamination between MaxSize runs.
			topic := fmt.Sprintf("%s-s%d", baseTopic, size)
			group := topic + "-g"
			createTopic(b, brokers[0], topic, 4)

			tr := node.KafkaTrigger().
				Brokers(brokers...).
				Topic(topic).
				Group(group).
				StartOffset("earliest").
				AggregateByPartition(size, 50*time.Millisecond)

			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()

			rt := newPerfTriggerRuntime()

			sub, err := tr.Activate(ctx, &types.TriggerActivateInput{
				WorkflowID: types.WorkflowID(fmt.Sprintf("wf-perf-kafka-%d", size)),
				NodeName:   "kafka-perf",
				Params:     tr.RawParams().(map[string]any),
				Runtime:    rt,
			})
			if err != nil {
				b.Fatal(err)
			}
			defer sub.Close(ctx)

			// Write b.N messages before timing starts.
			msgs := make([]kafka.Message, b.N)
			for i := range msgs {
				msgs[i] = kafka.Message{Value: []byte(strconv.Itoa(i))}
			}

			w := &kafka.Writer{
				Addr:        kafka.TCP(brokers...),
				Topic:       topic,
				MaxAttempts: 10,
			}
			writeCtx, writeCancel := context.WithTimeout(ctx, 30*time.Second)
			defer writeCancel()
			// Retry until topic metadata propagates.
			for {
				if err := w.WriteMessages(writeCtx, msgs...); err == nil {
					break
				}
				select {
				case <-writeCtx.Done():
					b.Fatalf("write kafka messages: %v", writeCtx.Err())
				case <-time.After(200 * time.Millisecond):
				}
			}
			w.Close()

			b.ReportAllocs()
			b.ResetTimer()

			// Poll until all b.N messages are consumed.
			// Use ticker+select — no time.Sleep.
			target := int64(b.N)
			ticker := time.NewTicker(2 * time.Millisecond)
			defer ticker.Stop()
		poll:
			for {
				select {
				case <-rt.signal:
					if atomic.LoadInt64(&rt.msgCount) >= target {
						break poll
					}
				case <-ticker.C:
					if atomic.LoadInt64(&rt.msgCount) >= target {
						break poll
					}
				case <-ctx.Done():
					b.Fatalf("got %d/%d messages: %v", atomic.LoadInt64(&rt.msgCount), target, ctx.Err())
				}
			}
		})
	}
}
