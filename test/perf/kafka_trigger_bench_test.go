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

	"github.com/xbcio/xflow/node/trigger"
	"github.com/xbcio/xflow/types"
)

// perfTriggerRuntime is a minimal types.TriggerRuntime that counts individual
// messages delivered via Emit callbacks.  It handles both "kafka" (single) and
// "kafka.batch" (aggregated) event kinds so that the counter tracks raw message
// throughput regardless of the MaxSize setting.
type perfTriggerRuntime struct {
	msgCount   int64
	batchCount int64
	signal     chan struct{}
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
	atomic.AddInt64(&r.batchCount, 1)
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

// waitMsgCount blocks until rt.msgCount >= target, using rt.signal + ticker.
// Returns false if ctx is cancelled first.
func waitMsgCount(ctx context.Context, rt *perfTriggerRuntime, target int64) bool {
	if atomic.LoadInt64(&rt.msgCount) >= target {
		return true
	}
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-rt.signal:
			if atomic.LoadInt64(&rt.msgCount) >= target {
				return true
			}
		case <-ticker.C:
			if atomic.LoadInt64(&rt.msgCount) >= target {
				return true
			}
		case <-ctx.Done():
			return false
		}
	}
}

// BenchmarkKafkaTriggerAggregateReal benchmarks KafkaTrigger with
// AggregateByPartition at three batch sizes (MaxSize=1/10/100).
//
// Design: consumer join happens once during warmup (probe message), then
// b.N iterations each write one full batch of `size` messages and wait for
// them to be delivered.  This measures steady-state aggregation latency,
// not consumer-group rebalance overhead.
func BenchmarkKafkaTriggerAggregateReal(b *testing.B) {
	brokers := realKafkaBrokers(b)
	baseTopic := fmt.Sprintf("xflow-perf-kafka-%d", time.Now().UnixNano())

	for _, size := range []int{1, 10, 100} {
		size := size
		b.Run(fmt.Sprintf("MaxSize=%d", size), func(b *testing.B) {
			// Each sub-bench gets its own topic and consumer group to avoid
			// cross-contamination between MaxSize runs.
			// Use 1 partition so all messages land in the same aggregator window.
			topic := fmt.Sprintf("%s-s%d", baseTopic, size)
			group := topic + "-g"
			createTopic(b, brokers[0], topic, 1)

			tr := trigger.Kafka().
				Brokers(brokers...).
				Topic(topic).
				Group(group).
				StartOffset("earliest").
				AggregateByPartition(size, 50*time.Millisecond)

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer cancel()

			rt := newPerfTriggerRuntime()

			// Step 1: Activate the trigger (consumer group created here).
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

			// Step 2: Warmup — write 1 probe message and wait until it is received.
			// This ensures the consumer has joined before timing starts.
			w := &kafka.Writer{
				Addr:        kafka.TCP(brokers...),
				Topic:       topic,
				MaxAttempts: 10,
			}
			defer w.Close()

			probeCtx, probeCancel := context.WithTimeout(ctx, 30*time.Second)
			defer probeCancel()
			probe := kafka.Message{Value: []byte("probe")}
			// Retry until topic metadata propagates — use ticker, no time.After.
			retryTicker := time.NewTicker(50 * time.Millisecond)
		probeWrite:
			for {
				if err := w.WriteMessages(probeCtx, probe); err == nil {
					retryTicker.Stop()
					break probeWrite
				}
				select {
				case <-probeCtx.Done():
					retryTicker.Stop()
					b.Fatalf("write probe message: %v", probeCtx.Err())
				case <-retryTicker.C:
				}
			}
			// Wait for the probe to be consumed (confirms consumer group joined).
			if !waitMsgCount(probeCtx, rt, 1) {
				b.Fatalf("probe message not received within 30s: %v", probeCtx.Err())
			}

			// Step 3: Reset counters and timer — warmup is done.
			atomic.StoreInt64(&rt.msgCount, 0)
			atomic.StoreInt64(&rt.batchCount, 0)
			// Drain any leftover signals from probe.
			for {
				select {
				case <-rt.signal:
				default:
					goto drained
				}
			}
		drained:

			b.ReportAllocs()
			b.ResetTimer()

			// Step 4: b.N iterations — each iteration writes one complete batch.
			for i := 0; i < b.N; i++ {
				// Snapshot count before writing so we can wait for exactly `size` new messages.
				baseline := atomic.LoadInt64(&rt.msgCount)
				target := baseline + int64(size)

				msgs := make([]kafka.Message, size)
				for j := range msgs {
					msgs[j] = kafka.Message{Value: []byte(strconv.Itoa(i*size + j))}
				}

				writeCtx, writeCancel := context.WithTimeout(ctx, 30*time.Second)
				if err := w.WriteMessages(writeCtx, msgs...); err != nil {
					writeCancel()
					b.Fatalf("iter %d: write messages: %v", i, err)
				}
				writeCancel()

				// Wait until at least `size` new messages have been emitted.
				if !waitMsgCount(ctx, rt, target) {
					b.Fatalf("iter %d: got %d messages, want %d: %v",
						i, atomic.LoadInt64(&rt.msgCount), target, ctx.Err())
				}
			}

			b.StopTimer()
			// Step 5: Close subscription.
			sub.Close(ctx) //nolint:errcheck
		})
	}
}
