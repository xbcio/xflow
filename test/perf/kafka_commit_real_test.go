//go:build perf

package perf

import (
	"context"
	"fmt"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"golang.org/x/time/rate"

	"github.com/xbcio/xflow/node/trigger"
	kafkatrigger "github.com/xbcio/xflow/node/trigger/kafka"
	"github.com/xbcio/xflow/types"
)

// This file re-measures the Kafka trigger's offset-commit hop against a real
// broker AFTER commit_coalescer.go (f91b942, 2026-09-06). Before that fix,
// docs/xflow-verification-findings.md §1.5 measured (against an
// SAS-in-process topology, not reproducible in this repo): 2927 commits over
// 411s on an 18-partition topic, mean 2.24s, 88.8% of partition-seconds spent
// waiting on commit, 20.2x the wasm evaluation time in the same window. The
// coalescer's own doc comment (commit_coalescer.go, observer.go) claims a
// narrow probe measurement of mean 2.049s -> 179ms, but that number came from
// an external SAS-side harness (pkg/xflow/commit_concurrency_test.go, not in
// this repo) that fetches and commits in one goroutine and is NOT the
// production aggregate.go code path. Nothing in this repo previously drove
// KafkaTrigger's actual AggregateByPartition path against a real broker with
// enough partitions and concurrent commits to reproduce the queueing effect
// the coalescer targets. This test does that, using only exported production
// APIs (trigger.Kafka(), kafkatrigger.SetObserver) — no production code is
// modified.
//
// What this test does NOT reproduce: the full SAS topology (kafka trigger ->
// map/wasm decode+clean -> group boundary -> sink). It measures the trigger's
// own commit hop and consumption rate in isolation, with a near-zero-cost
// Emit callback. That isolates the question "is offset commit still the
// pacer" from wasm/admission/sink cost, which is exactly what is needed to
// answer it — but it means the throughput figure here is a ceiling for the
// trigger alone, not an end-to-end pipeline number.

// realKafkaBrokersT resolves the broker list for a *testing.T (the existing
// realKafkaBrokers in kafka_helpers_test.go takes *testing.B). Under
// XFLOW_REQUIRE_KAFKA_INTEGRATION=1 an unreachable broker fails the test
// instead of skipping it, so a missing dependency cannot be misread as a
// passing (or silently absent) measurement.
func realKafkaBrokersT(t *testing.T) []string {
	t.Helper()
	raw := os.Getenv("XFLOW_TEST_KAFKA_BROKERS")
	if raw == "" {
		raw = "localhost:9092"
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if s := strings.TrimSpace(p); s != "" {
			out = append(out, s)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := kafka.DialContext(ctx, "tcp", out[0])
	if err != nil {
		if os.Getenv("XFLOW_REQUIRE_KAFKA_INTEGRATION") == "1" {
			t.Fatalf("XFLOW_REQUIRE_KAFKA_INTEGRATION=1: kafka unavailable at %s: %v", out[0], err)
		}
		t.Skipf("kafka unavailable: %v", err)
	}
	_ = c.Close()
	return out
}

func createTopicT(t *testing.T, broker, topic string, partitions int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	conn, err := kafka.DialContext(ctx, "tcp", broker)
	if err != nil {
		t.Fatalf("dial kafka broker: %v", err)
	}
	controller, err := conn.Controller()
	if err != nil {
		_ = conn.Close()
		t.Fatalf("get kafka controller: %v", err)
	}
	_ = conn.Close()
	controllerConn, err := kafka.DialContext(ctx, "tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		t.Fatalf("dial kafka controller: %v", err)
	}
	defer controllerConn.Close()
	if err := controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	}); err != nil {
		t.Fatalf("create kafka topic %q: %v", topic, err)
	}
	t.Cleanup(func() { deleteTopicT(t, broker, topic) })
}

func deleteTopicT(t *testing.T, broker, topic string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	conn, err := kafka.DialContext(ctx, "tcp", broker)
	if err != nil {
		t.Logf("leaked kafka topic %q: dial broker: %v", topic, err)
		return
	}
	controller, err := conn.Controller()
	_ = conn.Close()
	if err != nil {
		t.Logf("leaked kafka topic %q: resolve controller: %v", topic, err)
		return
	}
	controllerConn, err := kafka.DialContext(ctx, "tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		t.Logf("leaked kafka topic %q: dial controller: %v", topic, err)
		return
	}
	defer controllerConn.Close()
	if err := controllerConn.DeleteTopics(topic); err != nil {
		t.Errorf("leaked kafka topic %q: broker answered but refused the delete: %v", topic, err)
	}
}

// commitSampleReal is one OnOffsetCommit observation.
type commitSampleReal struct {
	d      time.Duration
	result string
}

// commitStatsObserver is a kafkatrigger.Observer that records every
// OnOffsetCommit call (the metric bfaf3ce2 added specifically because this
// hop had none) plus discard/overflow counts, so a run can report the same
// shape of numbers §1.5 reported: commit count, mean/p50/p99, and the
// fraction of partition-seconds spent waiting on commit.
type commitStatsObserver struct {
	mu      sync.Mutex
	commits []commitSampleReal

	discarded     atomic.Int64 // OnMessageDiscarded (schema/schema_fail/buffer_overflow)
	deadLettered  atomic.Int64
	batchFlushed  atomic.Int64
	admissionErrs atomic.Int64
}

func (o *commitStatsObserver) OnMessageDiscarded(_ context.Context, _, _ string) {
	o.discarded.Add(1)
}
func (o *commitStatsObserver) OnMessageDeadLettered(_ context.Context, _, _ string) {
	o.deadLettered.Add(1)
}
func (o *commitStatsObserver) OnConsumerLag(context.Context, string, int, int64, time.Time) {}
func (o *commitStatsObserver) OnConsumptionBlocked(context.Context, string, int, bool)      {}
func (o *commitStatsObserver) OnBatchFlushed(_ context.Context, _, _ string, _ int) {
	o.batchFlushed.Add(1)
}
func (o *commitStatsObserver) OnBatchFlushOutcome(context.Context, string, string, string) {}
func (o *commitStatsObserver) OnBatchAdmission(_ context.Context, _, state, _ string) {
	if state == "error" || state == "conflict" || state == "deterministic_error" {
		o.admissionErrs.Add(1)
	}
}
func (o *commitStatsObserver) OnOffsetCommit(_ context.Context, _, result string, _ int, d time.Duration) {
	o.mu.Lock()
	o.commits = append(o.commits, commitSampleReal{d: d, result: result})
	o.mu.Unlock()
}

var _ kafkatrigger.Observer = (*commitStatsObserver)(nil)

func (o *commitStatsObserver) snapshot() []commitSampleReal {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]commitSampleReal(nil), o.commits...)
}

// commitLoadRuntime is a types.TriggerRuntime whose Emit does the minimum
// possible amount of work (an atomic add), so any pacing this test observes
// is attributable to the trigger + broker, not to a slow downstream — that is
// the isolation this test exists to provide (see file-level comment).
type commitLoadRuntime struct {
	messages atomic.Int64
	batches  atomic.Int64
}

func (r *commitLoadRuntime) Emit(_ context.Context, _ types.WorkflowID, _ string, event *types.TriggerEvent) (types.ExecutionID, error) {
	n := int64(1)
	if event.Kind == "kafka.batch" {
		if c, ok := event.Data["count"]; ok {
			switch v := c.(type) {
			case int:
				n = int64(v)
			case int64:
				n = v
			}
		}
	}
	r.messages.Add(n)
	r.batches.Add(1)
	return "exec-commit-load", nil
}

func (r *commitLoadRuntime) Dedup(context.Context, string, time.Duration) (bool, error) {
	return true, nil
}
func (r *commitLoadRuntime) TryLock(context.Context, string, time.Duration) (types.TriggerLock, bool, error) {
	return commitLoadLock{}, true, nil
}
func (r *commitLoadRuntime) State(context.Context, string) types.TriggerState { return nil }

type commitLoadLock struct{}

func (commitLoadLock) Release(context.Context) error { return nil }

func durationPercentile(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * q)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// TestKafkaTriggerCommitThroughputReal drives KafkaTrigger's real
// AggregateByPartition code path (aggregate.go) against a real, multi-
// partition topic under sustained concurrent producer load, and reports
// whether the offset-commit hop (commitMessages -> commitCoalescer ->
// reader.CommitMessages) is still the pipeline's pacer post-f91b942.
//
// Methodology mirrors docs/xflow-verification-findings.md §1.5 as closely as
// this repo allows: 18 partitions (same count), a fixed wall-clock load
// window, and the commit-wait-as-fraction-of-partition-seconds statistic. It
// differs in two ways that must not be blurred into the old numbers: (1) the
// producer here is a plain kafka.Writer round-robining across partitions in
// N goroutines, not SAS's synthetic apisix traffic, so absolute msg/s is not
// comparable session-to-session or to §1.5's numbers; (2) Emit is a no-op
// counter, so this isolates the trigger's own commit/consumption capacity
// from wasm/admission/sink cost — it is not an end-to-end pipeline figure.
// commitLoadArm parameterizes one load shape. targetRate <= 0 means
// unlimited (producers write as fast as WriteMessages allows); otherwise
// producers are throttled to targetRate aggregate messages/sec using a
// shared token bucket, so the commit hop can be measured both (a) at a
// supply comparable to §1.5's SAS traffic (~4200-4800 msg/s) for an
// apples-to-apples wait-fraction comparison, and (b) at whatever ceiling
// this environment's producers can push, to see whether commit becomes the
// pacer again once supply is high enough.
type commitLoadArm struct {
	name             string
	producers        int
	messagesPerBatch int
	targetRate       float64 // aggregate msgs/sec across all producers; <=0 = unlimited
}

func TestKafkaTriggerCommitThroughputReal(t *testing.T) {
	if testing.Short() {
		t.Skip("skip real-Kafka commit throughput test in short mode")
	}

	brokers := realKafkaBrokersT(t)

	arms := []commitLoadArm{
		// Matches docs/xflow-verification-findings.md §1.5's reported supply
		// (4100-4800 msg/s) so the commit-wait-fraction number is comparable in
		// SHAPE (not in absolute msg/s, since the topology differs) to the
		// pre-fix 88.8% figure.
		{name: "SASComparableRate_4500mps", producers: 32, messagesPerBatch: 100, targetRate: 4500},
		// Unbounded: producers write as fast as this environment allows, to find
		// whether commit becomes the pacer again at a high enough supply.
		{name: "SaturatedCeiling", producers: 32, messagesPerBatch: 100, targetRate: 0},
	}

	for _, arm := range arms {
		arm := arm
		t.Run(arm.name, func(t *testing.T) {
			runKafkaCommitLoad(t, brokers, arm)
		})
	}
}

func runKafkaCommitLoad(t *testing.T, brokers []string, arm commitLoadArm) {
	const (
		partitionCount = 18
		loadWindow     = 30 * time.Second
		drainWindow    = 10 * time.Second
		maxSize        = 100
		flushInterval  = 100 * time.Millisecond
	)

	topic := fmt.Sprintf("xflow-perf-commit-%d", time.Now().UnixNano())
	group := topic + "-g"
	createTopicT(t, brokers[0], topic, partitionCount)

	obs := &commitStatsObserver{}
	kafkatrigger.SetObserver(obs)
	t.Cleanup(func() { kafkatrigger.SetObserver(nil) })

	tr := trigger.Kafka().
		Brokers(brokers...).
		Topic(topic).
		Group(group).
		StartOffset("earliest").
		MaxInflight(256).
		AggregateByPartition(maxSize, flushInterval)

	rt := &commitLoadRuntime{}

	activateCtx, activateCancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer activateCancel()
	sub, err := tr.Activate(activateCtx, &types.TriggerActivateInput{
		WorkflowID: "wf-perf-commit",
		NodeName:   "kafka-commit-perf",
		Params:     tr.RawParams().(map[string]any),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatalf("activate: %v", err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer closeCancel()
		if err := sub.Close(closeCtx); err != nil {
			t.Errorf("close subscription: %v", err)
		}
	}()

	writer := &kafka.Writer{
		Addr:        kafka.TCP(brokers...),
		Topic:       topic,
		Balancer:    &kafka.RoundRobin{},
		MaxAttempts: 10,
		BatchSize:   50,
	}
	defer writer.Close()

	// Warmup: write and wait for one message to confirm the consumer group has
	// joined before the timed window starts (mirrors
	// BenchmarkKafkaTriggerAggregateReal's probe step).
	probeCtx, probeCancel := context.WithTimeout(context.Background(), 30*time.Second)
	ticker := time.NewTicker(50 * time.Millisecond)
warmup:
	for {
		if err := writer.WriteMessages(probeCtx, kafka.Message{Value: []byte("probe")}); err == nil {
			break warmup
		}
		select {
		case <-probeCtx.Done():
			ticker.Stop()
			probeCancel()
			t.Fatalf("write probe message: %v", probeCtx.Err())
		case <-ticker.C:
		}
	}
	ticker.Stop()
	deadline := time.Now().Add(30 * time.Second)
	for rt.messages.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if rt.messages.Load() < 1 {
		probeCancel()
		t.Fatalf("probe message not consumed within 30s")
	}
	probeCancel()

	// Reset counters after warmup so the timed window measures steady state.
	rt.messages.Store(0)
	rt.batches.Store(0)
	obs.mu.Lock()
	obs.commits = nil
	obs.mu.Unlock()

	var produced atomic.Int64
	var producerErrs atomic.Int64
	loadCtx, loadCancel := context.WithTimeout(context.Background(), loadWindow+5*time.Second)
	defer loadCancel()

	// A shared limiter throttles the AGGREGATE rate across all producer
	// goroutines to arm.targetRate messages/sec; nil means unlimited.
	var limiter *rate.Limiter
	if arm.targetRate > 0 {
		burst := arm.messagesPerBatch * 2
		if burst < 1 {
			burst = 1
		}
		limiter = rate.NewLimiter(rate.Limit(arm.targetRate), burst)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	start := time.Now()
	for p := 0; p < arm.producers; p++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			seq := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				if limiter != nil {
					if err := limiter.WaitN(loadCtx, arm.messagesPerBatch); err != nil {
						return
					}
				}
				batch := make([]kafka.Message, arm.messagesPerBatch)
				for i := range batch {
					batch[i] = kafka.Message{Value: []byte(fmt.Sprintf("p%d-%d-%d", id, seq, i))}
				}
				seq++
				writeCtx, writeCancel := context.WithTimeout(loadCtx, 10*time.Second)
				if err := writer.WriteMessages(writeCtx, batch...); err != nil {
					producerErrs.Add(1)
				} else {
					produced.Add(int64(len(batch)))
				}
				writeCancel()
			}
		}(p)
	}

	time.Sleep(loadWindow)
	close(stop)
	wg.Wait()
	windowElapsed := time.Since(start)

	// Drain: let already-fetched messages finish aggregating and committing
	// before taking the final snapshot, so trailing batches are not
	// misread as "lost".
	time.Sleep(drainWindow)

	consumed := rt.messages.Load()
	batches := rt.batches.Load()
	commitSamples := obs.snapshot()

	durations := make([]time.Duration, 0, len(commitSamples))
	var sumCommit time.Duration
	var errCount int
	for _, s := range commitSamples {
		durations = append(durations, s.d)
		sumCommit += s.d
		if s.result != "ok" {
			errCount++
		}
	}
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })

	var mean time.Duration
	if len(durations) > 0 {
		mean = sumCommit / time.Duration(len(durations))
	}
	p50 := durationPercentile(durations, 0.50)
	p90 := durationPercentile(durations, 0.90)
	p99 := durationPercentile(durations, 0.99)
	var maxD time.Duration
	if len(durations) > 0 {
		maxD = durations[len(durations)-1]
	}

	partitionSeconds := float64(partitionCount) * windowElapsed.Seconds()
	commitWaitFraction := 0.0
	if partitionSeconds > 0 {
		commitWaitFraction = sumCommit.Seconds() / partitionSeconds * 100
	}

	consumedRatio := 0.0
	if produced.Load() > 0 {
		consumedRatio = float64(consumed) / float64(produced.Load()) * 100
	}

	t.Logf("kafka commit throughput (post-coalescer, this-repo measurement): "+
		"arm=%s window=%s partitions=%d producers=%d produced=%d consumed=%d ratio=%.1f%% "+
		"batches=%d commits=%d commit_errors=%d discarded=%d dead_lettered=%d admission_errs=%d "+
		"commit_mean=%s commit_p50=%s commit_p90=%s commit_p99=%s commit_max=%s "+
		"commit_wait_fraction_of_partition_seconds=%.2f%% producer_errs=%d",
		arm.name, windowElapsed.Round(time.Millisecond), partitionCount, arm.producers,
		produced.Load(), consumed, consumedRatio,
		batches, len(commitSamples), errCount, obs.discarded.Load(), obs.deadLettered.Load(), obs.admissionErrs.Load(),
		mean, p50, p90, p99, maxD,
		commitWaitFraction, producerErrs.Load())

	fmt.Printf("perf.metric topology=kafka-trigger-only test=kafka_commit_throughput arm=%s "+
		"partitions=%d window_s=%.0f produced=%d consumed=%d ratio_pct=%.1f "+
		"commit_count=%d commit_mean_ms=%.1f commit_p50_ms=%.1f commit_p90_ms=%.1f commit_p99_ms=%.1f commit_max_ms=%.1f "+
		"commit_wait_fraction_pct=%.2f discarded=%d producer_errs=%d\n",
		arm.name, partitionCount, windowElapsed.Seconds(), produced.Load(), consumed, consumedRatio,
		len(commitSamples), float64(mean.Microseconds())/1000, float64(p50.Microseconds())/1000,
		float64(p90.Microseconds())/1000, float64(p99.Microseconds())/1000, float64(maxD.Microseconds())/1000,
		commitWaitFraction, obs.discarded.Load(), producerErrs.Load())

	if producerErrs.Load() > 0 {
		t.Logf("warning: %d producer write errors during the load window (see broker load / timeouts)", producerErrs.Load())
	}
	if len(commitSamples) == 0 {
		t.Errorf("no OnOffsetCommit samples observed; the commit hop cannot be evaluated (probe/consumer wiring likely broken)")
	}
}
