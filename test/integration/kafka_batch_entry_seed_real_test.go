//go:build integration

package integration

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/xbcio/xflow/node/trigger"
	"github.com/xbcio/xflow/types"
)

// ---------------------------------------------------------------------------
// Fake EntrySeedRuntime that records seeds and always accepts
// ---------------------------------------------------------------------------

type batchSeedRecorder struct {
	mu    sync.Mutex
	seeds []types.EntrySeedRequest
	// failFromOffset, if > 0, rejects every batch starting at or after this
	// offset. An offset boundary rather than a call count keeps the failure
	// deterministic now that independent batches may execute concurrently.
	failFromOffset int64
	// seedDelay, if > 0, sleeps this duration inside SeedExecutionFromEntry
	// before returning. This slows down batch processing so that the polling
	// loop in Probe 2 can observe multiple intermediate committed-offset values.
	seedDelay time.Duration
}

func (r *batchSeedRecorder) SeedExecutionFromEntry(_ context.Context, req types.EntrySeedRequest) (types.EntrySeedResponse, error) {
	r.mu.Lock()
	if r.failFromOffset > 0 {
		if len(req.Exits) == 0 || req.Exits[0].Data == nil {
			r.mu.Unlock()
			return types.EntrySeedResponse{}, fmt.Errorf("simulated seed failure: batch has no exit data")
		}
		startOffset, ok := req.Exits[0].Data["start_offset"].(int64)
		if !ok {
			r.mu.Unlock()
			return types.EntrySeedResponse{}, fmt.Errorf("simulated seed failure: start_offset has type %T", req.Exits[0].Data["start_offset"])
		}
		if startOffset >= r.failFromOffset {
			r.mu.Unlock()
			return types.EntrySeedResponse{}, fmt.Errorf("simulated seed failure from offset %d", startOffset)
		}
	}
	r.seeds = append(r.seeds, req)
	delay := r.seedDelay
	r.mu.Unlock()
	if delay > 0 {
		time.Sleep(delay)
	}
	r.mu.Lock()
	n := len(r.seeds)
	r.mu.Unlock()
	return types.EntrySeedResponse{Accepted: true, ExecutionID: types.ExecutionID(fmt.Sprintf("exec-%d", n))}, nil
}

func (r *batchSeedRecorder) Emit(_ context.Context, _ types.WorkflowID, _ string, _ *types.TriggerEvent) (types.ExecutionID, error) {
	return "", fmt.Errorf("entry-seed runtime: Emit not supported")
}

func (r *batchSeedRecorder) Dedup(_ context.Context, _ string, _ time.Duration) (bool, error) {
	return false, fmt.Errorf("entry-seed runtime: Dedup not supported")
}

func (r *batchSeedRecorder) TryLock(_ context.Context, _ string, _ time.Duration) (types.TriggerLock, bool, error) {
	return nil, false, fmt.Errorf("entry-seed runtime: TryLock not supported")
}

func (r *batchSeedRecorder) State(_ context.Context, _ string) types.TriggerState { return nil }

func (r *batchSeedRecorder) seedCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.seeds)
}

func (r *batchSeedRecorder) allSeededValues() map[string]bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	values := map[string]bool{}
	for _, s := range r.seeds {
		for _, ex := range s.Exits {
			if msgs, ok := ex.Data["messages"].([]map[string]any); ok {
				for _, m := range msgs {
					if v, ok := m["value"].(string); ok {
						values[v] = true
					}
				}
			}
		}
	}
	return values
}

// ---------------------------------------------------------------------------
// Helper: fetch committed offset for a consumer group from Kafka
// ---------------------------------------------------------------------------

func fetchCommittedOffset(t *testing.T, brokers []string, group, topic string, partition int) int64 {
	t.Helper()
	client := &kafka.Client{Addr: kafka.TCP(brokers...)}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{
		GroupID: group,
		Topics:  map[string][]int{topic: {partition}},
	})
	if err != nil {
		t.Fatalf("OffsetFetch: %v", err)
	}
	partitions, ok := resp.Topics[topic]
	if !ok || len(partitions) == 0 {
		return -1
	}
	for _, p := range partitions {
		if p.Partition == partition {
			if p.Error != nil {
				return -1
			}
			return p.CommittedOffset
		}
	}
	return -1
}

// ---------------------------------------------------------------------------
// Probe 1: Redelivery Loses Nothing
//
// Asserts: when seed fails for a batch, offsets are NOT committed; when the
// reader is rebuilt, those messages are redelivered and eventually processed.
// Duplicates are acceptable (spec §4.1); missing messages are not.
//
// How "seed succeeds but commit doesn't land" is manufactured:
// The brief's literal scenario is "seed succeeds but commit doesn't land" (e.g.
// crash between seed and commit). We implement the EQUIVALENT scenario: "seed
// fails, so commit is withheld". Both verify the SAME invariant from the same
// direction: commit must not advance past messages whose seed has not durably
// succeeded. The equivalence holds because the production code path is:
//
//     if !seedKafkaEntryBatchMessages(...) { return false }  // no commit
//     ...
//     commitKafkaMessages(...)                               // commit
//
// A failed seed causes flush() to return false before reaching the commit call.
// A crash-after-seed-before-commit has the same effect: the commit never lands.
// In both cases, the invariant being tested is that Kafka redelivers the batch
// on reader rebuild. The failAfter approach is deterministic and does not
// require racing a context cancel against a sub-millisecond commit RPC.
//
// Failure condition: if the trigger committed offsets BEFORE/without the seed
// succeeding (ordering violation), the failed batch's offsets would already be
// committed, and rebuild would NOT redeliver them. The assertion would then
// find missing values.
// ---------------------------------------------------------------------------

func TestKafkaBatchEntrySeed_RedeliveryLosesNothing(t *testing.T) {
	brokers := requireKafka(t)
	_ = requireRedis(t)

	topic := uniqueTopic("xflow-batch-seed-redeliver")
	group := topic + "-group"
	newKafkaTopic(t, brokers, topic, 1)

	const totalMessages = 25
	const batchSize = 5
	// The first 3 batches (offsets 0-14) succeed; batches starting at offset 15
	// fail. Tying the fault to the offset avoids depending on concurrent batch
	// completion order.
	const failAfterBatches = 3

	// Produce 25 messages with unique values.
	msgs := make([]kafka.Message, 0, totalMessages)
	for i := 0; i < totalMessages; i++ {
		msgs = append(msgs, kafka.Message{
			Key:   []byte("k"),
			Value: []byte(fmt.Sprintf("msg-%03d", i)),
		})
	}
	writeKafkaMessages(t, brokers, topic, msgs)

	// --- Pass 1: consume with seed that fails after 3 batches. ---
	recorder1 := &batchSeedRecorder{failFromOffset: failAfterBatches * batchSize}

	tr1 := trigger.Kafka().
		Brokers(brokers...).
		Topic(topic).
		Group(group).
		StartOffset("earliest").
		AggregateByPartition(batchSize, 200*time.Millisecond).
		BlockOnOverflow()

	ctx1, cancel1 := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel1()

	sub1, err := tr1.Activate(ctx1, &types.TriggerActivateInput{
		WorkflowID: "wf-batch-redeliver",
		NodeName:   "kafka-batch",
		Params: map[string]any{
			"brokers":       brokers,
			"topic":         topic,
			"group":         group,
			"start_offset":  "earliest",
			"entry_seed":    true,
			"entry_unit_id": "kafka-batch",
			"aggregate": map[string]any{
				"enabled":        true,
				"by":             "partition",
				"max_size":       batchSize,
				"flush_interval": "200ms",
				"dedup":          "message",
				"on_overflow":    "block",
			},
		},
		Runtime: recorder1,
	})
	if err != nil {
		t.Fatalf("activate pass 1: %v", err)
	}

	// Wait until the first 3 seeds complete (the successful ones).
	deadline1 := time.After(20 * time.Second)
	for {
		if recorder1.seedCount() >= failAfterBatches {
			break
		}
		select {
		case <-deadline1:
			t.Fatalf("pass 1 timeout: only %d seeds (want >= %d)", recorder1.seedCount(), failAfterBatches)
		case <-time.After(100 * time.Millisecond):
		}
	}

	// Wait for the trigger to attempt the 4th batch (which will fail seed and
	// thus NOT commit). Poll committed offset until it stabilizes at the expected
	// value, rather than using a fixed sleep that may be too short on slow machines.
	expectedCommit := int64(failAfterBatches * batchSize)
	commitDeadline := time.After(10 * time.Second)
	for {
		off := fetchCommittedOffset(t, brokers, group, topic, 0)
		if off >= expectedCommit {
			break
		}
		select {
		case <-commitDeadline:
			t.Fatalf("pass 1: committed offset never reached %d (got %d)", expectedCommit, off)
		case <-time.After(100 * time.Millisecond):
		}
	}

	// Close the subscription.
	if err := sub1.Close(context.Background()); err != nil {
		t.Logf("sub1 close: %v", err)
	}
	cancel1()

	// Verify: committed offset should cover only the first 3 batches (15 msgs).
	committed := fetchCommittedOffset(t, brokers, group, topic, 0)
	// committed should be 15 (offset of the first uncommitted message).
	// It must NOT be >= 25, which would mean offsets were committed without
	// successful seed.
	if committed >= int64(totalMessages) {
		t.Fatalf("pass 1: committed offset = %d covers ALL messages despite seed failures; offset-safety violated", committed)
	}
	if committed < int64(failAfterBatches*batchSize) {
		t.Fatalf("pass 1: committed offset = %d, want >= %d (3 successful batches)", committed, failAfterBatches*batchSize)
	}
	t.Logf("pass 1: committed offset = %d (expected ~%d); %d messages uncommitted",
		committed, failAfterBatches*batchSize, int64(totalMessages)-committed)

	// --- Pass 2: rebuild reader with same group. Messages from offset
	// `committed` onward must be redelivered and successfully processed. ---
	recorder2 := &batchSeedRecorder{} // no failAfter — all seeds succeed

	tr2 := trigger.Kafka().
		Brokers(brokers...).
		Topic(topic).
		Group(group).
		StartOffset("earliest").
		AggregateByPartition(batchSize, 200*time.Millisecond).
		BlockOnOverflow()

	ctx2, cancel2 := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel2()

	sub2, err := tr2.Activate(ctx2, &types.TriggerActivateInput{
		WorkflowID: "wf-batch-redeliver",
		NodeName:   "kafka-batch",
		Params: map[string]any{
			"brokers":       brokers,
			"topic":         topic,
			"group":         group,
			"start_offset":  "earliest",
			"entry_seed":    true,
			"entry_unit_id": "kafka-batch",
			"aggregate": map[string]any{
				"enabled":        true,
				"by":             "partition",
				"max_size":       batchSize,
				"flush_interval": "200ms",
				"dedup":          "message",
				"on_overflow":    "block",
			},
		},
		Runtime: recorder2,
	})
	if err != nil {
		t.Fatalf("activate pass 2: %v", err)
	}

	// Wait for pass 2 to consume and seed the remaining messages.
	remaining := int64(totalMessages) - committed
	expectedSeeds2 := int((remaining + int64(batchSize) - 1) / int64(batchSize))
	if expectedSeeds2 < 1 {
		expectedSeeds2 = 1
	}
	deadline2 := time.After(20 * time.Second)
	for {
		if recorder2.seedCount() >= expectedSeeds2 {
			break
		}
		select {
		case <-deadline2:
			t.Fatalf("pass 2 timeout: only %d seeds (want >= %d)", recorder2.seedCount(), expectedSeeds2)
		case <-time.After(100 * time.Millisecond):
		}
	}

	// Grace period for commit.
	time.Sleep(500 * time.Millisecond)
	if err := sub2.Close(context.Background()); err != nil {
		t.Logf("sub2 close: %v", err)
	}
	cancel2()

	// Final committed offset must cover all messages.
	finalCommitted := fetchCommittedOffset(t, brokers, group, topic, 0)
	if finalCommitted < int64(totalMessages) {
		t.Fatalf("final committed offset = %d, want >= %d", finalCommitted, totalMessages)
	}

	// CRITICAL assertion: the union of pass 1 + pass 2 seeded values must cover
	// every produced message. Missing = silent message loss.
	allValues := map[string]bool{}
	for v := range recorder1.allSeededValues() {
		allValues[v] = true
	}
	for v := range recorder2.allSeededValues() {
		allValues[v] = true
	}
	for i := 0; i < totalMessages; i++ {
		want := fmt.Sprintf("msg-%03d", i)
		if !allValues[want] {
			t.Errorf("MISSING message value %q in union of seeded passes", want)
		}
	}
	if t.Failed() {
		t.Fatalf("redelivery lost messages: see MISSING errors above")
	}
	t.Logf("all %d message values covered across 2 passes (pass1=%d seeds, pass2=%d seeds)",
		totalMessages, recorder1.seedCount(), recorder2.seedCount())
}

// ---------------------------------------------------------------------------
// Probe 2: Per-Partition Serial Ordering Not Broken by Batching
//
// Asserts: within a single partition, committed offsets advance monotonically
// (never backwards). A batch for partition P must not cause a commit at an
// offset higher than a still-buffered lower offset on the same partition.
//
// This is a NEGATIVE assertion ("X must not happen"). Per project rules, we
// use a reverse wait: poll the committed offset repeatedly over a time window
// and confirm that at no point does it regress.
//
// To ensure the polling loop observes multiple intermediate committed-offset
// values (not just a single jump from -1 to N), the seed recorder introduces
// a per-seed delay of 80ms. With 10 batches of 3, processing takes ~800ms,
// giving the 50ms polling interval about 16 sampling opportunities.
//
// Guard assertion: the number of distinct committed-offset values observed must
// be >= 3. This prevents the probe from silently degrading back to single-sample
// if future changes speed up processing.
//
// Failure condition: if the aggregator flushed partition batches concurrently
// and a higher batch committed before a lower batch, the committed offset
// would regress between samples.
// ---------------------------------------------------------------------------

func TestKafkaBatchEntrySeed_PerPartitionSerialCommit(t *testing.T) {
	brokers := requireKafka(t)
	_ = requireRedis(t)

	topic := uniqueTopic("xflow-batch-serial")
	group := topic + "-group"
	newKafkaTopic(t, brokers, topic, 1)

	const totalMessages = 30
	const batchSize = 3
	const minDistinctOffsets = 3 // guard: must observe at least this many distinct values

	// Produce messages.
	msgs := make([]kafka.Message, 0, totalMessages)
	for i := 0; i < totalMessages; i++ {
		msgs = append(msgs, kafka.Message{
			Key:   []byte("k"),
			Value: []byte(fmt.Sprintf("serial-%03d", i)),
		})
	}
	writeKafkaMessages(t, brokers, topic, msgs)

	// Activate the trigger with a seed delay so batches take ~80ms each,
	// spreading 10 batches over ~800ms for observable intermediate commits.
	recorder := &batchSeedRecorder{seedDelay: 80 * time.Millisecond}

	tr := trigger.Kafka().
		Brokers(brokers...).
		Topic(topic).
		Group(group).
		StartOffset("earliest").
		AggregateByPartition(batchSize, 150*time.Millisecond).
		BlockOnOverflow()

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	sub, err := tr.Activate(ctx, &types.TriggerActivateInput{
		WorkflowID: "wf-batch-serial",
		NodeName:   "kafka-serial",
		Params: map[string]any{
			"brokers":       brokers,
			"topic":         topic,
			"group":         group,
			"start_offset":  "earliest",
			"entry_seed":    true,
			"entry_unit_id": "kafka-serial",
			"aggregate": map[string]any{
				"enabled":        true,
				"by":             "partition",
				"max_size":       batchSize,
				"flush_interval": "150ms",
				"dedup":          "message",
				"on_overflow":    "block",
			},
		},
		Runtime: recorder,
	})
	if err != nil {
		t.Fatalf("activate: %v", err)
	}

	// Reverse wait: poll committed offset every 50ms. Track all distinct values
	// seen and verify monotonicity at each sample.
	var (
		prevCommitted  int64 = -1
		violations     []string
		totalSamples   int
		distinctValues = map[int64]bool{}
		done           bool
	)

	pollDeadline := time.After(25 * time.Second)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

polling:
	for {
		select {
		case <-ticker.C:
			totalSamples++
			off := fetchCommittedOffset(t, brokers, group, topic, 0)
			if off < 0 {
				continue // not yet committed
			}
			distinctValues[off] = true
			if off < prevCommitted {
				violations = append(violations, fmt.Sprintf(
					"committed offset went BACKWARDS: %d -> %d (sample %d)",
					prevCommitted, off, totalSamples))
			}
			prevCommitted = off
			if off >= int64(totalMessages) {
				done = true
				break polling
			}
		case <-pollDeadline:
			break polling
		}
	}

	// Grace period and close.
	time.Sleep(300 * time.Millisecond)
	if err := sub.Close(context.Background()); err != nil {
		t.Logf("sub close: %v", err)
	}
	cancel()

	// Report violations.
	if len(violations) > 0 {
		for _, v := range violations {
			t.Errorf("SERIAL VIOLATION: %s", v)
		}
		t.Fatalf("per-partition serial commit invariant broken")
	}

	if !done {
		t.Fatalf("partition 0 did not reach committed offset %d within deadline; last=%d (samples=%d)",
			totalMessages, prevCommitted, totalSamples)
	}

	// Guard assertion: we must have observed enough distinct offset values to
	// prove we actually sampled intermediate states.
	if len(distinctValues) < minDistinctOffsets {
		t.Fatalf("sampling resolution too low: observed only %d distinct committed offset values (want >= %d); "+
			"probe cannot distinguish ordered from unordered commits",
			len(distinctValues), minDistinctOffsets)
	}

	// Post-close reverse wait: confirm no late regression over 2 seconds.
	time.Sleep(2 * time.Second)
	postOff := fetchCommittedOffset(t, brokers, group, topic, 0)
	if postOff < prevCommitted {
		t.Fatalf("post-close: committed offset went backwards: %d -> %d", prevCommitted, postOff)
	}

	t.Logf("per-partition serial invariant held: %d distinct offset values observed across %d samples; final committed offset = %d",
		len(distinctValues), totalSamples, prevCommitted)
}
