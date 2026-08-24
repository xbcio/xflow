package kafka

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// The consumer sets ReadLagInterval: -1, turning off kafka-go's own lag
// reporting, on the stated grounds that the trigger reports lag itself. These
// tests pin the mechanism that makes that statement true.

func TestLagSamplerReportsDistancePastTheFetchedOffset(t *testing.T) {
	sampler := newLagSampler(time.Second)
	now := time.Unix(1_700_000_000, 0)

	// Offset 10 consumed, high-water mark 100: offsets 11..99 remain, which is
	// 89 records. Not 90 — the high-water mark is the offset the NEXT produced
	// record will occupy, so it is one past the last existing record.
	behind, report := sampler.sample(kafkago.Message{Partition: 0, Offset: 10, HighWaterMark: 100}, now)
	if !report {
		t.Fatal("first sample from a partition must report")
	}
	if behind != 89 {
		t.Fatalf("lag = %d, want 89", behind)
	}
}

func TestLagSamplerReportsZeroAtTheTail(t *testing.T) {
	sampler := newLagSampler(time.Second)
	now := time.Unix(1_700_000_000, 0)

	behind, report := sampler.sample(kafkago.Message{Partition: 0, Offset: 99, HighWaterMark: 100}, now)
	if !report {
		t.Fatal("a caught-up partition must still report; silence is indistinguishable from a stall")
	}
	if behind != 0 {
		t.Fatalf("lag at the tail = %d, want 0", behind)
	}
}

// A partition that has just been assigned must show its lag immediately. If the
// throttle applied to the first sample, a rebalance onto a badly-lagging
// partition would look healthy for the whole first interval — which is exactly
// when an operator is watching.
func TestLagSamplerAlwaysReportsTheFirstSampleOfAPartition(t *testing.T) {
	sampler := newLagSampler(time.Hour)
	now := time.Unix(1_700_000_000, 0)

	for _, partition := range []int{0, 1, 2} {
		if _, report := sampler.sample(kafkago.Message{Partition: partition, HighWaterMark: 10}, now); !report {
			t.Fatalf("first sample of partition %d was throttled", partition)
		}
	}
}

// Partitions must throttle independently. Sharing one timer would let a busy
// partition spend the budget of a quiet one — and the quiet partition is the
// one whose lag is worth looking at, because "quiet" and "stalled" produce the
// same message rate.
func TestLagSamplerThrottlesEachPartitionSeparately(t *testing.T) {
	sampler := newLagSampler(time.Second)
	start := time.Unix(1_700_000_000, 0)

	if _, report := sampler.sample(kafkago.Message{Partition: 0, HighWaterMark: 10}, start); !report {
		t.Fatal("partition 0 first sample must report")
	}
	if _, report := sampler.sample(kafkago.Message{Partition: 0, HighWaterMark: 10}, start.Add(500*time.Millisecond)); report {
		t.Fatal("partition 0 reported twice inside one interval")
	}
	// Partition 1 has never been sampled; partition 0's traffic must not have
	// consumed its first report.
	if _, report := sampler.sample(kafkago.Message{Partition: 1, HighWaterMark: 10}, start.Add(500*time.Millisecond)); !report {
		t.Fatal("partition 1 was throttled by partition 0's sample")
	}
	if _, report := sampler.sample(kafkago.Message{Partition: 0, HighWaterMark: 10}, start.Add(1500*time.Millisecond)); !report {
		t.Fatal("partition 0 did not resume reporting after the interval elapsed")
	}
}

// kafka-go leaves HighWaterMark at zero on a message it did not populate. A
// fetched message occupies an offset, so a real high-water mark is at least
// offset+1 and can never be zero. Reporting HighWaterMark-Offset-1 in that case
// would publish a negative lag, which reads as "ahead of the broker" and is
// worse than publishing nothing.
func TestLagSamplerSkipsUnpopulatedHighWaterMark(t *testing.T) {
	sampler := newLagSampler(time.Second)
	now := time.Unix(1_700_000_000, 0)

	if _, report := sampler.sample(kafkago.Message{Partition: 0, Offset: 42}, now); report {
		t.Fatal("reported lag from a message with no high-water mark")
	}
	// Skipping must not consume the partition's first-sample slot: the next
	// message that does carry a mark has to report immediately.
	behind, report := sampler.sample(kafkago.Message{Partition: 0, Offset: 42, HighWaterMark: 50}, now)
	if !report {
		t.Fatal("a skipped sample consumed the partition's first-report slot")
	}
	if behind != 7 {
		t.Fatalf("lag = %d, want 7", behind)
	}
}

// Defensive: if a broker's reported high-water mark trails the offset we just
// read (observable across a leader change), clamp rather than export a negative
// gauge.
func TestLagSamplerClampsNegativeLagToZero(t *testing.T) {
	sampler := newLagSampler(time.Second)
	behind, report := sampler.sample(
		kafkago.Message{Partition: 0, Offset: 100, HighWaterMark: 90},
		time.Unix(1_700_000_000, 0),
	)
	if !report {
		t.Fatal("sample must report")
	}
	if behind != 0 {
		t.Fatalf("lag = %d, want it clamped to 0", behind)
	}
}

// --- wiring ---------------------------------------------------------------

// recordingLagObserver captures OnConsumerLag calls. It embeds noopObserver so
// the rest of the interface stays out of the way.
type recordingLagObserver struct {
	noopObserver
	mu      sync.Mutex
	samples []lagSample
}

type lagSample struct {
	topic     string
	partition int
	lag       int64
	fetchedAt time.Time
}

func (o *recordingLagObserver) OnConsumerLag(_ context.Context, topic string, partition int, lag int64, fetchedAt time.Time) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.samples = append(o.samples, lagSample{topic, partition, lag, fetchedAt})
}

func (o *recordingLagObserver) snapshot() []lagSample {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]lagSample(nil), o.samples...)
}

// TestConsumerReportsLagPerPartitionAgainstRealKafka is the only test here that
// proves the WIRE. The lagSampler tests above prove arithmetic against a struct
// literal I built myself; they would keep passing if kafkaGoConsumer.run never
// called the sampler at all, and they would keep passing if kafka-go stopped
// populating HighWaterMark on fetched messages. Both are the failure this
// metric would suffer in production, and neither is reachable from a unit test.
//
// Requires the disposable local broker (test/env), not the shared cluster:
// this creates topics and consumer groups.
func TestConsumerReportsLagPerPartitionAgainstRealKafka(t *testing.T) {
	brokers := kafkaIntegrationBrokers(t)
	topic := fmt.Sprintf("xflow-lag-%d", time.Now().UnixNano())
	createKafkaIntegrationTopic(t, brokers[0], topic, 2)

	// Written BEFORE the consumer starts, and in unequal amounts, so a
	// per-partition report has a distinct expected value on each partition and
	// a single collapsed series cannot satisfy both.
	const onPartition0, onPartition1 = 30, 10
	writeKafkaPartitionMessages(t, brokers[0], topic, 0, onPartition0)
	writeKafkaPartitionMessages(t, brokers[0], topic, 1, onPartition1)

	observer := &recordingLagObserver{}
	SetObserver(observer)
	defer SetObserver(nil)

	consumer, err := newKafkaGoConsumer(ConsumerConfig{
		Brokers:     brokers,
		Topic:       topic,
		Group:       fmt.Sprintf("xflow-lag-group-%d", time.Now().UnixNano()),
		StartOffset: "earliest",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = consumer.Close() }()

	// Drain everything: both partitions must be fetched for both to be sampled.
	deadline := time.After(60 * time.Second)
	for read := 0; read < onPartition0+onPartition1; {
		select {
		case _, ok := <-consumer.Messages():
			if !ok {
				t.Fatal("consumer channel closed before all messages arrived")
			}
			read++
		case <-deadline:
			t.Fatalf("timed out after %d/%d messages", read, onPartition0+onPartition1)
		}
	}

	// The first sample of each partition is the one with a known answer: it is
	// taken on the offset-0 message, before anything has been consumed, so lag
	// is everything behind it.
	first := map[int]lagSample{}
	before := time.Now()
	for _, sample := range observer.snapshot() {
		if sample.topic != topic {
			t.Fatalf("lag reported for topic %q, want %q", sample.topic, topic)
		}
		if _, seen := first[sample.partition]; !seen {
			first[sample.partition] = sample
		}
	}

	if len(first) != 2 {
		t.Fatalf("lag reported for %d partitions (%v), want 2; a metric that "+
			"reports one partition of an assignment lets a stalled partition hide "+
			"behind a healthy one", len(first), first)
	}
	for partition, want := range map[int]int64{0: onPartition0 - 1, 1: onPartition1 - 1} {
		if got := first[partition].lag; got != want {
			t.Errorf("partition %d first lag = %d, want %d", partition, got, want)
		}
		// fetchedAt is what makes a frozen gauge detectable; a zero value would
		// make every reading look infinitely stale.
		if stamp := first[partition].fetchedAt; stamp.IsZero() || stamp.After(before) {
			t.Errorf("partition %d fetchedAt = %v, want a real time no later than %v",
				partition, stamp, before)
		}
	}
}

// writeKafkaPartitionMessages writes exactly count messages to one partition.
// The Writer helper in kafka_test.go balances by key, which cannot place a
// known count on a known partition — and this test's whole point is that the
// two partitions differ.
//
// It retries, because a topic created moments ago has no elected leader yet:
// DialLeader resolves against metadata that is still converging and the write
// fails with "Not Leader For Partition". That is a property of writing to a
// brand-new topic, not a symptom worth failing a lag test over.
func writeKafkaPartitionMessages(t *testing.T, broker, topic string, partition, count int) {
	t.Helper()
	messages := make([]kafkago.Message, 0, count)
	for i := range count {
		messages = append(messages, kafkago.Message{Value: []byte(fmt.Sprintf("m%d", i))})
	}

	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for attempt := 0; time.Now().Before(deadline); attempt++ {
		if attempt > 0 {
			time.Sleep(250 * time.Millisecond)
		}
		lastErr = writeOnceToPartition(broker, topic, partition, messages)
		if lastErr == nil {
			return
		}
	}
	t.Fatalf("writing %d messages to %s/%d never succeeded: %v", count, topic, partition, lastErr)
}

func writeOnceToPartition(broker, topic string, partition int, messages []kafkago.Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := kafkago.DialLeader(ctx, "tcp", broker, topic, partition)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	_, err = conn.WriteMessages(messages...)
	return err
}
