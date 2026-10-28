package kafka

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/xbcio/xflow/backend/providers/distributed/internal/queue"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// waitFor polls cond every itStep until it holds, and fails the test after
// timeout. Assertions on timing use the delivery timestamps, never this loop.
func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", timeout, what)
		}
		select {
		case <-time.After(itStep):
		case <-t.Context().Done():
			t.Fatalf("test ended while waiting for %s", what)
		}
	}
}

// stays polls check every itStep for d and fails the test the first time it
// returns an error. It is the negative counterpart of waitFor: it proves that
// something does not happen, by looking for it throughout the window instead of
// sleeping and looking once.
func stays(t *testing.T, d time.Duration, check func() error) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if err := check(); err != nil {
			t.Errorf("within %v: %v", d, err)
			return
		}
		select {
		case <-time.After(itStep):
		case <-t.Context().Done():
			return
		}
	}
}

// delivery is one handler call as the collector saw it.
type delivery struct {
	id  types.ExecutionID
	ns  namespace.Namespace
	typ engine.TaskType
	at  time.Time
}

// collector is a task handler that records every call. fail, if set, decides
// the handler's result from the call and how many times that execution has been
// delivered so far (this call included).
type collector struct {
	mu   sync.Mutex
	got  []delivery
	fail func(d delivery, nth int) error
}

func (c *collector) handle(ctx context.Context, task *engine.Task) error {
	d := delivery{id: task.ExecutionID, ns: namespace.FromContext(ctx), typ: task.Type, at: time.Now()}
	c.mu.Lock()
	c.got = append(c.got, d)
	nth := 0
	for _, g := range c.got {
		if g.id == d.id {
			nth++
		}
	}
	fail := c.fail
	c.mu.Unlock()
	if fail != nil {
		return fail(d, nth)
	}
	return nil
}

func (c *collector) all() []delivery {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.got)
}

// ids is the set of distinct executions delivered so far.
func (c *collector) ids() map[types.ExecutionID]int {
	out := map[types.ExecutionID]int{}
	for _, d := range c.all() {
		out[d.id]++
	}
	return out
}

// readTopic reads topic straight from the broker, partition by partition from
// the start, until want messages arrived or timeout passed. A negative want
// reads for the whole timeout, which counts everything on the topic.
func (e *itEnv) readTopic(topic string, partitions, want int, timeout time.Duration) []kafkago.Message {
	ctx, cancel := context.WithTimeout(e.t.Context(), timeout)
	defer cancel()
	msgs := make(chan kafkago.Message, 1024)
	var wg sync.WaitGroup
	for p := range partitions {
		wg.Go(func() {
			r := kafkago.NewReader(kafkago.ReaderConfig{Brokers: e.brokers, Topic: topic, Partition: p,
				StartOffset: kafkago.FirstOffset, MaxWait: 200 * time.Millisecond})
			defer func() { _ = r.Close() }()
			for {
				m, err := r.ReadMessage(ctx)
				if err != nil {
					return
				}
				select {
				case msgs <- m:
				case <-ctx.Done():
					return
				}
			}
		})
	}
	var got []kafkago.Message
	for want < 0 || len(got) < want {
		select {
		case m := <-msgs:
			got = append(got, m)
			continue
		case <-ctx.Done():
		}
		break
	}
	cancel()
	wg.Wait()
	return got
}

// committed returns the offset the consumer group of kind (groupDefault,
// groupBatch or groupDelay) has committed on topic, summed over its partitions.
// A partition with nothing committed counts as zero.
func (e *itEnv) committed(kind, topic string, partitions int) int64 {
	e.t.Helper()
	tr := &kafkago.Transport{}
	defer tr.CloseIdleConnections()
	client := &kafkago.Client{Addr: kafkago.TCP(e.brokers...), Timeout: 10 * time.Second, Transport: tr}
	parts := make([]int, partitions)
	for i := range parts {
		parts[i] = i
	}
	res, err := client.OffsetFetch(e.t.Context(), &kafkago.OffsetFetchRequest{
		GroupID: e.names.Group(kind), Topics: map[string][]int{topic: parts}})
	if err != nil {
		e.t.Fatalf("offset fetch: %v", err)
	}
	var sum int64
	for _, p := range res.Topics[topic] {
		sum += max(p.CommittedOffset, 0)
	}
	return sum
}

// startConsumer starts tr's consumer with h. The returned stop may be called
// more than once, and it also runs at the end of the test.
func startConsumer(t *testing.T, tr *Transport, h queue.TaskHandler) func() {
	t.Helper()
	rawStop, err := tr.StartConsumer(queue.ConsumerConfig{Concurrency: 4}, h)
	if err != nil {
		t.Fatalf("StartConsumer: %v", err)
	}
	var once sync.Once
	stop := func() { once.Do(rawStop) }
	t.Cleanup(stop)
	return stop
}

// execID is a distinct execution id for task n of a test.
func execID(prefix string, n int) types.ExecutionID {
	return types.ExecutionID(fmt.Sprintf("%s-%03d", prefix, n))
}
