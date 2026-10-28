package kafka

import (
	"context"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// adminClient is a short-lived client for the broker's admin APIs; the caller
// closes the returned transport.
func (e *itEnv) adminClient() (*kafkago.Client, *kafkago.Transport) {
	tr := &kafkago.Transport{}
	return &kafkago.Client{Addr: kafkago.TCP(e.brokers...), Timeout: 10 * time.Second, Transport: tr}, tr
}

// groupMembers asks the broker which members the consumer group of kind has
// and how many partitions of topic each owns. It returns nil while the broker
// cannot answer yet (for instance, the group coordinator is still being found).
func (e *itEnv) groupMembers(kind, topic string) map[string]int {
	e.t.Helper()
	client, tr := e.adminClient()
	defer tr.CloseIdleConnections()
	res, err := client.DescribeGroups(e.t.Context(), &kafkago.DescribeGroupsRequest{GroupIDs: []string{e.names.Group(kind)}})
	if err != nil {
		return nil
	}
	owned := map[string]int{}
	for _, g := range res.Groups {
		if g.Error != nil {
			return nil
		}
		for _, m := range g.Members {
			owned[m.MemberID] = 0
			for _, tp := range m.MemberAssignments.Topics {
				if tp.Topic == topic {
					owned[m.MemberID] += len(tp.Partitions)
				}
			}
		}
	}
	return owned
}

// Two runners consume the same deployment. One of them stops while tasks are
// still arriving. The partitions it owned move to the other, and every task is
// handled at least once: nothing is lost, though some may be delivered twice.
func TestKafkaIntegrationTwoConsumersSurviveOneStopping(t *testing.T) {
	if testing.Short() {
		t.Skip("rebalances take several seconds")
	}
	const total = 200
	e := newITEnv(t)
	tr1 := e.transport(nil)
	e.ensure(tr1)
	tr2 := e.transport(nil)

	c := &collector{}
	handler := func(ctx context.Context, task *engine.Task) error {
		_ = c.handle(ctx, task)
		select { // slow enough that most tasks are still queued when one runner stops
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
		}
		return nil
	}
	stop1 := startConsumer(t, tr1, handler)
	startConsumer(t, tr2, handler)
	waitFor(t, "both members to own partitions of the default topic", 60*time.Second, func() bool {
		owned := e.groupMembers(groupDefault, e.names.Default())
		if len(owned) != 2 {
			return false
		}
		for _, n := range owned {
			if n == 0 {
				return false
			}
		}
		return true
	})

	sent := map[types.ExecutionID]bool{}
	for i := range total {
		task := itTask(execID("rb", i), i%5 == 0)
		sent[task.ExecutionID] = true
		if err := tr2.Enqueue(t.Context(), task); err != nil {
			t.Fatalf("Enqueue %d: %v", i, err)
		}
	}
	waitFor(t, "a few tasks to be handled", 60*time.Second, func() bool { return len(c.ids()) >= 20 })
	handledAtStop := len(c.ids())
	if handledAtStop > total*3/4 {
		t.Fatalf("%d of %d tasks were already handled when the first runner stopped; the stop is not midway", handledAtStop, total)
	}
	stop1()
	waitFor(t, "every task to be handled", 90*time.Second, func() bool { return len(c.ids()) == total })
	owned := e.groupMembers(groupDefault, e.names.Default())
	t.Logf("members of the default group after the stop: %v", owned)

	for id := range c.ids() {
		if !sent[id] {
			t.Errorf("handler ran for %s, which nobody produced", id)
		}
	}
	t.Logf("%d deliveries of %d tasks (%d redelivered); %d were handled when the first runner stopped",
		len(c.all()), total, len(c.all())-total, handledAtStop)
	wantBatch := int64(total / 5)
	waitFor(t, "every offset to be committed", 30*time.Second, func() bool {
		return e.committed(groupBatch, e.names.Batch(), 3) == wantBatch &&
			e.committed(groupDefault, e.names.Default(), 3) == int64(total)-wantBatch
	})
}

// endOffsets returns the log end offset of each of the first partitions
// partitions of topic.
func (e *itEnv) endOffsets(topic string, partitions int) []int64 {
	e.t.Helper()
	client, tr := e.adminClient()
	defer tr.CloseIdleConnections()
	reqs := make([]kafkago.OffsetRequest, partitions)
	for p := range reqs {
		reqs[p] = kafkago.LastOffsetOf(p)
	}
	res, err := client.ListOffsets(e.t.Context(), &kafkago.ListOffsetsRequest{Topics: map[string][]kafkago.OffsetRequest{topic: reqs}})
	if err != nil {
		e.t.Fatalf("list offsets of %s: %v", topic, err)
	}
	out := make([]int64, partitions)
	for _, po := range res.Topics[topic] {
		if po.Error != nil {
			e.t.Fatalf("list offsets of %s/%d: %v", topic, po.Partition, po.Error)
		}
		out[po.Partition] = po.LastOffset
	}
	return out
}

// A running consumer picks up partitions that are added to its topic. The topic
// starts with one partition and grows to three while the consumer runs; tasks
// with many different keys then spread over all three, and every one of them
// must be handled. The reader notices the change on its partition watch
// interval (10s) and rebalances, so the new partitions are served some seconds
// after they appear. The test logs how long that took.
func TestKafkaIntegrationConsumerPicksUpAddedPartitions(t *testing.T) {
	if testing.Short() {
		t.Skip("waits for the reader's partition watch interval")
	}
	e := newITEnv(t)
	tr := e.transport(func(c *Config) { c.Partitions = 1 })
	e.ensure(tr)
	c := &collector{}
	startConsumer(t, tr, c.handle)
	if err := tr.Enqueue(t.Context(), itTask("warm", false)); err != nil {
		t.Fatalf("Enqueue warm-up: %v", err)
	}
	waitFor(t, "the warm-up task", 60*time.Second, func() bool { return c.ids()["warm"] > 0 })

	client, adm := e.adminClient()
	defer adm.CloseIdleConnections()
	topic := e.names.Default()
	res, err := client.CreatePartitions(t.Context(), &kafkago.CreatePartitionsRequest{
		Topics: []kafkago.TopicPartitionsConfig{{Name: topic, Count: 3}}})
	if err != nil {
		t.Fatalf("CreatePartitions: %v", err)
	}
	if err := res.Errors[topic]; err != nil {
		t.Fatalf("CreatePartitions %s: %v", topic, err)
	}
	expanded := time.Now()
	waitFor(t, "the metadata to show 3 partitions", 30*time.Second, func() bool { return e.topicPartitions(topic) == 3 })

	n := 0
	send := func(count int) {
		for range count {
			if err := tr.Enqueue(t.Context(), itTask(execID("grow", n), false)); err != nil {
				t.Fatalf("Enqueue %d: %v", n, err)
			}
			n++
		}
	}
	// The writer refreshes its topic metadata every few seconds, so keep sending
	// until a task lands on an added partition.
	waitFor(t, "the producer to route a task to an added partition", 30*time.Second, func() bool {
		send(10)
		off := e.endOffsets(topic, 3)
		return off[1]+off[2] > 0
	})
	send(60)
	t.Logf("producer used the added partitions %v after they were created", time.Since(expanded).Round(10*time.Millisecond))

	onNew := map[types.ExecutionID]int{}
	for _, m := range e.readTopic(topic, 3, n+1, 30*time.Second) {
		if m.Partition > 0 {
			onNew[types.ExecutionID(m.Key)] = m.Partition
		}
	}
	t.Logf("%d of %d tasks were produced to the added partitions", len(onNew), n)

	// The tasks on the added partitions are the ones a reader that never
	// rebalanced would not see, so they are what the wait is about.
	deadline := time.Now().Add(60 * time.Second)
	for len(c.ids()) < n+1 && time.Now().Before(deadline) {
		select {
		case <-time.After(itStep):
		case <-t.Context().Done():
		}
	}
	seen := map[types.ExecutionID]bool{}
	var first, last time.Duration
	for _, d := range c.all() {
		if _, ok := onNew[d.id]; !ok || seen[d.id] {
			continue
		}
		seen[d.id] = true
		after := d.at.Sub(expanded)
		if first == 0 || after < first {
			first = after
		}
		last = max(last, after)
	}
	missing := map[int]int{}
	for id, p := range onNew {
		if !seen[id] {
			missing[p]++
		}
	}
	if len(c.ids()) < n+1 {
		t.Fatalf("handled %d of %d tasks after 60s; tasks never handled, by added partition: %v",
			len(c.ids()), n+1, missing)
	}
	t.Logf("tasks on the added partitions were handled between %v and %v after the topic grew",
		first.Round(100*time.Millisecond), last.Round(100*time.Millisecond))
}
