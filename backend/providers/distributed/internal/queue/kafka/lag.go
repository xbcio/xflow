package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/xbcio/xflow/backend/providers/distributed/internal/queue"
)

// LagRoles lists every role OnKafkaLag reports, one per consumed topic and named
// after its consumer group kind, so the adapter that turns them into a metric
// label can be checked against it.
func LagRoles() []string {
	return []string{groupDefault, groupBatch, groupDelay}
}

// offsetReader is the part of *kafkago.Client the lag sampler uses. Tests
// replace Transport.offsets with a fake; clientOffsets is the real one.
type offsetReader interface {
	Metadata(ctx context.Context, req *kafkago.MetadataRequest) (*kafkago.MetadataResponse, error)
	ListOffsets(ctx context.Context, req *kafkago.ListOffsetsRequest) (*kafkago.ListOffsetsResponse, error)
	OffsetFetch(ctx context.Context, req *kafkago.OffsetFetchRequest) (*kafkago.OffsetFetchResponse, error)
	// Close releases the connections the reader holds. It stays usable.
	Close()
}

// clientOffsets is the offsetReader backed by a kafka-go Client. It has its own
// transport, like clientAdmin, so closing it never touches the producer's or a
// consumer's connections. Metadata requests do not ask the broker to create a
// topic (see clientAdmin), so sampling a missing topic cannot make one.
type clientOffsets struct {
	*kafkago.Client
	transport *kafkago.Transport
}

func newClientOffsets(cfg Config) (*clientOffsets, error) {
	mechanism, err := buildSASLMechanism(cfg.SASLMechanism, cfg.SASLUsername, cfg.SASLPassword)
	if err != nil {
		return nil, err
	}
	tr := &kafkago.Transport{TLS: cfg.TLS, SASL: mechanism, MetadataTTL: adminMetadataTTL}
	return &clientOffsets{
		transport: tr,
		Client: &kafkago.Client{
			Addr:      kafkago.TCP(cfg.Brokers...),
			Timeout:   cfg.writeTimeout(),
			Transport: tr,
		},
	}, nil
}

func (c *clientOffsets) Close() { c.transport.CloseIdleConnections() }

// partitionRange is the retained part of one partition's log: first is the
// oldest offset still on the broker, end the offset the next record will get.
type partitionRange struct{ first, end int64 }

// groupLag is how many records a consumer group still has to commit on a topic:
// the sum over its partitions of end minus the group's committed offset. That
// includes records fetched but still being handled, which is what the group
// would redeliver if every runner died now.
//
// committed maps a partition to the group's committed offset, negative when it
// has none. Such a partition counts from where the group's reader would start:
// the first retained offset, or the end when the group starts from the latest.
// A committed offset behind the first retained one (retention deleted records
// the group never read) is taken as it is, as kafka-consumer-groups does: the
// group is that far behind until its reader resets and commits again.
func groupLag(ranges map[int]partitionRange, committed map[int]int64, fromLatest bool) int64 {
	var lag int64
	for p, r := range ranges {
		pos, ok := committed[p]
		if !ok || pos < 0 {
			pos = r.first
			if fromLatest {
				pos = r.end
			}
		}
		lag += max(r.end-pos, 0)
	}
	return lag
}

// retained is the number of records a topic holds right now: the sum over its
// partitions of end minus first.
func retained(ranges map[int]partitionRange) int64 {
	var n int64
	for _, r := range ranges {
		n += max(r.end-r.first, 0)
	}
	return n
}

// lagSampler reads the offsets of the deployment's topics and reports consumer
// group lag and dead-letter depth to a queue.KafkaStatsObserver.
type lagSampler struct {
	client offsetReader
	names  topicNames
	obs    queue.KafkaStatsObserver
	// fromLatest mirrors Config.StartFromLatest: where a group with no committed
	// offset begins reading.
	fromLatest bool
	warn       func(msg string, args ...any)
}

// run samples at once and then every interval until ctx ends. The interval is
// the point: this is an alerting signal read in minutes, and a round is several
// broker requests.
func (s *lagSampler) run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		s.sample(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// sample reads and reports one round. Each topic stands alone: one that cannot be
// read yields no sample while the others are still reported, and a failed read
// is never reported as zero, which would paint a backlog-free dashboard during a
// broker outage.
func (s *lagSampler) sample(ctx context.Context) {
	topics := s.names.All()
	ranges, err := s.ranges(ctx, topics)
	if err != nil {
		s.fail(ctx, err)
		return
	}
	for _, c := range []struct{ role, topic string }{
		{groupDefault, s.names.Default()},
		{groupBatch, s.names.Batch()},
		{groupDelay, s.names.Delay()},
	} {
		r, ok := ranges[c.topic]
		if !ok {
			continue
		}
		committed, err := s.committed(ctx, s.names.Group(c.role), c.topic, r)
		if err != nil {
			s.fail(ctx, err, "topic", c.topic)
			continue
		}
		lag := groupLag(r, committed, s.fromLatest)
		safely(func() { s.obs.OnKafkaLag(c.role, lag) })
	}
	if r, ok := ranges[s.names.DLQ()]; ok {
		depth := retained(r)
		safely(func() { s.obs.OnKafkaDLQDepth(depth) })
	}
}

// fail logs a failed read, unless the consumer is stopping: a call cancelled by
// the stop is not a broker fault.
func (s *lagSampler) fail(ctx context.Context, err error, args ...any) {
	if ctx.Err() != nil {
		return
	}
	s.warn("kafka lag sample failed", append(args, "error", err)...)
}

// safely isolates an observer call: the sampler runs on a background goroutine,
// where a panicking observer would take the process down.
func safely(call func()) {
	defer func() { _ = recover() }()
	call()
}

// ranges returns the retained offset range of every partition of each topic that
// could be read in full. A topic the broker reports an error for, or one missing
// a partition's offsets, is left out; the error is for a failed call as a whole.
func (s *lagSampler) ranges(ctx context.Context, topics []string) (map[string]map[int]partitionRange, error) {
	meta, err := s.client.Metadata(ctx, &kafkago.MetadataRequest{Topics: topics})
	if err != nil {
		return nil, fmt.Errorf("describe topics: %w", err)
	}
	req := make(map[string][]kafkago.OffsetRequest, len(meta.Topics))
	for _, tp := range meta.Topics {
		if tp.Error != nil || len(tp.Partitions) == 0 {
			s.warn("kafka lag sample skipped an unusable topic", "topic", tp.Name, "error", tp.Error)
			continue
		}
		for _, p := range tp.Partitions {
			req[tp.Name] = append(req[tp.Name], kafkago.FirstOffsetOf(p.ID), kafkago.LastOffsetOf(p.ID))
		}
	}
	if len(req) == 0 {
		return nil, nil
	}
	res, err := s.client.ListOffsets(ctx, &kafkago.ListOffsetsRequest{Topics: req})
	if err != nil {
		return nil, fmt.Errorf("list offsets: %w", err)
	}
	out := make(map[string]map[int]partitionRange, len(req))
	for topic, reqs := range req {
		parts := res.Topics[topic]
		got := make(map[int]partitionRange, len(parts))
		// kafka-go answers every partition it was asked about; a response that
		// does not is incomplete, and a sum over part of the partitions would
		// under-report.
		healthy := len(parts)*2 == len(reqs)
		for _, po := range parts {
			// An offset is never negative; kafka-go leaves -1 where the broker
			// gave no answer.
			if po.Error != nil || po.FirstOffset < 0 || po.LastOffset < 0 {
				healthy = false
				break
			}
			got[po.Partition] = partitionRange{first: po.FirstOffset, end: po.LastOffset}
		}
		if !healthy {
			s.warn("kafka lag sample skipped a topic with unreadable offsets", "topic", topic)
			continue
		}
		out[topic] = got
	}
	return out, nil
}

// committed fetches the offsets group has committed on topic's partitions. A
// partition without a commit is left out of the result.
func (s *lagSampler) committed(ctx context.Context, group, topic string, ranges map[int]partitionRange) (map[int]int64, error) {
	parts := make([]int, 0, len(ranges))
	for p := range ranges {
		parts = append(parts, p)
	}
	res, err := s.client.OffsetFetch(ctx, &kafkago.OffsetFetchRequest{GroupID: group, Topics: map[string][]int{topic: parts}})
	if err != nil {
		return nil, fmt.Errorf("fetch committed offsets: %w", err)
	}
	if res.Error != nil {
		return nil, fmt.Errorf("fetch committed offsets: %w", res.Error)
	}
	out := make(map[int]int64, len(parts))
	var errs []error
	for _, p := range res.Topics[topic] {
		if p.Error != nil {
			errs = append(errs, fmt.Errorf("partition %d: %w", p.Partition, p.Error))
			continue
		}
		out[p.Partition] = p.CommittedOffset
	}
	if err := errors.Join(errs...); err != nil {
		return nil, fmt.Errorf("fetch committed offsets: %w", err)
	}
	return out, nil
}
