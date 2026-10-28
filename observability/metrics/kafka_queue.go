package metrics

import "time"

// Kafka task-transport metric names. They are registered lazily, on the first
// observation, so a deployment that keeps the default Asynq transport never
// creates the series: nothing calls the adapter below unless the Kafka
// transport was selected and wired to it.
const (
	// metricKafkaProduceErrors counts failed producer-side enqueues on the Kafka
	// transport, partitioned by a bounded kind.
	metricKafkaProduceErrors = "xflow_kafka_produce_errors_total"
	// metricKafkaLag is the uncommitted backlog of a consumed topic's consumer
	// group, partitioned by role.
	metricKafkaLag = "xflow_kafka_lag"
	// metricKafkaDLQDepth is the number of records the dead-letter topic holds.
	metricKafkaDLQDepth = "xflow_kafka_dlq_depth"
)

// Kafka produce-error kinds. The set is closed: KafkaQueueMetrics maps anything
// else to kafkaKindUnknown, so a transport that gains a kind before this table
// does cannot widen the label's cardinality. They mirror the vocabulary of the
// Kafka transport (backend/providers/distributed/internal/queue/kafka), which
// this package must not import; a test in the distributed package keeps the two
// lists equal.
const (
	kafkaKindQueueFull = "queue_full"
	kafkaKindRetryable = "retryable"
	kafkaKindPermanent = "permanent"
	kafkaKindCaller    = "caller"
	kafkaKindOther     = "other"
	kafkaKindUnknown   = "unknown"
)

// kafkaProduceKinds is every value of the kind label this adapter can emit.
var kafkaProduceKinds = map[string]struct{}{
	kafkaKindQueueFull: {},
	kafkaKindRetryable: {},
	kafkaKindPermanent: {},
	kafkaKindCaller:    {},
	kafkaKindOther:     {},
}

// KafkaProduceKinds returns the values the kind label of
// xflow_kafka_produce_errors_total can take: the transport's kinds plus
// "unknown" for a kind this table does not know. It exists so the distributed
// package can check the two vocabularies against each other.
func KafkaProduceKinds() []string {
	return []string{kafkaKindQueueFull, kafkaKindRetryable, kafkaKindPermanent, kafkaKindCaller, kafkaKindOther, kafkaKindUnknown}
}

// Kafka lag roles: the consumed topics, named after their consumer groups. The
// set is closed like the kinds above: KafkaQueueMetrics maps anything else to
// kafkaKindUnknown. A test in the distributed package keeps this list equal to
// the transport's.
const (
	kafkaRoleDefault = "default"
	kafkaRoleBatch   = "batch"
	kafkaRoleDelay   = "delay"
)

// kafkaLagRoles is every value of the role label this adapter can emit.
var kafkaLagRoles = map[string]struct{}{
	kafkaRoleDefault: {},
	kafkaRoleBatch:   {},
	kafkaRoleDelay:   {},
}

// KafkaLagRoles returns the values the role label of xflow_kafka_lag can take:
// the transport's roles plus "unknown" for one this table does not know. It
// exists so the distributed package can check the two vocabularies against each
// other.
func KafkaLagRoles() []string {
	return []string{kafkaRoleDefault, kafkaRoleBatch, kafkaRoleDelay, kafkaKindUnknown}
}

// kafkaQueueObserver mirrors the observers of the distributed backend
// (queue.Observer, queue.ProduceErrorObserver and queue.KafkaStatsObserver via
// its aliases). It is declared with primitive parameters so this package does
// not import backend providers.
type kafkaQueueObserver interface {
	OnEnqueue(op string, elapsed time.Duration, err error)
	OnProduceError(kind string)
	OnKafkaLag(role string, lag int64)
	OnKafkaDLQDepth(depth int64)
}

// KafkaQueueMetrics observes the Kafka task transport: producer-side enqueue
// failures, and the consumer group lag and dead-letter depth its consumer
// samples. Install it with distributed.WithQueueObserver only when the Kafka
// transport is selected; the default deployment must not register it.
//
// Only failures are counted, and only through OnProduceError, which carries the
// kind the transport itself classified. OnEnqueue is deliberately empty: it is
// the Asynq-shared hook, handed the raw error, and deriving a label from that
// error here would re-implement the transport's classification (or worse, label
// by error text).
type KafkaQueueMetrics struct {
	Metrics *Metrics
}

// NewKafkaQueueMetrics creates a Kafka produce-error observer backed by Metrics.
func NewKafkaQueueMetrics(metrics *Metrics) KafkaQueueMetrics {
	return KafkaQueueMetrics{Metrics: metrics}
}

// OnEnqueue is a no-op; see KafkaQueueMetrics.
func (KafkaQueueMetrics) OnEnqueue(string, time.Duration, error) {}

// OnProduceError counts one failed enqueue under its kind. No namespace label:
// the transport is per deployment, like the queue-depth series, and a kind is
// all an alert on broker trouble needs. A success is never reported here, so a
// healthy transport exports no series at all.
func (k KafkaQueueMetrics) OnProduceError(kind string) {
	if _, ok := kafkaProduceKinds[kind]; !ok {
		kind = kafkaKindUnknown
	}
	k.Metrics.Inc(metricKafkaProduceErrors, map[string]string{"kind": kind})
}

// OnKafkaLag sets the lag of one consumed topic's group. The gauge is written by
// every runner of the deployment, all with the same value, so the last write
// wins harmlessly. A role outside the closed set is counted under "unknown"
// rather than minting a series. No series exists until the first sample: a
// sample that could not be read is never reported, so an absent series means
// "not measured", not zero.
func (k KafkaQueueMetrics) OnKafkaLag(role string, lag int64) {
	if _, ok := kafkaLagRoles[role]; !ok {
		role = kafkaKindUnknown
	}
	k.Metrics.Set(metricKafkaLag, map[string]string{"role": role}, float64(lag))
}

// OnKafkaDLQDepth sets the number of records the dead-letter topic holds.
func (k KafkaQueueMetrics) OnKafkaDLQDepth(depth int64) {
	k.Metrics.Set(metricKafkaDLQDepth, nil, float64(depth))
}

var _ kafkaQueueObserver = KafkaQueueMetrics{}
