// Package queue defines the technology-neutral task transport abstraction used
// by distributed backends. It lets the queue technology (Asynq, Redis Streams,
// NATS, Kafka, ...) be swapped without touching the engine or the distributed
// backend wiring: a broker only has to implement Transport.
package queue

import (
	"context"
	"time"

	"github.com/xbcio/xflow/engine"
)

// Observer receives producer-side enqueue outcomes. Implementations must be
// non-blocking; a Transport calls these on the enqueue hot path. A nil observer
// is permitted (transports substitute a no-op). It mirrors the audit/lease
// observer pattern and is technology-neutral so metrics code does not depend on
// a concrete broker.
type Observer interface {
	// OnEnqueue records an enqueue attempt for the named operation
	// ("enqueue" or "enqueue_delayed"). err is nil on success.
	OnEnqueue(op string, elapsed time.Duration, err error)
}

// TaskHandler processes a single dequeued task. It is supplied by the backend
// (typically the embedded execution dispatcher) and invoked by a Transport's
// consumer loop for every delivered task.
type TaskHandler func(ctx context.Context, t *engine.Task) error

// StatsObserver receives periodic queue-depth samples from a consumer's
// queues. It is the broker-side half of the residency signal: it reports a
// backlog while tasks are still queued, where the consumer-side delivery-lag
// histogram can only report what it has already consumed. Without it, a
// completely stalled consumer is invisible until tasks start dropping.
//
// The parameters are primitives rather than a struct because the interface must
// be implementable outside this internal package: the distributed backend
// re-exports it as an alias and the metrics adapter lives in
// observability/metrics, which must not import backend providers.
type StatsObserver interface {
	// OnQueueStats reports one sample of the named queue: the pending, active
	// and retry task counts and the age of the oldest pending task (how long
	// the head of the queue has been waiting). Implementations must be
	// non-blocking; a sample that fails to read is simply not reported, so a
	// missing sample means "not measured", never "zero".
	OnQueueStats(queue string, pending, active, retry int, oldestPendingAge time.Duration)
}

// DefaultStatsInterval is how often a consumer samples its queues when
// ConsumerConfig.StatsInterval is unset. It is deliberately slow: this is an
// alerting signal read in minutes, and on a shared Redis the sample is real
// load that must stay negligible next to task traffic.
const DefaultStatsInterval = 30 * time.Second

// ConsumerConfig configures a Transport's consumer loop.
type ConsumerConfig struct {
	// Concurrency is the number of tasks a consumer may process in parallel.
	Concurrency int

	// Transient selects fire-and-forget delivery semantics: a failed task is
	// dropped instead of retried. Each Transport translates this into its own
	// broker-native policy (e.g. Asynq's SkipRetry). Default/durable mode
	// (false) keeps retryable failures retryable.
	Transient bool

	// StatsObserver, when non-nil, receives a periodic sample of the queues
	// this consumer reads. A nil observer disables sampling entirely; a
	// transport that cannot measure its queues never calls it.
	StatsObserver StatsObserver

	// StatsInterval overrides DefaultStatsInterval. Values <= 0 mean the
	// default.
	StatsInterval time.Duration
}

// Transport is the pluggable task transport: it both produces (enqueues) and
// consumes tasks. It abstracts the broker so the distributed backend depends
// only on this interface, never on a concrete queue technology.
//
// Producer methods come from engine.TaskQueue (Enqueue / EnqueueDelayed).
// StartConsumer runs the worker side; API-only instances that disable the
// consumer never call it.
type Transport interface {
	engine.TaskQueue

	// StartConsumer starts delivering tasks to handler until the returned stop
	// function is called. Implementations should run the consumer loop
	// asynchronously and return stop for graceful shutdown. It is only invoked
	// on instances that consume tasks.
	StartConsumer(cfg ConsumerConfig, handler TaskHandler) (stop func(), err error)

	// Close releases producer-side resources (connections, clients). It is safe
	// to call on API-only instances that never started a consumer.
	Close() error
}
