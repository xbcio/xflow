package distributed

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/xbcio/xflow/backend/providers/distributed/internal/queue"
	kafkaqueue "github.com/xbcio/xflow/backend/providers/distributed/internal/queue/kafka"
)

// kafkaEnsureTimeout bounds the startup check of the Kafka topics in New.
const kafkaEnsureTimeout = 30 * time.Second

// KafkaConfig configures the optional Kafka task transport selected with
// WithKafkaQueue. It is an alias for the internal transport's Config, so callers
// can fill its fields (brokers, TLS, SASL, producer and consumer limits, topic
// provisioning) without a second copy of the struct that could drift from it.
// Topic names are not configurable: they derive from the deployment identity.
type KafkaConfig = kafkaqueue.Config

// WithKafkaQueue replaces the default Asynq task transport with Kafka. Without
// this option the backend uses Asynq, exactly as before.
//
// Redis stays the scheduling authority: execution state, the durable outbox and
// task leases all remain in Redis, and Kafka only carries the tasks. A Redis
// address (or WithRedisConfig) is therefore still required. Every process of one
// deployment must pick the same transport, and tasks already waiting in Asynq
// queues are not moved to Kafka when a running deployment switches over; drain
// them first.
//
// New builds the transport and then checks that the deployment's four topics
// (default, batch, delay, dlq; suffixed per deployment like the Asynq queues)
// exist and are usable, within 30 seconds. API-only instances
// (WithConsumer(false)) run the same check because they produce tasks too. When
// KafkaConfig.CreateTopics is set, missing topics are created, and then
// Partitions and ReplicationFactor must be set explicitly: there are no hidden
// defaults. Otherwise the topics must exist before the process starts, and a
// missing one fails New. Any failure here, including an invalid KafkaConfig or
// an unreachable broker, makes New return an error and release everything it
// had opened.
//
// Disable the broker's auto.create.topics.enable, or create the topics up front.
// The consumer's metadata requests allow auto-creation, so with it enabled a
// topic that goes missing at runtime could be created by the broker with its
// own defaults instead of failing loudly.
//
// Interaction with other options:
//   - WithQueueObserver applies: it observes Kafka produce outcomes. An
//     observer that also implements QueueProduceErrorObserver is told the kind
//     of each failure (queue_full, retryable, permanent, caller, other); the
//     server wiring uses it for xflow_kafka_produce_errors_total.
//   - WithConcurrency still sets the number of consumer workers, split between
//     the default and batch topics.
//   - WithQueueStatsObserver has no effect. Kafka consumer lag counts records
//     the group has not committed, per partition, and a retry is a record on the
//     delay topic rather than a retry-set entry, so neither maps onto the
//     pending/active/retry/oldest-age samples that alerts built for Asynq
//     expect. The transport never calls that observer; no sample is emitted,
//     which reads as "not measured", not as an empty queue.
//   - An observer given to WithQueueObserver that also implements
//     QueueKafkaStatsObserver is told, by every runner that consumes, every
//     WithQueueStatsInterval (30s if unset): the lag of the default, batch and
//     delay consumer groups, and the number of records on the dlq topic. The
//     server wiring uses it for xflow_kafka_lag and xflow_kafka_dlq_depth. A
//     round that cannot be read is skipped, never reported as zero.
//   - WithTransport is mutually exclusive: giving both makes New return an
//     error.
//
// Differences from Asynq: tasks the consumer gives up on are published to the
// dlq topic, not to Asynq's archive, so Asynq tooling (asynq CLI, asynqmon)
// sees nothing. The engine's dead-letter store lives in the state store and is
// unaffected. Calling this option again replaces the earlier configuration.
//
// Operations notes:
//   - A task that fails before it takes its lease and lands on the dlq topic is
//     not redelivered by anything: the execution stays running until someone
//     reruns it. In v1 the dlq topic is write-only, so inspect it with Kafka's
//     own tools.
//   - The LeaseSweeper runs only inside the control plane (ControlPlane.Start).
//     An embedded NewCluster stack has no background sweeper, so a task that
//     fails after it took its lease is not recovered automatically there either.
//   - EnqueueDelayed rejects a delay above KafkaConfig.MaxEnqueueDelay (default
//     15 minutes) with an error that names the field and is not
//     engine.ErrQueueFull, instead of parking it on the delay topic, whose relay
//     waits out messages in arrival order. The engine's own delays stay under 6
//     minutes.
func WithKafkaQueue(cfg KafkaConfig) Option {
	return func(c *config) {
		// Copy the broker list so the caller changing its slice later cannot alter
		// a transport that has not been built yet.
		cfg.Brokers = slices.Clone(cfg.Brokers)
		c.kafkaQueue = &cfg
	}
}

// validateKafkaSelection rejects an unusable Kafka selection before any
// connection is opened: the option cannot be combined with an injected
// transport, and the configuration itself must be valid.
func validateKafkaSelection(cfg *config) error {
	if cfg.kafkaQueue == nil {
		return nil
	}
	if cfg.transport != nil {
		return errors.New("distributed backend: WithKafkaQueue and WithTransport are mutually exclusive; " +
			"WithTransport injects a custom transport, WithKafkaQueue selects the built-in Kafka one")
	}
	if err := cfg.kafkaQueue.Validate(); err != nil {
		return fmt.Errorf("kafka queue: %w", err)
	}
	return nil
}

// buildKafkaTransport is newKafkaTransport behind a variable so tests can run
// the Kafka selection path of New without a broker. Production code never
// reassigns it.
var buildKafkaTransport = newKafkaTransport

// newKafkaTransport builds the Kafka transport for the deployment and verifies
// its topics. On any failure the transport is closed before the error is
// returned, so the caller only has to release what it opened itself.
func newKafkaTransport(cfg *config, deploymentSuffix string) (queue.Transport, error) {
	var opts []kafkaqueue.Option
	if cfg.queueObserver != nil {
		opts = append(opts, kafkaqueue.WithObserver(cfg.queueObserver))
	}
	if cfg.logger != nil {
		opts = append(opts, kafkaqueue.WithLogger(cfg.logger))
	}
	t, err := kafkaqueue.New(*cfg.kafkaQueue, deploymentSuffix, opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka queue: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), kafkaEnsureTimeout)
	defer cancel()
	if err := t.EnsureTopics(ctx); err != nil {
		if closeErr := t.Close(); closeErr != nil && cfg.logger != nil {
			cfg.logger.Warn("distributed backend: closing kafka transport after failed topic check", "error", closeErr)
		}
		return nil, fmt.Errorf("kafka queue: %w", err)
	}
	return t, nil
}
