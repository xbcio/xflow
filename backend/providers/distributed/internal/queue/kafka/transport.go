package kafka

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"sync"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"

	"github.com/xbcio/xflow/backend/providers/distributed/internal/queue"
	"github.com/xbcio/xflow/engine"
)

// Producer defaults, applied when the matching Config field is zero or negative.
const (
	// defaultBatchBytes is the writer's batch size limit. It also bounds the
	// largest single record the writer will send, so it must stay at or below the
	// topic's max.message.bytes; otherwise the broker rejects a batch the writer
	// considered acceptable.
	defaultBatchBytes int64 = 8 << 20
	// defaultWriteTimeout bounds one produce round trip to a broker.
	defaultWriteTimeout = 10 * time.Second
	// defaultMaxAttempts is how many times the writer retries inside a single
	// WriteMessages call before it reports the failure.
	defaultMaxAttempts = 5
	// writerBatchTimeout is how long the writer waits to fill a batch. Writes are
	// synchronous, so this is latency added to every produce that does not fill a
	// batch by itself.
	writerBatchTimeout = 5 * time.Millisecond
	// writerMetadataTTL is how long the writer's transport caches topic metadata.
	// kafka-go defaults to 6s; a shorter TTL lets the writer see a new partition
	// leader sooner after a failover, at the cost of more metadata requests.
	writerMetadataTTL = 3 * time.Second
	// defaultMaxEnqueueDelay is the delay EnqueueDelayed accepts at most. The
	// engine's own delays stay under 6 minutes, so this leaves room above them.
	defaultMaxEnqueueDelay = 15 * time.Minute
	// minMaxEnqueueDelay and maxMaxEnqueueDelay bound Config.MaxEnqueueDelay. The
	// floor is the engine's longest retry delay: its backoff is capped at 5
	// minutes and then gets up to 20% jitter, which stays below 6 minutes. A
	// limit under that would reject the engine's own retries.
	minMaxEnqueueDelay = 6 * time.Minute
	maxMaxEnqueueDelay = time.Hour
)

// Config configures the Kafka transport's producer. The topic names are not
// configurable: they derive from the deployment suffix passed to New.
type Config struct {
	// Brokers is the bootstrap broker list ("host:port"). Required.
	Brokers []string
	// TLS, when non-nil, encrypts broker connections. Nil means plaintext.
	TLS *tls.Config
	// SASLMechanism selects authentication: "", "plain", "scram-sha-256" or
	// "scram-sha-512". Empty disables SASL.
	SASLMechanism string
	// SASLUsername and SASLPassword are required when SASLMechanism is set.
	SASLUsername, SASLPassword string
	// BatchBytes is the writer's batch size limit. Default 8 MiB. It must stay
	// at or below the topic's max.message.bytes.
	BatchBytes int64
	// WriteTimeout bounds one produce round trip. Default 10s.
	WriteTimeout time.Duration
	// MaxAttempts is how many times the writer retries inside one WriteMessages
	// call. Default 5.
	MaxAttempts int
	// MaxEnqueueDelay is the longest delay EnqueueDelayed accepts. A longer one
	// is rejected with ErrDelayTooLong instead of being parked on the delay
	// topic: the relay waits out each message in arrival order, so a far-off due
	// time would hold up every message behind it. Zero selects the default, 15m;
	// a negative value is an error. It must lie between 6m and 1h, because the
	// engine's retry delays reach almost 6m (see minMaxEnqueueDelay).
	MaxEnqueueDelay time.Duration

	// The fields below configure the consumer. Zero or negative values select
	// the default.

	// SessionTimeout is how long the group coordinator waits for a heartbeat
	// before it evicts a member. Default 30s.
	SessionTimeout time.Duration
	// HeartbeatInterval is how often a member heartbeats. It runs on its own
	// goroutine, so a long task never delays it. Default 3s.
	HeartbeatInterval time.Duration
	// RebalanceTimeout is how long the coordinator waits for members to rejoin
	// during a rebalance. It should not be shorter than a typical task, because
	// a member that is still running one cannot rejoin. Default 60s.
	RebalanceTimeout time.Duration
	// FetchMaxBytes is the reader's MaxBytes, the largest fetch response. It must
	// hold at least one whole record. Default BatchBytes, or 8 MiB if that is
	// unset.
	FetchMaxBytes int64
	// MaxRetries is how many redeliveries a failing task gets before it is
	// dead-lettered. Default 25, the same as Asynq.
	MaxRetries int
	// MaxPending bounds, per consumed topic, the messages fetched but not yet
	// covered by a committable offset: in flight, plus finished behind a slower
	// earlier message. Default 1024.
	MaxPending int
	// HandlerTimeout bounds one handler call. A handler that overruns it has its
	// context cancelled, and the task is retried like any other failure. Default
	// 30m, the same as Asynq's, which applies it to tasks enqueued without a
	// timeout.
	HandlerTimeout time.Duration
	// ShutdownTimeout is how long stopping the consumer waits for running
	// handlers before it cancels their context. Default 30s.
	ShutdownTimeout time.Duration
	// MaxRetryBackoff caps the delay before a failed task is redelivered.
	// Default 5m.
	MaxRetryBackoff time.Duration
	// StartFromLatest makes a consumer group that has no committed offset start
	// at the end of each partition. The default starts at the beginning, so tasks
	// produced before the first consumer joined are not lost.
	StartFromLatest bool

	// The fields below are used only by Transport.EnsureTopics. They have no
	// defaults: a topic's partition count and replication factor depend on the
	// cluster (a single broker cannot hold three replicas), so the transport
	// does not guess them.

	// CreateTopics makes EnsureTopics create the topics that are missing. When
	// false, EnsureTopics only reports them. Existing topics are never changed.
	CreateTopics bool
	// Partitions is the partition count of a topic EnsureTopics creates. Required
	// when CreateTopics is true, and an error otherwise.
	Partitions int
	// ReplicationFactor is the replica count of a topic EnsureTopics creates.
	// Required when CreateTopics is true, and an error otherwise.
	ReplicationFactor int
}

// Option configures a Transport at construction time.
type Option func(*Transport)

// WithObserver installs a producer-side enqueue observer. It composes with the
// distributed backend's WithQueueObserver option.
func WithObserver(obs queue.Observer) Option {
	return func(t *Transport) {
		if obs != nil {
			t.observer = obs
		}
	}
}

// WithLogger installs the logger used for construction-time warnings.
func WithLogger(l engine.Logger) Option {
	return func(t *Transport) { t.logger = l }
}

// withWriter replaces the Kafka writer. It lets tests run the producer against
// a fake without a broker.
func withWriter(w messageWriter) Option {
	return func(t *Transport) { t.writer = w }
}

// withReaderFactory replaces the consumer-group reader factory. It lets tests
// run the consumer against fake readers without a broker.
func withReaderFactory(f readerFactory) Option {
	return func(t *Transport) { t.newReader = f }
}

// messageWriter is the part of *kafkago.Writer the transport uses.
type messageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafkago.Message) error
	Close() error
}

// messageReader is the part of *kafkago.Reader the consumer uses.
type messageReader interface {
	FetchMessage(ctx context.Context) (kafkago.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafkago.Message) error
	Close() error
}

// readerFactory builds the group reader of one topic.
type readerFactory func(topic, groupID string) messageReader

// Transport is the Kafka task transport. It owns one lazily connecting writer
// that serves every topic of the deployment: each message names its own topic.
type Transport struct {
	writer    messageWriter
	newReader readerFactory
	admin     topicAdmin
	offsets   offsetReader
	cfg       Config
	names     topicNames
	observer  queue.Observer
	logger    engine.Logger

	// pause sleeps between retries of a failed fetch or publish; it reports
	// false when ctx ended first. waitUntil holds the delay relay until a
	// message is due, with the same result. Both are fields so tests can run
	// the retry and relay paths without real waiting.
	pause     func(ctx context.Context, d time.Duration) bool
	waitUntil func(ctx context.Context, until time.Time) bool

	closeOnce sync.Once
	closeErr  error
}

var (
	_ engine.TaskQueue            = (*Transport)(nil)
	_ engine.NonBlockingTaskQueue = (*Transport)(nil)
)

// New builds a Kafka transport for the deployment identified by
// deploymentSuffix (namespace.DeploymentSuffix; empty for the default
// deployment). It does not contact a broker: the writer connects on first use.
func New(cfg Config, deploymentSuffix string, opts ...Option) (*Transport, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	names, err := newTopicNames(deploymentSuffix)
	if err != nil {
		return nil, err
	}
	t := &Transport{
		names:     names,
		observer:  noopObserver{},
		cfg:       cfg.withConsumerDefaults(),
		pause:     sleepFor,
		waitUntil: sleepUntil,
	}
	for _, o := range opts {
		o(t)
	}
	if t.logger != nil && suffixCollisionRisk(deploymentSuffix) {
		t.logger.Warn("kafka namespace suffix contains '.' or '_'; topics that differ only in those "+
			"characters share Kafka metric names",
			"suffix", deploymentSuffix)
	}
	if t.newReader == nil {
		// Build one reader config now so a bad SASL setting fails here rather
		// than when the consumer starts, even if the writer was injected.
		if _, err := readerConfig(t.cfg, t.names.Default(), t.names.Group(groupDefault)); err != nil {
			return nil, err
		}
		t.newReader = groupReaderFactory(t.cfg)
	}
	if t.admin == nil {
		a, err := newClientAdmin(cfg)
		if err != nil {
			return nil, err
		}
		t.admin = a
	}
	if t.offsets == nil {
		o, err := newClientOffsets(cfg)
		if err != nil {
			return nil, err
		}
		t.offsets = o
	}
	if t.writer == nil {
		w, err := newWriter(cfg)
		if err != nil {
			return nil, err
		}
		t.writer = transportWriter{w}
	}
	return t, nil
}

// newWriter builds the shared producer. The topic is left unset on purpose:
// every message carries its own, which is what lets one writer (and one set of
// broker connections) serve the default, batch, delay and DLQ topics.
//
// Delivery settings: RequireAll waits for the full in-sync replica set, topics
// are never auto-created (a missing topic is a deployment error, not something
// to paper over), and writes are synchronous so a returned nil means the broker
// acknowledged the record. The Hash balancer keeps all records with one key on
// one partition, which is what orders an execution's tasks. kafka-go has no
// idempotent producer, so a retry after an acknowledgement lost in flight can
// duplicate a record; consumers are already at-least-once.
func newWriter(cfg Config) (*kafkago.Writer, error) {
	mechanism, err := buildSASLMechanism(cfg.SASLMechanism, cfg.SASLUsername, cfg.SASLPassword)
	if err != nil {
		return nil, err
	}
	maxAttempts := cfg.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = defaultMaxAttempts
	}
	return &kafkago.Writer{
		Addr:                   kafkago.TCP(cfg.Brokers...),
		Balancer:               &kafkago.Hash{},
		RequiredAcks:           kafkago.RequireAll,
		AllowAutoTopicCreation: false,
		Async:                  false,
		BatchTimeout:           writerBatchTimeout,
		BatchBytes:             cfg.batchBytes(),
		MaxAttempts:            maxAttempts,
		WriteTimeout:           cfg.writeTimeout(),
		Transport:              &kafkago.Transport{TLS: cfg.TLS, SASL: mechanism, MetadataTTL: writerMetadataTTL},
	}, nil
}

// transportWriter adapts *kafkago.Writer to messageWriter. kafka-go closes the
// idle connections only of a transport it created itself, and newWriter hands
// the writer its own, so Close has to release them explicitly.
type transportWriter struct{ *kafkago.Writer }

func (w transportWriter) Close() error {
	err := w.Writer.Close()
	if tr, ok := w.Transport.(*kafkago.Transport); ok {
		tr.CloseIdleConnections()
	}
	return err
}

// buildSASLMechanism constructs the kafka-go SASL mechanism for a Config. An
// empty mechanism means no SASL. It mirrors the trigger consumer's builder,
// which is unexported in another package.
func buildSASLMechanism(mechanism, username, password string) (sasl.Mechanism, error) {
	if mechanism == "" {
		return nil, nil
	}
	if username == "" || password == "" {
		return nil, fmt.Errorf("kafka: sasl mechanism %q requires a username and a password", mechanism)
	}
	switch strings.ToLower(mechanism) {
	case "plain":
		return &plain.Mechanism{Username: username, Password: password}, nil
	case "scram-sha-256":
		m, err := scram.Mechanism(scram.SHA256, username, password)
		if err != nil {
			return nil, fmt.Errorf("kafka sasl scram-sha-256: %w", err)
		}
		return m, nil
	case "scram-sha-512":
		m, err := scram.Mechanism(scram.SHA512, username, password)
		if err != nil {
			return nil, fmt.Errorf("kafka sasl scram-sha-512: %w", err)
		}
		return m, nil
	default:
		return nil, fmt.Errorf("kafka: unsupported sasl_mechanism %q (supported: plain, scram-sha-256, scram-sha-512)", mechanism)
	}
}

// publish writes messages straight to the broker and returns the writer's error
// unclassified. The consumer's retry, relay and dead-letter paths use it: they
// decide for themselves what a failed produce means.
func (t *Transport) publish(ctx context.Context, msgs ...kafkago.Message) error {
	return t.writer.WriteMessages(ctx, msgs...)
}

// Close releases the writer, the topic admin, the lag sampler's client and their
// broker connections. It is idempotent: every call returns the result of the
// first.
func (t *Transport) Close() error {
	t.closeOnce.Do(func() {
		t.closeErr = t.writer.Close()
		t.admin.Close()
		t.offsets.Close()
	})
	return t.closeErr
}

// noopObserver is the default enqueue observer; it performs no work.
type noopObserver struct{}

func (noopObserver) OnEnqueue(string, time.Duration, error) {}
