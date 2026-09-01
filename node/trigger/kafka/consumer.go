package kafka

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
)

const (
	consumerRetryDelay     = 100 * time.Millisecond
	consumerCloseGraceTime = 250 * time.Millisecond
)

// kafkaConnTracker makes kafka-go Reader shutdown interruptible. kafka-go
// v0.4.49 uses background contexts for parts of its consumer-group protocol,
// so Reader.Close can otherwise wait for a lost broker until a network
// deadline. All sockets created by this reader pass through the tracker; after
// a short graceful-close window, closing them interrupts those protocol calls.
type kafkaConnTracker struct {
	dial func(context.Context, string, string) (net.Conn, error)

	ctx    context.Context
	cancel context.CancelFunc

	mu     sync.Mutex
	closed bool
	epoch  uint64
	conns  map[*trackedKafkaConn]struct{}
}

type trackedKafkaConn struct {
	net.Conn
	owner *kafkaConnTracker
	once  sync.Once
}

func newKafkaConnTracker(dialer *kafkago.Dialer) *kafkaConnTracker {
	dial := dialer.DialFunc
	if dial == nil {
		netDialer := &net.Dialer{
			LocalAddr:     dialer.LocalAddr,
			DualStack:     dialer.DualStack,
			FallbackDelay: dialer.FallbackDelay,
			KeepAlive:     dialer.KeepAlive,
		}
		dial = netDialer.DialContext
	}
	ctx, cancel := context.WithCancel(context.Background())
	tracker := &kafkaConnTracker{
		dial:   dial,
		ctx:    ctx,
		cancel: cancel,
		conns:  make(map[*trackedKafkaConn]struct{}),
	}
	dialer.DialFunc = tracker.dialContext
	return tracker
}

func (t *kafkaConnTracker) dialContext(ctx context.Context, network, address string) (net.Conn, error) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, net.ErrClosed
	}
	trackerCtx := t.ctx
	epoch := t.epoch
	t.mu.Unlock()

	dialCtx, cancel := context.WithCancel(ctx)
	stopCancel := context.AfterFunc(trackerCtx, cancel)
	conn, err := t.dial(dialCtx, network, address)
	stopCancel()
	cancel()
	if err != nil {
		return nil, err
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed || t.epoch != epoch {
		_ = conn.Close()
		return nil, net.ErrClosed
	}
	tracked := &trackedKafkaConn{Conn: conn, owner: t}
	t.conns[tracked] = struct{}{}
	return tracked, nil
}

// interrupt cancels the current dial generation and closes every socket it
// produced, but deliberately leaves the tracker open for a subsequent dial.
// kafka-go's Reader.Close needs that second generation to send LeaveGroup
// after its blocked fetch has been interrupted; permanently closing the dialer
// here strands the old member until SessionTimeout and stalls an immediate
// same-group replacement.
func (t *kafkaConnTracker) interrupt() {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return
	}
	oldCancel := t.cancel
	t.ctx, t.cancel = context.WithCancel(context.Background())
	t.epoch++
	conns := make([]*trackedKafkaConn, 0, len(t.conns))
	for conn := range t.conns {
		conns = append(conns, conn)
	}
	t.mu.Unlock()

	oldCancel()
	for _, conn := range conns {
		_ = conn.Close()
	}
}

func (t *kafkaConnTracker) close() {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return
	}
	t.closed = true
	t.epoch++
	conns := make([]*trackedKafkaConn, 0, len(t.conns))
	for conn := range t.conns {
		conns = append(conns, conn)
	}
	t.mu.Unlock()

	// Cancel first to interrupt dials which have not produced a socket yet.
	t.cancel()
	for _, conn := range conns {
		_ = conn.Close()
	}
}

func (c *trackedKafkaConn) Close() error {
	var err error
	c.once.Do(func() {
		c.owner.mu.Lock()
		delete(c.owner.conns, c)
		c.owner.mu.Unlock()
		err = c.Conn.Close()
	})
	return err
}

type kafkaGoConsumer struct {
	reader      *kafkago.Reader
	connections *kafkaConnTracker

	ctx    context.Context
	cancel context.CancelFunc

	messages chan Message
	done     chan struct{}

	closeOnce sync.Once
	closeErr  error
}

func newKafkaGoConsumer(cfg ConsumerConfig) (Consumer, error) {
	readerCfg, err := readerConfigFor(cfg)
	if err != nil {
		return nil, err
	}
	connections := newKafkaConnTracker(readerCfg.Dialer)
	ctx, cancel := context.WithCancel(context.Background())
	consumer := &kafkaGoConsumer{
		reader:      kafkago.NewReader(readerCfg),
		connections: connections,
		ctx:         ctx,
		cancel:      cancel,
		messages:    make(chan Message, readerCfg.QueueCapacity),
		done:        make(chan struct{}),
	}
	go consumer.run()
	return consumer, nil
}

// readerConfigFor builds the kafka-go reader config, applying cfg.Tuning over
// the values that used to be inlined literals here.
//
// It returns an error rather than letting kafka-go decide: NewReader panics on
// a config its Validate rejects, and these values now come from operator-edited
// params, so a typo must fail this trigger's activation instead of taking the
// whole runner process down.
func readerConfigFor(cfg ConsumerConfig) (kafkago.ReaderConfig, error) {
	startOffset, err := startOffsetFor(cfg.StartOffset)
	if err != nil {
		return kafkago.ReaderConfig{}, err
	}
	tuning := cfg.Tuning
	if tuning.isZero() {
		tuning = defaultTuning()
	}
	if err := tuning.validate(); err != nil {
		return kafkago.ReaderConfig{}, err
	}
	queueCapacity := cfg.MaxInflight
	if queueCapacity <= 0 {
		queueCapacity = defaultTriggerMaxInflight
	}

	// The dialer is built unconditionally. It used to exist only on the SASL
	// path, which left dial_timeout with nowhere to land: kafka-go substituted
	// DefaultDialer and the configured value vanished with no diagnostic.
	dialer := &kafkago.Dialer{
		Timeout:   tuning.DialTimeout,
		DualStack: true,
	}
	if cfg.SASLMechanism != "" {
		mechanism, saslErr := buildSASLMechanism(cfg.SASLMechanism, cfg.SASLUsername, cfg.SASLPassword)
		if saslErr != nil {
			return kafkago.ReaderConfig{}, saslErr
		}
		dialer.SASLMechanism = mechanism
	}

	return kafkago.ReaderConfig{
		Brokers:                cfg.Brokers,
		GroupID:                cfg.Group,
		Topic:                  cfg.Topic,
		StartOffset:            startOffset,
		QueueCapacity:          queueCapacity,
		MinBytes:               tuning.FetchMinBytes,
		MaxBytes:               tuning.FetchMaxBytes,
		MaxWait:                tuning.MaxWait,
		WatchPartitionChanges:  true,
		PartitionWatchInterval: 5 * time.Second,
		ReadLagInterval:        -1,
		RebalanceTimeout:       tuning.RebalanceTimeout,
		SessionTimeout:         tuning.SessionTimeout,
		HeartbeatInterval:      tuning.HeartbeatInterval,
		JoinGroupBackoff:       time.Second,
		RetentionTime:          24 * time.Hour,
		OffsetOutOfRangeError:  false,
		ReadBackoffMin:         100 * time.Millisecond,
		ReadBackoffMax:         time.Second,
		// Synchronous commit. A periodic commit would let an offset land before
		// the side effect it belongs to succeeded, which is exactly the loss the
		// per-partition emit-then-commit worker exists to prevent. Not a knob.
		CommitInterval: 0,
		GroupBalancers: []kafkago.GroupBalancer{kafkago.RangeGroupBalancer{}, kafkago.RoundRobinGroupBalancer{}},
		Dialer:         dialer,
	}, nil
}

// buildSASLMechanism constructs the appropriate kafka-go SASL mechanism.
func buildSASLMechanism(mechanism, username, password string) (sasl.Mechanism, error) {
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

func startOffsetFor(offset string) (int64, error) {
	switch strings.ToLower(strings.TrimSpace(offset)) {
	case "", "latest", "last", "newest":
		return kafkago.LastOffset, nil
	case "earliest", "first", "oldest", "beginning":
		return kafkago.FirstOffset, nil
	default:
		return 0, fmt.Errorf("kafka start_offset %q is not supported", offset)
	}
}

func (c *kafkaGoConsumer) Messages() <-chan Message { return c.messages }

func (c *kafkaGoConsumer) Close() error {
	c.closeOnce.Do(func() {
		c.cancel()

		readerClosed := make(chan error, 1)
		go func() { readerClosed <- c.reader.Close() }()
		timer := time.NewTimer(consumerCloseGraceTime)
		select {
		case c.closeErr = <-readerClosed:
			if !timer.Stop() {
				<-timer.C
			}
		case <-timer.C:
			// Reader.Close has no context in kafka-go v0.4.49. Force its
			// current fetch/dial generation out of network waits. Keep the
			// tracker open until Reader.Close has sent LeaveGroup on a fresh
			// connection, then close it permanently below.
			c.connections.interrupt()
			c.closeErr = <-readerClosed
		}
		c.connections.close()
		<-c.done
	})
	return c.closeErr
}

func (c *kafkaGoConsumer) CommitMessages(ctx context.Context, messages ...Message) error {
	commits := make([]kafkago.Message, 0, len(messages))
	for _, msg := range messages {
		commits = append(commits, kafkago.Message{
			Topic:     msg.Topic,
			Partition: msg.Partition,
			Offset:    msg.Offset,
		})
	}
	return c.reader.CommitMessages(ctx, commits...)
}

func (c *kafkaGoConsumer) run() {
	defer close(c.done)
	defer close(c.messages)
	lag := newLagSampler(consumerLagSampleInterval)
	for {
		msg, err := c.reader.FetchMessage(c.ctx)
		if err != nil {
			if c.ctx.Err() != nil || errors.Is(err, io.EOF) {
				return
			}
			if !sleepConsumerRetry(c.ctx) {
				return
			}
			continue
		}
		// Sampled here rather than from a separate poller because the fetched
		// message already carries the high-water mark: lag costs no broker round
		// trip, and this one call site covers every runtime that consumes from
		// this channel.
		now := time.Now()
		if behind, report := lag.sample(msg, now); report {
			obs().OnConsumerLag(c.ctx, msg.Topic, msg.Partition, behind, now)
		}
		select {
		case c.messages <- messageFromReader(msg):
		case <-c.ctx.Done():
			return
		}
	}
}

// consumerLagSampleInterval bounds how often lag is reported per partition.
//
// The sample itself is free, but the report is not: the observer resolves a
// gauge by label set, and doing that per message would put a map lookup and a
// label-slice comparison on the ingest path at Kafka rates. A gauge is a level,
// not an event stream — a scrape only ever sees the last value written within
// its interval, so sampling faster than the scrape produces work nobody reads.
const consumerLagSampleInterval = time.Second

// lagSampler throttles lag reporting per partition. It is not synchronized:
// the only caller is kafkaGoConsumer.run, which is a single goroutine.
type lagSampler struct {
	interval time.Duration
	last     map[int]time.Time
}

func newLagSampler(interval time.Duration) *lagSampler {
	return &lagSampler{interval: interval, last: make(map[int]time.Time)}
}

// sample returns how far behind the high-water mark this message sits, and
// whether that reading is due to be reported for its partition.
//
// The first message from a partition always reports, so a newly assigned
// partition that is far behind is visible immediately rather than one interval
// later. Partitions throttle independently — a busy partition must not consume
// a quiet one's budget, because the quiet one is the one whose lag is
// interesting.
func (s *lagSampler) sample(msg kafkago.Message, now time.Time) (int64, bool) {
	// A fetched message occupies an offset, so the high-water mark is at least
	// offset+1 and cannot be zero. Zero means kafka-go did not populate it;
	// reporting the resulting negative number as lag would be worse than
	// reporting nothing.
	if msg.HighWaterMark <= 0 {
		return 0, false
	}
	if last, seen := s.last[msg.Partition]; seen && now.Sub(last) < s.interval {
		return 0, false
	}
	s.last[msg.Partition] = now
	behind := msg.HighWaterMark - msg.Offset - 1
	if behind < 0 {
		behind = 0
	}
	return behind, true
}

func sleepConsumerRetry(ctx context.Context) bool {
	timer := time.NewTimer(consumerRetryDelay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func messageFromReader(msg kafkago.Message) Message {
	headers := make(map[string]string, len(msg.Headers))
	for _, header := range msg.Headers {
		headers[header.Key] = string(header.Value)
	}
	return Message{
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Key:       msg.Key,
		Value:     msg.Value,
		Time:      msg.Time,
		Headers:   headers,
	}
}
