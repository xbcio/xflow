package kafka

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/xbcio/xflow/backend/providers/distributed/internal/queue"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// Consumer defaults, applied when the matching Config field is zero or negative.
const (
	defaultSessionTimeout    = 30 * time.Second
	defaultHeartbeatInterval = 3 * time.Second
	defaultRebalanceTimeout  = 60 * time.Second
	// defaultMaxRetries matches Asynq's default of 25 retries.
	defaultMaxRetries = 25
	defaultMaxPending = 1024
	// defaultHandlerTimeout is Asynq's defaultTimeout (asynq v0.26.0,
	// client.go), which it applies to a task enqueued without Timeout or
	// Deadline. The Asynq transport here sets neither.
	defaultHandlerTimeout  = 30 * time.Minute
	defaultShutdownTimeout = 30 * time.Second
	defaultMaxRetryBackoff = 5 * time.Minute

	// dialTimeout bounds one broker connection attempt of a reader.
	dialTimeout = 10 * time.Second
	// fetchMaxWait is how long the broker may hold a fetch open waiting for data.
	fetchMaxWait = 500 * time.Millisecond
	// partitionWatchInterval is how often a reader polls its topic's partition
	// count.
	partitionWatchInterval = 10 * time.Second
)

// withConsumerDefaults returns c with every unset consumer field replaced by
// its default. It is idempotent.
func (c Config) withConsumerDefaults() Config {
	if c.SessionTimeout <= 0 {
		c.SessionTimeout = defaultSessionTimeout
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = defaultHeartbeatInterval
	}
	if c.RebalanceTimeout <= 0 {
		c.RebalanceTimeout = defaultRebalanceTimeout
	}
	if c.FetchMaxBytes <= 0 {
		c.FetchMaxBytes = c.batchBytes()
	}
	if c.MaxRetries <= 0 {
		c.MaxRetries = defaultMaxRetries
	}
	if c.MaxPending <= 0 {
		c.MaxPending = defaultMaxPending
	}
	if c.HandlerTimeout <= 0 {
		c.HandlerTimeout = defaultHandlerTimeout
	}
	if c.ShutdownTimeout <= 0 {
		c.ShutdownTimeout = defaultShutdownTimeout
	}
	if c.MaxRetryBackoff <= 0 {
		c.MaxRetryBackoff = defaultMaxRetryBackoff
	}
	return c
}

// readerConfig builds the kafka-go ReaderConfig of one consumer group member.
//
// Offsets are committed synchronously (CommitInterval 0): the committer
// goroutine, not the caller, absorbs the wait, and it is what keeps the
// committed position monotonic. Only the Range balancer is offered, so runners
// on different versions cannot pick different balancers for the same group and
// disagree about the assignment. Each topic has its own group, so Range does
// not line partitions up across topics.
func readerConfig(cfg Config, topic, groupID string) (kafkago.ReaderConfig, error) {
	cfg = cfg.withConsumerDefaults()
	mechanism, err := buildSASLMechanism(cfg.SASLMechanism, cfg.SASLUsername, cfg.SASLPassword)
	if err != nil {
		return kafkago.ReaderConfig{}, err
	}
	start := kafkago.FirstOffset
	if cfg.StartFromLatest {
		start = kafkago.LastOffset
	}
	return kafkago.ReaderConfig{
		Brokers: cfg.Brokers,
		GroupID: groupID,
		Topic:   topic,
		Dialer: &kafkago.Dialer{
			Timeout:       dialTimeout,
			DualStack:     true,
			TLS:           cfg.TLS,
			SASLMechanism: mechanism,
		},
		MinBytes: 1,
		MaxBytes: int(cfg.FetchMaxBytes),
		MaxWait:  fetchMaxWait,
		// The watcher rebalances the group when the partition count of an
		// existing topic changes, so partitions added later get assigned.
		WatchPartitionChanges:  true,
		PartitionWatchInterval: partitionWatchInterval,
		CommitInterval:         0,
		StartOffset:            start,
		SessionTimeout:         cfg.SessionTimeout,
		HeartbeatInterval:      cfg.HeartbeatInterval,
		RebalanceTimeout:       cfg.RebalanceTimeout,
		GroupBalancers:         []kafkago.GroupBalancer{kafkago.RangeGroupBalancer{}},
	}, nil
}

// newGroupReader builds the real reader of one topic. kafka.NewReader starts a
// background goroutine that joins the group right away, so the member is in the
// group from this call on, before the first fetch.
func newGroupReader(cfg Config, topic, groupID string) (messageReader, error) {
	rc, err := readerConfig(cfg, topic, groupID)
	if err != nil {
		return nil, err
	}
	return kafkago.NewReader(rc), nil
}

// groupReaderFactory adapts newGroupReader to a readerFactory. New has already
// validated the one input that can fail (the SASL settings), so the error branch
// is unreachable in practice; it still yields a reader that reports the error
// instead of panicking.
func groupReaderFactory(cfg Config) readerFactory {
	return func(topic, groupID string) messageReader {
		r, err := newGroupReader(cfg, topic, groupID)
		if err != nil {
			return failedReader{err}
		}
		return r
	}
}

// failedReader is a messageReader that cannot be built. Every fetch and commit
// returns the construction error.
type failedReader struct{ err error }

func (r failedReader) FetchMessage(context.Context) (kafkago.Message, error) {
	return kafkago.Message{}, r.err
}

func (r failedReader) CommitMessages(context.Context, ...kafkago.Message) error { return r.err }

func (failedReader) Close() error { return nil }

// sleepFor waits d and reports false if ctx ended first.
func sleepFor(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// sleepUntil waits until the given time and reports false if ctx ended first.
// A time that has already passed returns at once.
func sleepUntil(ctx context.Context, until time.Time) bool {
	return sleepFor(ctx, time.Until(until))
}

var _ queue.Transport = (*Transport)(nil)

// Bounds of the pause between retries of a failed fetch or publish. The pause
// doubles from the first to the second and stays there.
const (
	retryPauseMin = 100 * time.Millisecond
	retryPauseMax = 5 * time.Second
)

// workerSplit divides cfg.Concurrency between the default and batch topics.
//
// Asynq weights its two queues 8:1; Kafka has no queue weights, so the batch
// topic gets one worker in nine and the default topic the rest. Each side gets
// at least one worker, which means Concurrency 1 runs two workers in total: a
// topic with no worker would never drain. A Concurrency of zero or less counts
// as 1.
func workerSplit(concurrency int) (defaultWorkers, batchWorkers int) {
	n := max(concurrency, 1)
	batchWorkers = max(1, n/9)
	defaultWorkers = max(1, n-batchWorkers)
	return defaultWorkers, batchWorkers
}

// backoff is the delay before redelivery number attempt: one second doubled for
// each attempt after the first, capped at limit. An attempt of 1 or less waits
// one second.
func backoff(attempt int, limit time.Duration) time.Duration {
	d := time.Second
	for i := 1; i < attempt; i++ {
		// d*2 would reach the cap, and checking first also keeps it from
		// overflowing, however large attempt is.
		if d > limit/2 {
			return limit
		}
		d *= 2
	}
	return min(d, limit)
}

// warn logs through the transport's logger, if it has one.
func (t *Transport) warn(msg string, args ...any) {
	if t.logger != nil {
		t.logger.Warn(msg, args...)
	}
}

// logError logs at error level through the transport's logger, if it has one.
func (t *Transport) logError(msg string, args ...any) {
	if t.logger != nil {
		t.logger.Error(msg, args...)
	}
}

// stopGrace is how long stop waits for handlers to return after it has
// cancelled their context. A handler that ignores its context would otherwise
// block stop forever; Asynq's Shutdown likewise gives up on running tasks.
const stopGrace = 5 * time.Second

// StartConsumer starts the three consumer-group readers of the deployment: the
// default topic, the batch topic and the delay relay. It returns at once; the
// readers join their groups and connect in the background.
//
// Concurrency is the total worker count, split by workerSplit between the
// default and batch topics. The delay relay is one extra serial goroutine.
//
// Offsets are committed by a continuous-completion watermark (see tracker): a
// message counts as done once its handler returned, a retry or dead-letter copy
// was published, or it was dropped. A message that is in flight when the
// consumer stops is never committed, so the group redelivers it; lease fencing
// absorbs the duplicate.
//
// cfg.StatsObserver is ignored and never called. The queue contract allows a
// transport that cannot measure its queues to skip it, and Kafka's lag and age
// do not mean what Asynq's pending and oldest-pending-age mean: lag counts
// records the group has not committed, per partition, and a retry is a record
// on the delay topic rather than a retry-set entry. Mapping them onto one gauge
// would make alerts configured for Asynq lie. Kafka's own numbers go to the
// transport's observer instead, when it implements queue.KafkaStatsObserver: a
// sampler samples consumer group lag and the dead-letter topic's depth every
// cfg.StatsInterval (queue.DefaultStatsInterval if unset) until stop is called.
// Every runner of a deployment samples the same groups, so they report the same
// numbers; an instance that never starts a consumer does not sample at all.
//
// The returned stop function is idempotent. It stops the lag sampler and waits
// for it, stops fetching, waits up to Config.ShutdownTimeout for running
// handlers, cancels their context if they are still running, flushes the
// committable offsets and closes the readers.
func (t *Transport) StartConsumer(cfg queue.ConsumerConfig, handler queue.TaskHandler) (func(), error) {
	if handler == nil {
		return nil, errors.New("kafka: a consumer needs a handler")
	}
	defaultWorkers, batchWorkers := workerSplit(cfg.Concurrency)

	c := &consumer{t: t, handler: handler, transient: cfg.Transient}
	c.fetchCtx, c.fetchCancel = context.WithCancel(context.Background())
	c.workCtx, c.workCancel = context.WithCancel(context.Background())
	c.commitCtx, c.commitCancel = context.WithCancel(context.Background())

	c.startLoop(t.names.Default(), t.names.Group(groupDefault), defaultWorkers)
	c.startLoop(t.names.Batch(), t.names.Group(groupBatch), batchWorkers)
	c.startRelay()
	if obs, ok := t.observer.(queue.KafkaStatsObserver); ok {
		c.startLagSampler(obs, cfg.StatsInterval)
	}
	return c.stop, nil
}

// consumer is the running state of one StartConsumer call.
type consumer struct {
	t         *Transport
	handler   queue.TaskHandler
	transient bool

	// The three contexts end in this order on stop: fetching, then the handlers,
	// then the committers. Each stage must outlive the one before it.
	fetchCtx, workCtx, commitCtx          context.Context
	fetchCancel, workCancel, commitCancel context.CancelFunc

	// fetchers covers the fetch loops and the relay, workers the handler
	// goroutines, committers the committer goroutines, sampler the lag sampler.
	fetchers, workers, committers, sampler sync.WaitGroup
	// samplerCancel ends the lag sampler; nil when none was started.
	samplerCancel context.CancelFunc

	readers  []messageReader
	stopOnce sync.Once
}

// startLoop starts the reader, tracker, committer, fetch loop and workers of
// one default or batch topic.
func (c *consumer) startLoop(topic, group string, workers int) {
	reader := c.t.newReader(topic, group)
	c.readers = append(c.readers, reader)
	l := &topicLoop{
		t:         c.t,
		topic:     topic,
		reader:    reader,
		handler:   c.handler,
		transient: c.transient,
		tracker:   newTracker(),
		committer: c.newCommitter(topic, reader),
		work:      make(chan work),
	}
	c.committers.Go(func() { l.committer.Run(c.commitCtx) })
	c.fetchers.Go(func() { l.fetchLoop(c.fetchCtx) })
	for range workers {
		c.workers.Go(func() { l.worker(c.workCtx) })
	}
}

// startRelay starts the delay topic's reader, committer and relay loop.
func (c *consumer) startRelay() {
	topic := c.t.names.Delay()
	reader := c.t.newReader(topic, c.t.names.Group(groupDelay))
	c.readers = append(c.readers, reader)
	cm := c.newCommitter(topic, reader)
	c.committers.Go(func() { cm.Run(c.commitCtx) })
	c.fetchers.Go(func() { c.t.runRelay(c.fetchCtx, reader, cm) })
}

// startLagSampler starts the goroutine that reports Kafka lag and dead-letter
// depth to obs every interval, the first sample at once. Its context is its own,
// not fetchCtx: a broker call the sampler is waiting on must end when stop
// begins, but sampling has nothing to do with the order in which fetching,
// handlers and committers wind down.
func (c *consumer) startLagSampler(obs queue.KafkaStatsObserver, interval time.Duration) {
	if interval <= 0 {
		interval = queue.DefaultStatsInterval
	}
	s := &lagSampler{
		client:     c.t.offsets,
		names:      c.t.names,
		obs:        obs,
		fromLatest: c.t.cfg.StartFromLatest,
		warn:       c.t.warn,
	}
	var ctx context.Context
	ctx, c.samplerCancel = context.WithCancel(context.Background())
	c.sampler.Go(func() { s.run(ctx, interval) })
}

// newCommitter builds the committer of one reader. A failed commit is only
// logged: the committer keeps the offset and retries it.
func (c *consumer) newCommitter(topic string, reader messageReader) *committer {
	return newCommitter(topic, reader.CommitMessages, func(err error) {
		c.t.warn("kafka offset commit failed", "topic", topic, "error", err)
	}, 0, 0)
}

// stop shuts the consumer down in dependency order.
func (c *consumer) stop() {
	c.stopOnce.Do(func() {
		if c.samplerCancel != nil {
			c.samplerCancel()
			c.sampler.Wait()
		}
		c.fetchCancel()
		c.fetchers.Wait()
		if !waitTimeout(&c.workers, c.t.cfg.ShutdownTimeout) {
			c.workCancel()
			if !waitTimeout(&c.workers, stopGrace) {
				c.t.warn("kafka consumer stopped with handlers still running")
			}
		}
		c.workCancel()
		c.commitCancel()
		c.committers.Wait()
		for _, r := range c.readers {
			if err := r.Close(); err != nil {
				c.t.warn("kafka reader close failed", "error", err)
			}
		}
	})
}

// waitTimeout waits for wg and reports false if it is still running after d.
func waitTimeout(wg *sync.WaitGroup, d time.Duration) bool {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}

// work is one fetched message handed to a worker, with the tracker token that
// completes it.
type work struct {
	msg kafkago.Message
	tk  token
}

// topicLoop is the consuming side of one default or batch topic: one reader,
// the tracker and committer that turn finished messages into commits, and the
// channel that carries fetched messages to the workers.
type topicLoop struct {
	t         *Transport
	topic     string
	reader    messageReader
	handler   queue.TaskHandler
	transient bool
	tracker   *tracker
	committer *committer
	work      chan work
}

// fetchLoop reads messages and hands them to the workers until ctx ends or the
// reader is closed. It closes the work channel on the way out, which is what
// lets the workers return once they are idle.
//
// A fetch error other than a closed reader is retried after a growing pause. The
// kafka-go reader already reconnects on its own, so an error that reaches here
// is either a bug in the broker setup or a rebalance in progress, and neither
// improves by spinning.
func (l *topicLoop) fetchLoop(ctx context.Context) {
	defer close(l.work)
	pause := retryPauseMin
	for {
		if l.tracker.WaitBelow(ctx, l.t.cfg.MaxPending) != nil {
			return
		}
		msg, err := l.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, io.EOF) {
				return
			}
			l.t.warn("kafka fetch failed", "topic", l.topic, "error", err)
			if !l.t.pause(ctx, pause) {
				return
			}
			pause = min(pause*2, retryPauseMax)
			continue
		}
		pause = retryPauseMin
		tk := l.tracker.Begin(msg.Partition, msg.Offset)
		select {
		case l.work <- work{msg: msg, tk: tk}:
		case <-ctx.Done():
			return
		}
	}
}

// worker processes messages until the work channel is closed and drained. ctx is
// the handlers' context: it ends only when stop gave up waiting for them.
func (l *topicLoop) worker(ctx context.Context) {
	for w := range l.work {
		if l.process(ctx, w.msg) {
			l.complete(w.tk)
		}
	}
}

// complete marks a message done and, if that moved the partition's watermark,
// hands the new commit position to the committer.
func (l *topicLoop) complete(tk token) {
	if offset, advanced := l.tracker.Done(tk); advanced {
		l.committer.Advance(tk.partition, offset)
	}
}

// process handles one message and reports whether it is finished, meaning its
// offset may be committed. It is not finished only when a retry or dead-letter
// copy could not be published before ctx ended; the message is then redelivered
// and lease fencing absorbs the repeat.
//
// The error policy is Asynq's (../asynq/retry.go): an undecodable message or a
// types.ErrPermanent error is not retried, Transient mode drops a failed task,
// and any other error is retried until MaxRetries is used up.
func (l *topicLoop) process(ctx context.Context, msg kafkago.Message) bool {
	task, ns, err := decodeTask(msg)
	if err != nil {
		return l.deadLetter(ctx, msg, fmt.Errorf("decode task: %w", err))
	}
	err = l.invoke(namespace.WithNamespace(ctx, ns), task)
	switch {
	case err == nil:
		return true
	case l.transient:
		return true
	case errors.Is(err, types.ErrPermanent):
		return l.deadLetter(ctx, msg, err)
	}
	return l.retry(ctx, msg, err)
}

// invoke runs the handler under Config.HandlerTimeout and turns a panic into a
// retryable error. The panic value is left out of the error: it can carry task
// data, and the message ends up in a log line and a dead-letter header.
//
// A handler that overruns the timeout has its context cancelled. If it then
// fails, the error is replaced by a plain timeout error, so the task is retried
// whatever the handler returned. A cancelled parent ctx is not a timeout: stop
// gave up waiting, and the message stays uncommitted.
func (l *topicLoop) invoke(ctx context.Context, task *engine.Task) (err error) {
	timeout := l.t.cfg.HandlerTimeout
	tctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("kafka: task handler panic (%T)", r)
		}
	}()
	err = l.handler(tctx, task)
	if err != nil && ctx.Err() == nil && errors.Is(tctx.Err(), context.DeadlineExceeded) {
		err = fmt.Errorf("kafka: task handler timed out after %s: %v", timeout, err)
	}
	return err
}

// retry parks the message on the delay topic for its next attempt, or
// dead-letters it once attempts run out. The delay relay moves it back to the
// topic named in hdrTarget when it is due.
func (l *topicLoop) retry(ctx context.Context, msg kafkago.Message, cause error) bool {
	attempt := attemptOf(msg) + 1
	if attempt > l.t.cfg.MaxRetries {
		return l.deadLetter(ctx, msg, cause)
	}
	target, _ := headerValue(msg, hdrTarget)
	target = cmp.Or(target, msg.Topic, l.topic)
	due := time.Now().Add(backoff(attempt, l.t.cfg.MaxRetryBackoff))

	headers := setHeader(msg.Headers, hdrAttempt, strconv.Itoa(attempt))
	headers = setHeader(headers, hdrNotBefore, strconv.FormatInt(due.UnixMilli(), 10))
	headers = setHeader(headers, hdrTarget, target)
	return l.t.publishCopy(ctx, kafkago.Message{
		Topic:   l.t.names.Delay(),
		Key:     msg.Key,
		Value:   msg.Value,
		Headers: headers,
	}, msg, l.topic, "retry copy too large: "+cause.Error())
}

// deadLetter writes the message to the dead-letter topic.
func (l *topicLoop) deadLetter(ctx context.Context, msg kafkago.Message, cause error) bool {
	return l.t.deadLetterMessage(ctx, msg, l.topic, cause.Error())
}

// dlqMessage builds the dead-letter copy of msg. fallbackSource names the topic
// the message came from when the fetched message does not say.
func (t *Transport) dlqMessage(msg kafkago.Message, fallbackSource, cause string) kafkago.Message {
	headers := setHeader(msg.Headers, hdrSource, cmp.Or(msg.Topic, fallbackSource))
	headers = setHeader(headers, hdrError, truncateUTF8(cause, maxErrorHeaderBytes))
	return kafkago.Message{
		Topic:   t.names.DLQ(),
		Key:     msg.Key,
		Value:   msg.Value,
		Headers: headers,
	}
}

// publishResult is how publishRetrying ended.
type publishResult int

const (
	// publishWritten: the broker accepted the message.
	publishWritten publishResult = iota
	// publishCancelled: ctx ended first. The message was not written and the
	// caller must not commit.
	publishCancelled
	// publishTooLarge: the message exceeds a size limit, so writing it again
	// can never succeed.
	publishTooLarge
)

// publishRetrying writes msg, retrying a failed write after a growing pause.
//
// Almost every failure is retried until ctx ends, on purpose. A missing topic,
// an authorization error or a broker outage is a deployment problem, and a
// message that cannot be parked or dead-lettered must not be committed:
// committing it would lose the task. The worker stays on it and logs each
// failure, which backs the topic up where an operator can see it. A message
// that is too large is the one failure that is about the message itself and
// cannot clear, so it is reported to the caller instead.
func (t *Transport) publishRetrying(ctx context.Context, msg kafkago.Message) publishResult {
	pause := retryPauseMin
	for {
		if ctx.Err() != nil {
			return publishCancelled
		}
		err := t.publish(ctx, msg)
		if err == nil {
			return publishWritten
		}
		if ctx.Err() != nil {
			return publishCancelled
		}
		if isMessageTooLarge(err) {
			return publishTooLarge
		}
		t.warn("kafka publish failed; retrying", "topic", msg.Topic, "error", err)
		if !t.pause(ctx, pause) {
			return publishCancelled
		}
		pause = min(pause*2, retryPauseMax)
	}
}

// minimalDLQBytes caps hdrError on the reduced dead-letter copy.
const minimalDLQBytes = 128

// publishCopy writes a retry or forwarded copy of a consumed message. A copy
// that turns out too large goes to the dead-letter topic instead, with cause
// saying why. It reports whether the message is finished and may be committed.
func (t *Transport) publishCopy(ctx context.Context, out, orig kafkago.Message, source, cause string) bool {
	switch t.publishRetrying(ctx, out) {
	case publishWritten:
		return true
	case publishTooLarge:
		return t.deadLetterMessage(ctx, orig, source, cause)
	}
	return false
}

// deadLetterMessage writes the dead-letter copy of msg. If the broker refuses it
// as too large, one smaller copy is tried: key, value and the source and error
// headers only. If that is refused too, the message cannot be kept anywhere. It
// is logged without its value and abandoned, which reports it finished: a
// message no topic will take would otherwise hold its partition forever.
func (t *Transport) deadLetterMessage(ctx context.Context, msg kafkago.Message, source, cause string) bool {
	dlq := t.dlqMessage(msg, source, cause)
	switch t.publishRetrying(ctx, dlq) {
	case publishWritten:
		return true
	case publishCancelled:
		return false
	}
	reduced := kafkago.Message{Topic: dlq.Topic, Key: msg.Key, Value: msg.Value}
	reduced.Headers = setHeader(nil, hdrSource, cmp.Or(msg.Topic, source))
	reduced.Headers = setHeader(reduced.Headers, hdrError, truncateUTF8(cause, minimalDLQBytes))
	switch t.publishRetrying(ctx, reduced) {
	case publishWritten:
		return true
	case publishCancelled:
		return false
	}
	t.logError("kafka message too large for the dead-letter topic; dropped",
		"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "key", string(msg.Key))
	return true
}
