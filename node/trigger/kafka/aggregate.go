package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/xbcio/xflow/node/internal/utils/conv"
	"github.com/spf13/cast"

	"github.com/xbcio/xflow/types"
)

const defaultAggregateMaxSize = 100
const defaultAggregateFlushInterval = 100 * time.Millisecond

// maxBufferedBatches bounds a partition's in-memory buffer at a multiple of
// MaxSize. It exists because a failed flush now holds its messages for a timed
// retry instead of re-running them on every arrival, so a downstream that stays
// broken would otherwise grow the buffer without limit.
//
// Overflow drops the ARRIVING message, which is safe precisely because its
// offset was never committed: Kafka redelivers from the lowest uncommitted
// offset, which is the retained head of this buffer, so a dropped message comes
// back. Dropping the newest rather than the oldest is what keeps the retained
// run contiguous — the same reasoning the idle path already relies on.
const maxBufferedBatches = 4

// defaultEntrySeedFlushInterval is the flush timeout for the entry-seed
// batch path. It is 10x the legacy Emit default because entry-seed flushes cross
// the network to the control plane, and batching exists precisely to cut those
// round trips — a 100ms window would make 10 of them per second per partition.
//
// The legacy default is deliberately left alone: changing it would silently
// alter the data-path timing of deployments already running.
const defaultEntrySeedFlushInterval = time.Second
const aggregateByPartition = "partition"
const aggregateDedupMessage = "message"

// AggregateConfig configures batch aggregation. Reachable from outside the
// package for the first time — Node.Aggregate takes it, and before the split
// the type was exported from an internal package with no re-export, so the
// method could not be called at all from outside.
type AggregateConfig struct {
	Enabled       bool
	By            string
	MaxSize       int
	FlushInterval time.Duration
	Dedup         string
}

// defaultFlushIntervalFor returns the mode-aware flush interval default.
// This only takes effect on the YAML/JSON params path; the Go DSL path
// normalizes at construction time before the mode is known — see the NOTE in
// RawParams for the documented limitation.
func defaultFlushIntervalFor(entrySeed bool) time.Duration {
	if entrySeed {
		return defaultEntrySeedFlushInterval
	}
	return defaultAggregateFlushInterval
}

type aggregateRuntime struct {
	in          *types.TriggerActivateInput
	cfg         AggregateConfig
	consumer    Consumer
	emitSem     chan struct{}
	mu          sync.Mutex
	closeOnce   sync.Once
	aggregators map[partitionKey]*partitionAggregator
	// messageSchema validates each message BEFORE it enters a batch. An earlier
	// revision omitted this field entirely, so an aggregate-mode trigger that
	// declared message_schema had it silently ignored — validation existed only
	// on the per-message path. Filtering pre-batch (rather than post-flush) is
	// what keeps one malformed record from invalidating a whole batch.
	messageSchema *MessageSchema
	// entrySeed selects the entry-unit seed admission path over the legacy Emit
	// path when a batch flushes. Set once at activation (see isEntrySeedActivation).
	entrySeed bool
	// deadLetterPublisher is non-nil only when messageSchema.OnInvalid is
	// dead_letter. Owned by this runtime: closed by close().
	deadLetterPublisher DeadLetterPublisher
}

func (r *aggregateRuntime) schema() *MessageSchema { return r.messageSchema }

func (r *aggregateRuntime) deadLetters() DeadLetterPublisher {
	return r.deadLetterPublisher
}

type partitionAggregator struct {
	key         partitionKey
	rt          *aggregateRuntime
	ch          chan Message
	done        chan struct{}
	idleTimeout time.Duration
}

func activateAggregate(ctx context.Context, in *types.TriggerActivateInput, cfg ConsumerConfig, consumer Consumer, deadLetters DeadLetterPublisher) types.TriggerSubscription {
	runCtx, cancel := context.WithCancel(ctx)
	rt := &aggregateRuntime{
		in:                  in,
		cfg:                 cfg.Aggregate,
		consumer:            consumer,
		emitSem:             make(chan struct{}, cfg.MaxInflight),
		aggregators:         make(map[partitionKey]*partitionAggregator),
		messageSchema:       cfg.MessageSchema,
		entrySeed:           isEntrySeedActivation(in),
		deadLetterPublisher: deadLetters,
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer rt.close(context.Background())
		for {
			select {
			case <-runCtx.Done():
				return
			case msg, ok := <-consumer.Messages():
				if !ok {
					return
				}
				if !rt.submit(runCtx, msg) {
					return
				}
			}
		}
	}()
	return types.CloseFunc(func(closeCtx context.Context) error {
		// Cancel first so any submit blocked on a full worker/aggregator channel
		// wakes immediately rather than waiting for consumer.Close to drain it.
		cancel()
		err := consumer.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
		}
		rt.close(closeCtx)
		return err
	})
}

// submit hands a message to its partition's aggregator, retrying against a
// fresh one if the aggregator self-terminates while the send is blocked.
//
// The retry is load-bearing rather than defensive, and the deadlock it prevents
// is permanent. A send blocks whenever the aggregator is inside a flush, because
// flush runs on the aggregator's own goroutine and so does not drain agg.ch
// (cap = MaxSize) meanwhile; at the live topic's measured 329 msg/s per
// partition the 100-slot channel fills in 0.3s, while one flush can take up to
// the group execution deadline plus the admission timeout. If the aggregator
// then reaps itself on its idle timer, it closes done and evictAggregator drops
// it from the map — and without the done case here, this send waits forever on a
// channel nobody will ever read again, never returning to aggregator() to pick
// up the replacement.
//
// The caller is the SINGLE goroutine reading consumer.Messages() for every
// partition, so one stuck send stops consumption for the whole assignment. That
// is what froze scenario A's pipeline at t+50s with no recovery: consumed sat at
// 0.3% of produced while lag climbed linearly, and goroutines and heap both
// SETTLED rather than growing — the signature of work having stopped, not of
// resources being exhausted.
//
// done is closed after evictAggregator runs (run's defers are LIFO), so the
// aggregator() call on the next iteration cannot hand back the corpse.
func (r *aggregateRuntime) submit(ctx context.Context, msg Message) bool {
	key := partitionKey{topic: msg.Topic, partition: msg.Partition}
	for {
		// Checked before aggregator() rather than relying on the select alone: with
		// both done and ctx.Done() ready, select picks either, and taking the done
		// branch during shutdown would spawn a replacement aggregator after
		// rt.close had already drained the set.
		if ctx.Err() != nil {
			return false
		}
		agg := r.aggregator(key)
		select {
		case agg.ch <- msg:
			return true
		case <-agg.done:
			// Aggregator reaped itself; loop to spawn its replacement.
		case <-ctx.Done():
			return false
		}
	}
}

func (r *aggregateRuntime) aggregator(key partitionKey) *partitionAggregator {
	r.mu.Lock()
	defer r.mu.Unlock()
	agg, ok := r.aggregators[key]
	if ok {
		return agg
	}
	agg = &partitionAggregator{
		key:         key,
		rt:          r,
		ch:          make(chan Message, r.cfg.MaxSize),
		done:        make(chan struct{}),
		idleTimeout: aggregatorIdleTimeout(r.cfg.FlushInterval),
	}
	r.aggregators[key] = agg
	go agg.run()
	return agg
}

// evictAggregator removes an idle aggregator that self-terminated. kafka-go does
// not expose partition-revocation callbacks, so an aggregator whose partition
// was revoked by a rebalance would otherwise block on an empty channel forever
// (leaking the goroutine and map entry for the process lifetime). The
// aggregator instead exits after an idle window; this call reclaims its entry
// so a later message for that partition lazily spawns a fresh aggregator.
func (r *aggregateRuntime) evictAggregator(key partitionKey, agg *partitionAggregator) {
	r.mu.Lock()
	if existing, ok := r.aggregators[key]; ok && existing == agg {
		delete(r.aggregators, key)
	}
	r.mu.Unlock()
}

// aggregatorIdleTimeout bounds how long an aggregator waits for a new
// message before assuming its partition was revoked and self-terminating. Set
// well above FlushInterval so a low-traffic but still-assigned partition is not
// prematurely reaped; the next message simply spawns a new aggregator.
func aggregatorIdleTimeout(flushInterval time.Duration) time.Duration {
	return max(flushInterval*10, 5*time.Second)
}

// idleFlushTimeout bounds the last-gasp flush an aggregator attempts before
// reaping itself.
//
// It covers one full downstream attempt — the group execution deadline plus the
// admission request timeout — so a reap that lands while the pipeline is merely
// busy still delivers its buffer rather than discarding it. Past that the buffer
// is dropped and Kafka redelivers, which is the cheaper trade: the aggregator's
// goroutine and map entry are freed, and its partition gets a fresh aggregator
// on the next message.
const idleFlushTimeout = 60 * time.Second

func (r *aggregateRuntime) close(ctx context.Context) {
	r.closeOnce.Do(func() {
		r.mu.Lock()
		aggregators := make([]*partitionAggregator, 0, len(r.aggregators))
		for _, agg := range r.aggregators {
			aggregators = append(aggregators, agg)
		}
		r.mu.Unlock()
		for _, agg := range aggregators {
			close(agg.ch)
		}
		// Close the publisher only after every aggregator has drained, so a
		// final flush's dead-letter publishes are not cut off. Deferred because
		// the ctx.Done() path below returns early.
		defer func() {
			if r.deadLetterPublisher != nil {
				_ = r.deadLetterPublisher.Close()
			}
		}()
		for _, agg := range aggregators {
			select {
			case <-agg.done:
			case <-ctx.Done():
				return
			}
		}
	})
}

func (a *partitionAggregator) run() {
	defer close(a.done)
	defer a.rt.evictAggregator(a.key, a)
	var buffer []Message
	// discarded holds offsets of schema-invalid messages that were resolved
	// (discarded or dead-lettered) but whose offsets must NOT be committed
	// independently. Committing one immediately would move the group offset past
	// lower offsets still sitting in buffer, so a rebalance right then would skip
	// them — the same skip hazard the per-partition serial design exists to
	// prevent. They ride along with the next successful flush instead.
	var discarded []Message
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	timerActive := false
	defer timer.Stop()
	// retrying records that the last flush failed and its messages are still
	// buffered awaiting a TIMED retry. Without it, a failed flush left the buffer
	// full, so the size threshold was met again by the very next message and every
	// subsequent arrival re-ran the whole buffer through the downstream. Measured
	// against live traffic: 46.8 evals per committed record where 2 were expected,
	// i.e. 95.7% of downstream work spent re-processing messages that never
	// committed. Retries also grew the batch, making the next deadline breach more
	// likely — positive feedback rather than a linear shortfall.
	retrying := false
	// overflowDropped counts messages shed while a retry is pending, so the
	// buffer cap cannot silently discard traffic.
	overflowDropped := 0
	maxBuffered := a.rt.cfg.MaxSize * maxBufferedBatches
	// idleTimer reaps the aggregator after a quiet window so a partition
	// revoked by rebalance does not leak the goroutine and map entry.
	idleTimer := time.NewTimer(a.idleTimeout)
	defer idleTimer.Stop()
	for {
		select {
		case msg, ok := <-a.ch:
			if !ok {
				a.flush(context.Background(), buffer, discarded, "close")
				return
			}
			// A new message arrived: this partition is still assigned — reset
			// the idle reap window.
			if !idleTimer.Stop() {
				select {
				case <-idleTimer.C:
				default:
				}
			}
			idleTimer.Reset(a.idleTimeout)
			// Schema validation happens here, before the message joins a batch,
			// so one malformed record cannot invalidate an otherwise good batch.
			if a.rt.messageSchema != nil && !validateMessageSchema(msg, a.rt.messageSchema) {
				if handleInvalidMessage(context.Background(), a.rt, msg) {
					discarded = append(discarded, msg)
					// Start the flush timer even for an all-invalid stream, or
					// these offsets would sit uncommitted until a valid message
					// happened to arrive.
					if len(buffer) == 0 && len(discarded) == 1 {
						resetAggregateTimer(timer, &timerActive, a.rt.cfg.FlushInterval)
					}
				}
				continue
			}
			// No pre-emit dedup: a message must never be marked "seen" before its
			// side effect is durable, or a crash between the two loses it for good.
			// At-least-once here rests on flush's emit-then-commit ordering, and the
			// downstream absorbs duplicates via the host idempotency contract.
			buffer = append(buffer, msg)
			if len(buffer) == 1 {
				resetAggregateTimer(timer, &timerActive, a.rt.cfg.FlushInterval)
			}
			// While a retry is pending, arriving messages only accumulate: the
			// retry is owned by the timer. Re-flushing here is what produced the
			// divergence described at `retrying`.
			if retrying {
				if len(buffer) > maxBuffered {
					// Shed the message just appended. Its offset is uncommitted,
					// so Kafka redelivers it once the retained head commits.
					buffer = buffer[:len(buffer)-1]
					overflowDropped++
					if emit, count := discardLog.allow(time.Now(), msg.Topic+"\x00overflow"); emit {
						slog.Warn("kafka aggregate buffer at cap while a flush retry is pending; "+
							"shedding messages for redelivery",
							"topic", msg.Topic,
							"partition", msg.Partition,
							"cap", maxBuffered,
							"dropped_since_last_success", overflowDropped,
							"occurrences", count)
					}
					obs().OnMessageDiscarded(context.Background(), msg.Topic, "buffer_overflow")
				}
				continue
			}
			if len(buffer) >= a.rt.cfg.MaxSize {
				if a.flush(context.Background(), buffer, discarded, "size") {
					buffer = nil
					discarded = nil
					overflowDropped = 0
					stopAggregateTimer(timer, &timerActive)
				} else {
					// Hold these messages for the timed retry rather than
					// re-flushing on the next arrival.
					retrying = true
					resetAggregateTimer(timer, &timerActive, a.rt.cfg.FlushInterval)
				}
			}
		case <-timer.C:
			timerActive = false
			if a.flush(context.Background(), buffer, discarded, "timeout") {
				buffer = nil
				discarded = nil
				retrying = false
				overflowDropped = 0
			} else if len(buffer) > 0 || len(discarded) > 0 {
				retrying = true
				resetAggregateTimer(timer, &timerActive, a.rt.cfg.FlushInterval)
			}
		case <-idleTimer.C:
			// No message for the idle window: assume the partition was revoked
			// by a rebalance. Flush any pending buffer, then self-terminate so
			// the goroutine and map entry are reclaimed. A failed flush here
			// drops the in-memory buffer, which is safe: the offsets were never
			// committed, so Kafka redelivers to whoever owns the partition next.
			//
			// The flush is bounded because this path is the one that must not
			// hang: run() cannot return, and so the aggregator cannot be
			// evicted, until it comes back. context.Background() gave flush's
			// emitSem acquire no escape at all — Background().Done() is a nil
			// channel — and left a shutdown unable to interrupt the attempt.
			// Since the buffer is discardable here by the argument above,
			// waiting past one downstream attempt buys nothing.
			if len(buffer) > 0 || len(discarded) > 0 {
				ctx, cancel := context.WithTimeout(context.Background(), idleFlushTimeout)
				a.flush(ctx, buffer, discarded, "idle")
				cancel()
			}
			return
		}
	}
}

// flush emits messages as one batch and commits their offsets, plus the offsets
// of any discarded messages that were withheld from independent commit.
//
// discarded offsets are committed only alongside a successful emit, or on their
// own when there is nothing to emit. They are never committed after a failed
// emit: the failed batch will be redelivered from the lowest uncommitted offset,
// and advancing past a discarded offset that sits below it would skip valid
// messages.
func (a *partitionAggregator) flush(ctx context.Context, messages, discarded []Message, trigger string) bool {
	if len(messages) == 0 {
		if len(discarded) == 0 {
			return true
		}
		// Nothing to emit — an all-invalid window. Commit the resolved offsets
		// so the group does not stall on messages that will never be emitted.
		_ = commitMessages(ctx, a.rt.consumer, discarded...)
		return true
	}
	select {
	case a.rt.emitSem <- struct{}{}:
		defer func() { <-a.rt.emitSem }()
	case <-ctx.Done():
		return false
	}
	// Observed BEFORE the emit, so the histogram describes what was ATTEMPTED.
	// It used to sit after the success path, which meant failed large batches
	// never entered it: the distribution then described only the small batches
	// that survived, and reported mean=3.64 while buffers were stuck at 100+.
	// A metric whose sample set is conditioned on success cannot be used to
	// diagnose failure, which is exactly what it was needed for.
	obs().OnBatchFlushed(ctx, messages[0].Topic, trigger, len(messages))
	if !a.emitBatch(ctx, messages) {
		obs().OnBatchFlushOutcome(ctx, messages[0].Topic, trigger, "error")
		return false
	}
	obs().OnBatchFlushOutcome(ctx, messages[0].Topic, trigger, "ok")
	// Copy rather than append(messages, discarded...): appending would write
	// into buffer's spare capacity, aliasing a slice the caller still holds.
	commits := make([]Message, 0, len(messages)+len(discarded))
	commits = append(commits, messages...)
	commits = append(commits, discarded...)
	_ = commitMessages(ctx, a.rt.consumer, commits...)
	return true
}

// emitBatch delivers one batch downstream, reporting only whether it succeeded.
// Split out of flush so the flush-outcome metric has a single success and a
// single failure edge; folding it back in means three separate return-false
// sites, and a metric that a later branch can bypass is the shape that produced
// the selection bias documented above.
func (a *partitionAggregator) emitBatch(ctx context.Context, messages []Message) bool {
	// Entry-seed mode admits the batch to the control plane instead of emitting
	// locally. Both paths share the SAME commit rule in flush: the batch is only
	// durable-enough-to-commit after the side effect succeeded.
	if a.rt.entrySeed {
		// A trigger-group activation's Runtime additionally implements
		// types.GroupExecRuntime (see groupExecTriggerRuntime in
		// service/runner) — when present, run the batch through the group's
		// real member nodes and admit the real resulting exits instead of
		// the raw-message exits the plain EntrySeedRuntime path would
		// synthesize.
		if gr, ok := a.rt.in.Runtime.(interface {
			types.EntrySeedRuntime
			types.GroupExecRuntime
		}); ok {
			return seedEntryBatchViaGroupExec(ctx, a.rt.in, gr, messages)
		}
		if rt, ok := a.rt.in.Runtime.(types.EntrySeedRuntime); ok {
			return seedEntryBatchMessages(ctx, a.rt.in, rt, messages)
		}
		// isEntrySeedActivation already required at least EntrySeedRuntime,
		// so reaching here means the runtime changed under us. Withhold the
		// commit rather than silently falling back to Emit with a different
		// key space.
		return false
	}
	event := batchEvent(a.rt.in.NodeName, messages)
	_, err := a.rt.in.Emit(ctx, event)
	return err == nil
}

func resetAggregateTimer(timer *time.Timer, active *bool, d time.Duration) {
	if *active {
		timer.Stop()
	}
	timer.Reset(d)
	*active = true
}

func stopAggregateTimer(timer *time.Timer, active *bool) {
	if !*active {
		return
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	*active = false
}

func aggregateConfigFromParamForMode(v any, entrySeed bool) (AggregateConfig, error) {
	if v == nil {
		return AggregateConfig{}, nil
	}
	raw, ok := v.(map[string]any)
	if !ok {
		rawAny, err := cast.ToStringMapE(v)
		if err != nil {
			return AggregateConfig{}, fmt.Errorf("kafka aggregate must be an object")
		}
		raw = rawAny
	}
	cfg := AggregateConfig{
		Enabled:       cast.ToBool(raw["enabled"]),
		By:            cast.ToString(raw["by"]),
		MaxSize:       conv.PositiveInt(raw["max_size"], defaultAggregateMaxSize),
		FlushInterval: defaultFlushIntervalFor(entrySeed),
		Dedup:         cast.ToString(raw["dedup"]),
	}
	if !cfg.Enabled {
		return AggregateConfig{}, nil
	}
	if raw["flush_interval"] != nil {
		flushInterval, err := conv.PositiveDuration(raw["flush_interval"])
		if err != nil {
			return AggregateConfig{}, fmt.Errorf("kafka aggregate flush_interval: %w", err)
		}
		cfg.FlushInterval = flushInterval
	}
	cfg = normalizeAggregateConfig(cfg)
	if cfg.By != aggregateByPartition {
		return AggregateConfig{}, fmt.Errorf("kafka aggregate by %q is not supported", cfg.By)
	}
	if cfg.Dedup != aggregateDedupMessage {
		return AggregateConfig{}, fmt.Errorf("kafka aggregate dedup %q is not supported", cfg.Dedup)
	}
	return cfg, nil
}

func normalizeAggregateConfig(cfg AggregateConfig) AggregateConfig {
	if !cfg.Enabled {
		return AggregateConfig{}
	}
	if cfg.By == "" {
		cfg.By = aggregateByPartition
	}
	if cfg.MaxSize <= 0 {
		cfg.MaxSize = defaultAggregateMaxSize
	}
	// This fallback only fires on the Go DSL path (Aggregate/AggregateByPartition
	// at construction time) where the mode is not yet known. The runtime params
	// path sets mode-aware defaults before calling normalize, so this branch is
	// unreachable there. Always falls back to the legacy 100ms — entry-seed mode
	// is handled upstream by aggregateConfigFromParamForMode.
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = defaultAggregateFlushInterval
	}
	if cfg.Dedup == "" {
		cfg.Dedup = aggregateDedupMessage
	}
	return cfg
}
