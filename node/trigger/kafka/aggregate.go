package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/xbcio/xflow/node/internal/utils/conv"
	"github.com/spf13/cast"

	"github.com/xbcio/xflow/types"
)

const defaultAggregateMaxSize = 100
const defaultAggregateFlushInterval = 100 * time.Millisecond

// maxBufferedBatches bounds the queue behind a partition's ordered in-flight
// window. The total retained bound is this queue plus maxPartitionPendingBatches.
// A failed head therefore cannot grow memory without limit while completed
// followers wait for it.
//
// Overflow drops the ARRIVING message under the default discard policy. That is
// loss, not deferral: a later commit of a higher offset sweeps past the dropped
// offset. The only signal is OnMessageDiscarded("buffer_overflow");
// TestKafkaAggregateShedMessagesAreSilentlySkipped pins that kafka-go
// behaviour. A deployment that cannot afford the loss selects on_overflow:
// block instead and pays for it in cross-partition backpressure — see the
// on_overflow constants for why one shared reader makes those two exclusive.
const maxBufferedBatches = 4

// maxPartitionPendingBatches is a separate bound from aggregateRuntime.emitSem.
// emitSem limits work actively using downstream capacity process-wide. This
// bound retains per-partition batches that are running, waiting for an earlier
// offset, or waiting for retry. A completed out-of-order batch releases emitSem
// but keeps one of these slots until the contiguous prefix commits.
const maxPartitionPendingBatches = 4

const (
	aggregateCloseDrainTimeout = 500 * time.Millisecond
	aggregateCommitTimeout     = 15 * time.Second
)

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

// on_overflow selects what happens when a partition holds maxRetained records
// and another arrives. The two options are a real trade, not a preference, and
// one shared reader goroutine is what makes them exclusive:
//
//	discard  drop the arriving message. The coordinator keeps draining its
//	         channel, so submit never blocks and the OTHER partitions keep
//	         being consumed. The dropped offset is never redelivered — a later
//	         commit of a higher offset sweeps past it (see maxBufferedBatches).
//
//	block    stop draining until the backlog clears. Nothing is lost, and
//	         nothing is consumed either: agg.ch fills, submit blocks, and
//	         because ONE goroutine reads consumer.Messages() for every
//	         partition, the whole assignment stops. Lag then grows, which is
//	         the honest signal that capacity is short.
//
// There is no third setting that avoids both, and the missing option is not an
// oversight. Partition-selective backpressure would need the Reader to pause a
// single partition, which kafka-go's group Reader does not expose; without it,
// bounded memory forces a choice between losing the record and stopping the
// reader that every partition shares.
const (
	onOverflowDiscard = "discard"
	onOverflowBlock   = "block"
)

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
	// OnOverflow selects discard (default) or block when a partition is at its
	// retained bound. Empty means discard, which is the behaviour every
	// deployment had before this field existed.
	OnOverflow string
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
	closeDone   chan struct{}
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
	// valueJSON splices a JSON payload into the item verbatim instead of
	// escaping it into a string. Resolved once at activation by
	// resolveValueJSON; only the group path below honours it.
	valueJSON bool
	// baseCtx is the activation context with its cancellation removed. It exists
	// because two different things were being taken from one context and only
	// one of them was wanted. Every partitionAggregator reads it, so a
	// hand-built runtime must set it; leaving it nil panics in run() rather than
	// quietly reinstating the bug.
	//
	// The values are wanted: observability/metrics reads the namespace off the
	// context for every label set (withNamespace), so a report made from
	// context.Background() is filed under the default namespace. For the
	// Set-based gauges that is worse than a missing label — two namespaces
	// consuming a topic of the same name overwrite each other's value, and the
	// survivor looks authoritative.
	//
	// The cancellation is not: a partition aggregator drains and exits on its
	// own schedule, and its in-flight emit/commit attempts must not be cut short
	// when runCtx is canceled. That is why these paths reached for Background in
	// the first place; WithoutCancel gives the namespace back without giving the
	// Done channel back with it.
	baseCtx context.Context
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
	// stop is closed by rt.close alongside ch, and is the ONLY shutdown signal
	// that survives on_overflow=block.
	//
	// Every other path learns about shutdown by receiving the zero value from a
	// closed ch. A blocked partition has removed that receive from its select on
	// purpose — that is how it stops consuming — so it would never see the close,
	// never close done, and rt.close would wait out its timeout and leave the
	// coordinator running for the life of the process. A close signal cannot be
	// carried on the channel whose receive is the thing being disabled.
	stop chan struct{}
}

func activateAggregate(ctx context.Context, in *types.TriggerActivateInput, cfg ConsumerConfig, consumer Consumer, deadLetters DeadLetterPublisher) types.TriggerSubscription {
	runCtx, cancel := context.WithCancel(ctx)
	baseCtx := context.WithoutCancel(runCtx)
	maxInflight := cfg.MaxInflight
	if maxInflight <= 0 {
		maxInflight = defaultTriggerMaxInflight
	}
	rt := &aggregateRuntime{
		in:                  in,
		cfg:                 cfg.Aggregate,
		consumer:            consumer,
		emitSem:             make(chan struct{}, maxInflight),
		closeDone:           make(chan struct{}),
		baseCtx:             baseCtx,
		aggregators:         make(map[partitionKey]*partitionAggregator),
		messageSchema:       cfg.MessageSchema,
		entrySeed:           isEntrySeedActivation(in),
		deadLetterPublisher: deadLetters,
	}
	rt.valueJSON = resolveValueJSON(cfg, in, rt.entrySeed)
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
		// Cancel first so any submit blocked on a full aggregator channel wakes.
		cancel()
		err := consumer.Close()
		waitCtx, waitCancel := context.WithTimeout(closeCtx, aggregateCloseDrainTimeout)
		defer waitCancel()
		select {
		case <-done:
		case <-waitCtx.Done():
		}
		rt.close(waitCtx)
		return err
	})
}

// submit hands a message to its partition coordinator, retrying against a
// replacement if an idle coordinator exits while this send is blocked. The
// context check before aggregator() is load-bearing: shutdown has already taken
// its snapshot, so spawning a replacement after cancellation would leak it.
//
// The caller is the single goroutine reading consumer.Messages() for every
// partition. run therefore keeps draining its channel while downstream work is
// asynchronous; only the explicit bounded queue can apply cross-partition
// backpressure now, rather than every emit occupying the coordinator loop.
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
		stop:        make(chan struct{}),
		idleTimeout: aggregatorIdleTimeout(r.cfg.FlushInterval),
	}
	r.aggregators[key] = agg
	go agg.run()
	return agg
}

// evictAggregator removes an idle coordinator that self-terminated. kafka-go
// does not expose partition-revocation callbacks, so an empty coordinator uses
// an idle window as the only available reclamation signal. A coordinator that
// still owns an uncommitted frontier cannot be reaped safely and keeps retrying.
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

func (r *aggregateRuntime) close(ctx context.Context) {
	// Start shutdown exactly once, but do the wait outside sync.Once. A caller
	// timing out must not block behind another caller currently inside Once.Do.
	r.closeOnce.Do(func() {
		if r.closeDone == nil {
			r.closeDone = make(chan struct{})
		}
		r.mu.Lock()
		aggregators := make([]*partitionAggregator, 0, len(r.aggregators))
		for _, agg := range r.aggregators {
			aggregators = append(aggregators, agg)
		}
		r.mu.Unlock()
		for _, agg := range aggregators {
			// stop before ch. A coordinator that is NOT blocked reaches the closed
			// ch first either way and takes its normal drain path; one that IS
			// blocked has no receive on ch to reach, and this is what wakes it.
			close(agg.stop)
			close(agg.ch)
		}
		go func() {
			defer close(r.closeDone)
			for _, agg := range aggregators {
				<-agg.done
			}
			if r.deadLetterPublisher != nil {
				_ = r.deadLetterPublisher.Close()
			}
		}()
	})
	select {
	case <-r.closeDone:
	case <-ctx.Done():
	}
}

type aggregateBatchState uint8

const (
	batchNew aggregateBatchState = iota
	batchRunning
	batchRetryWait
	batchSucceeded
)

// aggregateBufferedMessage keeps schema classification and arrival order in one
// queue. Separate valid/discarded slices made it possible for an all-invalid
// stream to evade the size and memory bounds, and made offset cutoffs depend on
// three independently-mutated slices.
type aggregateBufferedMessage struct {
	message Message
	valid   bool
}

type aggregateBatch struct {
	messages   []Message
	discarded  []Message
	unresolved []Message
	itemCount  int
	trigger    string
	state      aggregateBatchState
	attempts   int
	retryAt    time.Time
	closeRetry bool
}

type aggregateAttemptResult struct {
	batch      *aggregateBatch
	ok         bool
	discarded  []Message
	unresolved []Message
}

type aggregateCommitResult struct {
	count int
	err   error
}

// run is the per-partition reorder coordinator. Batches may execute out of
// order, but only the contiguous successful prefix is ever handed to
// CommitMessages. A completed follower releases the process-wide emit slot but
// remains in batches (and therefore occupies the bounded reorder window) until
// every lower batch succeeds and the prefix commit completes.
func (a *partitionAggregator) run() {
	defer close(a.done)
	defer a.rt.evictAggregator(a.key, a)

	maxSize := a.rt.cfg.MaxSize
	if maxSize <= 0 {
		maxSize = defaultAggregateMaxSize
	}
	maxPending := min(maxPartitionPendingBatches, cap(a.rt.emitSem))
	if maxPending <= 0 {
		maxPending = 1
	}
	// Keep the old four-batch retry queue in addition to the concurrent reorder
	// window. Counting both pending and buffered records makes the bound hold for
	// valid, discarded, and unresolved-invalid traffic alike.
	maxRetained := maxSize * (maxBufferedBatches + maxPending)

	var buffer []aggregateBufferedMessage
	var batches []*aggregateBatch
	attemptResults := make(chan aggregateAttemptResult, maxPending)
	commitResults := make(chan aggregateCommitResult, 1)
	attemptCtx, cancelAttempts := context.WithCancel(a.rt.baseCtx)
	defer cancelAttempts()

	flushTimer := time.NewTimer(time.Hour)
	flushTimer.Stop()
	flushTimerActive := false
	defer flushTimer.Stop()
	retryTimer := time.NewTimer(time.Hour)
	retryTimer.Stop()
	retryTimerActive := false
	defer retryTimer.Stop()
	idleTimer := time.NewTimer(a.idleTimeout)
	defer idleTimer.Stop()

	commitInFlight := false
	commitRetryAt := time.Time{}
	// commitAttempts is the batch.attempts equivalent for the commit round trip.
	// A broker that keeps rejecting the commit freezes this partition's frontier
	// exactly as a poison batch does, and re-issuing at the flush cadence against
	// an unavailable coordinator is the same wasted capacity. Reset on every
	// successful commit, so a transient rejection costs no backoff debt.
	commitAttempts := 0
	closeCommitRetry := false
	overflowDropped := 0
	backpressured := false
	closing := false
	inputC := (<-chan Message)(a.ch)
	stopC := (<-chan struct{})(a.stop)
	var closeTimer *time.Timer
	var closeC <-chan time.Time

	retainedCount := func() int {
		n := len(buffer)
		for _, batch := range batches {
			n += batch.itemCount
		}
		return n
	}
	resetIdle := func() {
		if !idleTimer.Stop() {
			select {
			case <-idleTimer.C:
			default:
			}
		}
		idleTimer.Reset(a.idleTimeout)
	}
	appendBatch := func(items []aggregateBufferedMessage, trigger string) {
		batch := &aggregateBatch{
			itemCount: len(items),
			trigger:   trigger,
			state:     batchNew,
		}
		for _, item := range items {
			if item.valid {
				batch.messages = append(batch.messages, item.message)
			} else {
				batch.unresolved = append(batch.unresolved, item.message)
			}
		}
		batches = append(batches, batch)
	}
	promoteSize := func() bool {
		if len(batches) >= maxPending || len(buffer) < maxSize {
			return false
		}
		valid := 0
		cut := 0
		for i, item := range buffer {
			if item.valid {
				valid++
			}
			switch {
			case valid == maxSize:
				// Keep the configured batch size in terms of messages that are
				// actually emitted; resolved invalid offsets ride beside them.
				cut = i + 1
			case i+1 == maxSize && valid == 0:
				// An all-invalid stream still has to advance without waiting for
				// the interval or for a valid record that may never arrive.
				cut = i + 1
			}
			if cut != 0 {
				break
			}
		}
		if cut == 0 {
			return false
		}
		appendBatch(buffer[:cut], "size")
		buffer = buffer[cut:]
		return true
	}
	promoteTail := func(trigger string) bool {
		if len(batches) >= maxPending || len(buffer) == 0 {
			return false
		}
		appendBatch(buffer, trigger)
		buffer = nil
		return true
	}

	launchBatch := func(batch *aggregateBatch) {
		batch.state = batchRunning
		batch.attempts++
		go func() {
			result := a.executeBatch(attemptCtx, batch)
			select {
			case attemptResults <- result:
			case <-attemptCtx.Done():
			}
		}()
	}
	launchEligible := func(now time.Time) {
		// Every new follower gets one speculative attempt. After a failure, only
		// the first non-successful batch retries: later failures cannot advance the
		// commit frontier and would only consume capacity while the head is blocked.
		for _, batch := range batches {
			if batch.state == batchNew {
				launchBatch(batch)
			}
		}
		for _, batch := range batches {
			if batch.state == batchSucceeded {
				continue
			}
			if batch.state == batchRetryWait && !now.Before(batch.retryAt) {
				launchBatch(batch)
			}
			break
		}
	}
	fillWindow := func(trigger string, includeTail bool) {
		for len(batches) < maxPending && promoteSize() {
		}
		if includeTail && len(batches) < maxPending {
			promoteTail(trigger)
		}
		launchEligible(time.Now())
		if len(buffer) == 0 {
			stopAggregateTimer(flushTimer, &flushTimerActive)
		} else if !flushTimerActive {
			resetAggregateTimer(flushTimer, &flushTimerActive, a.rt.cfg.FlushInterval)
		}
	}
	startCommit := func(now time.Time) {
		if commitInFlight || len(batches) == 0 ||
			(!commitRetryAt.IsZero() && now.Before(commitRetryAt)) {
			return
		}
		count := 0
		commitCount := 0
		for _, batch := range batches {
			if batch.state != batchSucceeded {
				break
			}
			count++
			commitCount += len(batch.messages) + len(batch.discarded)
		}
		if count == 0 {
			return
		}
		commits := make([]Message, 0, commitCount)
		for _, batch := range batches[:count] {
			commits = append(commits, batch.messages...)
			commits = append(commits, batch.discarded...)
		}
		// Schema-invalid records are carried beside valid records, so restore the
		// partition order before calling kafka-go. The highest offset is still the
		// effective group commit, but passing the full ordered prefix preserves the
		// package contract and makes ordering directly testable.
		sort.SliceStable(commits, func(i, j int) bool { return commits[i].Offset < commits[j].Offset })
		commitInFlight = true
		go func(count int, commits []Message) {
			ctx, cancel := context.WithTimeout(attemptCtx, aggregateCommitTimeout)
			err := commitMessages(ctx, a.rt.consumer, commits...)
			cancel()
			select {
			case commitResults <- aggregateCommitResult{count: count, err: err}:
			case <-attemptCtx.Done():
			}
		}(count, commits)
	}
	resetRetryTimer := func(now time.Time) {
		var next time.Time
		if !commitRetryAt.IsZero() {
			next = commitRetryAt
		}
		for _, batch := range batches {
			if batch.state == batchSucceeded {
				continue
			}
			if batch.state == batchRetryWait && (next.IsZero() || batch.retryAt.Before(next)) {
				next = batch.retryAt
			}
			break
		}
		if next.IsZero() {
			stopAggregateTimer(retryTimer, &retryTimerActive)
			return
		}
		d := time.Until(next)
		if d < 0 {
			d = 0
		}
		resetAggregateTimer(retryTimer, &retryTimerActive, d)
	}
	advance := func() {
		now := time.Now()
		fillWindow("close", closing)
		launchEligible(now)
		startCommit(now)
		resetRetryTimer(now)
	}
	// beginClose runs the one-time bookkeeping that moves this coordinator into
	// its drain phase. Extracted because two different events now reach it: the
	// input channel closing (the normal path) and a.stop closing (the only path
	// a block-policy partition sitting at its cap can take, since it has no
	// receive on the input channel left to notice).
	beginClose := func() {
		closing = true
		inputC = nil
		stopC = nil
		stopAggregateTimer(flushTimer, &flushTimerActive)
		stopAggregateTimer(retryTimer, &retryTimerActive)
		closeTimer = time.NewTimer(aggregateCloseDrainTimeout)
		closeC = closeTimer.C
		// A batch already waiting for retry gets one immediate close attempt.
		// New or currently-running batches also get at most one retry below.
		for _, batch := range batches {
			if batch.state == batchRetryWait {
				batch.retryAt = time.Time{}
				batch.closeRetry = true
			}
		}
		if !commitRetryAt.IsZero() {
			commitRetryAt = time.Time{}
			closeCommitRetry = true
		}
		advance()
	}
	reportOverflow := func(msg Message) {
		overflowDropped++
		if emit, count := discardLog.allow(time.Now(), msg.Topic+"\x00overflow"); emit {
			slog.Warn("kafka aggregate buffer at cap; DISCARDING messages "+
				"(they are not redelivered — a later commit sweeps past them)",
				"topic", msg.Topic,
				"partition", msg.Partition,
				"cap", maxRetained,
				"dropped_since_last_success", overflowDropped,
				"occurrences", count)
		}
		obs().OnMessageDiscarded(a.rt.baseCtx, msg.Topic, "buffer_overflow")
	}
	// reportBackpressure announces this partition entering or leaving the state
	// where it has stopped receiving.
	//
	// It needs its own signal because the obvious one lies here. OnConsumerLag
	// samples only when a message is FETCHED, so a partition that has stopped
	// fetching leaves its lag gauge frozen at the last value it saw — the
	// Observer doc says so in as many words. Under this policy that is not a
	// corner case, it is the steady state: the moment backpressure works, lag
	// stops updating and the consumer reads as healthy.
	reportBackpressure := func(blocked bool) {
		obs().OnConsumptionBlocked(a.rt.baseCtx, a.key.topic, a.key.partition, blocked)
		if blocked {
			slog.Warn("kafka aggregate at cap with on_overflow=block; HALTING consumption "+
				"(nothing is dropped; this partition's channel will fill and the shared "+
				"reader stops fetching for every partition until the backlog drains)",
				"topic", a.key.topic,
				"partition", a.key.partition,
				"cap", maxRetained)
			return
		}
		slog.Info("kafka aggregate backlog drained; resuming consumption",
			"topic", a.key.topic,
			"partition", a.key.partition)
	}

	for {
		if closing && len(batches) == 0 && len(buffer) == 0 && !commitInFlight {
			if closeTimer != nil {
				closeTimer.Stop()
			}
			return
		}
		// readC is inputC except while a block-policy partition sits at its
		// retained bound, when it is nil so this select simply stops offering the
		// receive. Ceasing to RECEIVE is the whole mechanism: the message stays in
		// a.ch, a.ch fills, submit blocks, and the shared reader stops fetching for
		// every partition. Nothing is dropped and nothing is consumed.
		//
		// Only this arm is disabled. Attempt results, commit results and every
		// timer keep being serviced, which is what lets the backlog drain and the
		// receive resume — disabling the whole select would deadlock instead.
		//
		// Under the default discard policy readC is always inputC, so the
		// coordinator drains unconditionally and TestKafkaAggregateHeadOfLine
		// keeps holding.
		// Computed explicitly rather than as readC == nil: inputC is ALSO nil once
		// the input closes, and reporting a shutdown as backpressure would page
		// someone for a clean stop.
		blocked := !closing && inputC != nil &&
			a.rt.cfg.OnOverflow == onOverflowBlock && retainedCount() >= maxRetained
		readC := inputC
		if blocked {
			readC = nil
		}
		// Edge-triggered, not sampled every iteration: the gauge carries the
		// state, and the log is the thing an operator greps for after finding a
		// consumer that stopped. Reporting only transitions also means a partition
		// that flaps is visible as flapping.
		if blocked != backpressured {
			backpressured = blocked
			reportBackpressure(blocked)
		}
		select {
		case msg, ok := <-readC:
			if !ok {
				beginClose()
				continue
			}
			resetIdle()
			if retainedCount() >= maxRetained {
				reportOverflow(msg)
				continue
			}
			valid := a.rt.messageSchema == nil || validateMessageSchema(msg, a.rt.messageSchema)
			buffer = append(buffer, aggregateBufferedMessage{message: msg, valid: valid})
			if len(buffer) == 1 {
				resetAggregateTimer(flushTimer, &flushTimerActive, a.rt.cfg.FlushInterval)
			}
			fillWindow("size", false)

		case result := <-attemptResults:
			batch := result.batch
			batch.discarded = result.discarded
			batch.unresolved = result.unresolved
			if result.ok {
				batch.state = batchSucceeded
			} else {
				batch.state = batchRetryWait
				if closing {
					if batch.closeRetry {
						if closeTimer != nil {
							closeTimer.Stop()
						}
						return
					}
					batch.closeRetry = true
					batch.retryAt = time.Time{}
				} else {
					delay := aggregateRetryDelay(a.rt.cfg.FlushInterval, batch.attempts)
					batch.retryAt = time.Now().Add(delay)
					reportStuckBatch(a.key, batch, delay)
				}
			}
			advance()

		case result := <-commitResults:
			commitInFlight = false
			if result.err == nil {
				if result.count > len(batches) {
					result.count = len(batches)
				}
				batches = batches[result.count:]
				commitRetryAt = time.Time{}
				commitAttempts = 0
				closeCommitRetry = false
				overflowDropped = 0
			} else {
				slog.Warn("kafka aggregate offset commit failed; retaining successful prefix for retry",
					"topic", a.key.topic, "partition", a.key.partition, "error", result.err)
				if closing {
					if closeCommitRetry {
						if closeTimer != nil {
							closeTimer.Stop()
						}
						return
					}
					closeCommitRetry = true
					commitRetryAt = time.Time{}
				} else {
					commitAttempts++
					commitRetryAt = time.Now().Add(aggregateRetryDelay(a.rt.cfg.FlushInterval, commitAttempts))
				}
			}
			advance()

		case <-flushTimer.C:
			flushTimerActive = false
			fillWindow("timeout", true)
			advance()

		case <-retryTimer.C:
			retryTimerActive = false
			advance()

		case <-idleTimer.C:
			if len(batches) == 0 && len(buffer) == 0 && !commitInFlight {
				return
			}
			// Outstanding work owns the partition's commit frontier. There is no
			// safe permanent-failure skip: retain it and continue bounded retries.
			idleTimer.Reset(a.idleTimeout)

		case <-stopC:
			// Reached only when the input receive above was disabled by
			// backpressure; otherwise the closed input channel wins the race often
			// enough that this is redundant, and beginClose is idempotent by way of
			// nilling stopC.
			//
			// Anything still sitting in a.ch is abandoned here rather than drained.
			// That is the correct direction for this policy: those offsets were
			// never committed, so the next generation redelivers them. Draining
			// them would mean waiting for the same wedged downstream that caused
			// the backpressure, which is what the close timeout exists to bound.
			beginClose()

		case <-closeC:
			return
		}
	}
}

// executeBatch performs downstream work only. The coordinator above owns all
// commits, so an out-of-order completion can never advance the Kafka offset.
func (a *partitionAggregator) executeBatch(ctx context.Context, batch *aggregateBatch) aggregateAttemptResult {
	result := aggregateAttemptResult{
		batch:      batch,
		discarded:  append([]Message(nil), batch.discarded...),
		unresolved: append([]Message(nil), batch.unresolved...),
	}
	// Persist progress within the batch: a successful dead-letter publication is
	// not repeated merely because a later invalid record or the valid emit fails.
	for len(result.unresolved) > 0 {
		msg := result.unresolved[0]
		if !handleInvalidMessage(ctx, a.rt, msg) {
			return result
		}
		result.discarded = append(result.discarded, msg)
		result.unresolved = result.unresolved[1:]
	}
	if len(batch.messages) == 0 {
		result.ok = true
		return result
	}
	select {
	case a.rt.emitSem <- struct{}{}:
		defer func() { <-a.rt.emitSem }()
	case <-ctx.Done():
		return result
	}
	obs().OnBatchFlushed(ctx, batch.messages[0].Topic, batch.trigger, len(batch.messages))
	if !a.emitBatch(ctx, batch.messages) {
		obs().OnBatchFlushOutcome(ctx, batch.messages[0].Topic, batch.trigger, "error")
		return result
	}
	obs().OnBatchFlushOutcome(ctx, batch.messages[0].Topic, batch.trigger, "ok")
	result.ok = true
	return result
}

// emitBatch dispatches one batch through the same entry-seed/group or legacy
// path used before ordered concurrency. Success means only that downstream work
// completed; offset commit is deliberately separate.
func (a *partitionAggregator) emitBatch(ctx context.Context, messages []Message) bool {
	if a.rt.entrySeed {
		if gr, ok := a.rt.in.Runtime.(interface {
			types.EntrySeedRuntime
			types.GroupExecRuntime
		}); ok {
			return seedEntryBatchViaGroupExec(ctx, a.rt.in, gr, messages, a.rt.valueJSON)
		}
		if rt, ok := a.rt.in.Runtime.(types.EntrySeedRuntime); ok {
			return seedEntryBatchMessages(ctx, a.rt.in, rt, messages)
		}
		return false
	}
	event := batchEvent(a.rt.in.NodeName, messages)
	_, err := a.rt.in.Emit(ctx, event)
	return err == nil
}

// reportStuckBatch names a partition whose commit frontier has stopped moving.
//
// Without it a poison batch is invisible except as an absence: the per-attempt
// admission WARN says the batch failed, but says nothing about the batch having
// failed for the eightieth time, and nothing at all about the partition behind
// it. In the incident this was written for, an operator reading the logs saw a
// steady trickle of ordinary batch failures and no indication that four
// partitions had not advanced an offset in ninety seconds.
//
// The offsets are the actionable part: they bound the poison record to a
// hundred-message range that can be dumped and inspected. Rate-limited per
// topic+partition through the same limiter the discard paths use, so a
// permanently stuck partition costs one line per 30s rather than one per retry.
func reportStuckBatch(key partitionKey, batch *aggregateBatch, nextRetry time.Duration) {
	if batch.attempts <= aggregateFlatRetryAttempts || len(batch.messages) == 0 {
		return
	}
	emit, occurrences := discardLog.allow(time.Now(),
		key.topic+"\x00stuck_batch\x00"+strconv.Itoa(key.partition))
	if !emit {
		return
	}
	slog.Warn("kafka aggregate batch has failed past the flat retry window; this "+
		"partition's commit frontier cannot advance until it succeeds",
		"topic", key.topic,
		"partition", key.partition,
		"start_offset", batch.messages[0].Offset,
		"end_offset", batch.messages[len(batch.messages)-1].Offset,
		"messages", len(batch.messages),
		"attempts", batch.attempts,
		"next_retry_in", nextRetry.String(),
		"suppressed_since_last", occurrences-1)
}

// aggregateFlatRetryAttempts is how many retries run at the plain flush cadence
// before the delay starts growing, and aggregateMaxRetryDelay caps how far it
// grows.
//
// The flat window is what keeps the property the comment in aggregateRetryDelay
// describes: a downstream that is down for a few seconds recovers on the very
// next tick, with no backoff debt to pay off. Eight attempts covers that.
//
// Past it, the batch is not waiting on an outage, it is a POISON PILL — a
// record the workflow cannot process no matter how often it is re-run. That
// case was observed costing 30 group executions per second per partition, each
// re-running 100 messages through the guest, forever, while the partition's
// commit frontier stayed frozen (startCommit only advances over a contiguous
// succeeded prefix). Retrying is still correct — committing would silently
// discard the whole batch — but retrying at full rate spends the runner's
// capacity on work that provably cannot succeed, and that capacity belongs to
// the partitions that are still healthy.
const (
	aggregateFlatRetryAttempts = 8
	aggregateMaxRetryDelay     = 30 * time.Second
)

func aggregateRetryDelay(base time.Duration, attempts int) time.Duration {
	if base <= 0 {
		base = defaultAggregateFlushInterval
	}
	// Preserve the pre-concurrency retry cadence. Exponential backoff FROM THE
	// FIRST ATTEMPT made a partition remain stalled long after a short downstream
	// outage recovered; the bounded reorder window already limits work and memory
	// while it is down. The flat window below keeps that fix intact.
	if attempts <= aggregateFlatRetryAttempts {
		return base
	}
	delay := base
	for i := aggregateFlatRetryAttempts; i < attempts; i++ {
		delay *= 2
		if delay >= aggregateMaxRetryDelay {
			return aggregateMaxRetryDelay
		}
	}
	return delay
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
		OnOverflow:    strings.ToLower(strings.TrimSpace(cast.ToString(raw["on_overflow"]))),
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
	// Rejected rather than defaulted, matching on_invalid: a typo here silently
	// choosing "lose records under load" is the one outcome an operator who
	// bothered to set this field cannot have wanted.
	if cfg.OnOverflow != onOverflowDiscard && cfg.OnOverflow != onOverflowBlock {
		return AggregateConfig{}, fmt.Errorf("kafka aggregate on_overflow %q is not supported (want %q or %q)",
			cfg.OnOverflow, onOverflowDiscard, onOverflowBlock)
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
	// Unset means discard, so every deployment that predates this field keeps
	// the behaviour it already had. Opting into block is a decision to trade
	// assignment-wide consumption for completeness, and nobody gets moved onto
	// that trade by upgrading.
	if cfg.OnOverflow == "" {
		cfg.OnOverflow = onOverflowDiscard
	}
	return cfg
}
