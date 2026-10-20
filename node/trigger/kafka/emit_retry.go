package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cast"

	"github.com/xbcio/xflow/node/internal/utils/conv"
)

// Emit-failure policies for the legacy per-message emit path. They are a closed
// enum with the same shape as the two dead-letter axes in dlq.go, and for the
// same reason: the value is written into a stored definition and read back by
// configFromParams, so it is part of the trigger's config surface, not a local
// switch.
const (
	// onEmitFailureRetry holds the partition and retries the same record forever,
	// stopping only when the activation is canceled. It is the default and the
	// only behaviour when no dead-letter topic is configured: the two bounded
	// alternatives are both worse — committing past a record that was never
	// emitted loses it, and stopping consumption forever with nowhere for the
	// record to go is a manual-intervention outage with no preserved copy.
	onEmitFailureRetry = "retry"
	// onEmitFailureDeadLetter parks the record on EmitRetryConfig.DeadLetterTopic
	// after the retry budget (MaxAttempts / MaxBlock) is exhausted, and commits
	// its offset only once the republish succeeded.
	onEmitFailureDeadLetter = "dead_letter"
)

// Retry cadence for the legacy emit path. The initial delay is short so a
// downstream that hiccups for one retry costs almost nothing, and the cap is
// low enough that a recovered downstream is noticed quickly while still
// bounded: a partition retries at most once every emitRetryMaxDelay.
const (
	emitRetryInitialDelay = 100 * time.Millisecond
	emitRetryMaxDelay     = 5 * time.Second
)

// Defaults for the bounded phase. They are consulted only when a dead-letter
// topic is configured (see EmitRetryConfig.deadLetterReady): without one, the
// retry is deliberately unbounded, so a "default budget" would describe a limit
// that does not exist.
const (
	// defaultEmitRetryMaxAttempts covers the flat part of the backoff plus a few
	// capped attempts: with the 100ms-start/5s-cap cadence the 10th failure
	// lands around 16 seconds after the first.
	defaultEmitRetryMaxAttempts = 10
	// defaultEmitRetryMaxBlock bounds the wall time the partition may spend on
	// one record before it is parked, so a downstream that fails slowly (each
	// emit taking seconds) cannot hold the partition indefinitely even though
	// the attempt count has not been reached.
	defaultEmitRetryMaxBlock = 30 * time.Second
)

// EmitRetryConfig is the legacy emit path's failure policy: how long one record
// may hold its partition, and where it goes when that budget is exhausted.
//
// The zero value means "retry forever, no dead-letter topic". That is the
// safe default and it is also the only policy available without a destination:
// MaxAttempts and MaxBlock are ignored unless OnFailure selects dead_letter
// with a non-empty DeadLetterTopic, because there is no fourth outcome between
// delivering the record, parking it durably somewhere, and losing it.
type EmitRetryConfig struct {
	// OnFailure is onEmitFailureDeadLetter to park a record whose retry budget
	// is exhausted. Empty (or onEmitFailureRetry) retries forever.
	OnFailure string
	// DeadLetterTopic receives the parked record. Required when OnFailure is
	// dead_letter; configFromParams rejects the policy without it rather than
	// silently falling back to the unbounded retry.
	DeadLetterTopic string
	// MaxAttempts bounds the number of emit attempts before the record is
	// parked. Zero selects defaultEmitRetryMaxAttempts. Read only under
	// OnFailure=dead_letter.
	MaxAttempts int
	// MaxBlock bounds the wall time the partition may spend holding one record
	// before it is parked. Zero selects defaultEmitRetryMaxBlock. Read only
	// under OnFailure=dead_letter.
	MaxBlock time.Duration
}

// deadLetterReady reports whether the record can actually be parked: the policy
// is selected AND a destination is configured. A direct-constructed
// ConsumerConfig with dead_letter but no topic keeps retrying instead of
// committing, which is the fail-closed direction the params path cannot reach
// (emitRetryFromParams rejects that combination at activation).
func (c EmitRetryConfig) deadLetterReady() bool {
	return c.OnFailure == onEmitFailureDeadLetter && c.DeadLetterTopic != ""
}

func (c EmitRetryConfig) effectiveMaxAttempts() int {
	if c.MaxAttempts > 0 {
		return c.MaxAttempts
	}
	return defaultEmitRetryMaxAttempts
}

func (c EmitRetryConfig) effectiveMaxBlock() time.Duration {
	if c.MaxBlock > 0 {
		return c.MaxBlock
	}
	return defaultEmitRetryMaxBlock
}

// isZero reports whether nothing was configured, so RawParams can omit the
// object entirely and leave stored workflow definitions byte-identical.
func (c EmitRetryConfig) isZero() bool { return c == EmitRetryConfig{} }

// rawParams serializes the settings that were explicitly set. Durations
// serialize as strings so the YAML path and the Go DSL path agree on the wire
// form, the same convention tuning.rawParams and aggregate.flush_interval use.
//
// The topic is written only for the policy that reads it. A dead_letter policy
// with an empty topic is written as the policy alone (no topic key), which
// configFromParams rejects at activation: serializing nothing would look like a
// working definition that quietly retries forever instead of parking records.
func (c EmitRetryConfig) rawParams() map[string]any {
	out := map[string]any{}
	if c.OnFailure != "" {
		out["on_failure"] = c.OnFailure
	}
	if c.OnFailure == onEmitFailureDeadLetter && c.DeadLetterTopic != "" {
		out["dead_letter_topic"] = c.DeadLetterTopic
	}
	if c.MaxAttempts > 0 {
		out["max_attempts"] = c.MaxAttempts
	}
	if c.MaxBlock > 0 {
		out["max_block"] = c.MaxBlock.String()
	}
	return out
}

// emitRetryFromParams parses the optional `emit_retry` param object:
//
//	{"on_failure": "retry|dead_letter", "dead_letter_topic": "t-dlq",
//	 "max_attempts": 10, "max_block": "30s"}
//
// An explicit "retry" is normalized to the zero value so that the YAML form and
// the absent form are the same config (and the same definition hash). An
// unknown on_failure, a dead_letter policy without a topic, or a non-positive
// bound is an error rather than a fallback: a config that asked not to lose
// messages must never be quietly downgraded to a policy that loses them.
func emitRetryFromParams(v any) (EmitRetryConfig, error) {
	if v == nil {
		return EmitRetryConfig{}, nil
	}
	raw, ok := v.(map[string]any)
	if !ok {
		converted, err := cast.ToStringMapE(v)
		if err != nil {
			return EmitRetryConfig{}, fmt.Errorf("kafka emit_retry must be an object")
		}
		raw = converted
	}
	cfg := EmitRetryConfig{
		OnFailure:       strings.ToLower(strings.TrimSpace(cast.ToString(raw["on_failure"]))),
		DeadLetterTopic: strings.TrimSpace(cast.ToString(raw["dead_letter_topic"])),
	}
	switch cfg.OnFailure {
	case "", onEmitFailureRetry:
		// Normalize to the zero value, topic included: a destination without the
		// policy that reads it is not part of this config, and keeping it would
		// make an explicit "retry" round-trip differently from an absent block.
		cfg = EmitRetryConfig{}
	case onEmitFailureDeadLetter:
		if cfg.DeadLetterTopic == "" {
			return EmitRetryConfig{}, fmt.Errorf("kafka emit_retry on_failure %q requires dead_letter_topic", cfg.OnFailure)
		}
	default:
		return EmitRetryConfig{}, fmt.Errorf("kafka emit_retry on_failure %q is not supported (supported: %s, %s)",
			cfg.OnFailure, onEmitFailureRetry, onEmitFailureDeadLetter)
	}
	if raw["max_attempts"] != nil {
		n, err := cast.ToIntE(raw["max_attempts"])
		if err != nil || n <= 0 {
			return EmitRetryConfig{}, fmt.Errorf("kafka emit_retry max_attempts must be a positive number")
		}
		cfg.MaxAttempts = n
	}
	if raw["max_block"] != nil {
		d, err := conv.PositiveDuration(raw["max_block"])
		if err != nil {
			return EmitRetryConfig{}, fmt.Errorf("kafka emit_retry max_block: %w", err)
		}
		cfg.MaxBlock = d
	}
	return cfg, nil
}

// retryBudgetExhausted reports whether a record that has already failed
// `failures` times, for `blockedFor` so far, must be parked instead of retried
// again.
func (c EmitRetryConfig) retryBudgetExhausted(failures int, blockedFor time.Duration) bool {
	return failures >= c.effectiveMaxAttempts() || blockedFor >= c.effectiveMaxBlock()
}

// deliverLegacy is the legacy (non-entry-seed, non-aggregate) emit path's
// per-record delivery policy. It emits msg and, when the emit fails, holds this
// partition and retries the SAME record with exponential backoff until one of
// three things happens:
//
//   - the emit succeeds: the offset is committed and the worker moves on;
//   - the retry budget is exhausted and a dead-letter topic is configured: the
//     record is republished there, and only a successful republish commits its
//     offset;
//   - ctx is canceled: the function returns without committing anything.
//
// This function replacing "emit once, then move on" is the whole fix. The
// previous code left the failed record's offset uncommitted but continued with
// the next record, and the commit that followed a later success advanced the
// partition's committed offset past every earlier one. Kafka replays only from
// the committed offset, so the failed record was never redelivered in that
// process: the comment's "leaving the message redelivered" was true only across
// a restart or rebalance, and the normal case lost the record. Holding the
// partition is what makes "not committed" mean "will be retried" — the same
// property the entry-seed path gets from seedEntryBatch's refusal loop.
//
// No dead-letter topic means NO bound: the retry runs until cancellation. A
// bounded retry with nowhere to put the record would have to end in a commit
// past it (loss) or in an indefinitely stopped partition with no preserved
// copy (an outage), so the budget fields are consulted only when the record can
// actually be parked. The hold is visible as OnConsumptionBlocked and as the
// emit-failure log line.
//
// Delivery stays at-least-once. A retry of a record is an extra delivery of the
// same event ID (topic/partition/offset), and a crash after a successful Emit
// but before its commit redelivers that event on restart; both are absorbed by
// the downstream idempotency contract keyed on that ID. There is deliberately
// no pre-emit dedup: the removed SETNX marked a record "seen" before emitting
// it, so a crash in between lost it forever (see emitMessage's P0-1 note).
func (w *partitionWorker) deliverLegacy(ctx context.Context, msg Message) {
	if ctx.Err() != nil {
		// The activation is shutting down. Records already queued on this
		// worker's channel are left uncommitted (the next generation
		// redelivers them) rather than emitted against a dead context: a real
		// Emit would fail anyway, and starting a fresh retry loop here would
		// only delay shutdown.
		return
	}
	rt := w.rt
	cfg := rt.emitRetry
	// The publisher is built eagerly at activation whenever the policy needs it,
	// so a nil here means a hand-built runtime or a construction seam that
	// returned nil without an error. Treat it as "cannot park": retry the emit
	// forever rather than commit past a record that is not durably anywhere.
	deadLetter := cfg.deadLetterReady() && rt.deadLetterPublisher != nil

	attempts := 0
	var lastErr error
	started := time.Now()
	blocked := false
	defer func() {
		if blocked {
			// Cleared on EVERY exit path, cancellation included: a gauge left at 1
			// by a shutdown would report the partition as blocked forever.
			obs().OnConsumptionBlocked(ctx, msg.Topic, msg.Partition, false)
		}
	}()

	for {
		err := emitMessage(ctx, rt.in, msg)
		if err == nil {
			if attempts > 0 {
				slog.Info("kafka trigger emit succeeded after retries; committing offset",
					"topic", msg.Topic,
					"partition", msg.Partition,
					"offset", msg.Offset,
					"node", rt.in.NodeName,
					"attempts", attempts+1)
			}
			if blocked {
				blocked = false
				obs().OnConsumptionBlocked(ctx, msg.Topic, msg.Partition, false)
			}
			_ = commitMessages(rt.baseCtx, rt.consumer, msg)
			return
		}
		lastErr = err
		attempts++
		if ctx.Err() != nil {
			return
		}
		if !blocked {
			blocked = true
			obs().OnConsumptionBlocked(ctx, msg.Topic, msg.Partition, true)
			slog.Warn("kafka trigger emit is failing; HOLDING this partition and retrying "+
				"the same record (nothing behind it is emitted, and no commit may pass it)",
				"topic", msg.Topic,
				"partition", msg.Partition,
				"offset", msg.Offset,
				"node", rt.in.NodeName,
				"attempt", attempts,
				"dead_letter", deadLetter,
				"max_attempts", cfg.effectiveMaxAttempts(),
				"max_block", cfg.effectiveMaxBlock().String(),
				"err", err)
		}
		if deadLetter && cfg.retryBudgetExhausted(attempts, time.Since(started)) {
			break
		}
		if !waitEmitRetry(ctx, attempts) {
			return
		}
	}

	// Dead-letter phase. The emit budget is spent, so no further emit is
	// attempted: the only obligation left is to park the record durably, and
	// racing the two outcomes would make the commit decision depend on which
	// one happened to succeed. A failed republish is backpressure, not a drop —
	// the partition stays held and the republish is retried, exactly as the
	// aggregate overflow path treats a failing dead-letter write.
	reason := emitFailureReason(lastErr)
	slog.Warn("kafka trigger emit retry budget exhausted; publishing the record to the "+
		"dead-letter topic before committing its offset",
		"topic", msg.Topic,
		"partition", msg.Partition,
		"offset", msg.Offset,
		"node", rt.in.NodeName,
		"dead_letter_topic", cfg.DeadLetterTopic,
		"attempts", attempts,
		"blocked_for", time.Since(started).String(),
		"reason", reason)
	publishAttempts := 0
	for {
		err := rt.deadLetterPublisher.Publish(ctx, cfg.DeadLetterTopic, msg, reason)
		if err == nil {
			obs().OnMessageDeadLettered(rt.baseCtx, msg.Topic, "ok")
			logEmitDeadLettered(msg, true, cfg.DeadLetterTopic, "")
			if blocked {
				blocked = false
				obs().OnConsumptionBlocked(ctx, msg.Topic, msg.Partition, false)
			}
			_ = commitMessages(rt.baseCtx, rt.consumer, msg)
			return
		}
		obs().OnMessageDeadLettered(rt.baseCtx, msg.Topic, "error")
		publishAttempts++
		logEmitDeadLettered(msg, false, cfg.DeadLetterTopic, err.Error())
		if ctx.Err() != nil {
			return
		}
		if !waitEmitRetry(ctx, publishAttempts) {
			return
		}
	}
}

// emitRetryDelay returns how long to wait before the next attempt of a record
// that has already failed `attempt` times.
//
// The un-jittered delay doubles from emitRetryInitialDelay to
// emitRetryMaxDelay, and the wait is then drawn uniformly from the top half of
// that window ("equal jitter"): the first wait is 50-100ms and the capped ones
// 2.5-5s. The jitter is not decoration — a downstream outage fails every
// partition's record at roughly the same moment, and without it they would
// retry in lockstep forever, turning one shared dependency's recovery into a
// synchronised pulse of load.
func emitRetryDelay(attempt int) time.Duration {
	delay := emitRetryInitialDelay
	for i := 1; i < attempt && delay < emitRetryMaxDelay; i++ {
		delay *= 2
	}
	if delay > emitRetryMaxDelay {
		delay = emitRetryMaxDelay
	}
	half := delay / 2
	return half + time.Duration(rand.Int63n(int64(half)+1))
}

// waitEmitRetry blocks the partition's worker until the next attempt is due.
// It reports whether the wait completed; false means the activation was
// canceled and the record must not be emitted or committed again.
func waitEmitRetry(ctx context.Context, attempt int) bool {
	timer := time.NewTimer(emitRetryDelay(attempt))
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// logEmitDeadLettered reports one emit-failed record's dead-letter disposition.
//
// Throttled per topic+partition and keyed by partition for the same reasons
// logOverflowDeadLetter is: one worker runs per partition, so a topic-only key
// would let the busiest partition's line suppress a different partition's, and
// the offset printed on the surviving line would belong only to the winner. The
// two outcomes get different discriminators because a partition whose DLQ
// writes are landing and one whose DLQ is unreachable are separate incidents.
func logEmitDeadLettered(msg Message, ok bool, topic, detail string) {
	discriminator := "emit_dlq"
	action := "republished to the dead-letter topic " + topic + "; its offset may now " +
		"be committed past, because the record and its provenance are durably preserved there"
	if !ok {
		discriminator = "emit_dlq_failed"
		action = "dead-letter publish FAILED (" + detail + "); the partition stays " +
			"halted and no commit may pass this offset until the record is durably parked"
	}
	emit, count := discardLog.allow(time.Now(),
		msg.Topic+"\x00"+discriminator+"\x00"+strconv.Itoa(msg.Partition))
	if !emit {
		return
	}
	slog.Warn("kafka trigger legacy emit failed past its retry budget",
		"topic", msg.Topic,
		"partition", msg.Partition,
		"offset", msg.Offset,
		"dead_letter_topic", topic,
		"action", action,
		"occurrences", count)
}
