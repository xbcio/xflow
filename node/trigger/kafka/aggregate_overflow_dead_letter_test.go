package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/trigger/triggertest"
	"github.com/xbcio/xflow/types"
)

// --- dead-letter publisher fake for the overflow axis --------------------

// dlqRecord is one Publish call: what was written, where, and with which reason.
// The reason is part of the record because it is the one thing about a parked
// message that the message itself cannot carry, and the two axes that publish
// through one shared publisher must not be able to claim each other's
// provenance.
type dlqRecord struct {
	topic  string
	reason string
	msg    Message
}

// overflowPublisher substitutes for newDeadLetterPublisher's product. It records
// every ATTEMPT (succeeded or not) and can be switched to failing at runtime,
// which is what lets a test observe the move from "the DLQ is down and the
// partition is holding" to "the DLQ recovered and the record is parked" without
// rebuilding the trigger.
type overflowPublisher struct {
	mu       sync.Mutex
	attempts []dlqRecord
	parked   []dlqRecord
	failing  atomic.Bool
	closed   bool
	notify   chan struct{}
}

func newOverflowPublisher() *overflowPublisher {
	return &overflowPublisher{notify: make(chan struct{})}
}

func (p *overflowPublisher) Publish(_ context.Context, topic string, msg Message, reason string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	rec := dlqRecord{topic: topic, reason: reason, msg: msg}
	p.attempts = append(p.attempts, rec)
	if p.failing.Load() {
		// Deliberately NOT recorded as parked: a publisher that returns an error
		// has not durably preserved anything, and a fake that recorded it anyway
		// would let a test prove preservation out of its own bookkeeping.
		close(p.notify)
		p.notify = make(chan struct{})
		return errors.New("dead-letter broker unavailable (overflow probe)")
	}
	p.parked = append(p.parked, rec)
	close(p.notify)
	p.notify = make(chan struct{})
	return nil
}

func (p *overflowPublisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	return nil
}

func (p *overflowPublisher) snapshot() (attempts, parked []dlqRecord) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]dlqRecord(nil), p.attempts...), append([]dlqRecord(nil), p.parked...)
}

// waitForParked blocks until at least n publishes have SUCCEEDED.
func (p *overflowPublisher) waitForParked(n int, timeout time.Duration) bool {
	return p.waitFor(timeout, func(_, parked []dlqRecord) bool { return len(parked) >= n })
}

// waitForAttempts blocks until at least n publishes have been ATTEMPTED,
// including failed ones — the only way to learn the offset of a record the DLQ
// refused.
func (p *overflowPublisher) waitForAttempts(n int, timeout time.Duration) bool {
	return p.waitFor(timeout, func(attempts, _ []dlqRecord) bool { return len(attempts) >= n })
}

func (p *overflowPublisher) waitFor(timeout time.Duration, pred func(attempts, parked []dlqRecord) bool) bool {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		attempts, parked := p.snapshot()
		if pred(attempts, parked) {
			return true
		}
		p.mu.Lock()
		notify := p.notify
		p.mu.Unlock()
		select {
		case <-notify:
		case <-deadline.C:
			return false
		}
	}
}

// --- activation harness --------------------------------------------------

// overflowHarness owns the one thing a test has to be able to do mid-run:
// release the wedged downstream, so the commit frontier starts moving.
type overflowHarness struct {
	rt      *triggertest.FakeRuntime
	release chan struct{}
	once    sync.Once
}

// releaseDownstream lets every failed emit succeed from now on. Idempotent:
// phase-based tests release mid-run and the harness releases again at cleanup.
func (h *overflowHarness) releaseDownstream() {
	h.once.Do(func() { close(h.release) })
}

// activateOverflowFixture activates a Kafka aggregate trigger whose downstream
// is wedged until releaseDownstream is called, against the supplied consumer and
// dead-letter publisher.
//
// It goes through Node.Activate rather than activateAggregate on purpose: that
// the publisher is reachable from the overflow path at all is part of what is
// under test, and a fixture that injected it straight into the runtime would
// never notice that activation declines to build one.
func activateOverflowFixture(t *testing.T, consumer Consumer, publisher DeadLetterPublisher, maxSize int, policy, dlqTopic string) *overflowHarness {
	t.Helper()

	origConsumer := newConsumer
	origPublisher := newDeadLetterPublisher
	t.Cleanup(func() {
		newConsumer = origConsumer
		newDeadLetterPublisher = origPublisher
	})
	newConsumer = func(ConsumerConfig) (Consumer, error) { return consumer, nil }
	newDeadLetterPublisher = func(ConsumerConfig) (DeadLetterPublisher, error) { return publisher, nil }

	release := make(chan struct{})
	rt := triggertest.NewFakeRuntime()
	rt.SetEmitFunc(func(ctx context.Context, _ types.WorkflowID, _ string, _ *types.TriggerEvent) (types.ExecutionID, error) {
		select {
		case <-release:
			return "exec-1", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	})

	tr := New().Brokers("localhost:9092").Topic("t").Group("g").
		AggregateByPartition(maxSize, 20*time.Millisecond)
	switch policy {
	case onOverflowBlock:
		tr = tr.BlockOnOverflow()
	case onOverflowDeadLetter:
		tr = tr.DeadLetterOnOverflow(dlqTopic)
	}
	sub, err := tr.Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-dlq",
		NodeName:   "kafka",
		Params:     tr.RawParams().(map[string]any),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatal(err)
	}
	h := &overflowHarness{rt: rt, release: release}
	t.Cleanup(func() {
		// Release BEFORE closing, so a run that ended while the downstream was
		// still wedged is not measuring the shutdown path instead of the policy.
		h.releaseDownstream()
		_ = sub.Close(context.Background())
	})
	return h
}

// overflowFixtureMessages builds the fixture every arm runs on: payloads and
// keys that name their own offset, so a test can prove the record that reached
// the dead-letter topic is the one that was read, with its context attached,
// and not a neighbour's.
func overflowFixtureMessages(topic string, n int) []Message {
	out := make([]Message, n)
	for i := range out {
		out[i] = Message{
			Topic: topic, Partition: 0, Offset: int64(i),
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("payload-%d", i)),
		}
	}
	return out
}

// overflowRetainedBound is maxRetained for the fixtures in this file: maxSize
// records per pending batch and buffer slot, maxPartitionPendingBatches batches
// plus maxBufferedBatches buffered slots (emitSem is 64 by default, so maxPending
// is maxPartitionPendingBatches).
func overflowRetainedBound(maxSize int) int64 {
	return int64(maxSize * (maxBufferedBatches + maxPartitionPendingBatches))
}

// --- the three-way divergence --------------------------------------------

// TestKafkaAggregateOverflowPoliciesDivergeThreeWays runs ONE fixture through
// all three policies.
//
// Each arm is the positive control for the others, which is the only shape in
// which any of them is evidence. "dead_letter loses nothing" is satisfied by a
// fixture that never reached the cap, and "discard still discards" is satisfied
// by a build that discards under every policy; only the difference between the
// three arms over identical input distinguishes "a third policy was added" from
// "the policy field stopped being read".
//
// The dead_letter arm additionally asserts the whole of what the policy
// promises: the parked record carries the original topic, partition, offset,
// key and payload; it went to the configured dead-letter topic with the OVERFLOW
// reason (not the schema one, which would misattribute every parked record); it
// was counted as dead-lettered rather than discarded; and, the point of the
// exercise, the aggregator did not report it as a permanent loss.
func TestKafkaAggregateOverflowPoliciesDivergeThreeWays(t *testing.T) {
	const (
		maxSize        = 2
		deliveredCount = 60
		dlqTopic       = "t-overflow-dlq"
	)

	run := func(t *testing.T, policy string) (*recordingObserver, *overflowPublisher, *replayableKafkaConsumer) {
		t.Helper()
		consumer := newReplayableKafkaConsumer(overflowFixtureMessages("t", deliveredCount), nil)
		publisher := newOverflowPublisher()
		observer := installRecordingObserver(t)
		h := activateOverflowFixture(t, consumer, publisher, maxSize, policy, dlqTopic)

		// Wait for the state this policy is supposed to produce, then let it sit
		// so a policy that merely DELAYS the outcome is not mistaken for one that
		// prevents it. The wait is per-policy because the three have different
		// observable evidence; waiting on one signal for all three would leave two
		// arms waiting for something they can never produce.
		switch policy {
		case onOverflowDiscard:
			observer.waitFor(5*time.Second, func(d, _ []string) bool { return len(d) >= 1 })
		case onOverflowBlock:
			observer.waitForBlocked(5*time.Second, func(b []string) bool { return len(b) >= 1 })
		case onOverflowDeadLetter:
			publisher.waitForParked(1, 5*time.Second)
		}
		time.Sleep(300 * time.Millisecond)
		h.releaseDownstream()
		return observer, publisher, consumer
	}

	t.Run("discard still drops", func(t *testing.T) {
		observer, publisher, _ := run(t, onOverflowDiscard)
		discarded, deadLettered := observer.snapshot()
		if !containsString(discarded, "t/buffer_overflow") {
			t.Fatalf("discard arm discarded %v, want a buffer_overflow: the default policy "+
				"stopped dropping, which is a behaviour change nobody opted into", discarded)
		}
		if len(deadLettered) != 0 {
			t.Errorf("discard arm dead-lettered %v; it must not touch the DLQ", deadLettered)
		}
		if attempts, parked := publisher.snapshot(); len(attempts) != 0 || len(parked) != 0 {
			t.Errorf("discard arm published %d records; on_overflow=discard must not open a "+
				"dead-letter writer at all", len(attempts))
		}
	})

	t.Run("block still stalls and drops nothing", func(t *testing.T) {
		observer, publisher, _ := run(t, onOverflowBlock)
		discarded, deadLettered := observer.snapshot()
		if len(discarded) != 0 {
			t.Errorf("block arm discarded %v; the policy exists so that this list stays empty", discarded)
		}
		if len(deadLettered) != 0 {
			t.Errorf("block arm dead-lettered %v; it must not touch the DLQ", deadLettered)
		}
		if attempts, _ := publisher.snapshot(); len(attempts) != 0 {
			t.Errorf("block arm published %d records; on_overflow=block must not open a "+
				"dead-letter writer", len(attempts))
		}
		blocked := observer.blockedSnapshot()
		if len(blocked) == 0 {
			t.Fatal("block arm never reported OnConsumptionBlocked, so it neither discarded " +
				"nor stopped — the arm proves nothing about the policy")
		}
		if blocked[0] != "t/0/blocked" {
			t.Errorf("first block transition = %q, want t/0/blocked", blocked[0])
		}
	})

	t.Run("dead_letter parks the record and loses nothing", func(t *testing.T) {
		observer, publisher, _ := run(t, onOverflowDeadLetter)

		discarded, deadLettered := observer.snapshot()
		if len(discarded) != 0 {
			t.Fatalf("dead_letter arm discarded %v. The record was read and then reported as "+
				"a permanent loss, which is the exact outcome this policy was chosen to avoid",
				discarded)
		}
		if blocked := observer.blockedSnapshot(); len(blocked) != 0 {
			t.Errorf("dead_letter arm reported backpressure %v with a publisher that never "+
				"fails; the point of this policy over block is that it keeps consuming", blocked)
		}
		attempts, parked := publisher.snapshot()
		if len(parked) == 0 {
			t.Fatalf("dead_letter arm parked nothing (attempts=%d); the fixture reached the "+
				"retained bound of %d without the overflow record reaching the DLQ",
				len(attempts), overflowRetainedBound(maxSize))
		}
		if len(attempts) != len(parked) {
			t.Errorf("attempts = %d, parked = %d against a publisher that never fails; every "+
				"attempt here should have succeeded", len(attempts), len(parked))
		}
		okCount := 0
		for _, dl := range deadLettered {
			if dl == "t/ok" {
				okCount++
				continue
			}
			t.Errorf("dead_letter arm reported %q against a publisher that never fails", dl)
		}
		if okCount != len(parked) {
			t.Errorf("dead-letter observations = %d ok for %d parked records; the record is "+
				"being preserved without being counted, so an operator cannot see the "+
				"overflow this policy is absorbing", okCount, len(parked))
		}

		// The record's context travels with it. A DLQ entry whose provenance is
		// wrong is worse than none: a replay would re-drive the wrong offset.
		for _, rec := range parked {
			if rec.topic != dlqTopic {
				t.Fatalf("published to %q, want the configured %q", rec.topic, dlqTopic)
			}
			if rec.reason != deadLetterReasonOverflow {
				t.Fatalf("published with reason %q, want %q: the overflow axis must not claim "+
					"to be the schema axis inside the DLQ", rec.reason, deadLetterReasonOverflow)
			}
			if rec.msg.Topic != "t" || rec.msg.Partition != 0 {
				t.Errorf("parked record %s/%d, want t/0", rec.msg.Topic, rec.msg.Partition)
			}
			if want := fmt.Sprintf("payload-%d", rec.msg.Offset); string(rec.msg.Value) != want {
				t.Fatalf("parked offset %d carries payload %q, want %q — the record and its "+
					"offset do not belong together, so a replay would re-drive the wrong one",
					rec.msg.Offset, rec.msg.Value, want)
			}
			if want := fmt.Sprintf("key-%d", rec.msg.Offset); string(rec.msg.Key) != want {
				t.Errorf("parked offset %d carries key %q, want %q", rec.msg.Offset, rec.msg.Key, want)
			}
		}
		// The overflow branch, not some other path: the DLQ only starts receiving
		// once the retained bound is reached, so every parked offset must be at or
		// above it. A lower offset would mean records were parked while the
		// partition still had room — i.e. that this is not the overflow path.
		if first := parked[0].msg.Offset; first < overflowRetainedBound(maxSize) {
			t.Errorf("first parked offset = %d, below the retained bound %d; the DLQ is being "+
				"fed by something other than the overflow branch", first, overflowRetainedBound(maxSize))
		}
	})
}

// --- publish failure is backpressure, not loss ---------------------------

// TestKafkaAggregateOverflowDeadLetterPublishFailureHoldsAndStalls is the
// assertion that makes the policy safe rather than merely present.
//
// A dead-letter publish that fails must not be followed by a commit. If it were,
// the record would be gone — it was never emitted downstream (it is an overflow
// record, so it never entered a batch) and it is not durably anywhere else —
// while the metric reported it as safely parked. That is strictly worse than the
// plain discard, because the operator believes nothing was lost.
//
// Three separable claims, each with its own evidence:
//
//  1. Nothing commits past the held record's offset while its publish is failing.
//     The commit frontier is LIVE here (the downstream recovers and lower batches
//     commit), which is what makes this a real test: the hazard is a later,
//     higher commit sweeping past an unpublished offset.
//  2. Consumption STOPS. The producer hands messages over one at a time, so a
//     frozen handover count is the reader having stopped fetching — and it is
//     also why claim 1 holds for offsets not yet read.
//  3. It is recoverable, not wedged: when the DLQ comes back the record is
//     parked, the partition resumes, and the count moves again. A policy that
//     stalled forever on a transient DLQ blip would be block with extra steps.
func TestKafkaAggregateOverflowDeadLetterPublishFailureHoldsAndStalls(t *testing.T) {
	const (
		maxSize  = 2
		dlqTopic = "t-overflow-dlq"
	)

	observer := installRecordingObserver(t)
	publisher := newOverflowPublisher()
	publisher.failing.Store(true)
	consumer := newUnboundedProducerConsumer("t", 0)

	h := activateOverflowFixture(t, consumer, publisher, maxSize, onOverflowDeadLetter, dlqTopic)

	if !publisher.waitForAttempts(1, 5*time.Second) {
		t.Fatal("the DLQ was never asked to park anything in 5s, so the retained bound was " +
			"never reached and this run says nothing about a failed publish")
	}
	attempts, parked := publisher.snapshot()
	heldOffset := attempts[0].msg.Offset
	if len(parked) != 0 {
		t.Fatalf("a publish SUCCEEDED against a publisher set to fail (%v)", parked)
	}
	if attempts[0].msg.Topic != "t" || attempts[0].msg.Partition != 0 {
		t.Fatalf("first attempt was for %s/%d, want t/0", attempts[0].msg.Topic, attempts[0].msg.Partition)
	}
	if attempts[0].topic != dlqTopic || attempts[0].reason != deadLetterReasonOverflow {
		t.Errorf("failed attempt targeted %q with reason %q, want %q/%q",
			attempts[0].topic, attempts[0].reason, dlqTopic, deadLetterReasonOverflow)
	}

	// Claims 1 and 2 on the wedged phase. The error must be counted: a DLQ outage
	// is exactly when an operator needs the counter to fire.
	if !observer.waitFor(5*time.Second, func(_, deadLettered []string) bool { return len(deadLettered) >= 1 }) {
		t.Fatal("no dead-letter observation for a publish that failed; a DLQ outage would " +
			"be invisible")
	}
	if _, deadLettered := observer.snapshot(); !containsString(deadLettered, "t/error") {
		t.Fatalf("dead-letter observations = %v, want a t/error: a failed publish is being "+
			"reported as a successfully parked record", deadLettered)
	}
	if discarded, _ := observer.snapshot(); len(discarded) != 0 {
		t.Fatalf("discarded %v while the dead-letter publish was failing; a failed publish "+
			"must never fall back to the drop this policy was chosen to avoid", discarded)
	}
	settle(t, &consumer.sent)
	stoppedAt := consumer.sent.Load()
	if bound := overflowRetainedBound(maxSize); stoppedAt < bound {
		t.Fatalf("the producer handed over only %d messages, below the retained bound of %d, "+
			"so this run never reached the overflow it is testing", stoppedAt, bound)
	}
	if !observer.waitForBlocked(5*time.Second, func(b []string) bool { return len(b) >= 1 }) {
		t.Fatal("the partition kept consuming with a failed dead-letter publish; the next " +
			"record it reads has a higher offset than the held one, and committing it would " +
			"sweep past a record that is not durably anywhere")
	}
	if blocked := observer.blockedSnapshot(); blocked[0] != "t/0/blocked" {
		t.Errorf("first backpressure transition = %q, want t/0/blocked", blocked[0])
	}

	// Phase 2: a LIVE commit frontier. Release the downstream so the lower batches
	// succeed and commit while the DLQ is still down.
	h.releaseDownstream()
	time.Sleep(400 * time.Millisecond)

	committed := consumer.committedOffsets()
	highest := int64(-1)
	for _, off := range committed {
		if off > highest {
			highest = off
		}
	}
	if highest >= heldOffset {
		t.Fatalf("committed offset %d while offset %d is held unpublished (committed=%v); "+
			"Kafka commits everything below the highest offset it is given, so this erases a "+
			"record that was never emitted and is not in the DLQ — the silent loss this policy "+
			"exists to prevent", highest, heldOffset, committed)
	}
	if after := consumer.sent.Load(); after != stoppedAt {
		t.Errorf("consumption continued while a record was held (%d -> %d handovers); the held "+
			"record is the highest offset read, so reading further can only put offsets above "+
			"it at risk", stoppedAt, after)
	}

	// Phase 3: recovery. The record is parked and the partition resumes.
	publisher.failing.Store(false)
	if !publisher.waitForParked(1, 5*time.Second) {
		t.Fatal("the held record was never parked after the DLQ recovered; the retry path " +
			"does not fire, so this policy wedges the partition on a transient DLQ blip")
	}
	if !observer.waitForBlocked(5*time.Second, func(b []string) bool {
		return containsString(b, "t/0/resumed")
	}) {
		t.Errorf("the partition never reported resuming after the DLQ recovered: %v",
			observer.blockedSnapshot())
	}
	deadline := time.Now().Add(5 * time.Second)
	for consumer.sent.Load() == stoppedAt && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if consumer.sent.Load() == stoppedAt {
		t.Errorf("consumption never resumed after the held record was parked (%d handovers, "+
			"flat for 5s)", stoppedAt)
	}
}

// TestKafkaAggregateOverflowDeadLetterWithoutPublisherFailsClosed covers the one
// way to reach the overflow branch without a publisher: a runtime whose
// activation did not build one. It must withhold, not drop.
//
// The schema axis already has this branch (handleInvalidMessage's nil-publisher
// case) and for the same reason: "no publisher" is a deployment error, and the
// only safe reading of it is "this record cannot be parked yet".
func TestKafkaAggregateOverflowDeadLetterWithoutPublisherFailsClosed(t *testing.T) {
	observer := installRecordingObserver(t)

	consumer := newReplayableKafkaConsumer(overflowFixtureMessages("t", 60), nil)
	origConsumer := newConsumer
	origPublisher := newDeadLetterPublisher
	t.Cleanup(func() {
		newConsumer = origConsumer
		newDeadLetterPublisher = origPublisher
	})
	newConsumer = func(ConsumerConfig) (Consumer, error) { return consumer, nil }
	// Activation returns (nil, nil): no publisher, no error. That is the state a
	// runtime built around a validated config can never reach, and the one this
	// test exists to pin.
	newDeadLetterPublisher = func(ConsumerConfig) (DeadLetterPublisher, error) { return nil, nil }

	release := make(chan struct{})
	rt := triggertest.NewFakeRuntime()
	rt.SetEmitFunc(func(ctx context.Context, _ types.WorkflowID, _ string, _ *types.TriggerEvent) (types.ExecutionID, error) {
		select {
		case <-release:
			return "exec-1", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	})

	tr := New().Brokers("localhost:9092").Topic("t").Group("g").
		AggregateByPartition(2, 20*time.Millisecond).
		DeadLetterOnOverflow("t-overflow-dlq")
	sub, err := tr.Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-dlq",
		NodeName:   "kafka",
		Params:     tr.RawParams().(map[string]any),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		close(release)
		_ = sub.Close(context.Background())
	}()

	if !observer.waitFor(5*time.Second, func(_, deadLettered []string) bool {
		return containsString(deadLettered, "t/error")
	}) {
		_, deadLettered := observer.snapshot()
		t.Fatalf("dead-letter observations = %v, want t/error; an overflow that cannot be "+
			"parked must be reported as an error, not swallowed", deadLettered)
	}
	if discarded, _ := observer.snapshot(); len(discarded) != 0 {
		t.Fatalf("discarded %v with no publisher available; the policy must fail closed (hold "+
			"and stall) rather than downgrade to discard", discarded)
	}
	if !observer.waitForBlocked(5*time.Second, func(b []string) bool { return len(b) >= 1 }) {
		t.Error("no backpressure was reported with the record un-parked; the partition kept " +
			"consuming past an offset that is not durable anywhere")
	}
}

// --- validation and wiring -----------------------------------------------

// TestKafkaAggregateOnOverflowDeadLetterValidation pins the fail-closed rule and
// the compatibility rule on the same table.
//
// dead_letter without a topic is rejected rather than defaulted because there is
// no safe default: discard is the policy the operator was leaving, and block is a
// trade they did not choose. The unset row is the other half — upgrading must
// still land every existing deployment on discard — and it is asserted here as
// well as in aggregate_overflow_policy_test.go because this table is what a
// future change to the policy switch will be read against.
func TestKafkaAggregateOnOverflowDeadLetterValidation(t *testing.T) {
	tests := []struct {
		name     string
		overflow any
		topic    any
		want     string
		wantErr  bool
		errNames []string
	}{
		{name: "absent is discard", want: onOverflowDiscard},
		{name: "block needs no topic", overflow: "block", want: onOverflowBlock},
		{
			name: "dead_letter with a topic", overflow: "dead_letter", topic: "events-dlq",
			want: onOverflowDeadLetter,
		},
		{
			name: "dead_letter tolerates case and space", overflow: "  DEAD_LETTER ", topic: "events-dlq",
			want: onOverflowDeadLetter,
		},
		{
			name: "dead_letter without a topic", overflow: "dead_letter", wantErr: true,
			errNames: []string{"on_overflow", "dead_letter_topic"},
		},
		{
			name: "dead_letter with a blank topic", overflow: "dead_letter", topic: "   ", wantErr: true,
			errNames: []string{"dead_letter_topic"},
		},
		{
			name: "an unrecognised value still fails", overflow: "dead_letters", wantErr: true,
			errNames: []string{"on_overflow"},
		},
		{
			name: "a stray topic under discard is ignored", overflow: "discard", topic: "unused-dlq",
			want: onOverflowDiscard,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			raw := map[string]any{"enabled": true, "by": "partition", "max_size": 10}
			if tc.overflow != nil {
				raw["on_overflow"] = tc.overflow
			}
			if tc.topic != nil {
				raw["dead_letter_topic"] = tc.topic
			}
			cfg, err := aggregateConfigFromParamForMode(raw, false)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("on_overflow=%v topic=%v was accepted as %q; a config that asked "+
						"not to lose records must never be quietly downgraded",
						tc.overflow, tc.topic, cfg.OnOverflow)
				}
				for _, want := range tc.errNames {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("error %q does not name %q, the field the operator has to fix",
							err, want)
					}
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if cfg.OnOverflow != tc.want {
				t.Fatalf("OnOverflow = %q, want %q", cfg.OnOverflow, tc.want)
			}
		})
	}
}

// TestKafkaAggregateDeadLetterOnOverflowRoundTripsThroughParams is the wiring
// test for the builder, and nothing else in this file can replace it.
//
// AggregateConfig round-trips through RawParams, which rebuilds the aggregate
// map key by key. A key that is not listed there does not survive the trip: the
// setter appears to work, the config normalizes cleanly, and the runtime on the
// other side reads an empty topic and rejects the activation. The topic is
// therefore asserted AFTER a full serialize-and-reparse, not on the builder's
// struct.
func TestKafkaAggregateDeadLetterOnOverflowRoundTripsThroughParams(t *testing.T) {
	base := func() *Node {
		return New().Brokers("b").Topic("t").Group("g").
			AggregateByPartition(10, 100*time.Millisecond)
	}

	t.Run("dead_letter and its topic survive", func(t *testing.T) {
		params := base().DeadLetterOnOverflow("t-overflow-dlq").RawParams().(map[string]any)
		cfg, err := configFromParams(params, nil, false)
		if err != nil {
			t.Fatalf("configFromParams: %v", err)
		}
		if cfg.Aggregate.OnOverflow != onOverflowDeadLetter {
			t.Fatalf("OnOverflow = %q after a round trip, want %q; the deployment asked to "+
				"park overflowing records and would be discarding them instead",
				cfg.Aggregate.OnOverflow, onOverflowDeadLetter)
		}
		if cfg.Aggregate.DeadLetterTopic != "t-overflow-dlq" {
			t.Fatalf("DeadLetterTopic = %q after a round trip, want %q; the records would be "+
				"published to an empty topic name", cfg.Aggregate.DeadLetterTopic, "t-overflow-dlq")
		}
	})

	t.Run("neither key is serialized for an unset node", func(t *testing.T) {
		agg := base().RawParams().(map[string]any)["aggregate"].(map[string]any)
		for _, key := range []string{"on_overflow", "dead_letter_topic"} {
			if _, present := agg[key]; present {
				t.Errorf("%s is serialized for a node that never set it, which moves the "+
					"definition hash of every existing aggregating workflow: %v", key, agg)
			}
		}
		cfg, err := configFromParams(map[string]any{
			"brokers": []any{"b"}, "topic": "t", "group": "g", "aggregate": agg,
		}, nil, false)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Aggregate.OnOverflow != onOverflowDiscard {
			t.Fatalf("OnOverflow = %q for an unset node, want %q",
				cfg.Aggregate.OnOverflow, onOverflowDiscard)
		}
	})

	t.Run("a dead_letter node without a topic fails activation", func(t *testing.T) {
		params := base().RawParams().(map[string]any)
		params["aggregate"].(map[string]any)["on_overflow"] = onOverflowDeadLetter
		orig := newConsumer
		newConsumer = func(ConsumerConfig) (Consumer, error) {
			t.Error("a consumer was opened for a config that must be rejected first")
			return newReplayableKafkaConsumer(nil, nil), nil
		}
		t.Cleanup(func() { newConsumer = orig })
		_, err := New().Activate(context.Background(), &types.TriggerActivateInput{
			WorkflowID: "wf-1", NodeName: "kafka", Params: params,
			Runtime: triggertest.NewFakeRuntime(),
		})
		if err == nil {
			t.Fatal("activation succeeded with on_overflow=dead_letter and no dead_letter_topic; " +
				"the record would have nowhere to go and the drop this policy was chosen to " +
				"avoid would be silent")
		}
		if !strings.Contains(err.Error(), "dead_letter_topic") {
			t.Errorf("error %q does not name dead_letter_topic", err)
		}
	})
}

// TestKafkaActivateBuildsPublisherForOverflowDeadLetterPolicy pins that
// activation opens the writer for the overflow axis too, and only for a policy
// that needs it.
//
// Without this the policy is inert: the runtime would find a nil publisher and
// fail closed into a stalled partition on the first overflow, which reads as a
// trigger that stops consuming under load for no stated reason.
func TestKafkaActivateBuildsPublisherForOverflowDeadLetterPolicy(t *testing.T) {
	origConsumer := newConsumer
	origPublisher := newDeadLetterPublisher
	t.Cleanup(func() {
		newConsumer = origConsumer
		newDeadLetterPublisher = origPublisher
	})

	activate := func(t *testing.T, aggregateParams map[string]any) (*overflowPublisher, int) {
		t.Helper()
		consumer := newReplayableKafkaConsumer(nil, nil)
		newConsumer = func(ConsumerConfig) (Consumer, error) { return consumer, nil }
		publisher := newOverflowPublisher()
		built := 0
		newDeadLetterPublisher = func(ConsumerConfig) (DeadLetterPublisher, error) {
			built++
			return publisher, nil
		}
		sub, err := New().Activate(context.Background(), &types.TriggerActivateInput{
			WorkflowID: "wf-1",
			NodeName:   "kafka",
			Params: map[string]any{
				"brokers": []any{"localhost:9092"}, "topic": "t", "group": "g",
				"aggregate": aggregateParams,
			},
			Runtime: triggertest.NewFakeRuntime(),
		})
		if err != nil {
			t.Fatalf("Activate: %v", err)
		}
		// Each call needs its own consumer: Close closes it.
		t.Cleanup(func() { _ = sub.Close(context.Background()) })
		return publisher, built
	}

	t.Run("dead_letter opens exactly one writer", func(t *testing.T) {
		publisher, built := activate(t, map[string]any{
			"enabled": true, "by": "partition", "max_size": 10,
			"on_overflow": onOverflowDeadLetter, "dead_letter_topic": "t-overflow-dlq",
		})
		if built != 1 {
			t.Errorf("publisher constructed %d times, want 1 (eagerly at activation)", built)
		}
		if publisher == nil {
			t.Error("no publisher was built for on_overflow=dead_letter")
		}
	})

	t.Run("block opens none", func(t *testing.T) {
		_, built := activate(t, map[string]any{
			"enabled": true, "by": "partition", "max_size": 10, "on_overflow": onOverflowBlock,
		})
		if built != 0 {
			t.Errorf("publisher constructed %d times under on_overflow=block, want 0: a Kafka "+
				"producer connection per trigger that never uses it is a real resource cost", built)
		}
	})
}

// --- helpers -------------------------------------------------------------

func containsString(list []string, want string) bool {
	for _, item := range list {
		if item == want {
			return true
		}
	}
	return false
}
