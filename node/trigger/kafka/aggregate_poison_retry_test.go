package kafka

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/trigger/triggertest"
	"github.com/xbcio/xflow/types"
)

// TestAggregateRetryDelayEscalatesOnlyAfterTheFlatWindow pins both halves of the
// retry cadence against each other, because each one is the other's regression.
//
// The flat window exists because exponential backoff from the first attempt kept
// a partition stalled long after a short downstream outage had recovered. The
// escalation exists because without it a poison batch re-runs 100 messages
// through the guest at the flush cadence forever, on a partition whose commit
// frontier is frozen anyway. Remove either and the other's incident comes back.
func TestAggregateRetryDelayEscalatesOnlyAfterTheFlatWindow(t *testing.T) {
	const base = 200 * time.Millisecond

	for attempts := 1; attempts <= aggregateFlatRetryAttempts; attempts++ {
		if got := aggregateRetryDelay(base, attempts); got != base {
			t.Errorf("✗ attempt %d delay = %s, want the flat %s. Backing off inside "+
				"the flat window is the regression that kept a partition stalled "+
				"after a brief outage recovered.", attempts, got, base)
		}
	}

	prev := base
	for attempts := aggregateFlatRetryAttempts + 1; attempts <= aggregateFlatRetryAttempts+12; attempts++ {
		got := aggregateRetryDelay(base, attempts)
		if got < prev {
			t.Fatalf("✗ attempt %d delay = %s went backwards from %s", attempts, got, prev)
		}
		if got > aggregateMaxRetryDelay {
			t.Fatalf("✗ attempt %d delay = %s exceeds the cap %s; an unbounded delay "+
				"turns a recoverable outage into an outage that needs a restart",
				attempts, got, aggregateMaxRetryDelay)
		}
		prev = got
	}
	if prev != aggregateMaxRetryDelay {
		t.Errorf("✗ delay plateaued at %s, want the %s cap to be reached within "+
			"12 attempts past the flat window", prev, aggregateMaxRetryDelay)
	}

	// A zero base must not produce a zero delay: that would be a busy loop, not a
	// retry schedule.
	if got := aggregateRetryDelay(0, 1); got != defaultAggregateFlushInterval {
		t.Errorf("✗ zero base delay = %s, want %s", got, defaultAggregateFlushInterval)
	}
}

// TestAggregatePoisonBatchStopsBurningCapacity is the wiring proof.
//
// TestAggregateRetryDelayEscalatesOnlyAfterTheFlatWindow only proves the
// function computes the right number; calling it with batch.attempts is a
// separate fact, and batch.attempts was incremented but read by nothing at all
// before this. A permanently failing downstream is the observable difference:
// at the flat cadence it is re-attempted once per FlushInterval for the life of
// the process.
//
// Measured in the incident: 30 group executions per second per partition, each
// re-running 100 messages through the wasm guest, for as long as the poison
// record stayed at the head. Four partitions had not advanced an offset in 90
// seconds.
//
// The bound is measured against its own counterfactual: raising
// aggregateFlatRetryAttempts high enough that the escalation never engages takes
// this same harness from 18 downstream calls to 149.
func TestAggregatePoisonBatchStopsBurningCapacity(t *testing.T) {
	orig := newConsumer
	consumer := newSteadyConsumer("t", 0)
	newConsumer = func(ConsumerConfig) (Consumer, error) { return consumer, nil }
	t.Cleanup(func() { newConsumer = orig })

	var emits atomic.Int32
	rt := triggertest.NewFakeRuntime()
	rt.SetEmitFunc(func(context.Context, types.WorkflowID, string, *types.TriggerEvent) (types.ExecutionID, error) {
		emits.Add(1)
		// Stands in for the poison pill: a record the workflow cannot process, so
		// no amount of redelivery repairs it.
		return "", errors.New("deterministic: cannot decode record")
	})

	tr := New().
		Brokers("localhost:9092").
		Topic("t").
		Group("g").
		AggregateByPartition(10, 20*time.Millisecond)
	sub, err := tr.Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "kafka",
		Params:     tr.RawParams().(map[string]any),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close(context.Background()) }()

	const window = 3 * time.Second
	time.Sleep(window)
	got := emits.Load()
	t.Logf("downstream attempts over %s at a 20ms flush interval: %d", window, got)

	// Both sides measured on this harness, not derived: with the escalation the
	// downstream is called 18 times; with aggregateFlatRetryAttempts raised so
	// high that the escalation never engages, 149. 50 sits 2.8x above the first
	// and 3x below the second.
	//
	// Only the HEAD batch retries -- the ordered window holds the followers behind
	// it -- which is why this is ~150 and not ~600. That also means the number
	// counts exactly the wasted work: 149 re-runs of the same ten records.
	const maxAttempts = 50
	if got > maxAttempts {
		t.Errorf("✗ downstream called %d times in %s against a permanently failing "+
			"batch, want <= %d. The retry delay is not escalating: a poison record "+
			"spends the runner's capacity re-running work that provably cannot "+
			"succeed, on a partition whose frontier is frozen either way.",
			got, window, maxAttempts)
	}
	// Guard the bound: zero attempts satisfies it while proving nothing.
	if got == 0 {
		t.Error("✗ downstream was never called; the harness produced no attempt, so " +
			"the bound above proves nothing")
	}

	// Escalating must not become giving up. Committing here would silently
	// discard the batch, which is the one outcome worse than a stalled partition.
	consumer.mu.Lock()
	commits := len(consumer.commits)
	consumer.mu.Unlock()
	if commits != 0 {
		t.Errorf("✗ committed %d offset(s) while every attempt failed; backing off "+
			"must slow the retries down, never skip the records", commits)
	}
}
