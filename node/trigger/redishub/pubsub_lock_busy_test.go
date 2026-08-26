package redishub

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/trigger/triggertest"
	"github.com/xbcio/xflow/types"
)

// TestPubSubActivateStandsDownWhenAnotherRunnerHoldsTheLock pins
// redishub.go:155-158:
//
//	l, ok, err := in.Runtime.TryLock(ctx, "trigger:...:pubsub", pubSubLockTTL)
//	if err != nil || !ok {
//		return nil, err
//	}
//
// The !ok arm has never been driven anywhere in this repository.
// triggertest.FakeRuntime.TryLock (triggertest.go:87-89) unconditionally
// returns FakeLock{}, true, nil, and this package's own override,
// renewableLockRuntime.TryLock (redishub_test.go:215-217), hardcodes true as
// well. Those are the only two TryLock fakes that exist, so no test has ever
// seen a busy lock and the check is free to delete.
//
// The check is what makes pub/sub mode single-consumer. Redis pub/sub fans a
// message out to every subscriber — unlike the stream mode above it, which uses
// a consumer group and hands each message to exactly one member. The lock is the
// only thing standing between "one runner subscribes" and "every runner in the
// fleet subscribes". Lose it and an N-runner deployment starts N executions per
// published message, forever, with every runner reporting a healthy activation
// and nothing in the metrics distinguishing it from a genuinely N-times-busier
// channel.
//
// Two fixtures, for two different reasons:
//
//   - "busy, production shape" returns (nil, false, nil), which is exactly what
//     both real implementations return when the key is already held —
//     distributed/internal/trigger/trigger.go:70-71 and
//     providers/local/trigger_runtime.go:53-54. Removing the !ok guard against
//     this fixture does not merely proceed: l is a nil interface, so the
//     l.(types.RenewableTriggerLock) assertion fails and the l.Release(ctx)
//     on the next line panics. Worth knowing that the guard is also what stops
//     a nil dereference, but a panic aborts the whole package binary and would
//     not tell us whether the assertion below has teeth.
//   - "busy, handle returned" returns a usable lock with ok=false. No real
//     implementation does that, but it isolates the assertion: with the guard
//     removed, Activate proceeds normally and the consumer factory below fails
//     the test with its own message rather than crashing the process.
//
// The assertion is deliberately about the consumer factory, not about the
// returned subscription. Activate currently returns (nil, nil) here, which is a
// separate wart — the caller at service/runner/trigger_activation_handler.go:180
// stores a nil types.TriggerSubscription and records the activation as hosted —
// but pinning `sub == nil` would block anyone from fixing that by returning a
// no-op subscription instead. What must not change is that no consumer is
// created.
func TestPubSubActivateStandsDownWhenAnotherRunnerHoldsTheLock(t *testing.T) {
	cases := []struct {
		name string
		lock types.TriggerLock
	}{
		{name: "busy, production shape", lock: nil},
		{name: "busy, handle returned", lock: &scriptedTriggerLock{}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			origConsumer := newConsumer
			newConsumer = func(ConsumerConfig) (Consumer, error) {
				t.Error("a pub/sub consumer was created while another runner holds the " +
					"channel lock: redis pub/sub fans every message out to every " +
					"subscriber, so each runner that ignores the lock multiplies the " +
					"executions started per published message by one")
				return newBlockingConsumer(), nil
			}
			t.Cleanup(func() { newConsumer = origConsumer })

			rt := &busyLockRuntime{FakeRuntime: triggertest.NewFakeRuntime(), lock: tc.lock}

			sub, err := New().Mode("pubsub").Channel("orders").Activate(context.Background(), &types.TriggerActivateInput{
				WorkflowID: "wf-1",
				NodeName:   "redis",
				Params:     map[string]any{"mode": "pubsub", "channel": "orders", "max_inflight": 1},
				Runtime:    rt,
			})
			if sub != nil {
				_ = sub.Close(context.Background())
			}
			// A busy lock is not a failure — the other runner is doing the work.
			// Returning an error here would make the activation reconciler retry
			// and eventually mark a perfectly healthy fleet as failing to host
			// this trigger.
			if err != nil {
				t.Fatalf("Activate() error = %v, want nil: losing a lock race to "+
					"another runner is the normal outcome in a multi-runner fleet, "+
					"not an activation failure", err)
			}
			if !rt.triedLock {
				t.Fatal("Activate never asked for the pub/sub lock, so nothing about " +
					"single-consumer behaviour was exercised")
			}
		})
	}
}

// busyLockRuntime reports the pub/sub lock as held by someone else. Everything
// except TryLock comes from the embedded fake.
type busyLockRuntime struct {
	*triggertest.FakeRuntime
	lock      types.TriggerLock
	triedLock bool
}

func (r *busyLockRuntime) TryLock(context.Context, string, time.Duration) (types.TriggerLock, bool, error) {
	r.triedLock = true
	return r.lock, false, nil
}
