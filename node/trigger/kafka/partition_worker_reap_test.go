package kafka

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/trigger/triggertest"
	"github.com/xbcio/xflow/types"
)

// TestPartitionWorkerDoesNotReapWithQueuedMessage pins the idle-timer/message
// race: when the timer and a queued record are both ready, select used to pick
// the timer half the time and terminate the worker with the record still in its
// channel — fetched, never emitted, and not covered by any commit. The worker
// must drain the queued record first and only reap a genuinely idle channel.
//
// idleTimeout is zero so the timer is already (or immediately) expired when run
// starts; the scenario is repeated because select's choice between the two
// ready cases is random. The old code processes the record on only a subset of
// iterations with overwhelming probability.
func TestPartitionWorkerDoesNotReapWithQueuedMessage(t *testing.T) {
	for i := 0; i < 50; i++ {
		var emitted atomic.Int32
		fake := triggertest.NewFakeRuntime()
		fake.SetEmitFunc(func(context.Context, types.WorkflowID, string, *types.TriggerEvent) (types.ExecutionID, error) {
			emitted.Add(1)
			return "exec-ok", nil
		})
		rt := &perMessageRuntime{
			runCtx:  t.Context(),
			baseCtx: t.Context(),
			in:      &types.TriggerActivateInput{NodeName: "kafka", Runtime: fake},
			buffer:  1,
			workers: make(map[partitionKey]*partitionWorker),
		}
		key := partitionKey{topic: "reap-race", partition: 0}
		worker := &partitionWorker{
			key:         key,
			rt:          rt,
			ch:          make(chan Message, 1),
			done:        make(chan struct{}),
			idleTimeout: 0,
		}
		rt.workers[key] = worker
		worker.ch <- Message{Topic: key.topic, Partition: 0, Offset: 1, Value: []byte(`"x"`)}

		go worker.run()

		select {
		case <-worker.done:
		case <-time.After(2 * time.Second):
			t.Fatalf("iteration %d: worker did not reap after draining its channel", i)
		}
		if got := emitted.Load(); got != 1 {
			t.Fatalf("iteration %d: emitted %d records, want 1: a fired idle timer must not "+
				"terminate the worker while a record is queued", i, got)
		}
	}
}
