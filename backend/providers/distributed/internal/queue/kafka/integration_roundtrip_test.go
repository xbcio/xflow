package kafka

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// itTask is a task for execution id; batch tasks go to the batch topic.
func itTask(id types.ExecutionID, batch bool) *engine.Task {
	task := &engine.Task{ExecutionID: id, NodeName: "step", Type: engine.TaskTypeNodeExec, UnitIdx: engine.UnitIdxUnknown}
	if batch {
		task.Type = engine.TaskTypeNodeBatch
	}
	return task
}

// 50 tasks go through a real broker: every one reaches the handler with the
// namespace it was enqueued under, batch tasks included. After the consumer
// stops, the same group restarts and sees none of them again, which only holds
// if the offsets were really committed.
func TestKafkaIntegrationRoundTripCommitsOffsets(t *testing.T) {
	const n = 50
	e := newITEnv(t)
	ns := namespace.Namespace("tenant-" + e.suffix)
	tr := e.transport(nil)
	e.ensure(tr)

	c := &collector{}
	stop := startConsumer(t, tr, c.handle)
	ctx := namespace.WithNamespace(t.Context(), ns)
	batch := map[types.ExecutionID]bool{}
	for i := range n {
		isBatch := i%5 == 0
		task := itTask(execID("rt", i), isBatch)
		batch[task.ExecutionID] = isBatch
		if err := tr.Enqueue(ctx, task); err != nil {
			t.Fatalf("Enqueue %d: %v", i, err)
		}
	}
	waitFor(t, "all 50 tasks", 60*time.Second, func() bool { return len(c.ids()) == n })

	for _, d := range c.all() {
		if d.ns != ns {
			t.Errorf("task %s arrived under namespace %q, want %q", d.id, d.ns, ns)
		}
		if isBatch := d.typ == engine.TaskTypeNodeBatch; isBatch != batch[d.id] {
			t.Errorf("task %s arrived with type %v, batch=%v was enqueued", d.id, d.typ, batch[d.id])
		}
	}
	wantBatch := n / 5
	waitFor(t, "the batch and default offsets to be committed", 30*time.Second, func() bool {
		return e.committed(groupBatch, e.names.Batch(), 3) == int64(wantBatch) &&
			e.committed(groupDefault, e.names.Default(), 3) == int64(n-wantBatch)
	})
	stop()
	before := len(c.all())

	// The same group restarts. Nothing is left to deliver.
	c2 := &collector{}
	tr2 := e.transport(nil)
	startConsumer(t, tr2, c2.handle)
	if err := tr2.Enqueue(ctx, itTask("probe", false)); err != nil {
		t.Fatalf("Enqueue probe: %v", err)
	}
	waitFor(t, "the probe on the restarted consumer", 60*time.Second, func() bool { return c2.ids()["probe"] > 0 })
	if got := c2.ids(); len(got) != 1 {
		t.Errorf("restarted consumer redelivered %v, want only the probe", got)
	}
	if after := len(c.all()); after != before {
		t.Errorf("stopped consumer still received %d tasks", after-before)
	}
}

// A task enqueued with a delay is not delivered before it is due, and is
// delivered after. The lower bound allows 100ms for clock rounding between the
// producer's due stamp and the handler's clock.
func TestKafkaIntegrationEnqueueDelayed(t *testing.T) {
	const delay = 2 * time.Second
	e := newITEnv(t)
	tr := e.transport(nil)
	e.ensure(tr)
	c := &collector{}
	startConsumer(t, tr, c.handle)

	sent := time.Now()
	if err := tr.EnqueueDelayed(t.Context(), itTask("delayed", false), delay); err != nil {
		t.Fatalf("EnqueueDelayed: %v", err)
	}
	waitFor(t, "the delayed task", 30*time.Second, func() bool { return c.ids()["delayed"] > 0 })
	got := c.all()
	if len(got) != 1 {
		t.Fatalf("delivered %d times, want once", len(got))
	}
	if early := delay - got[0].at.Sub(sent); early > 100*time.Millisecond {
		t.Errorf("delivered %v after enqueue, %v before it was due", got[0].at.Sub(sent), early)
	}
	t.Logf("delay %v, delivered after %v", delay, got[0].at.Sub(sent))
}

// A handler that fails twice with an ordinary error succeeds on the third
// delivery. The retries travel through the delay topic, whose messages carry
// the attempt number.
func TestKafkaIntegrationRetryThenSucceed(t *testing.T) {
	e := newITEnv(t)
	tr := e.transport(nil)
	e.ensure(tr)
	c := &collector{fail: func(_ delivery, nth int) error {
		if nth <= 2 {
			return errors.New("transient failure")
		}
		return nil
	}}
	startConsumer(t, tr, c.handle)
	if err := tr.Enqueue(t.Context(), itTask("flaky", false)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	waitFor(t, "the third delivery", 60*time.Second, func() bool { return c.ids()["flaky"] >= 3 })
	// A stray fourth delivery would show up within this window.
	stays(t, 2*time.Second, func() error {
		if n := c.ids()["flaky"]; n != 3 {
			return fmt.Errorf("delivered %d times, want exactly 3", n)
		}
		return nil
	})
	retries := e.readTopic(e.names.Delay(), 3, 2, 10*time.Second)
	attempts := map[string]bool{}
	for _, m := range retries {
		v, _ := headerValue(m, hdrAttempt)
		attempts[v] = true
	}
	if len(retries) != 2 || !attempts["1"] || !attempts["2"] {
		t.Errorf("delay topic holds attempts %v over %d messages, want attempts 1 and 2", attempts, len(retries))
	}
	if dlq := e.readTopic(e.names.DLQ(), 3, -1, 2*time.Second); len(dlq) != 0 {
		t.Errorf("dead-letter topic holds %d messages, want none", len(dlq))
	}
}
