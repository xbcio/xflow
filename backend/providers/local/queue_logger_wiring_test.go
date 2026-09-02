package local

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// TestNewWiresQueueLoggerFromOption pins the WIRING, not the queue's behaviour.
//
// memory_queue_test.go already covers what the queue does with a logger, but
// both of its tests build the queue with newMemoryQueue and call SetLogger
// themselves. That proves the queue logs when it has a logger; it cannot notice
// that nothing in production ever gave it one. SetLogger had zero non-test call
// sites and config carried no logger field at all, so every dropped task —
// permanent failure and retry exhaustion alike — vanished with no record.
//
// This test therefore goes through New with the option, the way a caller does,
// and never touches queue.logger itself. Deleting the SetLogger call in New, or
// the field the option writes, must turn it red.
func TestNewWiresQueueLoggerFromOption(t *testing.T) {
	logs := &queueLogRecorder{}
	b := New(WithConcurrency(1), WithQueueLogger(logs))

	stop := b.BindHandler(func(context.Context, *engine.Task) error {
		return errors.Join(types.ErrPermanent, errors.New("hard error"))
	})
	defer stop()

	if err := b.queue.Enqueue(context.Background(), &engine.Task{
		ExecutionID: types.ExecutionID("e1"), NodeName: "n",
	}); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if logs.errorCount() > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("a task dropped for a permanent error produced no Error log. The " +
		"logger passed to New never reached the queue, so the drop is silent in " +
		"exactly the deployments that configured a logger to see it.")
}

// TestNewLeavesQueueLoggerUnsetWithoutOption pins the other half: the option is
// opt-in, and a caller that passes none keeps the previous behaviour rather
// than acquiring a default logger that writes somewhere it did not choose.
//
// The second case pins what WithQueueLogger's nil guard actually does — a later
// nil does not erase a logger an earlier option already installed, matching
// WithConcurrency's `if n > 0` and WithQueueCapacity's. It deliberately does NOT
// claim the guard defends against a typed-nil: `l != nil` is false for a nil
// interface and true for a nil *T stored in one, so the guard cannot help there.
//
// Both cases read the field directly BECAUSE there is no observable behaviour to
// assert — a nil logger's whole signature is that nothing happens.
func TestNewLeavesQueueLoggerUnsetWithoutOption(t *testing.T) {
	if got := New(WithConcurrency(1)).queue.logger; got != nil {
		t.Errorf("queue logger = %v, want nil: New must not invent a logger for "+
			"callers that passed none", got)
	}

	logs := &queueLogRecorder{}
	if got := New(WithQueueLogger(logs), WithQueueLogger(nil)).queue.logger; got != engine.Logger(logs) {
		t.Errorf("queue logger = %v, want the earlier non-nil logger: a later "+
			"WithQueueLogger(nil) must not silently disarm logging that another "+
			"option in the same list already turned on", got)
	}
}
