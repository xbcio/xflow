package kafka

import (
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
)

// EnqueueDelayed enforces Config.MaxEnqueueDelay against a real broker: a delay
// exactly at the limit is parked on the delay topic and delivered, one past it
// is rejected before anything is produced, and a delay of zero or less takes the
// TryEnqueue path straight to the task topic with no delay headers.
//
// The limit is the validated minimum (6m), so the test runs with a config an
// operator could actually set. The at-limit message is only checked on the delay
// topic, not waited out: delivering it would take six minutes.
func TestKafkaIntegrationEnqueueDelayedMaxDelay(t *testing.T) {
	e := newITEnv(t)
	tr := e.transport(func(c *Config) { c.MaxEnqueueDelay = minMaxEnqueueDelay })
	e.ensure(tr)

	atLimit := itTask("at-limit", false)
	if err := tr.EnqueueDelayed(t.Context(), atLimit, minMaxEnqueueDelay); err != nil {
		t.Fatalf("EnqueueDelayed at the limit: %v", err)
	}
	over := itTask("over-limit", false)
	err := tr.EnqueueDelayed(t.Context(), over, minMaxEnqueueDelay+time.Second)
	if !errors.Is(err, ErrDelayTooLong) {
		t.Fatalf("EnqueueDelayed past the limit = %v, want ErrDelayTooLong", err)
	}
	if errors.Is(err, engine.ErrQueueFull) {
		t.Errorf("EnqueueDelayed past the limit = %v: must not read as backpressure", err)
	}

	// A non-positive delay is a plain produce to the task topic.
	for i, delay := range []time.Duration{0, -time.Second} {
		if err := tr.EnqueueDelayed(t.Context(), itTask(execID("now", i), false), delay); err != nil {
			t.Fatalf("EnqueueDelayed(%v): %v", delay, err)
		}
	}

	parked := e.readTopic(e.names.Delay(), 3, -1, 3*time.Second)
	ids := map[string]bool{}
	for _, m := range parked {
		task, _, derr := decodeTask(m)
		if derr != nil {
			t.Fatalf("decode a delay topic message: %v", derr)
		}
		ids[string(task.ExecutionID)] = true
	}
	if len(parked) != 1 || !ids["at-limit"] {
		t.Errorf("delay topic holds %v over %d messages, want exactly at-limit", ids, len(parked))
	}

	immediate := e.readTopic(e.names.Default(), 3, 2, 10*time.Second)
	got := map[string]bool{}
	for _, m := range immediate {
		task, _, derr := decodeTask(m)
		if derr != nil {
			t.Fatalf("decode a task topic message: %v", derr)
		}
		got[string(task.ExecutionID)] = true
		if _, ok := headerValue(m, hdrNotBefore); ok {
			t.Errorf("task %s carries %s: a non-positive delay must not be delayed", task.ExecutionID, hdrNotBefore)
		}
	}
	if len(immediate) != 2 || got["over-limit"] || got["at-limit"] {
		t.Errorf("default topic holds %v over %d messages, want only the two non-positive-delay tasks", got, len(immediate))
	}
}
