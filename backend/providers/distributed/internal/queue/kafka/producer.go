package kafka

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/xbcio/xflow/backend/providers/distributed/internal/queue"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
)

// ErrDelayTooLong reports that an EnqueueDelayed delay exceeds
// Config.MaxEnqueueDelay and the task was not produced. It deliberately does
// not wrap engine.ErrQueueFull: this is not backpressure that waiting helps
// with, and an outbox flush must not treat it as such and hold the entry
// forever. It is permanent for the entry as given; shortening the delay would
// deliver before it is due, which is a semantic change the transport does not
// make on its own.
var ErrDelayTooLong = errors.New("kafka: delay too long")

// Enqueue produces a task to its topic and returns the writer's error as is.
//
// The error is deliberately not mapped to engine.ErrQueueFull. Submit's initial
// tasks and the timeout monitor's direct enqueues have no outbox entry behind
// them, so a transient broker failure must reach the caller as a failure, not be
// read as backpressure that something else will retry.
func (t *Transport) Enqueue(ctx context.Context, task *engine.Task) error {
	started := time.Now()
	kind, err := t.send(ctx, task, 0, false)
	t.observe("enqueue", started, kind, err)
	return err
}

// TryEnqueue is Enqueue for the outbox flush. Broker trouble that retrying can
// fix is reported as engine.ErrQueueFull, which tells FlushOutbox to keep the
// entry, release its leases and try again later without counting a delivery
// attempt. A permanent failure, or the caller's own context ending, is returned
// unchanged so the entry is dead-lettered or the flush is abandoned instead.
func (t *Transport) TryEnqueue(ctx context.Context, task *engine.Task) error {
	started := time.Now()
	kind, err := t.send(ctx, task, 0, true)
	t.observe("enqueue", started, kind, err)
	return err
}

// EnqueueDelayed produces a task that is due after delay. Kafka has no delivery
// timer, so the message goes to the delay topic stamped with the topic it
// belongs on and the time it is due; the delay relay moves it once it is due.
//
// A delay of zero or less is not delayed at all and behaves exactly like
// TryEnqueue, including reporting under the "enqueue" observer operation. The
// outbox flush calls this method for entries that carry a due time, and it has
// no other way to tell backpressure from a failed delivery, so a retryable
// failure is reported as engine.ErrQueueFull whether or not the delay is
// positive.
//
// A delay longer than Config.MaxEnqueueDelay is rejected with ErrDelayTooLong
// before anything is produced. Rejection, not truncation: delivering a timer
// wakeup earlier than its due time is a semantic change no caller asked for,
// and both call sites surface an error loudly rather than losing the task —
// the outbox flush records a delivery attempt (the intent dead-letters after
// the attempt limit, visible to observers), and the legacy retry path returns
// the error to the commit caller, which already documents its stuck state.
// The engine's own delays today stay under 6 minutes, so this guards against
// future callers, exactly as documented on the field.
func (t *Transport) EnqueueDelayed(ctx context.Context, task *engine.Task, delay time.Duration) error {
	if delay <= 0 {
		return t.TryEnqueue(ctx, task)
	}
	if limit := t.cfg.maxEnqueueDelay(); delay > limit {
		err := fmt.Errorf("%w: delay %v exceeds MaxEnqueueDelay %v", ErrDelayTooLong, delay, limit)
		t.observe("enqueue_delayed", time.Now(), ProduceKindOther, err)
		return err
	}
	started := time.Now()
	kind, err := t.send(ctx, task, delay, true)
	t.observe("enqueue_delayed", started, kind, err)
	return err
}

// observe reports one finished enqueue to the observer. A failed one is also
// reported with its kind when the observer asks for it (see
// queue.ProduceErrorObserver); kind is ignored on success.
func (t *Transport) observe(op string, started time.Time, kind string, err error) {
	t.observer.OnEnqueue(op, time.Since(started), err)
	if err == nil {
		return
	}
	if po, ok := t.observer.(queue.ProduceErrorObserver); ok {
		po.OnProduceError(kind)
	}
}

// send encodes a task and produces it. A positive delay routes it to the delay
// topic. When mapFull is set, a retryable produce error becomes
// engine.ErrQueueFull. Only the produce error is mapped: an encoding failure
// is permanent and passes through unchanged.
//
// The string is the kind the failure is reported under (see produceKind), empty
// when err is nil. An encoding failure never reached the broker, so it reads as
// ProduceKindOther rather than as a broker fault.
func (t *Transport) send(ctx context.Context, task *engine.Task, delay time.Duration, mapFull bool) (string, error) {
	ns := namespace.FromContext(ctx)
	var msg kafkago.Message
	var err error
	if delay > 0 {
		msg, err = t.delayedMessage(task, ns, delay)
	} else {
		msg, err = encodeTask(t.names.For(task), task, ns)
	}
	if err != nil {
		return ProduceKindOther, err
	}
	err = t.publish(ctx, msg)
	if mapFull {
		err = backpressure(ctx, err)
	}
	if err == nil {
		return "", nil
	}
	return produceKind(ctx, err), err
}

// delayedMessage builds the message that parks a task on the delay topic until
// it is due, stamped with where it goes then.
func (t *Transport) delayedMessage(task *engine.Task, ns namespace.Namespace, delay time.Duration) (kafkago.Message, error) {
	msg, err := encodeTask(t.names.Delay(), task, ns)
	if err != nil {
		return kafkago.Message{}, err
	}
	dueMs := time.Now().Add(delay).UnixMilli()
	msg.Headers = []kafkago.Header{
		{Key: hdrTarget, Value: []byte(t.names.For(task))},
		{Key: hdrNotBefore, Value: []byte(strconv.FormatInt(dueMs, 10))},
		{Key: hdrAttempt, Value: []byte("0")},
	}
	return msg, nil
}

// backpressure converts a retryable produce error into engine.ErrQueueFull while
// keeping the cause in the chain, so errors.Is matches both the sentinel and the
// broker error. Any other error, and nil, passes through unchanged.
func backpressure(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if classify(ctx, err) == kindRetryable {
		return fmt.Errorf("%w: %w", engine.ErrQueueFull, err)
	}
	return err
}
