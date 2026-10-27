package asynq

import (
	"context"
	"time"

	asynqlib "github.com/hibiken/asynq"

	"github.com/xbcio/xflow/backend/providers/distributed/internal/queue"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
)

// Enqueue submits a task for immediate processing.
func (t *Transport) Enqueue(ctx context.Context, task *engine.Task) error {
	started := time.Now()
	payload, err := queue.MarshalWithNamespace(task, namespace.FromContext(ctx))
	if err != nil {
		t.observer.OnEnqueue("enqueue", time.Since(started), err)
		return err
	}
	// Use EnqueueContext to propagate caller's ctx (timeout/cancel/trace).
	_, err = t.client.EnqueueContext(ctx, asynqlib.NewTask(taskType, payload),
		asynqlib.Queue(t.queues.For(task)))
	t.observer.OnEnqueue("enqueue", time.Since(started), err)
	return err
}

// EnqueueDelayed submits a task to be processed after delay.
func (t *Transport) EnqueueDelayed(ctx context.Context, task *engine.Task, delay time.Duration) error {
	started := time.Now()
	payload, err := queue.MarshalWithNamespace(task, namespace.FromContext(ctx))
	if err != nil {
		t.observer.OnEnqueue("enqueue_delayed", time.Since(started), err)
		return err
	}
	// Use EnqueueContext to propagate caller's ctx (timeout/cancel/trace).
	_, err = t.client.EnqueueContext(ctx, asynqlib.NewTask(taskType, payload),
		asynqlib.Queue(t.queues.For(task)), asynqlib.ProcessIn(delay))
	t.observer.OnEnqueue("enqueue_delayed", time.Since(started), err)
	return err
}

// TryEnqueue offers a task without waiting for queue room, reporting
// engine.ErrQueueFull when the task's own queue has reached the depth bound
// configured by WithMaxQueueDepth. It is the asynq transport's
// engine.NonBlockingTaskQueue capability.
//
// Only FlushOutbox calls it, and only because it must: the flush runs on a
// queue worker, so blocking there turns a fan-out wider than the queue into a
// permanent deadlock. The intent stays in the outbox and is redelivered once
// the workers drain, which is why reporting fullness is safe here but is not
// for Enqueue's callers (see engine.NonBlockingTaskQueue).
//
// Without a configured bound this is Enqueue: it always attempts the enqueue.
// With one, the check is against a periodically refreshed estimate, so a burst
// can overshoot the bound between samples; the estimate and the fail-open
// behavior on sampling errors are documented on queueFull.
func (t *Transport) TryEnqueue(ctx context.Context, task *engine.Task) error {
	started := time.Now()
	queueName := t.queues.For(task)
	if t.queueFull(queueName) {
		// Count the refusal like any other enqueue outcome: a flush silently
		// leaving work in the outbox is exactly the state an operator needs to
		// see as backpressure rather than as a healthy producer.
		t.observer.OnEnqueue("enqueue", time.Since(started), engine.ErrQueueFull)
		return engine.ErrQueueFull
	}
	payload, err := queue.MarshalWithNamespace(task, namespace.FromContext(ctx))
	if err != nil {
		t.observer.OnEnqueue("enqueue", time.Since(started), err)
		return err
	}
	// Use EnqueueContext to propagate caller's ctx (timeout/cancel/trace).
	_, err = t.client.EnqueueContext(ctx, asynqlib.NewTask(taskType, payload),
		asynqlib.Queue(queueName))
	t.observer.OnEnqueue("enqueue", time.Since(started), err)
	return err
}

var _ engine.NonBlockingTaskQueue = (*Transport)(nil)
