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
		asynqlib.Queue(queueFor(task)))
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
		asynqlib.Queue(queueFor(task)), asynqlib.ProcessIn(delay))
	t.observer.OnEnqueue("enqueue_delayed", time.Since(started), err)
	return err
}
