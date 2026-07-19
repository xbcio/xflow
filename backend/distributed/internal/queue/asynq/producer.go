package asynq

import (
	"context"
	"time"

	asynqlib "github.com/hibiken/asynq"

	"github.com/xbcio/xflow/backend/distributed/internal/queue"
	"github.com/xbcio/xflow/backend/tenant"
	"github.com/xbcio/xflow/engine"
)

// Enqueue submits a task for immediate processing.
func (t *Transport) Enqueue(ctx context.Context, task *engine.Task) error {
	started := time.Now()
	payload, err := queue.MarshalWithTenant(task, tenant.FromContext(ctx))
	if err != nil {
		t.observer.OnEnqueue("enqueue", time.Since(started), err)
		return err
	}
	_, err = t.client.Enqueue(asynqlib.NewTask(taskType, payload))
	t.observer.OnEnqueue("enqueue", time.Since(started), err)
	return err
}

// EnqueueDelayed submits a task to be processed after delay.
func (t *Transport) EnqueueDelayed(ctx context.Context, task *engine.Task, delay time.Duration) error {
	started := time.Now()
	payload, err := queue.MarshalWithTenant(task, tenant.FromContext(ctx))
	if err != nil {
		t.observer.OnEnqueue("enqueue_delayed", time.Since(started), err)
		return err
	}
	_, err = t.client.Enqueue(asynqlib.NewTask(taskType, payload), asynqlib.ProcessIn(delay))
	t.observer.OnEnqueue("enqueue_delayed", time.Since(started), err)
	return err
}
