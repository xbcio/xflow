package asynq

import (
	"context"
	"encoding/json"
	"time"

	asynqlib "github.com/hibiken/asynq"

	"github.com/xbcio/xflow/engine"
)

const asynqTaskType = "xflow:node"

// asynqQueue implements engine.TaskQueue backed by Asynq.
type asynqQueue struct {
	client *asynqlib.Client
}

func newAsynqQueue(redisAddr string) *asynqQueue {
	return &asynqQueue{
		client: asynqlib.NewClient(asynqlib.RedisClientOpt{Addr: redisAddr}),
	}
}

func (q *asynqQueue) Close() error { return q.client.Close() }

func (q *asynqQueue) Enqueue(_ context.Context, t *engine.Task) error {
	payload, err := json.Marshal(t)
	if err != nil {
		return err
	}
	_, err = q.client.Enqueue(asynqlib.NewTask(asynqTaskType, payload))
	return err
}

func (q *asynqQueue) EnqueueDelayed(_ context.Context, t *engine.Task, delay time.Duration) error {
	payload, err := json.Marshal(t)
	if err != nil {
		return err
	}
	_, err = q.client.Enqueue(asynqlib.NewTask(asynqTaskType, payload), asynqlib.ProcessIn(delay))
	return err
}
