package cluster

import (
	"context"
	"encoding/json"
	"time"

	"github.com/hibiken/asynq"

	"github.com/xbcio/xflow/engine"
)

const asynqTaskType = "xflow:node"

// asynqQueue implements engine.TaskQueue backed by Asynq.
type asynqQueue struct {
	client *asynq.Client
}

func newAsynqQueue(redisAddr string) *asynqQueue {
	return &asynqQueue{
		client: asynq.NewClient(asynq.RedisClientOpt{Addr: redisAddr}),
	}
}

func (q *asynqQueue) Close() error { return q.client.Close() }

func (q *asynqQueue) Enqueue(_ context.Context, t *engine.Task) error {
	payload, err := json.Marshal(t)
	if err != nil {
		return err
	}
	_, err = q.client.Enqueue(asynq.NewTask(asynqTaskType, payload))
	return err
}

func (q *asynqQueue) EnqueueDelayed(_ context.Context, t *engine.Task, delay time.Duration) error {
	payload, err := json.Marshal(t)
	if err != nil {
		return err
	}
	_, err = q.client.Enqueue(asynq.NewTask(asynqTaskType, payload), asynq.ProcessIn(delay))
	return err
}
