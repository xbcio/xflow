package asynq

import (
	"context"
	"encoding/json"
	"time"

	asynqlib "github.com/hibiken/asynq"

	"github.com/xbcio/xflow/engine"
)

const asynqTaskType = "xflow:node"

type queuedTask struct {
	engine.Task

	// These fields are queue-internal runtime metadata. engine.Task hides them
	// from public JSON so runner-facing protocols do not expose scheduler
	// internals, but the Asynq backend must still carry them between enqueue
	// and consume.
	AutoDepth    int `json:"_auto_depth,omitempty"`
	ActivationID int `json:"_activation_id,omitempty"`
}

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
	payload, err := marshalQueuedTask(t)
	if err != nil {
		return err
	}
	_, err = q.client.Enqueue(asynqlib.NewTask(asynqTaskType, payload))
	return err
}

func (q *asynqQueue) EnqueueDelayed(_ context.Context, t *engine.Task, delay time.Duration) error {
	payload, err := marshalQueuedTask(t)
	if err != nil {
		return err
	}
	_, err = q.client.Enqueue(asynqlib.NewTask(asynqTaskType, payload), asynqlib.ProcessIn(delay))
	return err
}

func marshalQueuedTask(t *engine.Task) ([]byte, error) {
	if t == nil {
		return json.Marshal((*queuedTask)(nil))
	}
	return json.Marshal(queuedTask{
		Task:         *t,
		AutoDepth:    t.AutoDepth,
		ActivationID: t.ActivationID,
	})
}

func unmarshalQueuedTask(data []byte) (*engine.Task, error) {
	var qt queuedTask
	if err := json.Unmarshal(data, &qt); err != nil {
		return nil, err
	}
	task := qt.Task
	task.AutoDepth = qt.AutoDepth
	task.ActivationID = qt.ActivationID
	return &task, nil
}
