package asynq

import (
	"context"

	asynqlib "github.com/hibiken/asynq"

	"github.com/xbcio/xflow/backend/distributed/internal/queue"
)

// StartConsumer starts an Asynq server that decodes each task and dispatches it
// to handler, translating the handler error into the Asynq retry policy. The
// returned stop function shuts the server down gracefully.
//
// Server.Start is the non-blocking entry point: it launches asynq's internal
// sub-goroutines (tracked by the server's own wait group) and returns
// immediately, unlike Server.Run which blocks on OS signals. The returned stop
// calls Shutdown, which drains those sub-goroutines. The server uses its own
// broker connection, independent of any state-store Redis client.
func (t *Transport) StartConsumer(cfg queue.ConsumerConfig, handler queue.TaskHandler) (func(), error) {
	srv := asynqlib.NewServer(
		asynqlib.RedisClientOpt{Addr: t.redisAddr},
		asynqlib.Config{Concurrency: cfg.Concurrency},
	)
	mux := asynqlib.NewServeMux()
	mux.HandleFunc(taskType, func(ctx context.Context, at *asynqlib.Task) error {
		task, err := queue.Unmarshal(at.Payload())
		if err != nil {
			return err
		}
		return handlerError(handler(ctx, task), cfg.Transient)
	})

	if err := srv.Start(mux); err != nil {
		return nil, err
	}
	return func() { srv.Shutdown() }, nil
}
