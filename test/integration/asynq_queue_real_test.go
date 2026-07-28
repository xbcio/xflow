//go:build integration

package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend/providers/distributed"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

func TestAsynqQueueRealEnqueueAndConsume(t *testing.T) {
	addr := requireRedis(t)

	b, err := distributed.New(addr, nil, distributed.WithConcurrency(1), distributed.WithConsumer(true))
	if err != nil {
		t.Fatalf("distributed.New: %v", err)
	}

	eng := engine.New(b.State(), b.Queue())

	var (
		mu      sync.Mutex
		seen    []string
		gotTask = make(chan struct{}, 1)
	)
	stop := b.BindHandler(eng, func(ctx context.Context, task *engine.Task) error {
		mu.Lock()
		seen = append(seen, string(task.ExecutionID))
		mu.Unlock()
		select {
		case gotTask <- struct{}{}:
		default:
		}
		return nil
	})
	t.Cleanup(stop)

	execID := types.ExecutionID("e2e-asynq-" + time.Now().Format("150405.000"))
	task := &engine.Task{ExecutionID: execID, NodeName: "n1"}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := b.Queue().Enqueue(ctx, task); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	select {
	case <-gotTask:
	case <-ctx.Done():
		t.Fatalf("handler not invoked: %v", ctx.Err())
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 1 || seen[0] != string(execID) {
		t.Fatalf("seen = %v, want [%s]", seen, execID)
	}
}
