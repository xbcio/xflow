//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	asynqlib "github.com/hibiken/asynq"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// blobE2EQueue is the queue the engine delivers through. Before each batch task
// it reads the outbox (like dpRecordingQueue) and also looks for the items
// marker in the run's Redis keys, which is the one moment the batch entry is
// sure to be stored there.
type blobE2EQueue struct {
	next engine.TaskQueue
	run  *blobE2ERun
}

func (q *blobE2EQueue) probe(ctx context.Context, t *engine.Task) {
	q.run.cap.outboxBodies(t.ExecutionID)
	if t.Type != engine.TaskTypeNodeBatch {
		return
	}
	holders, err := blobE2EHolders(ctx, q.run.rdb, q.run.ns, blobE2EMarker)
	if err != nil {
		q.run.cap.fail(err)
		return
	}
	q.run.mu.Lock()
	defer q.run.mu.Unlock()
	q.run.holders = append(q.run.holders, holders...)
	q.run.probes++
}

func (q *blobE2EQueue) Enqueue(ctx context.Context, t *engine.Task) error {
	q.probe(ctx, t)
	return q.next.Enqueue(ctx, t)
}

func (q *blobE2EQueue) EnqueueDelayed(ctx context.Context, t *engine.Task, d time.Duration) error {
	q.probe(ctx, t)
	return q.next.EnqueueDelayed(ctx, t, d)
}

// newBlobE2ERun assembles the stack the way sdk/xflow's NewCluster does, with
// the consumer swapped for blobE2ERun.serve. blobOn only switches writing: the
// blob store and index are wired either way, so a run with it off still proves
// that nothing reached the store.
func newBlobE2ERun(t *testing.T, addr string, blobOn bool) *blobE2ERun {
	t.Helper()
	r := newBlobE2EBase(t, addr, blobOn, false)
	b := r.b
	r.eng = engine.New(b.State(), &blobE2EQueue{next: b.Queue(), run: r},
		engine.WithDefaultLeaseTTL(time.Minute),
		engine.WithNamespace(r.ns),
		engine.WithBatchBodyExecutor(dpBodyExecutor(t, b)),
	)
	stop, err := b.StartBinding(r.eng) // consumer off: this only closes what the backend owns
	if err != nil {
		t.Fatalf("StartBinding: %v", err)
	}
	t.Cleanup(stop)
	// Without a consumer the backend starts no outbox dispatcher, and the
	// outbox is what hands a batch task to the queue, so the run starts one.
	dctx, stopDispatch := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		engine.NewOutboxDispatcher(r.eng, time.Second).Run(dctx)
	}()
	t.Cleanup(func() { stopDispatch(); <-done })
	return r
}

// serve starts the consumer of the run: an asynq server on the run's two queues
// whose handler counts the calls per task and then dispatches the task as the
// backend's consumer does. A permanent error skips the retries, which is the
// production classification (the transport keeps that helper unexported);
// any other error is retried by asynq. The retry delay is short so a task that
// is retried shows up within a test's time bounds.
func (r *blobE2ERun) serve(t *testing.T) {
	t.Helper()
	disp := execution.NewEmbeddedDispatcher(r.eng, r.b.Registry())
	mux := asynqlib.NewServeMux()
	mux.HandleFunc(dpTaskType, func(ctx context.Context, at *asynqlib.Task) error {
		id, _ := asynqlib.GetTaskID(ctx)
		r.cap.payload(id, at.Payload())
		r.mu.Lock()
		r.attempts[id]++
		r.mu.Unlock()
		task, ns, err := dpDecode(at.Payload())
		if err != nil {
			r.cap.fail(err)
			return fmt.Errorf("%w: %w", asynqlib.SkipRetry, err)
		}
		err = disp.HandleTask(namespace.WithNamespace(ctx, ns), task)
		if err == nil {
			return nil
		}
		r.mu.Lock()
		r.failures = append(r.failures, err)
		r.mu.Unlock()
		if errors.Is(err, types.ErrPermanent) {
			return fmt.Errorf("%w: %w", asynqlib.SkipRetry, err)
		}
		return err
	})
	srv := asynqlib.NewServer(asynqlib.RedisClientOpt{Addr: r.addr}, asynqlib.Config{
		Concurrency:    1,
		Queues:         map[string]int{dpDefaultQueue + string(r.ns): 8, dpBatchQueue + string(r.ns): 1},
		RetryDelayFunc: func(int, error, *asynqlib.Task) time.Duration { return 100 * time.Millisecond },
		// asynq looks at its retry set only this often (5s by default), so a
		// task that is retried would wait for the next look.
		DelayedTaskCheckInterval: 200 * time.Millisecond,
	})
	if err := srv.Start(mux); err != nil {
		t.Fatalf("start asynq server: %v", err)
	}
	t.Cleanup(srv.Shutdown)
}
