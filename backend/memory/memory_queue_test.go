package memory

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

type transientErr struct{ msg string }

func (t *transientErr) Error() string   { return t.msg }
func (t *transientErr) Transient() bool { return true }

type queueLogRecorder struct {
	mu    sync.Mutex
	infos []string
	errs  []string
}

func (r *queueLogRecorder) Info(msg string, _ ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.infos = append(r.infos, msg)
}

func (r *queueLogRecorder) Error(msg string, _ ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errs = append(r.errs, msg)
}

func (r *queueLogRecorder) errorCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.errs)
}

func TestMemoryQueueRequeuesTransientFailureUntilHandlerSucceeds(t *testing.T) {
	q := newMemoryQueue(1)
	logs := &queueLogRecorder{}
	q.SetLogger(logs)
	var calls atomic.Int32
	done := make(chan struct{})
	q.SetHandler(func(_ context.Context, task *engine.Task) error {
		n := calls.Add(1)
		if n < 3 {
			return &transientErr{msg: "no capacity"}
		}
		if task.NodeName != "n" {
			t.Errorf("unexpected node: %q", task.NodeName)
		}
		close(done)
		return nil
	})
	q.Start()
	defer q.Stop()

	if err := q.Enqueue(context.Background(), &engine.Task{
		ExecutionID: types.ExecutionID("e1"), NodeName: "n",
	}); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("handler did not eventually succeed; calls=%d", calls.Load())
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("handler calls = %d, want 3", got)
	}
}

func TestMemoryQueueDropsNonTransientFailureWithoutRequeue(t *testing.T) {
	q := newMemoryQueue(1)
	logs := &queueLogRecorder{}
	q.SetLogger(logs)
	var calls atomic.Int32
	q.SetHandler(func(_ context.Context, _ *engine.Task) error {
		calls.Add(1)
		return errors.New("hard error")
	})
	q.Start()
	defer q.Stop()

	if err := q.Enqueue(context.Background(), &engine.Task{
		ExecutionID: types.ExecutionID("e1"), NodeName: "n",
	}); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	// Allow worker to dispatch once.
	time.Sleep(50 * time.Millisecond)
	if got := calls.Load(); got != 1 {
		t.Fatalf("handler calls = %d, want 1 (no requeue for non-transient)", got)
	}
	if logs.errorCount() == 0 {
		t.Fatal("expected an Error log for the dropped task")
	}
}
