package memory

import (
	"context"
	"sync"
	"time"

	"github.com/xbcio/xflow/engine"
)

// memoryQueue implements engine.TaskQueue using a buffered channel and a goroutine pool.
type memoryQueue struct {
	ch          chan *engine.Task
	handler     func(ctx context.Context, t *engine.Task) error
	concurrency int
	wg          sync.WaitGroup
	stopCh      chan struct{}
}

func newMemoryQueue(concurrency int) *memoryQueue {
	return &memoryQueue{
		ch:          make(chan *engine.Task, 1024),
		concurrency: concurrency,
		stopCh:      make(chan struct{}),
	}
}

// SetHandler wires the queue consumer callback into the queue.
// Must be called before Start().
func (q *memoryQueue) SetHandler(fn func(ctx context.Context, t *engine.Task) error) {
	q.handler = fn
}

// Start launches the queue consumer goroutine pool.
func (q *memoryQueue) Start() {
	for i := 0; i < q.concurrency; i++ {
		q.wg.Add(1)
		go q.worker()
	}
}

func (q *memoryQueue) worker() {
	defer q.wg.Done()
	for {
		select {
		case <-q.stopCh:
			return
		case t, ok := <-q.ch:
			if !ok {
				return
			}
			if q.handler != nil {
				_ = q.handler(context.Background(), t)
			}
		}
	}
}

// Stop signals all queue consumers to exit and waits for them to drain.
func (q *memoryQueue) Stop() {
	close(q.stopCh)
	q.wg.Wait()
}

func (q *memoryQueue) Enqueue(_ context.Context, t *engine.Task) error {
	select {
	case q.ch <- t:
		return nil
	default:
		// Channel full — grow dynamically by spawning a temporary goroutine.
		go func() {
			q.ch <- t
		}()
		return nil
	}
}

func (q *memoryQueue) EnqueueDelayed(_ context.Context, t *engine.Task, delay time.Duration) error {
	go func() {
		time.Sleep(delay)
		q.ch <- t
	}()
	return nil
}
