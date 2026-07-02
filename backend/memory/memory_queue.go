package memory

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// transientRequeueInitial / transientRequeueCap bound the backoff between
// retries of a transient dispatch failure. Picked so a steady stream of "no
// runner" failures backs off quickly to ~30s while still recovering in seconds
// once a runner registers.
const (
	transientRequeueInitial = 100 * time.Millisecond
	transientRequeueCap     = 30 * time.Second
	transientRequeueMax     = 1000
)

// queueEnvelope carries scheduler-internal retry counters alongside the task.
// engine.Task hides AutoDepth/ActivationID from runner JSON but the in-process
// queue can use any fields it likes.
type queueEnvelope struct {
	task           *engine.Task
	transientTries int
}

// memoryQueue implements engine.TaskQueue using a buffered channel and a goroutine pool.
type memoryQueue struct {
	ch          chan queueEnvelope
	handler     func(ctx context.Context, t *engine.Task) error
	logger      engine.Logger
	concurrency int
	wg          sync.WaitGroup
	stopCh      chan struct{}
}

func newMemoryQueue(concurrency int) *memoryQueue {
	return &memoryQueue{
		ch:          make(chan queueEnvelope, 1024),
		concurrency: concurrency,
		stopCh:      make(chan struct{}),
	}
}

// SetHandler wires the queue consumer callback into the queue.
// Must be called before Start().
func (q *memoryQueue) SetHandler(fn func(ctx context.Context, t *engine.Task) error) {
	q.handler = fn
}

// SetLogger sets the logger used for dead-letter and dispatch-failure
// diagnostics. Optional; without one, dropped tasks are silent.
func (q *memoryQueue) SetLogger(l engine.Logger) { q.logger = l }

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
		case env, ok := <-q.ch:
			if !ok {
				return
			}
			q.dispatch(env)
		}
	}
}

func (q *memoryQueue) dispatch(env queueEnvelope) {
	if q.handler == nil {
		return
	}
	err := q.handler(context.Background(), env.task)
	if err == nil {
		return
	}
	if errors.Is(err, types.ErrPermanent) {
		// Permanent handler failure: nothing useful the queue can do. Surface
		// for ops without losing the rest of the workflow.
		if q.logger != nil {
			q.logger.Error("dropping task after permanent handler error",
				"exec", string(env.task.ExecutionID),
				"node", env.task.NodeName,
				"err", err,
			)
		}
		return
	}
	if env.transientTries >= transientRequeueMax {
		if q.logger != nil {
			q.logger.Error("dropping task after exhausting transient retries",
				"exec", string(env.task.ExecutionID),
				"node", env.task.NodeName,
				"attempts", env.transientTries,
				"err", err,
			)
		}
		return
	}
	delay := transientBackoff(env.transientTries)
	env.transientTries++
	if q.logger != nil {
		q.logger.Errorf("requeueing task after transient dispatch failure: exec=%s node=%s attempt=%d delay=%s err=%v",
			env.task.ExecutionID, env.task.NodeName, env.transientTries, delay, err)
	}
	go func(env queueEnvelope, delay time.Duration) {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-q.stopCh:
			return
		case <-timer.C:
		}
		select {
		case q.ch <- env:
		case <-q.stopCh:
		}
	}(env, delay)
}

// transientBackoff doubles each attempt up to transientRequeueCap. No jitter:
// the in-memory queue is single-process so collision avoidance is unnecessary.
func transientBackoff(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	d := transientRequeueInitial << attempt
	if d <= 0 || d > transientRequeueCap {
		return transientRequeueCap
	}
	return d
}

// Stop signals all queue consumers to exit and waits for them to drain.
func (q *memoryQueue) Stop() {
	close(q.stopCh)
	q.wg.Wait()
}

func (q *memoryQueue) Enqueue(_ context.Context, t *engine.Task) error {
	env := queueEnvelope{task: t}
	select {
	case q.ch <- env:
		return nil
	default:
		// Channel full — grow dynamically by spawning a temporary goroutine.
		go func() {
			select {
			case q.ch <- env:
			case <-q.stopCh:
			}
		}()
		return nil
	}
}

func (q *memoryQueue) EnqueueDelayed(_ context.Context, t *engine.Task, delay time.Duration) error {
	env := queueEnvelope{task: t}
	go func() {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-q.stopCh:
			return
		case <-timer.C:
		}
		select {
		case q.ch <- env:
		case <-q.stopCh:
		}
	}()
	return nil
}
