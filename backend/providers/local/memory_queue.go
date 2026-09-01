package local

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
	transientRequeueInitial    = 100 * time.Millisecond
	transientRequeueCap        = 30 * time.Second
	transientRequeueMax        = 1000
	defaultMemoryQueueCapacity = 1024
)

// interactiveBurst is how many ordinary tasks a worker may serve before the
// batch lane gets a guaranteed turn. It buys latency isolation without letting
// a steady stream of ordinary tasks starve an in-flight map: at 8 the batch
// lane keeps at least ~1/9 of every worker's throughput.
//
// Strict priority was measured to be as fast but is not safe here — a map that
// never gets a slot never completes, and nothing reports it.
const interactiveBurst = 8

// queueEnvelope carries scheduler-internal retry counters alongside the task.
// engine.Task hides AutoDepth/ActivationID from runner JSON but the in-process
// queue can use any fields it likes.
type queueEnvelope struct {
	task           *engine.Task
	transientTries int
}

// memoryQueue implements engine.TaskQueue using buffered channels and a
// goroutine pool.
//
// Tasks ride one of two lanes. Batch continuations (an expanded map's items) go
// to batchCh; everything else goes to ch. With a single FIFO, one wide map
// occupies every slot ahead of an unrelated execution's first node: measured at
// 200 batches x 20ms, a small execution submitted into the saturated queue
// waited 854ms to run its first node. Splitting the lanes brings that to 10ms
// with the map's own wall-clock unchanged.
type memoryQueue struct {
	ch          chan queueEnvelope
	batchCh     chan queueEnvelope
	handler     func(ctx context.Context, t *engine.Task) error
	logger      engine.Logger
	concurrency int
	wg          sync.WaitGroup
	stopCh      chan struct{}
}

func newMemoryQueue(concurrency int) *memoryQueue {
	return newMemoryQueueWithCapacity(concurrency, defaultMemoryQueueCapacity)
}

func newMemoryQueueWithCapacity(concurrency, capacity int) *memoryQueue {
	return &memoryQueue{
		ch:          make(chan queueEnvelope, capacity),
		batchCh:     make(chan queueEnvelope, capacity),
		concurrency: concurrency,
		stopCh:      make(chan struct{}),
	}
}

// laneFor routes a task to its lane. Only engine-internal batch continuations
// take the batch lane; delayed and requeued tasks route through here too, so a
// retried batch does not sneak back onto the interactive lane.
func (q *memoryQueue) laneFor(t *engine.Task) chan queueEnvelope {
	if t != nil && t.Type == engine.TaskTypeNodeBatch {
		return q.batchCh
	}
	return q.ch
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

// worker serves the interactive lane ahead of the batch lane, but only for
// interactiveBurst tasks in a row. After that it takes one batch task if any is
// waiting, then resets. Both lanes therefore make progress under any mix, and a
// task from either lane is served immediately when the other lane is idle.
func (q *memoryQueue) worker() {
	defer q.wg.Done()
	served := 0
	for {
		if served < interactiveBurst {
			// Interactive first, without blocking: if nothing is waiting there,
			// fall through to the two-lane select rather than idling.
			select {
			case <-q.stopCh:
				return
			case env, ok := <-q.ch:
				if !ok {
					return
				}
				served++
				q.dispatch(env)
				continue
			default:
			}
		} else {
			// The burst is spent. Give the batch lane its guaranteed turn.
			served = 0
			select {
			case <-q.stopCh:
				return
			case env, ok := <-q.batchCh:
				if !ok {
					return
				}
				q.dispatch(env)
				continue
			default:
			}
		}
		select {
		case <-q.stopCh:
			return
		case env, ok := <-q.ch:
			if !ok {
				return
			}
			served++
			q.dispatch(env)
		case env, ok := <-q.batchCh:
			if !ok {
				return
			}
			served = 0
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
	// Track the requeue goroutine in wg so Stop waits for pending retries.
	q.wg.Add(1)
	go func(env queueEnvelope, delay time.Duration) {
		defer q.wg.Done()
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-q.stopCh:
			return
		case <-timer.C:
		}
		select {
		case q.laneFor(env.task) <- env:
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

func (q *memoryQueue) Enqueue(ctx context.Context, t *engine.Task) error {
	env := queueEnvelope{task: t}
	lane := q.laneFor(t)
	// Respect the caller's ctx: block until space is available, the queue is
	// stopped, or the context is canceled. No unbounded goroutines.
	//
	// Blocking is right for callers that hold no durable retry: Submit's
	// initial tasks and the legacy lease-revoke redelivery both have no outbox
	// behind them, and lease.go documents that a failed enqueue there strands
	// the node where the sweeper cannot see it. Only FlushOutbox — which does
	// have a durable intent to fall back on, and which runs on a worker
	// goroutine where blocking deadlocks — takes the non-blocking path below.
	select {
	case lane <- env:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-q.stopCh:
		return errors.New("local: queue stopped")
	}
}

// TryEnqueue offers a task without blocking, reporting engine.ErrQueueFull
// when the buffer has no room.
//
// This exists because FlushOutbox runs on a queue worker goroutine: a fan-out
// wider than the channel's capacity would park every worker inside the send it
// needs those same workers to make room for. The deadlock is permanent and
// emits nothing — no error, no log, no metric beyond a backlog that stops
// moving. Reporting fullness instead lets the engine leave the intent durable
// and redeliver once the workers drain.
func (q *memoryQueue) TryEnqueue(_ context.Context, t *engine.Task) error {
	select {
	case q.laneFor(t) <- queueEnvelope{task: t}:
		return nil
	case <-q.stopCh:
		return errors.New("local: queue stopped")
	default:
		return engine.ErrQueueFull
	}
}

func (q *memoryQueue) EnqueueDelayed(_ context.Context, t *engine.Task, delay time.Duration) error {
	env := queueEnvelope{task: t}
	lane := q.laneFor(t)
	// Track the delayed goroutine in wg so Stop waits for it instead of
	// silently dropping scheduled tasks.
	q.wg.Add(1)
	go func() {
		defer q.wg.Done()
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-q.stopCh:
			return
		case <-timer.C:
		}
		select {
		case lane <- env:
		case <-q.stopCh:
		}
	}()
	return nil
}
