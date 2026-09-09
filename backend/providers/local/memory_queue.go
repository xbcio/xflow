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

	// mu 用于保护 stopped，且每一次后台重试/延迟 goroutine 的 wg.Add(1)
	// 都必须在持有 mu 的情况下发生。sync.WaitGroup 自身文档（以及其运行时
	// 自检——已实测验证：本队列在未加此保护的版本上，在一次 EnqueueDelayed
	// 与 Stop 并发的压力测试的第一轮试验中就会 panic："sync: WaitGroup is
	// reused before previous Wait has returned" / "Add called concurrently
	// with Wait"）都禁止正在进行的 Add 与 Wait 并发。Stop() 先取得 mu、
	// 置位 stopped，然后才 close(stopCh) 并调用 wg.Wait —— 因此任何将要
	// 发生的 Add，要么已经在持有 mu 的情况下完成（严格早于 Stop 的
	// wg.Wait），要么被直接拒绝（观察到 stopped 为 true，完全不发生
	// Add）。无论哪种情况，wg.Add 都不可能与 wg.Wait 并发。
	mu      sync.Mutex
	stopped bool
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
	// 把这个重新入队的 goroutine 记录进 wg，这样 Stop 会等待未完成的重试。
	if !q.beginBackground() {
		// Stop 已经开始：不会再有任何 worker 去消费重新投递的任务，
		// 这次重试永远不可能被服务到。在此记录日志，而不是静默丢弃——
		// 之前的做法是零痕迹丢弃，这正是上面「耗尽重试次数/永久性错误」
		// 两个分支特意通过记录日志来避免的同一种「静默 continue」模式。
		if q.logger != nil {
			q.logger.Error("dropping task: queue stopped before transient retry could be scheduled",
				"exec", string(env.task.ExecutionID),
				"node", env.task.NodeName,
				"attempt", env.transientTries,
			)
		}
		return
	}
	go func(env queueEnvelope, delay time.Duration) {
		defer q.wg.Done()
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-q.stopCh:
			if q.logger != nil {
				q.logger.Error("dropping task: queue stopped before transient retry delay elapsed",
					"exec", string(env.task.ExecutionID),
					"node", env.task.NodeName,
					"attempt", env.transientTries,
				)
			}
			return
		case <-timer.C:
		}
		select {
		case q.laneFor(env.task) <- env:
		case <-q.stopCh:
			if q.logger != nil {
				q.logger.Error("dropping task: queue stopped before transient retry could be redelivered",
					"exec", string(env.task.ExecutionID),
					"node", env.task.NodeName,
					"attempt", env.transientTries,
				)
			}
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

// beginBackground 为一个后台重试/延迟 goroutine 向 wg 注册，但仅在 Stop
// 尚未开始时才会注册。当队列已经在停止过程中时返回 false，此时调用方绝不能
// 再启动该 goroutine —— 即使启动了，它也只会白白等完延迟，最终落到下面的
// stopCh 分支上被丢弃，与其立即丢弃相比只是多了一段无意义的等待。
//
// 这正是让 Stop 可以与 EnqueueDelayed / 瞬时失败重试的重新入队安全地并发
// 调用的机制：为何在此处对每一次 Add 加门禁就能消除与 Stop 的 Wait 之间的
// 竞态，见 struct 上 mu 字段的文档注释。
func (q *memoryQueue) beginBackground() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.stopped {
		return false
	}
	q.wg.Add(1)
	return true
}

// Stop 通知所有队列消费者退出，并等待它们全部退出。
//
// 在 close(stopCh)/wait 之前，先在持有 mu 的情况下置位 stopped，这一步是
// 承重的而非装饰性的：正是它阻止了 beginBackground 在下面的 wg.Wait 期间
// 并发地调用 wg.Add（见 struct 上 mu 字段的文档注释）。Stop 的返回也是本
// 队列唯一可观察的"已完全静止"信号——已验证：wg.Wait 要等到每一个 worker
// goroutine（来自 Start）以及每一个被 beginBackground 记录过的重试/延迟
// goroutine 都调用过其 deferred wg.Done 之后，才会返回。
func (q *memoryQueue) Stop() {
	q.mu.Lock()
	q.stopped = true
	q.mu.Unlock()
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
	// 把这个延迟 goroutine 记录进 wg，这样 Stop 会等待它完成，
	// 而不是静默丢弃已排定的任务。
	if !q.beginBackground() {
		return errors.New("local: queue stopped")
	}
	go func() {
		defer q.wg.Done()
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-q.stopCh:
			if q.logger != nil {
				q.logger.Error("dropping delayed task: queue stopped before delay elapsed",
					"exec", string(t.ExecutionID),
					"node", t.NodeName,
				)
			}
			return
		case <-timer.C:
		}
		select {
		case lane <- env:
		case <-q.stopCh:
			if q.logger != nil {
				q.logger.Error("dropping delayed task: queue stopped before redelivery",
					"exec", string(t.ExecutionID),
					"node", t.NodeName,
				)
			}
		}
	}()
	return nil
}
