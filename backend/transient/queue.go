package transient

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

const (
	transientRequeueInitial = 100 * time.Millisecond
	transientRequeueCap     = 30 * time.Second
	transientRequeueMax     = 1000
)

type queueEnvelope struct {
	task           *engine.Task
	transientTries int
}

type queue struct {
	ch          chan queueEnvelope
	handler     func(ctx context.Context, t *engine.Task) error
	state       *state
	logger      engine.Logger
	concurrency int
	wg          sync.WaitGroup
	stopCh      chan struct{}
}

func newQueue(concurrency int, state *state) *queue {
	return &queue{
		ch:          make(chan queueEnvelope, 1024),
		state:       state,
		concurrency: concurrency,
		stopCh:      make(chan struct{}),
	}
}

func (q *queue) SetHandler(fn func(ctx context.Context, t *engine.Task) error) {
	q.handler = fn
}

func (q *queue) SetLogger(l engine.Logger) { q.logger = l }

func (q *queue) Start() {
	for i := 0; i < q.concurrency; i++ {
		q.wg.Add(1)
		go q.worker()
	}
}

func (q *queue) worker() {
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

func (q *queue) dispatch(env queueEnvelope) {
	if q.handler == nil {
		return
	}
	if q.state != nil && q.state.executionTerminal(env.task.ExecutionID) {
		return
	}
	if q.state != nil && !q.state.executionExists(env.task.ExecutionID) {
		return
	}
	err := q.handler(context.Background(), env.task)
	if err == nil {
		return
	}
	if q.state != nil && !q.state.executionExists(env.task.ExecutionID) {
		return
	}
	if errors.Is(err, types.ErrPermanent) {
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

func (q *queue) Stop() {
	close(q.stopCh)
	q.wg.Wait()
}

func (q *queue) Enqueue(_ context.Context, t *engine.Task) error {
	env := queueEnvelope{task: t}
	select {
	case q.ch <- env:
		return nil
	default:
		go func() {
			select {
			case q.ch <- env:
			case <-q.stopCh:
			}
		}()
		return nil
	}
}

func (q *queue) EnqueueDelayed(_ context.Context, t *engine.Task, delay time.Duration) error {
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
