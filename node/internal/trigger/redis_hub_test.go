package trigger

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/types"
)

func TestRedisHubTriggerDescriptor(t *testing.T) {
	n := RedisHubTrigger()
	desc := n.Descriptor()
	if desc.Type != "xflow.trigger.redis_hub" || desc.Kind != types.NodeKindTrigger {
		t.Fatalf("descriptor = %+v", desc)
	}
}

func TestRedisHubTriggerStreamRequiresStreamAndGroup(t *testing.T) {
	_, err := RedisHubTrigger().Mode("stream").Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "redis",
		Params:     map[string]any{"mode": "stream"},
		Runtime:    newFakeTriggerRuntime(),
	})
	if err == nil {
		t.Fatal("expected missing stream/group error")
	}
}

func TestRedisHubTriggerPubSubRequiresChannel(t *testing.T) {
	_, err := RedisHubTrigger().Mode("pubsub").Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "redis",
		Params:     map[string]any{"mode": "pubsub"},
		Runtime:    newFakeTriggerRuntime(),
	})
	if err == nil {
		t.Fatal("expected missing channel error")
	}
}

func TestRedisHubTriggerSkipsEmitWhenDedupErrors(t *testing.T) {
	orig := newRedisHubConsumer
	consumer := newScriptedRedisHubConsumer([]RedisHubMessage{{ID: "1", Stream: "orders", Payload: []byte("one")}})
	newRedisHubConsumer = func(RedisHubConsumerConfig) (RedisHubConsumer, error) { return consumer, nil }
	t.Cleanup(func() { newRedisHubConsumer = orig })

	rt := newFakeTriggerRuntime()
	rt.dedupFunc = func(context.Context, string, time.Duration) (bool, error) {
		return true, errors.New("boom")
	}
	tr := RedisHubTrigger().Mode("stream").Stream("orders").Group("workers")
	sub, err := tr.Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "redis",
		Params:     tr.RawParams().(map[string]any),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close(context.Background()) }()

	if !rt.waitDedup(time.Second) {
		t.Fatal("redis hub trigger did not attempt dedup")
	}
	if got := rt.emitCount(); got != 0 {
		t.Fatalf("emit count = %d, want 0", got)
	}
}

func TestRedisHubTriggerContinuesAfterEmitError(t *testing.T) {
	orig := newRedisHubConsumer
	consumer := newScriptedRedisHubConsumer([]RedisHubMessage{
		{ID: "1", Stream: "orders", Payload: []byte("one")},
		{ID: "2", Stream: "orders", Payload: []byte("two")},
	})
	newRedisHubConsumer = func(RedisHubConsumerConfig) (RedisHubConsumer, error) { return consumer, nil }
	t.Cleanup(func() { newRedisHubConsumer = orig })

	rt := newFakeTriggerRuntime()
	var calls int
	rt.emitFunc = func(context.Context, types.WorkflowID, string, *types.TriggerEvent) (types.ExecutionID, error) {
		calls++
		if calls == 1 {
			return "", errors.New("boom")
		}
		return "exec-2", nil
	}
	tr := RedisHubTrigger().Mode("stream").Stream("orders").Group("workers").MaxInflight(1)
	sub, err := tr.Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "redis",
		Params:     tr.RawParams().(map[string]any),
		Runtime:    rt,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close(context.Background()) }()

	if !rt.waitForEmitCount(2, time.Second) {
		t.Fatalf("emit count = %d, want at least 2", rt.emitCount())
	}
}

func TestRedisHubTriggerPubSubStopsWhenLockRenewalFails(t *testing.T) {
	origConsumer := newRedisHubConsumer
	consumer := newBlockingRedisHubConsumer()
	newRedisHubConsumer = func(RedisHubConsumerConfig) (RedisHubConsumer, error) { return consumer, nil }
	t.Cleanup(func() { newRedisHubConsumer = origConsumer })

	origTTL := redisHubPubSubLockTTL
	redisHubPubSubLockTTL = 20 * time.Millisecond
	t.Cleanup(func() { redisHubPubSubLockTTL = origTTL })

	lock := newScriptedRenewableTriggerLock(renewResult{ok: false})
	rt := &renewableLockRuntime{
		fakeTriggerRuntime: newFakeTriggerRuntime(),
		lock:               lock,
	}

	sub, err := RedisHubTrigger().Mode("pubsub").Channel("orders").Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "redis",
		Params:     map[string]any{"mode": "pubsub", "channel": "orders", "max_inflight": 1},
		Runtime:    rt,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := sub.Close(context.Background()); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	}()

	if !lock.waitRenew(time.Second) {
		t.Fatal("lock was not renewed")
	}
	if !consumer.waitClosed(time.Second) {
		t.Fatal("consumer was not closed after renewal failure")
	}
	if !lock.waitRelease(time.Second) {
		t.Fatal("lock was not released after renewal failure")
	}
	if got := lock.releaseCount(); got != 1 {
		t.Fatalf("release count = %d, want 1", got)
	}
}

func TestRedisHubTriggerPubSubRequiresRenewableLock(t *testing.T) {
	origConsumer := newRedisHubConsumer
	newRedisHubConsumer = func(RedisHubConsumerConfig) (RedisHubConsumer, error) {
		t.Fatal("consumer factory should not be called for non-renewable pub/sub lock")
		return nil, nil
	}
	t.Cleanup(func() { newRedisHubConsumer = origConsumer })

	lock := &scriptedTriggerLock{}
	rt := &renewableLockRuntime{
		fakeTriggerRuntime: newFakeTriggerRuntime(),
		lock:               lock,
	}

	sub, err := RedisHubTrigger().Mode("pubsub").Channel("orders").Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "redis",
		Params:     map[string]any{"mode": "pubsub", "channel": "orders", "max_inflight": 1},
		Runtime:    rt,
	})
	if err == nil {
		if sub != nil {
			_ = sub.Close(context.Background())
		}
		t.Fatal("Activate() error = nil, want non-renewable lock error")
	}
	if got := lock.releaseCount(); got != 1 {
		t.Fatalf("release count = %d, want 1", got)
	}
}

type scriptedRedisHubConsumer struct {
	ch chan RedisHubMessage
}

func newScriptedRedisHubConsumer(messages []RedisHubMessage) *scriptedRedisHubConsumer {
	ch := make(chan RedisHubMessage, len(messages))
	for _, msg := range messages {
		ch <- msg
	}
	return &scriptedRedisHubConsumer{ch: ch}
}

func (c *scriptedRedisHubConsumer) Messages() <-chan RedisHubMessage { return c.ch }

func (c *scriptedRedisHubConsumer) Close() error {
	close(c.ch)
	return nil
}

type renewableLockRuntime struct {
	*fakeTriggerRuntime
	lock types.TriggerLock
}

func (r *renewableLockRuntime) TryLock(context.Context, string, time.Duration) (types.TriggerLock, bool, error) {
	return r.lock, true, nil
}

type blockingRedisHubConsumer struct {
	ch     chan RedisHubMessage
	closed chan struct{}
	once   sync.Once
}

func newBlockingRedisHubConsumer() *blockingRedisHubConsumer {
	return &blockingRedisHubConsumer{
		ch:     make(chan RedisHubMessage),
		closed: make(chan struct{}),
	}
}

func (c *blockingRedisHubConsumer) Messages() <-chan RedisHubMessage { return c.ch }

func (c *blockingRedisHubConsumer) Close() error {
	c.once.Do(func() {
		close(c.closed)
		close(c.ch)
	})
	return nil
}

func (c *blockingRedisHubConsumer) waitClosed(timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-c.closed:
		return true
	case <-timer.C:
		return false
	}
}

type renewResult struct {
	ok  bool
	err error
}

type scriptedTriggerLock struct {
	mu       sync.Mutex
	releases int
}

func (l *scriptedTriggerLock) Release(context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.releases++
	return nil
}

func (l *scriptedTriggerLock) releaseCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.releases
}

type scriptedRenewableTriggerLock struct {
	results       chan renewResult
	renewSignal   chan struct{}
	releaseSignal chan struct{}
	releaseOnce   sync.Once
	mu            sync.Mutex
	releases      int
}

func newScriptedRenewableTriggerLock(results ...renewResult) *scriptedRenewableTriggerLock {
	ch := make(chan renewResult, len(results))
	for _, result := range results {
		ch <- result
	}
	return &scriptedRenewableTriggerLock{
		results:       ch,
		renewSignal:   make(chan struct{}, 8),
		releaseSignal: make(chan struct{}),
	}
}

func (l *scriptedRenewableTriggerLock) Renew(context.Context, time.Duration) (bool, error) {
	select {
	case l.renewSignal <- struct{}{}:
	default:
	}
	result := <-l.results
	return result.ok, result.err
}

func (l *scriptedRenewableTriggerLock) Release(context.Context) error {
	l.mu.Lock()
	l.releases++
	l.mu.Unlock()
	l.releaseOnce.Do(func() { close(l.releaseSignal) })
	return nil
}

func (l *scriptedRenewableTriggerLock) waitRenew(timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-l.renewSignal:
		return true
	case <-timer.C:
		return false
	}
}

func (l *scriptedRenewableTriggerLock) waitRelease(timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-l.releaseSignal:
		return true
	case <-timer.C:
		return false
	}
}

func (l *scriptedRenewableTriggerLock) releaseCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.releases
}
