package redishub

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/trigger/triggertest"
	"github.com/xbcio/xflow/types"
)

func TestRedisHubTriggerDescriptor(t *testing.T) {
	n := New()
	desc := n.Descriptor()
	if desc.Type != "xflow.trigger.redis_hub" || desc.Kind != types.NodeKindTrigger {
		t.Fatalf("descriptor = %+v", desc)
	}
}

func TestRedisHubTriggerStreamRequiresStreamAndGroup(t *testing.T) {
	_, err := New().Mode("stream").Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "redis",
		Params:     map[string]any{"mode": "stream"},
		Runtime:    triggertest.NewFakeRuntime(),
	})
	if err == nil {
		t.Fatal("expected missing stream/group error")
	}
}

func TestRedisHubTriggerPubSubRequiresChannel(t *testing.T) {
	_, err := New().Mode("pubsub").Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "redis",
		Params:     map[string]any{"mode": "pubsub"},
		Runtime:    triggertest.NewFakeRuntime(),
	})
	if err == nil {
		t.Fatal("expected missing channel error")
	}
}

func TestRedisHubTriggerSkipsEmitWhenDedupErrors(t *testing.T) {
	orig := newConsumer
	consumer := newScriptedConsumer([]Message{{ID: "1", Stream: "orders", Payload: []byte("one")}})
	newConsumer = func(ConsumerConfig) (Consumer, error) { return consumer, nil }
	t.Cleanup(func() { newConsumer = orig })

	rt := triggertest.NewFakeRuntime()
	rt.SetDedupFunc(func(context.Context, string, time.Duration) (bool, error) {
		return true, errors.New("boom")
	})
	tr := New().Mode("stream").Stream("orders").Group("workers")
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

	if !rt.WaitDedup(time.Second) {
		t.Fatal("redis hub trigger did not attempt dedup")
	}
	if got := rt.EmitCount(); got != 0 {
		t.Fatalf("emit count = %d, want 0", got)
	}
}

func TestRedisHubTriggerContinuesAfterEmitError(t *testing.T) {
	orig := newConsumer
	consumer := newScriptedConsumer([]Message{
		{ID: "1", Stream: "orders", Payload: []byte("one")},
		{ID: "2", Stream: "orders", Payload: []byte("two")},
	})
	newConsumer = func(ConsumerConfig) (Consumer, error) { return consumer, nil }
	t.Cleanup(func() { newConsumer = orig })

	rt := triggertest.NewFakeRuntime()
	// MaxInflight(1) is load-bearing here: the semaphore serializes the emit
	// goroutines, which is what makes this unsynchronized counter safe.
	var calls int
	rt.SetEmitFunc(func(context.Context, types.WorkflowID, string, *types.TriggerEvent) (types.ExecutionID, error) {
		calls++
		if calls == 1 {
			return "", errors.New("boom")
		}
		return "exec-2", nil
	})
	tr := New().Mode("stream").Stream("orders").Group("workers").MaxInflight(1)
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

	if !rt.WaitForEmitCount(2, time.Second) {
		t.Fatalf("emit count = %d, want at least 2", rt.EmitCount())
	}
}

func TestRedisHubTriggerPubSubStopsWhenLockRenewalFails(t *testing.T) {
	origConsumer := newConsumer
	consumer := newBlockingConsumer()
	newConsumer = func(ConsumerConfig) (Consumer, error) { return consumer, nil }
	t.Cleanup(func() { newConsumer = origConsumer })

	origTTL := pubSubLockTTL
	pubSubLockTTL = 20 * time.Millisecond
	t.Cleanup(func() { pubSubLockTTL = origTTL })

	lock := newScriptedRenewableTriggerLock(renewResult{ok: false})
	rt := &renewableLockRuntime{
		FakeRuntime: triggertest.NewFakeRuntime(),
		lock:        lock,
	}

	sub, err := New().Mode("pubsub").Channel("orders").Activate(context.Background(), &types.TriggerActivateInput{
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
	origConsumer := newConsumer
	newConsumer = func(ConsumerConfig) (Consumer, error) {
		t.Fatal("consumer factory should not be called for non-renewable pub/sub lock")
		return nil, nil
	}
	t.Cleanup(func() { newConsumer = origConsumer })

	lock := &scriptedTriggerLock{}
	rt := &renewableLockRuntime{
		FakeRuntime: triggertest.NewFakeRuntime(),
		lock:        lock,
	}

	sub, err := New().Mode("pubsub").Channel("orders").Activate(context.Background(), &types.TriggerActivateInput{
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

type scriptedConsumer struct {
	ch chan Message
}

func newScriptedConsumer(messages []Message) *scriptedConsumer {
	ch := make(chan Message, len(messages))
	for _, msg := range messages {
		ch <- msg
	}
	return &scriptedConsumer{ch: ch}
}

func (c *scriptedConsumer) Messages() <-chan Message { return c.ch }

func (c *scriptedConsumer) Close() error {
	close(c.ch)
	return nil
}

// renewableLockRuntime overrides only TryLock; everything else (Emit, Dedup,
// State, the emit bookkeeping the assertions read) comes from the embedded fake.
type renewableLockRuntime struct {
	*triggertest.FakeRuntime
	lock types.TriggerLock
}

func (r *renewableLockRuntime) TryLock(context.Context, string, time.Duration) (types.TriggerLock, bool, error) {
	return r.lock, true, nil
}

type blockingConsumer struct {
	ch     chan Message
	closed chan struct{}
	once   sync.Once
}

func newBlockingConsumer() *blockingConsumer {
	return &blockingConsumer{
		ch:     make(chan Message),
		closed: make(chan struct{}),
	}
}

func (c *blockingConsumer) Messages() <-chan Message { return c.ch }

func (c *blockingConsumer) Close() error {
	c.once.Do(func() {
		close(c.closed)
		close(c.ch)
	})
	return nil
}

func (c *blockingConsumer) waitClosed(timeout time.Duration) bool {
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

func TestRedisHubNodeTypeAndParamsAreFrozen(t *testing.T) {
	n := New().Mode("stream").Stream("orders").Group("workers").Channel("c")
	if n.NodeType() != "xflow.trigger.redis_hub" {
		t.Fatalf("NodeType = %q, want xflow.trigger.redis_hub", n.NodeType())
	}
	params := n.RawParams().(map[string]any)
	want := map[string]any{
		"mode":         "stream",
		"stream":       "orders",
		"group":        "workers",
		"channel":      "c",
		"max_inflight": 64,
	}
	if len(params) != len(want) {
		t.Fatalf("RawParams keys = %v, want exactly %v", params, want)
	}
	for k, v := range want {
		if params[k] != v {
			t.Fatalf("RawParams[%q] = %#v, want %#v", k, params[k], v)
		}
	}
}

func TestRedisHubRawParamsNormalizesEmptyModeAndBadInflight(t *testing.T) {
	// A zero-valued node (the YAML path's shape) must still serialize a usable
	// mode and a positive inflight window, otherwise the semaphore in Activate
	// would be unbuffered and the trigger would deadlock on its first message.
	params := (&Node{}).RawParams().(map[string]any)
	if params["mode"] != "stream" {
		t.Fatalf("mode = %#v, want stream", params["mode"])
	}
	if params["max_inflight"] != 64 {
		t.Fatalf("max_inflight = %#v, want 64", params["max_inflight"])
	}
}

func TestRedisHubConfigFromParamsRejectsUnknownMode(t *testing.T) {
	if _, err := configFromParams(map[string]any{"mode": "queue"}); err == nil {
		t.Fatal("expected an unsupported-mode error")
	}
}
