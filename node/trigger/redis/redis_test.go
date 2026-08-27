package redis

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/trigger/triggertest"
	"github.com/xbcio/xflow/types"
)

func TestRedisTriggerDescriptor(t *testing.T) {
	n := New()
	desc := n.Descriptor()
	if desc.Type != "xflow.trigger.redis" || desc.Kind != types.NodeKindTrigger {
		t.Fatalf("descriptor = %+v", desc)
	}
}

func TestRedisTriggerStreamRequiresStreamAndGroup(t *testing.T) {
	_, err := New().Mode("stream").Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "redis",
		Params:     map[string]any{"addr": "127.0.0.1:6379", "mode": "stream"},
		Runtime:    triggertest.NewFakeRuntime(),
	})
	if err == nil {
		t.Fatal("expected missing stream/group error")
	}
}

func TestRedisTriggerPubSubRequiresChannel(t *testing.T) {
	_, err := New().Mode("pubsub").Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "redis",
		Params:     map[string]any{"addr": "127.0.0.1:6379", "mode": "pubsub"},
		Runtime:    triggertest.NewFakeRuntime(),
	})
	if err == nil {
		t.Fatal("expected missing channel error")
	}
}

func TestRedisTriggerSkipsEmitWhenDedupErrors(t *testing.T) {
	orig := newConsumer
	consumer := newScriptedConsumer([]Message{{ID: "1", Stream: "orders", Payload: []byte("one")}})
	newConsumer = func(ConsumerConfig) (Consumer, error) { return consumer, nil }
	t.Cleanup(func() { newConsumer = orig })

	rt := triggertest.NewFakeRuntime()
	rt.SetDedupFunc(func(context.Context, string, time.Duration) (bool, error) {
		return true, errors.New("boom")
	})
	tr := New().Addr("127.0.0.1:6379").Mode("stream").Stream("orders").Group("workers")
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
		t.Fatal("redis trigger did not attempt dedup")
	}
	if got := rt.EmitCount(); got != 0 {
		t.Fatalf("emit count = %d, want 0", got)
	}
}

func TestRedisTriggerContinuesAfterEmitError(t *testing.T) {
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
	tr := New().Addr("127.0.0.1:6379").Mode("stream").Stream("orders").Group("workers").MaxInflight(1)
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

func TestRedisTriggerPubSubStopsWhenLockRenewalFails(t *testing.T) {
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

	sub, err := New().Addr("127.0.0.1:6379").Mode("pubsub").Channel("orders").Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "redis",
		Params:     map[string]any{"addr": "127.0.0.1:6379", "mode": "pubsub", "channel": "orders", "max_inflight": 1},
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

func TestRedisTriggerPubSubRequiresRenewableLock(t *testing.T) {
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

	sub, err := New().Addr("127.0.0.1:6379").Mode("pubsub").Channel("orders").Activate(context.Background(), &types.TriggerActivateInput{
		WorkflowID: "wf-1",
		NodeName:   "redis",
		Params:     map[string]any{"addr": "127.0.0.1:6379", "mode": "pubsub", "channel": "orders", "max_inflight": 1},
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

func TestRedisNodeTypeAndParamsAreFrozen(t *testing.T) {
	n := New().Addr("127.0.0.1:6379").Mode("stream").Stream("orders").Group("workers").Channel("c")
	if n.NodeType() != "xflow.trigger.redis" {
		t.Fatalf("NodeType = %q, want xflow.trigger.redis", n.NodeType())
	}
	params := n.RawParams().(map[string]any)
	want := map[string]any{
		"addr":         "127.0.0.1:6379",
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

func TestRedisRawParamsNormalizesEmptyModeAndBadInflight(t *testing.T) {
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

func TestRedisConfigFromParamsRejectsUnknownMode(t *testing.T) {
	if _, err := configFromParams(map[string]any{"addr": "127.0.0.1:6379", "mode": "queue"}, nil); err == nil {
		t.Fatal("expected an unsupported-mode error")
	}
}

func TestRedisConfigFromParamsRequiresAddr(t *testing.T) {
	// Without this the consumer factory would build a client against
	// go-redis's own default (localhost:6379) and the trigger would silently
	// consume from whatever Redis happens to be on the runner's own host.
	if _, err := configFromParams(map[string]any{
		"mode":   "stream",
		"stream": "orders",
		"group":  "workers",
	}, nil); err == nil {
		t.Fatal("configFromParams() error = nil, want a missing-addr error")
	}
}

func TestRedisConfigFromParamsTakesCredentialsOnlyFromSupply(t *testing.T) {
	// The param surface has no password field at all, so this pins the other
	// half of that arrangement: a definition that smuggles one in anyway must
	// not be honoured, or an operator could put a credential into the stored,
	// hashed workflow definition and have it work.
	cfg, err := configFromParams(map[string]any{
		"addr":     "127.0.0.1:6379",
		"mode":     "stream",
		"stream":   "orders",
		"group":    "workers",
		"username": "from-params",
		"password": "from-params",
	}, map[string]any{"username": "app", "password": "s3cret"})
	if err != nil {
		t.Fatalf("configFromParams() error = %v", err)
	}
	if cfg.Username != "app" || cfg.Password != "s3cret" {
		t.Fatalf("credentials = %q/%q, want the supply's app/s3cret", cfg.Username, cfg.Password)
	}
}

func TestRedisConfigFromParamsAppliesStreamDefaults(t *testing.T) {
	cfg, err := configFromParams(map[string]any{
		"addr":   "127.0.0.1:6379",
		"mode":   "stream",
		"stream": "orders",
		"group":  "workers",
	}, nil)
	if err != nil {
		t.Fatalf("configFromParams() error = %v", err)
	}
	// start_id "$" is the one default with a data consequence: "0" would
	// replay the entire stream the first time any workflow is activated.
	if cfg.StartID != "$" {
		t.Fatalf("StartID = %q, want $", cfg.StartID)
	}
	if cfg.ClaimMinIdle != time.Minute {
		t.Fatalf("ClaimMinIdle = %v, want 1m", cfg.ClaimMinIdle)
	}
	if cfg.DialTimeout != 10*time.Second {
		t.Fatalf("DialTimeout = %v, want 10s", cfg.DialTimeout)
	}
}

func TestRedisConfigFromParamsReadsTuning(t *testing.T) {
	cfg, err := configFromParams(map[string]any{
		"addr":   "127.0.0.1:6379",
		"mode":   "stream",
		"stream": "orders",
		"group":  "workers",
		"tuning": map[string]any{
			"db":             2,
			"consumer":       "runner-a",
			"start_id":       "0",
			"payload_field":  "body",
			"claim_min_idle": "30s",
			"dial_timeout":   "2s",
		},
	}, nil)
	if err != nil {
		t.Fatalf("configFromParams() error = %v", err)
	}
	want := ConsumerConfig{
		Addr: "127.0.0.1:6379", DB: 2, DialTimeout: 2 * time.Second,
		Mode: "stream", Stream: "orders", Group: "workers",
		Consumer: "runner-a", StartID: "0",
		PayloadField: "body", ClaimMinIdle: 30 * time.Second,
		MaxInflight: defaultTriggerMaxInflight,
	}
	if cfg != want {
		t.Fatalf("config = %+v, want %+v", cfg, want)
	}
}

func TestRedisConfigFromParamsRejectsMalformedTuning(t *testing.T) {
	// Both of these would otherwise leave the trigger running on defaults an
	// operator believes they overrode, which is invisible until the stream
	// misbehaves months later.
	for name, tuning := range map[string]any{
		"not an object":        "claim_min_idle=30s",
		"unparseable duration": map[string]any{"claim_min_idle": "half an hour"},
		"non-positive":         map[string]any{"dial_timeout": "0s"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := configFromParams(map[string]any{
				"addr":   "127.0.0.1:6379",
				"mode":   "stream",
				"stream": "orders",
				"group":  "workers",
				"tuning": tuning,
			}, nil); err == nil {
				t.Fatalf("configFromParams() error = nil, want a tuning error for %v", tuning)
			}
		})
	}
}

func TestRedisTuningIsOmittedFromRawParamsWhenUnset(t *testing.T) {
	// A key that is always written would move the definition hash of every
	// workflow that never asked for it, which is why Tuning is omitted rather
	// than serialized as an empty object.
	params := New().Addr("127.0.0.1:6379").Stream("orders").Group("workers").RawParams().(map[string]any)
	if _, ok := params["tuning"]; ok {
		t.Fatalf("RawParams carries a tuning key for an untuned node: %#v", params["tuning"])
	}

	tuned := New().Addr("127.0.0.1:6379").Stream("orders").Group("workers").
		Tuning(Tuning{ClaimMinIdle: 30 * time.Second}).RawParams().(map[string]any)
	tuning, ok := tuned["tuning"].(map[string]any)
	if !ok {
		t.Fatalf("RawParams[tuning] = %#v, want an object", tuned["tuning"])
	}
	// Only the knob that was set: the others must not appear either, for the
	// same hash-stability reason.
	if want := map[string]any{"claim_min_idle": "30s"}; !reflect.DeepEqual(tuning, want) {
		t.Fatalf("tuning = %#v, want %#v", tuning, want)
	}
}
