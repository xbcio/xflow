package asynq

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	asynqlib "github.com/hibiken/asynq"

	"github.com/xbcio/xflow/backend/providers/distributed/internal/queue"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

func TestNewUsesRedisClientOpt(t *testing.T) {
	tr := New("127.0.0.1:6379")
	if tr.client == nil {
		t.Fatal("transport client is nil")
	}
	opt, ok := tr.connOpt.(asynqlib.RedisClientOpt)
	if !ok {
		t.Fatalf("connOpt type = %T, want RedisClientOpt", tr.connOpt)
	}
	if opt.Addr != "127.0.0.1:6379" {
		t.Fatalf("RedisClientOpt.Addr = %q, want 127.0.0.1:6379", opt.Addr)
	}
	if err := tr.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestNewWithConnOptRedisClusterClientOpt(t *testing.T) {
	opt := asynqlib.RedisClusterClientOpt{
		Addrs: []string{"127.0.0.1:6379", "127.0.0.1:6380"},
	}
	tr := NewWithConnOpt(opt)
	if tr.client == nil {
		t.Fatal("transport client is nil")
	}
	got, ok := tr.connOpt.(asynqlib.RedisClusterClientOpt)
	if !ok {
		t.Fatalf("connOpt type = %T, want RedisClusterClientOpt", tr.connOpt)
	}
	if len(got.Addrs) != 2 || got.Addrs[0] != "127.0.0.1:6379" || got.Addrs[1] != "127.0.0.1:6380" {
		t.Fatalf("cluster Addrs = %v, want [127.0.0.1:6379 127.0.0.1:6380]", got.Addrs)
	}
	if err := tr.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestNewWithConnOptRedisFailoverClientOpt(t *testing.T) {
	opt := asynqlib.RedisFailoverClientOpt{
		MasterName:    "mymaster",
		SentinelAddrs: []string{"127.0.0.1:26379", "127.0.0.1:26380"},
	}
	tr := NewWithConnOpt(opt)
	got, ok := tr.connOpt.(asynqlib.RedisFailoverClientOpt)
	if !ok {
		t.Fatalf("connOpt type = %T, want RedisFailoverClientOpt", tr.connOpt)
	}
	if got.MasterName != "mymaster" {
		t.Fatalf("MasterName = %q, want mymaster", got.MasterName)
	}
	if len(got.SentinelAddrs) != 2 {
		t.Fatalf("SentinelAddrs = %v, want 2 entries", got.SentinelAddrs)
	}
	if err := tr.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestNewWithNilConnOptFallsBack(t *testing.T) {
	tr := NewWithConnOpt(nil)
	if tr.client == nil {
		t.Fatal("transport client is nil")
	}
	if tr.connOpt == nil {
		t.Fatal("connOpt is nil after fallback")
	}
	if err := tr.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestTransportEnqueueCarriesNamespace(t *testing.T) {
	server, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	defer server.Close()

	transport := New(server.Addr())
	defer func() { _ = transport.Close() }()

	ctx := namespace.WithNamespace(context.Background(), "namespace-acme")
	task := &engine.Task{
		ExecutionID: types.ExecutionID("exec-1"),
		NodeName:    "node-a",
		NodeIdx:     0,
		Type:        engine.TaskTypeNodeExec,
	}
	if err := transport.Enqueue(ctx, task); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	var gotNamespace namespace.Namespace
	var gotTask *engine.Task
	var wg sync.WaitGroup
	wg.Add(1)

	stop, err := transport.StartConsumer(queue.ConsumerConfig{Concurrency: 1}, func(ctx context.Context, t *engine.Task) error {
		gotTask = t
		gotNamespace = namespace.FromContext(ctx)
		wg.Done()
		return nil
	})
	if err != nil {
		t.Fatalf("StartConsumer() error = %v", err)
	}
	defer stop()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("consumer did not receive task")
	}

	if gotNamespace != "namespace-acme" {
		t.Fatalf("consumer namespace = %q, want namespace-acme", gotNamespace)
	}
	if gotTask == nil || string(gotTask.ExecutionID) != "exec-1" {
		t.Fatalf("consumer task = %+v, want exec-1", gotTask)
	}
}

func TestTransportConsumerDefaultsToDefaultNamespace(t *testing.T) {
	server, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	defer server.Close()

	transport := New(server.Addr())
	defer func() { _ = transport.Close() }()

	task := &engine.Task{
		ExecutionID: types.ExecutionID("exec-default"),
		NodeName:    "node-a",
		NodeIdx:     0,
		Type:        engine.TaskTypeNodeExec,
	}
	if err := transport.Enqueue(context.Background(), task); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	var gotNamespace namespace.Namespace
	var received atomic.Bool

	stop, err := transport.StartConsumer(queue.ConsumerConfig{Concurrency: 1}, func(ctx context.Context, t *engine.Task) error {
		gotNamespace = namespace.FromContext(ctx)
		received.Store(true)
		return nil
	})
	if err != nil {
		t.Fatalf("StartConsumer() error = %v", err)
	}
	defer stop()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && !received.Load() {
		time.Sleep(10 * time.Millisecond)
	}
	if !received.Load() {
		t.Fatal("consumer did not receive task")
	}
	if gotNamespace != namespace.Default {
		t.Fatalf("namespace = %q, want default", gotNamespace)
	}
}

// recordingStatsObserver captures queue-stats samples for the consumer-side
// sampler test.
type recordingStatsObserver struct {
	mu      sync.Mutex
	samples []statsSample
	ch      chan struct{}
}

type statsSample struct {
	queue   string
	pending int
	active  int
	retry   int
	age     time.Duration
}

func (o *recordingStatsObserver) OnQueueStats(queue string, pending, active, retry int, oldestPendingAge time.Duration) {
	o.mu.Lock()
	o.samples = append(o.samples, statsSample{queue: queue, pending: pending, active: active, retry: retry, age: oldestPendingAge})
	o.mu.Unlock()
	select {
	case o.ch <- struct{}{}:
	default:
	}
}

// TestConsumerSamplesQueueStats pins the broker-side residency signal: a
// consumer configured with a stats observer must report both queues it reads
// (depth and oldest-pending age), and stopping the consumer must not panic
// while the sampler is in flight.
func TestConsumerSamplesQueueStats(t *testing.T) {
	server, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	defer server.Close()

	transport := NewWithConnOpt(asynqlib.RedisClientOpt{Addr: server.Addr()})
	defer func() { _ = transport.Close() }()

	// Block the handler so the task is measurable as pending or active while
	// the sample is taken, instead of being consumed and completed.
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	obs := &recordingStatsObserver{ch: make(chan struct{}, 8)}

	stop, err := transport.StartConsumer(queue.ConsumerConfig{
		Concurrency:   1,
		StatsObserver: obs,
		StatsInterval: 20 * time.Millisecond,
	}, func(ctx context.Context, t *engine.Task) error {
		select {
		case started <- struct{}{}:
		default:
		}
		<-release
		return nil
	})
	if err != nil {
		t.Fatalf("StartConsumer() error = %v", err)
	}

	task := &engine.Task{
		ExecutionID: types.ExecutionID("exec-stats"),
		NodeName:    "start",
		NodeIdx:     0,
		Type:        engine.TaskTypeNodeExec,
	}
	if err := transport.Enqueue(context.Background(), task); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("consumer did not start the task")
	}

	// The sampler fires immediately at StartConsumer and then every 20ms, and
	// every sample reports both queues. Wait until the recorded samples contain
	// the actual observation under test — the in-flight task visible as
	// pending or active on the default queue — and both queue names, bounded by
	// the deadline. Exiting on "one sample per queue name" alone was the flake
	// this loop used to have: the immediate pre-enqueue sample satisfies that
	// condition, so the loop could stop before any sample was taken after
	// Enqueue, and the visibility assertion then raced the sampler (observed
	// once under a full-package -race run). Waiting for the observation itself
	// removes the dependency on sampler/enqueue interleaving: the handler
	// blocks on release, so once the task has been enqueued it cannot complete
	// or leave pending/active, and every subsequent sample reports it. The
	// only way to reach the deadline is a sampler that never observes an
	// enqueued task — a real failure, not a scheduling race.
	deadline := time.After(5 * time.Second)
	seen := map[string]bool{}
	visible := false
	for !visible || !seen[defaultQueueName] || !seen[batchQueueName] {
		obs.mu.Lock()
		for _, s := range obs.samples {
			seen[s.queue] = true
			if s.queue == defaultQueueName && s.pending+s.active > 0 {
				visible = true
			}
		}
		obs.mu.Unlock()
		if visible && seen[defaultQueueName] && seen[batchQueueName] {
			break
		}
		select {
		case <-obs.ch:
		case <-deadline:
			obs.mu.Lock()
			samples := append([]statsSample(nil), obs.samples...)
			obs.mu.Unlock()
			t.Fatalf("queue stats samples = %+v, want both %s and %s and an in-flight %s task "+
				"reported as pending or active", samples, defaultQueueName, batchQueueName, defaultQueueName)
		}
	}

	close(release)
	stop()
}

// TestStartConsumerStopIsIdempotent pins the restored contract: the stop
// function is safe to call more than once, both for the plain consumer and for
// one with the stats sampler — whose done-channel close made a second call
// panic with "close of closed channel" before the Once was added.
func TestStartConsumerStopIsIdempotent(t *testing.T) {
	server, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	defer server.Close()

	transport := New(server.Addr())
	defer func() { _ = transport.Close() }()

	handle := func(context.Context, *engine.Task) error { return nil }

	stop, err := transport.StartConsumer(queue.ConsumerConfig{Concurrency: 1}, handle)
	if err != nil {
		t.Fatalf("StartConsumer() error = %v", err)
	}
	stop()
	stop()

	obs := &recordingStatsObserver{ch: make(chan struct{}, 8)}
	stopWithStats, err := transport.StartConsumer(queue.ConsumerConfig{
		Concurrency:   1,
		StatsObserver: obs,
		StatsInterval: 20 * time.Millisecond,
	}, handle)
	if err != nil {
		t.Fatalf("StartConsumer(stats) error = %v", err)
	}
	stopWithStats()
	stopWithStats()
}
