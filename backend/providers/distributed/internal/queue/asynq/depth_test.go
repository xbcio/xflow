package asynq

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	asynqlib "github.com/hibiken/asynq"

	"github.com/xbcio/xflow/backend/providers/distributed/internal/queue"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// newDepthTask builds the smallest task the codec accepts; Type selects the
// queue lane (node-exec -> default, node-batch -> batch).
func newDepthTask(execID string, typ engine.TaskType) *engine.Task {
	return &engine.Task{
		ExecutionID: types.ExecutionID(execID),
		NodeName:    "node-a",
		NodeIdx:     0,
		Type:        typ,
	}
}

// newDepthTransport starts a transport over a fresh miniredis. Close is
// registered as cleanup, so a test may also call Close itself to assert
// idempotence.
func newDepthTransport(t *testing.T, opts ...Option) *Transport {
	t.Helper()
	server, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(server.Close)
	tr := NewWithConnOpt(asynqlib.RedisClientOpt{Addr: server.Addr()}, opts...)
	t.Cleanup(func() { _ = tr.Close() })
	return tr
}

// waitForSampledDepth blocks until the producer-side cache reports want for
// the named queue. It polls the cache rather than sleeping a fixed interval:
// the sampler runs on its own goroutine, and the assertion is about what the
// cache eventually contains, not about when a particular tick landed.
func waitForSampledDepth(t *testing.T, tr *Transport, name string, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		tr.depthMu.RLock()
		depth, measured := tr.depths[name]
		tr.depthMu.RUnlock()
		if measured && depth == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("sampled depth for %q = %d (measured=%v), want %d", name, depth, measured, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestTryEnqueueBackpressuresAtConfiguredDepth is the core behaviour: with a
// bound of one, a queue holding one pending task must refuse further
// TryEnqueue calls, and the refusal must be engine.ErrQueueFull (the sentinel
// engine's flush path keys on) rather than any other error.
//
// The bound is per queue, so the assertion is made twice: the default lane is
// full and refuses, while the untouched batch lane still admits.
func TestTryEnqueueBackpressuresAtConfiguredDepth(t *testing.T) {
	tr := newDepthTransport(t,
		WithMaxQueueDepth(1),
		WithDepthSampleInterval(10*time.Millisecond),
	)
	ctx := context.Background()

	if err := tr.Enqueue(ctx, newDepthTask("exec-filled", engine.TaskTypeNodeExec)); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	waitForSampledDepth(t, tr, defaultQueueName, 1)

	err := tr.TryEnqueue(ctx, newDepthTask("exec-rejected", engine.TaskTypeNodeExec))
	if !errors.Is(err, engine.ErrQueueFull) {
		t.Fatalf("TryEnqueue on a full queue = %v, want engine.ErrQueueFull", err)
	}
	// A refusal must not have enqueued anything: the broker still holds exactly
	// the one task Enqueue put there.
	info, err := tr.depthInspector.GetQueueInfo(defaultQueueName)
	if err != nil {
		t.Fatalf("GetQueueInfo() error = %v", err)
	}
	if got := info.Pending + info.Active; got != 1 {
		t.Fatalf("default queue depth after refusal = %d, want 1", got)
	}

	// The sibling lane is independent: the batch queue is empty, so its bound
	// is not reached.
	if err := tr.TryEnqueue(ctx, newDepthTask("batch-admitted", engine.TaskTypeNodeBatch)); err != nil {
		t.Fatalf("TryEnqueue on an empty sibling lane = %v, want nil", err)
	}
}

// TestTryEnqueueAllowsAfterQueueDrains pins the other half of the contract: a
// refusal lasts only as long as the backlog does. Once a consumer drains the
// queue and a fresh sample observes the empty queue, TryEnqueue admits again.
func TestTryEnqueueAllowsAfterQueueDrains(t *testing.T) {
	tr := newDepthTransport(t,
		WithMaxQueueDepth(1),
		WithDepthSampleInterval(10*time.Millisecond),
	)
	ctx := context.Background()

	if err := tr.Enqueue(ctx, newDepthTask("exec-drain", engine.TaskTypeNodeExec)); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	waitForSampledDepth(t, tr, defaultQueueName, 1)
	if err := tr.TryEnqueue(ctx, newDepthTask("exec-blocked", engine.TaskTypeNodeExec)); !errors.Is(err, engine.ErrQueueFull) {
		t.Fatalf("TryEnqueue before drain = %v, want engine.ErrQueueFull", err)
	}

	// Drain through the real consumer, then stop it so its polling cannot race
	// the assertion below.
	stop, err := tr.StartConsumer(queue.ConsumerConfig{Concurrency: 1},
		func(context.Context, *engine.Task) error { return nil })
	if err != nil {
		t.Fatalf("StartConsumer() error = %v", err)
	}
	waitForSampledDepth(t, tr, defaultQueueName, 0)
	stop()

	if err := tr.TryEnqueue(ctx, newDepthTask("exec-after-drain", engine.TaskTypeNodeExec)); err != nil {
		t.Fatalf("TryEnqueue after drain = %v, want nil", err)
	}
}

// TestTryEnqueueAlwaysAllowsWithoutBound pins the default: maxDepth 0 is
// disabled, no sampler is started, and even a visibly deep queue admits every
// TryEnqueue. The seam reports a large depth on purpose — if the bound switch
// were ignored, this would be the test that catches it.
func TestTryEnqueueAlwaysAllowsWithoutBound(t *testing.T) {
	tr := newDepthTransport(t,
		WithDepthSampleInterval(time.Hour),
		withDepthFunc(func() (map[string]int, error) {
			return map[string]int{defaultQueueName: 99}, nil
		}),
	)
	ctx := context.Background()

	if tr.depthDone != nil {
		t.Fatal("depth sampler started without a configured bound")
	}
	tr.refreshDepths()
	if tr.queueFull(defaultQueueName) {
		t.Fatalf("queueFull(%q) = true, want false when maxDepth is disabled", defaultQueueName)
	}

	for i := 0; i < 3; i++ {
		if err := tr.Enqueue(ctx, newDepthTask("exec-unbounded", engine.TaskTypeNodeExec)); err != nil {
			t.Fatalf("Enqueue() error = %v", err)
		}
	}
	if err := tr.TryEnqueue(ctx, newDepthTask("exec-unbounded-try", engine.TaskTypeNodeExec)); err != nil {
		t.Fatalf("TryEnqueue with the bound disabled = %v, want nil", err)
	}
}

// TestTryEnqueueUsesSampledDepth drives the cache through the test seam so the
// decision can be asserted without depending on real sampling timing, and pins
// the per-queue reading: the default lane's sample is full while the batch
// lane's is empty in the very same snapshot.
func TestTryEnqueueUsesSampledDepth(t *testing.T) {
	tr := newDepthTransport(t,
		WithMaxQueueDepth(1),
		WithDepthSampleInterval(time.Hour),
		withDepthFunc(func() (map[string]int, error) {
			return map[string]int{
				defaultQueueName: 1,
				batchQueueName:   0,
			}, nil
		}),
	)
	ctx := context.Background()
	tr.refreshDepths()

	err := tr.TryEnqueue(ctx, newDepthTask("exec-sampled-full", engine.TaskTypeNodeExec))
	if !errors.Is(err, engine.ErrQueueFull) {
		t.Fatalf("TryEnqueue at sampled depth 1 = %v, want engine.ErrQueueFull", err)
	}
	if err := tr.TryEnqueue(ctx, newDepthTask("exec-sampled-empty", engine.TaskTypeNodeBatch)); err != nil {
		t.Fatalf("TryEnqueue at sampled depth 0 = %v, want nil", err)
	}
}

// TestTryEnqueueFailsOpenWhenSamplingFails pins the failure policy: an
// unreadable sample is not a full queue, and it also must not leave the
// previous reading standing. The seam first reports the queue at the bound
// (the control leg — the bound really does apply), then fails. A stale cache
// would keep refusing producers for the whole duration of a Redis problem,
// which is exactly when refusing is least defensible, so the failure must
// clear the snapshot and admit.
func TestTryEnqueueFailsOpenWhenSamplingFails(t *testing.T) {
	var failing atomic.Bool
	sampled := make(chan struct{}, 1)
	tr := newDepthTransport(t,
		WithMaxQueueDepth(1),
		WithDepthSampleInterval(time.Hour),
		withDepthFunc(func() (map[string]int, error) {
			select {
			case sampled <- struct{}{}:
			default:
			}
			if failing.Load() {
				return nil, errors.New("simulated sample failure")
			}
			return map[string]int{defaultQueueName: 1}, nil
		}),
	)
	ctx := context.Background()

	// Consume the sampler's immediate first read, so the toggles below are the
	// only thing driving the cache. The next tick is an hour away.
	select {
	case <-sampled:
	case <-time.After(5 * time.Second):
		t.Fatal("depth sampler never sampled")
	}

	if err := tr.Enqueue(ctx, newDepthTask("exec-real-backlog", engine.TaskTypeNodeExec)); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	tr.refreshDepths()
	if !tr.queueFull(defaultQueueName) {
		t.Fatal("queueFull = false for a sampled depth at the bound; the seam is not reaching the cache")
	}
	if err := tr.TryEnqueue(ctx, newDepthTask("exec-bound-applies", engine.TaskTypeNodeExec)); !errors.Is(err, engine.ErrQueueFull) {
		t.Fatalf("TryEnqueue at the bound = %v, want engine.ErrQueueFull", err)
	}

	failing.Store(true)
	tr.refreshDepths()
	if tr.queueFull(defaultQueueName) {
		t.Fatal("queueFull = true after a failed sample; a stale depth must not survive a sampling gap")
	}
	if err := tr.TryEnqueue(ctx, newDepthTask("exec-fail-open", engine.TaskTypeNodeExec)); err != nil {
		t.Fatalf("TryEnqueue after a failed sample = %v, want nil (fail open)", err)
	}
}

// TestCloseStopsDepthSampler pins the lifecycle: Close stops the sampling
// goroutine it started and is safe to call again. depthExited is closed by the
// goroutine itself, so waiting on it proves the exit rather than inferring it
// from a goroutine-count heuristic.
func TestCloseStopsDepthSampler(t *testing.T) {
	tr := newDepthTransport(t,
		WithMaxQueueDepth(1),
		WithDepthSampleInterval(10*time.Millisecond),
	)
	if tr.depthExited == nil {
		t.Fatal("depth sampler was not started for a configured bound")
	}
	if err := tr.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	select {
	case <-tr.depthExited:
	case <-time.After(5 * time.Second):
		t.Fatal("depth sampler goroutine did not exit after Close")
	}
	// Idempotent: the backend's shutdown path may reach Close twice, and the
	// done-channel close that stops the sampler must not panic the second time.
	if err := tr.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
}
