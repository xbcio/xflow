package asynq

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
)

// TestCloseIsIdempotentAndConcurrentWithoutSampler pins the teardown contract
// for the common unbounded transport: every caller of Close observes the first
// call's result, even when the calls race, and no call panics rewriting the
// closed client or a nil sampler.
//
// Mutation note: dropping the sync.Once guard makes the losing goroutines
// report asynq's "client is closed" instead of nil, so this test turns red
// under -race and often without it.
func TestCloseIsIdempotentAndConcurrentWithoutSampler(t *testing.T) {
	tr := newDepthTransport(t)
	if tr.depthDone != nil {
		t.Fatal("depth sampler started without a configured bound")
	}

	const callers = 16
	results := make([]error, callers)
	var wg sync.WaitGroup
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = tr.Close()
		}(i)
	}
	wg.Wait()

	for i, err := range results {
		if err != nil {
			t.Fatalf("Close() caller %d error = %v, want the first call's nil result", i, err)
		}
	}

	// A closed transport must not silently accept work: the producer reports
	// the closed connection rather than panicking or succeeding.
	if err := tr.Enqueue(context.Background(), newDepthTask("exec-after-close", engine.TaskTypeNodeExec)); err == nil {
		t.Fatal("Enqueue after Close = nil, want a closed-client error")
	}
}

// TestCloseStopsSamplingAndIsSafeConcurrently covers the bounded transport's
// teardown under concurrent Close calls: every call returns the first result,
// the sampler goroutine exits, and it starts no further samples once exited.
// The last part is why depthExited is awaited before the count is compared — an
// in-flight sample may still be running when Close returns (Close deliberately
// does not wait on the broker), but none can start afterwards.
func TestCloseStopsSamplingAndIsSafeConcurrently(t *testing.T) {
	var samples atomic.Int64
	tr := newDepthTransport(t,
		WithMaxQueueDepth(1),
		WithDepthSampleInterval(5*time.Millisecond),
		withDepthFunc(func() (map[string]int, error) {
			samples.Add(1)
			return map[string]int{defaultQueueName: 1}, nil
		}),
	)
	// Wait for the immediate first sample, so a running sampler is definitely
	// in place before Close is called.
	waitForSampledDepth(t, tr, defaultQueueName, 1)

	const callers = 16
	results := make([]error, callers)
	var wg sync.WaitGroup
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = tr.Close()
		}(i)
	}
	wg.Wait()
	for i, err := range results {
		if err != nil {
			t.Fatalf("Close() caller %d error = %v, want the first call's nil result", i, err)
		}
	}

	select {
	case <-tr.depthExited:
	case <-time.After(5 * time.Second):
		t.Fatal("depth sampler goroutine did not exit after Close")
	}
	atExit := samples.Load()
	// Short bounded observation: the sampler cannot start another sample after
	// depthExited, so the count must be frozen from here on.
	time.Sleep(20 * time.Millisecond)
	if got := samples.Load(); got != atExit {
		t.Fatalf("sampler kept running after Close: %d samples at exit, %d later", atExit, got)
	}

	// Cache reads after Close are safe and must not reach the sampler.
	_ = tr.queueFull(defaultQueueName)
}
