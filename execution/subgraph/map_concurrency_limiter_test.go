package subgraph

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/types"
)

type batchGateHandler struct {
	firstGate    chan struct{}
	firstStarted chan struct{}
	secondStart  chan struct{}

	mu          sync.Mutex
	firstActive int
	overlapped  bool
}

func newBatchGateHandler() *batchGateHandler {
	return &batchGateHandler{
		firstGate:    make(chan struct{}),
		firstStarted: make(chan struct{}, 2),
		secondStart:  make(chan struct{}, 2),
	}
}

func (h *batchGateHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.overlap"}
}

func (h *batchGateHandler) Execute(ctx context.Context, in *types.Input) (*types.Output, error) {
	item, _ := in.Data["$item"].(string)
	if strings.HasPrefix(item, "first-") {
		h.mu.Lock()
		h.firstActive++
		h.mu.Unlock()
		h.firstStarted <- struct{}{}
		defer func() {
			h.mu.Lock()
			h.firstActive--
			h.mu.Unlock()
		}()
		select {
		case <-h.firstGate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	} else {
		h.mu.Lock()
		if h.firstActive != 0 {
			h.overlapped = true
		}
		h.mu.Unlock()
		h.secondStart <- struct{}{}
	}
	return &types.Output{Data: map[string]any{"item": item}}, nil
}

func (h *batchGateHandler) sawOverlap() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.overlapped
}

func newLimitedBody(
	reg *execution.Registry,
	limiter *MapConcurrencyLimiter,
) *MapBodyExecutor {
	executor := NewExecutor(reg, NewPackageCache(PackageCacheConfig{
		MaxEntries: 4, MaxPackageBytes: 1 << 20,
	}), func() Backend {
		return local.New(local.WithRegistry(reg), local.WithConcurrency(1))
	}, WithMapConcurrencyLimiter(limiter))
	return NewMapBodyExecutor(executor, false, time.Time{})
}

// TestMapConcurrencyLimiterBoundsActiveBatches proves that the item semaphore
// does not replace the batch gate. With one active-batch permit, a second batch
// cannot make partial progress even though item capacity exists.
func TestMapConcurrencyLimiterBoundsActiveBatches(t *testing.T) {
	limiter := NewMapConcurrencyLimiter(1, 4)
	handler := newBatchGateHandler()
	reg := execution.NewRegistry()
	reg.RegisterGlobal("test.overlap", handler)
	pkg, hash := buildOverlapBodyPackage(t)
	run := func(items ...any) <-chan error {
		done := make(chan error, 1)
		go func() {
			results, err := newLimitedBody(reg, limiter).ExecuteBatchBody(context.Background(), engine.BatchBodyRequest{
				Body: pkg, BodyHash: hash, Items: items, BatchSize: len(items), BodyConcurrency: 2,
			})
			if err == nil && len(results) != len(items) {
				err = fmt.Errorf("results = %d, want %d", len(results), len(items))
			}
			done <- err
		}()
		return done
	}

	firstDone := run("first-0", "first-1")
	for range 2 {
		select {
		case <-handler.firstStarted:
		case <-time.After(time.Second):
			t.Fatal("first batch did not start both items")
		}
	}
	secondDone := run("second-0", "second-1")
	waitForPermitWaiters(t, limiter.batches, 1)

	select {
	case <-handler.secondStart:
		t.Fatal("second batch started before the active first batch completed")
	default:
	}
	close(handler.firstGate)
	waitBatchResults(t, map[string]<-chan error{"first": firstDone, "second": secondDone})
	if handler.sawOverlap() {
		t.Fatal("second batch overlapped the first despite active-batch capacity 1")
	}
}

// TestMapConcurrencyLimiterLetsAdmittedBatchesOverlap distinguishes this design
// from whole-batch weighted reservation. Once one item of the first batch
// completes, another admitted batch may use that item permit while a slow item
// from the first batch is still running.
func TestMapConcurrencyLimiterLetsAdmittedBatchesOverlap(t *testing.T) {
	limiter := NewMapConcurrencyLimiter(2, 2)
	handler := newBatchGateHandler()
	reg := execution.NewRegistry()
	reg.RegisterGlobal("test.overlap", handler)
	pkg, hash := buildOverlapBodyPackage(t)
	run := func(items ...any) <-chan error {
		done := make(chan error, 1)
		go func() {
			results, err := newLimitedBody(reg, limiter).ExecuteBatchBody(context.Background(), engine.BatchBodyRequest{
				Body: pkg, BodyHash: hash, Items: items, BatchSize: len(items), BodyConcurrency: 2,
			})
			if err == nil && len(results) != len(items) {
				err = fmt.Errorf("results = %d, want %d", len(results), len(items))
			}
			done <- err
		}()
		return done
	}

	// Only one first item is used, leaving one item permit available while its
	// batch remains active and blocked in the handler.
	firstDone := run("first-0")
	select {
	case <-handler.firstStarted:
	case <-time.After(time.Second):
		t.Fatal("first batch did not start")
	}
	secondDone := run("second-0")
	select {
	case <-handler.secondStart:
	case <-time.After(time.Second):
		t.Fatal("second admitted batch did not use the free item permit")
	}
	if !handler.sawOverlap() {
		t.Fatal("admitted batches did not overlap; item slots appear reserved by whole batch")
	}
	close(handler.firstGate)
	waitBatchResults(t, map[string]<-chan error{"first": firstDone, "second": secondDone})
}

func waitBatchResults(t *testing.T, batches map[string]<-chan error) {
	t.Helper()
	for name, done := range batches {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("%s batch: %v", name, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("%s batch did not finish", name)
		}
	}
}

func waitForPermitWaiters(t *testing.T, limiter *permitLimiter, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		limiter.mu.Lock()
		got := len(limiter.waiters)
		limiter.mu.Unlock()
		if got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("permit waiter count did not reach %d", want)
}

func TestMapConcurrencyLimiterCapsItemsAcrossConcurrentBatches(t *testing.T) {
	const itemLimit = 3
	limiter := NewMapConcurrencyLimiter(2, itemLimit)
	handler := &overlapHandler{hold: 40 * time.Millisecond}
	reg := execution.NewRegistry()
	reg.RegisterGlobal("test.overlap", handler)
	first := newLimitedBody(reg, limiter)
	second := newLimitedBody(reg, limiter)
	pkg, hash := buildOverlapBodyPackage(t)
	req := engine.BatchBodyRequest{
		Body:            pkg,
		BodyHash:        hash,
		Items:           itemsN(8),
		BatchSize:       8,
		BodyConcurrency: 8,
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, body := range []*MapBodyExecutor{first, second} {
		wg.Add(1)
		go func(body *MapBodyExecutor) {
			defer wg.Done()
			<-start
			results, err := body.ExecuteBatchBody(context.Background(), req)
			if err == nil && len(results) != len(req.Items) {
				t.Errorf("results = %d, want %d", len(results), len(req.Items))
			}
			errs <- err
		}(body)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("execute batch: %v", err)
		}
	}

	peak, invoked := handler.stats()
	if peak != itemLimit {
		t.Fatalf("peak items across both batches = %d, want %d", peak, itemLimit)
	}
	if invoked != 16 {
		t.Fatalf("body invoked %d times, want 16", invoked)
	}
}

func TestMapConcurrencyLimiterClampsBatchWidthToItemCapacity(t *testing.T) {
	limiter := NewMapConcurrencyLimiter(8, 2)
	if got := limiter.effectiveItemWidth(99); got != 2 {
		t.Fatalf("effective item width = %d, want 2", got)
	}
	if got := limiter.effectiveItemWidth(0); got != 1 {
		t.Fatalf("non-positive item width = %d, want 1", got)
	}
}

func TestMapConcurrencyLimiterWaitingCancellationDoesNotLeakPermits(t *testing.T) {
	limiter := NewMapConcurrencyLimiter(1, 1)
	for name, permits := range map[string]*permitLimiter{
		"batch": limiter.batches,
		"item":  limiter.items,
	} {
		t.Run(name, func(t *testing.T) {
			releaseHeld, err := permits.acquire(context.Background())
			if err != nil {
				t.Fatalf("fill capacity: %v", err)
			}

			ctx, cancel := context.WithCancel(context.Background())
			acquireErr := make(chan error, 1)
			go func() {
				_, err := permits.acquire(ctx)
				acquireErr <- err
			}()
			waitForPermitWaiters(t, permits, 1)
			cancel()
			select {
			case err := <-acquireErr:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("canceled acquire = %v, want context.Canceled", err)
				}
			case <-time.After(time.Second):
				t.Fatal("canceled acquire remained blocked")
			}
			releaseHeld()

			ctx, cancel = context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			release, err := permits.acquire(ctx)
			if err != nil {
				t.Fatalf("acquire after cancellation = %v; permit leaked", err)
			}
			release()
			release() // release functions are intentionally idempotent
		})
	}
}
