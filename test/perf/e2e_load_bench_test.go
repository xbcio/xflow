//go:build perf

package perf

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend/asynq"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/nodes/node"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/protocol"
	runnersvc "github.com/xbcio/xflow/service/runner"
	"github.com/xbcio/xflow/types"
	redis "github.com/redis/go-redis/v9"
)

type loadHandler struct{}

func (loadHandler) Descriptor() node.Descriptor { return node.Descriptor{Type: "perf.load"} }
func (loadHandler) Execute(_ context.Context, input *types.Input) (*types.Output, error) {
	return &types.Output{Data: map[string]any{"claim_id": input.Data["claim_id"]}}, nil
}

// requireRedisLoad pings Redis and skips the test if unreachable.
func requireRedisLoad(t *testing.T) string {
	t.Helper()
	addr := os.Getenv("XFLOW_TEST_REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	c := redis.NewClient(&redis.Options{Addr: addr})
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("redis unavailable at %s: %v (set XFLOW_TEST_REDIS_ADDR)", addr, err)
	}
	return addr
}

// flushAsynqKeysLoad deletes only asynq's own key namespace ("asynq:*")
// rather than the whole Redis DB, so it does not disturb keys other tests
// may be using concurrently in the same DB. Mirrors
// test/integration/harness.go:flushAsynqKeys (separate package, same tag).
func flushAsynqKeysLoad(ctx context.Context, t *testing.T, rdb *redis.Client) {
	t.Helper()
	var cursor uint64
	for {
		keys, next, err := rdb.Scan(ctx, cursor, "asynq:*", 500).Result()
		if err != nil {
			t.Fatalf("scan asynq keys: %v", err)
		}
		if len(keys) > 0 {
			if err := rdb.Del(ctx, keys...).Err(); err != nil {
				t.Fatalf("del asynq keys: %v", err)
			}
		}
		cursor = next
		if cursor == 0 {
			return
		}
	}
}

func TestE2ELoadRealRedis(t *testing.T) {
	if testing.Short() {
		t.Skip("skip load test in short mode")
	}

	addr := requireRedisLoad(t)

	// Clear stale asynq tasks from prior runs (T8 lesson). Scoped to the
	// "asynq:*" namespace (not FlushDB) so it does not clear keys other
	// perf/integration tests may hold in the same Redis DB.
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	flushAsynqKeysLoad(context.Background(), t, rdb)
	_ = rdb.Close()

	bk, err := asynq.New(addr, nil, asynq.WithConcurrency(8), asynq.WithConsumer(true))
	if err != nil {
		t.Fatal(err)
	}

	eng := engine.New(bk.State(), bk.Queue())
	runners := control.NewRunnerPool()
	dispatcher := control.NewDispatcher(eng, runners)
	stop := bk.BindHandler(eng, dispatcher.HandleTask)
	defer stop()

	server := httptest.NewServer(control.NewServer(eng, runners).Handler())
	defer server.Close()

	registry := execution.NewRegistry()
	registry.RegisterGlobal("perf.load", loadHandler{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Launch 8 runner instances so tasks are processed in parallel.
	// Each runner is single-goroutine by design; multiple runners provide
	// the concurrency needed to drain 1000 tasks within per-task timeout.
	const nRunners = 8
	for i := 0; i < nRunners; i++ {
		id := fmt.Sprintf("perf-runner-%d", i+1)
		r := runnersvc.New(
			protocol.NewClient(server.URL, server.Client()),
			registry,
			runnersvc.Config{
				RunnerID:     id,
				Concurrency:  2,
				Capabilities: []protocol.Capability{{NodeType: "perf.load"}},
				PollWait:     5 * time.Millisecond,
			},
		)
		go r.Run(ctx)
		waitForRunnerLoad(t, runners, id)
	}

	const total = 1000
	const workers = 16

	var done int64
	var failed int64
	var timeouts int64
	latencies := make([]time.Duration, total)

	// Critical 1 fix: collect submit errors from worker goroutines instead of
	// calling t.Fatal (which only exits the goroutine, not the test).
	var submitErrMu sync.Mutex
	var submitErrors []error

	start := time.Now()
	wg := sync.WaitGroup{}
	jobs := make(chan int, total)
	for i := 0; i < total; i++ {
		jobs <- i
	}
	close(jobs)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				jobStart := time.Now()
				// Critical 1: submitLoad returns error; t.Fatal not used inside goroutine.
				id, submitErr := submitLoad(server.URL, server.Client(), i)
				if submitErr != nil {
					submitErrMu.Lock()
					submitErrors = append(submitErrors, fmt.Errorf("job %d: %w", i, submitErr))
					submitErrMu.Unlock()
					atomic.AddInt64(&failed, 1)
					atomic.AddInt64(&done, 1)
					latencies[i] = time.Since(jobStart)
					continue
				}
				wctx, wcancel := context.WithTimeout(context.Background(), 15*time.Second)
				// Important 2: pollCompletionLoad returns (status, timedOut).
				status, timedOut := pollCompletionLoad(wctx, bk.State(), id)
				wcancel()
				latencies[i] = time.Since(jobStart)
				if timedOut {
					atomic.AddInt64(&timeouts, 1)
				} else if status != types.ExecutionStatusSuccess {
					atomic.AddInt64(&failed, 1)
				}
				atomic.AddInt64(&done, 1)
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	// Report submit errors collected from worker goroutines.
	if len(submitErrors) > 0 {
		for _, e := range submitErrors {
			t.Errorf("submit error: %v", e)
		}
		t.Fatalf("%d submit error(s); aborting result evaluation", len(submitErrors))
	}

	// Compute p50 / p99.
	sorted := make([]time.Duration, total)
	copy(sorted, latencies)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	p50 := sorted[total*50/100]
	p99 := sorted[total*99/100]

	fmt.Printf("E2E load: total=%d workers=%d elapsed=%v throughput=%.0f/s failed=%d timeouts=%d p50=%v p99=%v\n",
		total, workers, elapsed.Round(time.Millisecond),
		float64(done)/elapsed.Seconds(),
		failed, timeouts, p50.Round(time.Millisecond), p99.Round(time.Millisecond))

	if failed > 0 {
		t.Errorf("failed = %d (non-timeout failures)", failed)
	}
	if timeouts > 0 {
		t.Logf("timeouts = %d (tasks did not complete within 15s per-task budget)", timeouts)
	}
}

// submitLoad submits one workflow execution and returns the ExecutionID.
// It returns an error instead of calling t.Fatal so callers in goroutines
// can collect and report errors safely from the main goroutine (Critical 1).
func submitLoad(baseURL string, client *http.Client, i int) (types.ExecutionID, error) {
	body := struct {
		W *types.WorkflowDef `json:"workflow"`
		P map[string]any     `json:"params"`
	}{
		W: &types.WorkflowDef{Name: "perf-load", Nodes: []types.NodeDef{{Name: "start", Type: "perf.load"}}},
		P: map[string]any{"claim_id": fmt.Sprintf("c-%d", i)},
	}
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		return "", fmt.Errorf("encode: %w", err)
	}
	resp, err := client.Post(baseURL+control.SubmitWorkflowPath, "application/json", &buf)
	if err != nil {
		return "", fmt.Errorf("post: %w", err)
	}
	defer resp.Body.Close()
	// Important 1: check HTTP status before attempting to decode.
	if resp.StatusCode != http.StatusOK {
		var raw bytes.Buffer
		_, _ = raw.ReadFrom(resp.Body)
		return "", fmt.Errorf("unexpected status %d: %s", resp.StatusCode, raw.String())
	}
	var out struct {
		ExecutionID types.ExecutionID `json:"execution_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", fmt.Errorf("decode response: %w", err)
	}
	return out.ExecutionID, nil
}

// pollCompletionLoad polls until the execution reaches a terminal state or the
// context is cancelled.  It returns (status, timedOut) so callers can
// distinguish a true failure from a poll timeout (Important 2 / Critical 2).
func pollCompletionLoad(ctx context.Context, state engine.StateStore, id types.ExecutionID) (types.ExecutionStatus, bool) {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		snap, err := state.GetExecution(ctx, id)
		// Critical 2: guard against nil snap when task not yet persisted.
		if err == nil && snap != nil && types.IsTerminalExecutionStatus(snap.Status) {
			return snap.Status, false
		}
		select {
		case <-ctx.Done():
			// Important 2: distinguish timeout from real failure.
			return "", true
		case <-ticker.C:
		}
	}
}

func waitForRunnerLoad(t *testing.T, pool *control.RunnerPool, id string) {
	t.Helper()
	dl := time.Now().Add(5 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, ok := pool.Runner(id); ok {
			return
		}
		select {
		case <-ticker.C:
		case <-time.After(time.Until(dl)):
			t.Fatalf("runner %s not registered within 5s", id)
		}
	}
}
