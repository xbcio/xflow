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

func TestE2ELoadRealRedis(t *testing.T) {
	if testing.Short() {
		t.Skip("skip load test in short mode")
	}

	addr := requireRedisLoad(t)

	// FlushDB to clear stale asynq tasks from prior runs (T8 lesson).
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	if err := rdb.FlushDB(context.Background()).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
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
	latencies := make([]time.Duration, total)

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
				id := submitLoad(t, server.URL, server.Client(), i)
				wctx, wcancel := context.WithTimeout(context.Background(), 15*time.Second)
				r := pollCompletionLoad(wctx, t, bk.State(), id)
				wcancel()
				latencies[i] = time.Since(jobStart)
				if r.Status != types.ExecutionStatusSuccess {
					atomic.AddInt64(&failed, 1)
				}
				atomic.AddInt64(&done, 1)
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	// Compute p50 / p99.
	sorted := make([]time.Duration, total)
	copy(sorted, latencies)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	p50 := sorted[total*50/100]
	p99 := sorted[total*99/100]

	fmt.Printf("E2E load: total=%d workers=%d elapsed=%v throughput=%.0f/s failed=%d p50=%v p99=%v\n",
		total, workers, elapsed.Round(time.Millisecond),
		float64(done)/elapsed.Seconds(),
		failed, p50.Round(time.Millisecond), p99.Round(time.Millisecond))

	if failed > 0 {
		t.Errorf("failed = %d", failed)
	}
}

func submitLoad(t *testing.T, baseURL string, client *http.Client, i int) types.ExecutionID {
	t.Helper()
	body := struct {
		W *types.WorkflowDef `json:"workflow"`
		P map[string]any     `json:"params"`
	}{
		W: &types.WorkflowDef{Name: "perf-load", Nodes: []types.NodeDef{{Name: "start", Type: "perf.load"}}},
		P: map[string]any{"claim_id": fmt.Sprintf("c-%d", i)},
	}
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		t.Fatalf("encode: %v", err)
	}
	resp, err := client.Post(baseURL+control.SubmitWorkflowPath, "application/json", &buf)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out struct {
		ExecutionID types.ExecutionID `json:"execution_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return out.ExecutionID
}

func pollCompletionLoad(ctx context.Context, t *testing.T, state engine.StateStore, id types.ExecutionID) types.Result {
	t.Helper()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		snap, err := state.GetExecution(ctx, id)
		if err == nil && isTerminalLoad(snap.Status) {
			return types.Result{ExecutionID: id, Status: snap.Status}
		}
		select {
		case <-ctx.Done():
			return types.Result{ExecutionID: id, Status: types.ExecutionStatusFailed}
		case <-ticker.C:
		}
	}
}

func isTerminalLoad(s types.ExecutionStatus) bool {
	switch s {
	case types.ExecutionStatusSuccess, types.ExecutionStatusFailed,
		types.ExecutionStatusCanceled, types.ExecutionStatusTimeout:
		return true
	}
	return false
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
