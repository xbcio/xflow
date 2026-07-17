package xflow

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// TestLoadTransientVsDefault is a real-Redis throughput/latency load test that
// quantifies the practical payoff of the transient optimizations — chiefly
// optimization 3 (skipping the MySQL/audit dual-write on the hot path).
//
// It is gated on XFLOW_LOADTEST_REDIS and skipped otherwise, so it never runs in
// the normal `go test` suite. The Redis instance MUST be dedicated: the test
// FLUSHDBs between scenarios.
//
//	XFLOW_LOADTEST_REDIS=127.0.0.1:7654 \
//	XFLOW_LOADTEST_N=3000 XFLOW_LOADTEST_CONCURRENCY=100 \
//	XFLOW_LOADTEST_STORE_LATENCY=800us \
//	go test ./sdk/xflow/ -run TestLoadTransientVsDefault -v -timeout 10m
//
// Three scenarios, all running the same 4-node pass-through workflow:
//   - transient          : ExecutionModeTransient, no Store (opt 1-3 active)
//   - default_no_store    : default mode, Store=nil (isolates opt 1/2 overhead)
//   - default_with_store  : default mode + a latency-injecting Store (models the
//     synchronous MySQL/audit dual-write that transient skips)
func TestLoadTransientVsDefault(t *testing.T) {
	addr := os.Getenv("XFLOW_LOADTEST_REDIS")
	if addr == "" {
		t.Skip("set XFLOW_LOADTEST_REDIS=host:port (DEDICATED redis) to run the load test")
	}
	n := envInt("XFLOW_LOADTEST_N", 3000)
	concurrency := envInt("XFLOW_LOADTEST_CONCURRENCY", 100)
	storeLatency := envDuration("XFLOW_LOADTEST_STORE_LATENCY", 800*time.Microsecond)

	t.Logf("load config: redis=%s N=%d concurrency=%d store_latency=%s", addr, n, concurrency, storeLatency)

	type scenario struct {
		name  string
		mode  ExecutionMode
		store store.Store
	}
	scenarios := []scenario{
		{name: "transient", mode: ExecutionModeTransient},
		{name: "default_no_store", mode: ""},
		{name: "default_with_store", mode: "", store: newLatencyStore(storeLatency)},
	}

	results := make([]loadResult, 0, len(scenarios))
	for _, sc := range scenarios {
		flushRedis(t, addr)

		rec := newCompletionRecorder()
		opts := []Option{WithConcurrency(concurrency), WithHooks(rec)}
		if sc.mode == ExecutionModeTransient {
			opts = append(opts,
				WithExecutionMode(ExecutionModeTransient),
				WithTransientTTL(10*time.Minute),
				WithTransientCompletionTTL(30*time.Second),
			)
		}
		eng, err := NewCluster(ClusterConfig{RedisAddr: addr, Store: sc.store}, opts...)
		if err != nil {
			t.Fatalf("[%s] NewCluster() error = %v", sc.name, err)
		}

		res := runLoad(t, eng, rec, sc.name, n, concurrency)
		eng.Stop()
		results = append(results, res)
		t.Logf("[%s] %s", sc.name, res)
	}

	// Summary table + the headline comparison.
	t.Log("================ LOAD TEST SUMMARY ================")
	t.Logf("%-20s %10s %10s %10s %10s %8s", "scenario", "qps", "p50", "p90", "p99", "errors")
	for _, r := range results {
		t.Logf("%-20s %10.0f %10s %10s %10s %8d", r.name, r.qps, r.p50, r.p90, r.p99, r.errors)
	}
	var transient, defWithStore *loadResult
	for i := range results {
		switch results[i].name {
		case "transient":
			transient = &results[i]
		case "default_with_store":
			defWithStore = &results[i]
		}
	}
	if transient != nil && defWithStore != nil && defWithStore.qps > 0 {
		t.Logf("transient vs default_with_store: %.2fx throughput, p99 %s -> %s",
			transient.qps/defWithStore.qps, defWithStore.p99, transient.p99)
	}
}

type loadResult struct {
	name     string
	qps      float64
	p50, p90 time.Duration
	p99      time.Duration
	errors   int64
	total    int
	wall     time.Duration
}

func (r loadResult) String() string {
	return fmt.Sprintf("qps=%.0f p50=%s p90=%s p99=%s errors=%d wall=%s (n=%d)",
		r.qps, r.p50, r.p90, r.p99, r.errors, r.wall, r.total)
}

// completionRecorder tracks per-execution completion via the OnExecutionComplete
// hook so the load harness can saturate the consumer pool (fire-and-forget)
// rather than pacing each execution with a blocking Wait — which would be bound
// by asynq's ~1s idle-fetch backoff instead of the store/CPU cost under test.
type completionRecorder struct {
	engine.BaseHooks
	mu     sync.Mutex
	starts map[types.ExecutionID]time.Time
	done   []time.Duration
	count  int64
}

func newCompletionRecorder() *completionRecorder {
	return &completionRecorder{starts: make(map[types.ExecutionID]time.Time)}
}

func (r *completionRecorder) markStart(id types.ExecutionID, t time.Time) {
	r.mu.Lock()
	r.starts[id] = t
	r.mu.Unlock()
}

func (r *completionRecorder) OnExecutionComplete(_ context.Context, id types.ExecutionID, _ types.ExecutionStatus) {
	now := time.Now()
	r.mu.Lock()
	if start, ok := r.starts[id]; ok {
		r.done = append(r.done, now.Sub(start))
	}
	r.mu.Unlock()
	atomic.AddInt64(&r.count, 1)
}

func (r *completionRecorder) completed() int64 { return atomic.LoadInt64(&r.count) }

func runLoad(t *testing.T, eng *Engine, rec *completionRecorder, name string, n, concurrency int) loadResult {
	t.Helper()
	ctx := context.Background()

	wf := Workflow("loadtest_" + name)
	start := wf.Node("start", node.Start())
	n1 := wf.Node("n1", node.Expr("1"))
	n2 := wf.Node("n2", node.Expr("2"))
	n3 := wf.Node("n3", node.Expr("3"))
	wf.Connect(start, n1)
	wf.Connect(n1, n2)
	wf.Connect(n2, n3)

	workflowID, err := eng.AddWorkflow(ctx, wf)
	if err != nil {
		t.Fatalf("[%s] AddWorkflow() error = %v", name, err)
	}

	var errCount int64
	var next int64 = -1

	// Fire all N invocations with a bounded submit pool; do NOT wait per-exec.
	// The consumer pool (WithConcurrency) then drains under saturation, so the
	// bottleneck is the store/CPU cost, not asynq's idle-fetch latency.
	var submitWG sync.WaitGroup
	wallStart := time.Now()
	for w := 0; w < concurrency; w++ {
		submitWG.Add(1)
		go func() {
			defer submitWG.Done()
			for {
				i := atomic.AddInt64(&next, 1)
				if i >= int64(n) {
					return
				}
				t0 := time.Now()
				id, err := eng.Invoke(ctx, workflowID, Start(), map[string]any{"i": i})
				if err != nil {
					atomic.AddInt64(&errCount, 1)
					continue
				}
				rec.markStart(id, t0)
			}
		}()
	}
	submitWG.Wait()

	// Wait for the consumer pool to drain all fired executions.
	target := int64(n) - atomic.LoadInt64(&errCount)
	deadline := time.Now().Add(2 * time.Minute)
	for rec.completed() < target {
		if time.Now().After(deadline) {
			t.Logf("[%s] drain timeout: completed %d/%d", name, rec.completed(), target)
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	wall := time.Since(wallStart)

	rec.mu.Lock()
	done := append([]time.Duration(nil), rec.done...)
	rec.mu.Unlock()
	sort.Slice(done, func(a, b int) bool { return done[a] < done[b] })

	return loadResult{
		name:   name,
		qps:    float64(len(done)) / wall.Seconds(),
		p50:    percentile(done, 0.50),
		p90:    percentile(done, 0.90),
		p99:    percentile(done, 0.99),
		errors: errCount,
		total:  n,
		wall:   wall,
	}
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p * float64(len(sorted)))
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func flushRedis(t *testing.T, addr string) {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer func() { _ = rdb.Close() }()
	if err := rdb.FlushDB(context.Background()).Err(); err != nil {
		t.Fatalf("FlushDB(%s) error = %v", addr, err)
	}
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			return parsed
		}
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if parsed, err := time.ParseDuration(v); err == nil {
			return parsed
		}
	}
	return def
}

// latencyStore is a no-op store.Store that sleeps on every write path to model
// the synchronous cost of a MySQL/audit dual-write. Reads return empty.
type latencyStore struct {
	latency time.Duration
	writes  int64
}

func newLatencyStore(latency time.Duration) *latencyStore {
	return &latencyStore{latency: latency}
}

func (s *latencyStore) write() {
	atomic.AddInt64(&s.writes, 1)
	if s.latency > 0 {
		time.Sleep(s.latency)
	}
}

func (s *latencyStore) CreateExecution(context.Context, *store.ExecutionRecord) error {
	s.write()
	return nil
}

func (s *latencyStore) UpdateExecutionStatus(context.Context, types.ExecutionID, types.ExecutionStatus, string) error {
	s.write()
	return nil
}

func (s *latencyStore) GetExecution(context.Context, types.ExecutionID) (*store.ExecutionRecord, error) {
	return nil, nil
}

func (s *latencyStore) UpsertNode(context.Context, *store.NodeRecord) error {
	s.write()
	return nil
}

func (s *latencyStore) GetNode(context.Context, types.ExecutionID, string) (*store.NodeRecord, error) {
	return nil, nil
}

func (s *latencyStore) ListNodes(context.Context, types.ExecutionID, store.ListOptions) ([]*store.NodeRecord, error) {
	return nil, nil
}

func (s *latencyStore) ListSuspendedBySignal(context.Context, types.ExecutionID, string) ([]*store.NodeRecord, error) {
	return nil, nil
}

func (s *latencyStore) ListExpiredSuspensions(context.Context, time.Time, store.ListOptions) ([]*store.NodeRecord, error) {
	return nil, nil
}

func (s *latencyStore) SaveSignal(context.Context, *store.SignalRecord) error {
	s.write()
	return nil
}

func (s *latencyStore) ConsumeSignal(context.Context, types.ExecutionID, string) (*store.SignalRecord, error) {
	return nil, nil
}

func (s *latencyStore) RevokeSignal(context.Context, types.ExecutionID, string) (bool, error) {
	return true, nil
}

func (s *latencyStore) CountSignalsByNames(context.Context, types.ExecutionID, []string) (int, error) {
	return 0, nil
}

func (s *latencyStore) ListSignalsByNames(context.Context, types.ExecutionID, []string, store.ListOptions) ([]*store.SignalRecord, error) {
	return nil, nil
}
