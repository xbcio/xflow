//go:build perf

package perf

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	redis "github.com/redis/go-redis/v9"
	"github.com/xbcio/xflow/backend/providers/distributed"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/node/resource"
	"github.com/xbcio/xflow/service/apiserver"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/protocol"
	runnersvc "github.com/xbcio/xflow/service/runner"
	"github.com/xbcio/xflow/test/workflows"
	"github.com/xbcio/xflow/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// This file stresses the three complexity tiers from test/workflows under
// concurrent load. It measures the same definitions the coverage suite and the
// integration tier suite run, so a throughput number here describes a workflow
// whose per-node behaviour is separately asserted — not a synthetic handler that
// only echoes its input.
//
// Load runs through the production server+runner topology (HTTP submit into the
// control plane, tasks drawn from Redis by real runners over the gRPC protocol).
// Driving the tiers in-process would skip the serialization, lease, and signal
// paths that dominate a distributed run, which are exactly the parts load is
// supposed to exercise.
//
// Two consequences of running the real definitions are load-bearing:
//
//   - The high tier's insert is a plain INSERT against `$vars.db_row_id`, and
//     vars are baked into a definition at submit time. Each job therefore
//     submits its own definition carrying a per-job row id; sharing one
//     definition would turn every job after the first into a duplicate-key error.
//
//   - The high tier parks twice (approval, then signal wait). A worker cannot
//     hand a job to a background driver without either racing the runner to the
//     suspension or losing track of which execution failed, so each worker
//     drives its own execution. Latency for that tier therefore includes two
//     signal round trips, and its throughput is not comparable with the other
//     two.
//
// Reported as a perf.metric line in the same key=value shape
// e2e_load_bench_test.go established, so scripts/perf-sample.sh can trend it.
//
// Interpreting the numbers. In this topology a workflow's node-to-node latency
// is floored by the durable outbox dispatcher, which the distributed backend
// starts with a fixed 1s interval (backend/providers/distributed/backend.go).
// Any hop the engine cannot complete inline waits for the next tick, so the
// per-tier p50 below is dominated by that constant times the tier's node count,
// not by runner capacity: raising the worker count past the in-flight ceiling
// does not move it. Read the metric as a characterization of this topology's
// hand-off cost, where a change means either the tier definition or the dispatch
// path changed — both of which are what the suite exists to catch.
//
// The sample counts are sized for a gate that finishes in a couple of minutes,
// so the high tier's p99 is its slowest observed job rather than a smooth
// tail. Widen the counts to sharpen the tail, at proportional wall-clock cost.
const (
	// stressJobTimeout bounds one job end to end: submit, signal delivery, and
	// terminal status. The high tier's wait node has a 5s timer fallback, so a
	// budget below that would report a hang for a run that was merely waiting.
	stressJobTimeout = 30 * time.Second
	// stressPollEvery is the completion/suspension poll interval. It bounds how
	// long a signal waits for its node to park, which is why the high tier's
	// latency includes this constant per signal rather than the runner's.
	stressPollEvery = 10 * time.Millisecond
	// stressRunnerCacheEntries mirrors the SDK runner's package-cache size for
	// the map body's subgraph packages.
	stressRunnerCacheEntries = 64
	// stressRunners and stressRunnerConcurrency set the in-flight ceiling:
	// stressRunners * stressRunnerConcurrency tasks execute at once, and any
	// worker beyond that queues rather than increasing parallelism.
	stressRunners           = 4
	stressRunnerConcurrency = 2
	// Per-tier job counts and worker counts.
	stressLowJobs   = 40
	stressLowWorker = 16
	stressMedJobs   = 24
	stressMedWorker = 16
	stressHighJobs  = 12
	stressHighWork  = 8

	stressMaxReportedFailure = 10
)

// stressJob is one unit of load: a definition and the input it runs with. The
// definition is submitted inline, so per-job vars live here rather than in a
// shared registration.
type stressJob struct {
	def   *types.WorkflowDef
	input map[string]any
}

// TestWorkflowTierStress drives each complexity tier under concurrent load and
// reports throughput, latency percentiles, and error rate per tier.
//
// It fails when any execution does not reach success: a stress report whose
// failures are only printed is a report of a broken build, not of capacity.
func TestWorkflowTierStress(t *testing.T) {
	if testing.Short() {
		t.Skip("skip stress test in short mode")
	}

	t.Run("low", func(t *testing.T) {
		addr := requireRedisLoad(t)
		jobs := lowStressJobs(t, stressLowJobs)
		h := newStressHarness(t, addr, stressDefs(jobs), "stress-low", nil, nil)
		runStressTier(t, "low", h, jobs, stressLowWorker, nil)
	})

	t.Run("medium", func(t *testing.T) {
		addr := requireRedisLoad(t)

		enrich := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/enrich" {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"enriched": true}`))
		}))
		defer enrich.Close()

		jobs := mediumStressJobs(t, stressMedJobs, enrich.URL)
		h := newStressHarness(t, addr, stressDefs(jobs), "stress-medium", nil, nil)
		runStressTier(t, "medium", h, jobs, stressMedWorker, nil)
	})

	t.Run("high", func(t *testing.T) {
		addr := requireRedisLoad(t)
		dsn := requireMySQLStress(t)

		table := stressTableName(t)
		createStressTable(t, dsn, table)
		t.Cleanup(func() { dropStressTable(t, dsn, table) })

		grpcHost := startStressGRPCServer(t)

		pool := resource.NewDefaultResourcePool(types.DefaultResourcePoolConfig())
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = pool.Close(ctx)
		})
		// The resolver hands the node its DSN. It is never logged: the value
		// carries the MySQL password and the node reports only classified errors.
		resolver := func(_ namespace.Namespace, name string) map[string]any {
			if name != workflows.CredentialDB {
				return nil
			}
			return map[string]any{"driver": "mysql", "dsn": dsn}
		}

		jobs := highStressJobs(t, stressHighJobs, grpcHost, table)
		h := newStressHarness(t, addr, stressDefs(jobs), "stress-high", pool, resolver)
		runStressTier(t, "high", h, jobs, stressHighWork, driveHighStress)
	})
}

// --- load driver ---

// runStressTier submits every job across workers, drives any signals the
// definition needs, and reports the outcome. drive is called on the worker
// goroutine and returns errors instead of failing the test, so one bad
// execution cannot take the whole load run down.
func runStressTier(t *testing.T, tier string, h *stressHarness, jobs []stressJob, workers int, drive func(*stressHarness, types.ExecutionID) error) {
	t.Helper()

	total := len(jobs)
	// One writer per index, and each index is claimed by exactly one worker.
	latencies := make([]time.Duration, total)
	var failed, timeouts int64

	var errMu sync.Mutex
	var jobErrors []error
	record := func(err error) {
		errMu.Lock()
		defer errMu.Unlock()
		jobErrors = append(jobErrors, err)
	}

	queue := make(chan int, total)
	for i := 0; i < total; i++ {
		queue <- i
	}
	close(queue)

	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range queue {
				latencies[i] = runStressJob(h, jobs[i], drive, &failed, &timeouts, record)
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	reportStressTier(t, tier, total, workers, elapsed, latencies, failed, timeouts)

	if len(jobErrors) > 0 {
		shown := jobErrors
		if len(shown) > stressMaxReportedFailure {
			shown = shown[:stressMaxReportedFailure]
		}
		for _, err := range shown {
			t.Errorf("%s: %v", tier, err)
		}
		if len(jobErrors) > len(shown) {
			t.Errorf("%s: %d further failure(s) not listed", tier, len(jobErrors)-len(shown))
		}
	}
}

// runStressJob runs one job and returns the time it took. Counters are updated
// through the atomics the caller passes so the caller owns the report.
func runStressJob(h *stressHarness, job stressJob, drive func(*stressHarness, types.ExecutionID) error, failed, timeouts *int64, record func(error)) time.Duration {
	start := time.Now()

	id, err := submitStress(h, job.def, job.input)
	if err != nil {
		record(fmt.Errorf("submit: %w", err))
		atomic.AddInt64(failed, 1)
		return time.Since(start)
	}

	if drive != nil {
		if err := drive(h, id); err != nil {
			record(err)
			atomic.AddInt64(failed, 1)
			return time.Since(start)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), stressJobTimeout)
	defer cancel()
	snap, timedOut := awaitTerminalStress(ctx, h.state, id)
	if timedOut {
		atomic.AddInt64(timeouts, 1)
		record(fmt.Errorf("execution %s did not terminate within %v", id, stressJobTimeout))
		return time.Since(start)
	}
	if snap.Status != types.ExecutionStatusSuccess {
		atomic.AddInt64(failed, 1)
		record(fmt.Errorf("execution %s status = %s error = %q", id, snap.Status, snap.Error))
	}
	return time.Since(start)
}

// reportStressTier prints the human-readable summary and the structured metric
// line, then fails the test when any job did not succeed.
func reportStressTier(t *testing.T, tier string, total, workers int, elapsed time.Duration, latencies []time.Duration, failed, timeouts int64) {
	t.Helper()

	sorted := make([]time.Duration, len(latencies))
	copy(sorted, latencies)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	// percentileLatency is the package's nearest-rank helper (declared in
	// e2e_load_bench_test.go) so p50/p95/p99 map to observed samples.
	p50 := percentileLatency(sorted, 0.50)
	p95 := percentileLatency(sorted, 0.95)
	p99 := percentileLatency(sorted, 0.99)

	done := int64(len(latencies))
	throughput := float64(done) / elapsed.Seconds()

	fmt.Printf("%s stress: total=%d workers=%d elapsed=%v throughput=%.0f/s failed=%d timeouts=%d p50=%v p95=%v p99=%v\n",
		tier, total, workers, elapsed.Round(time.Millisecond), throughput,
		failed, timeouts, p50.Round(time.Millisecond), p95.Round(time.Millisecond), p99.Round(time.Millisecond))

	// Keep the key=value shape stable so the CI sampling job can parse it.
	// topology=server-runner so the report cannot be misread as embedded-engine
	// capacity.
	fmt.Printf("perf.metric topology=server-runner test=tier_%s_stress total=%d workers=%d throughput=%.0f/s failed=%d timeouts=%d p50_ms=%.3f p95_ms=%.3f p99_ms=%.3f\n",
		tier, total, workers, throughput, failed, timeouts,
		p50.Seconds()*1000, p95.Seconds()*1000, p99.Seconds()*1000)

	if failed > 0 {
		t.Errorf("%s: %d execution(s) failed", tier, failed)
	}
	if timeouts > 0 {
		t.Errorf("%s: %d execution(s) did not terminate within %v", tier, timeouts, stressJobTimeout)
	}
}

// --- tier job builders ---

func lowStressJobs(t *testing.T, n int) []stressJob {
	t.Helper()
	jobs := make([]stressJob, n)
	for i := range jobs {
		def, err := workflows.LowWorkflow().Definition()
		if err != nil {
			t.Fatalf("LowWorkflow().Definition(): %v", err)
		}
		// Alternate arms so both branch nodes are loaded; the coverage and
		// integration suites assert which arm each input selects.
		input := workflows.BelowThresholdInput()
		if i%2 == 0 {
			input = workflows.AboveThresholdInput()
		}
		jobs[i] = stressJob{def: def, input: input}
	}
	return jobs
}

func mediumStressJobs(t *testing.T, n int, httpURL string) []stressJob {
	t.Helper()
	jobs := make([]stressJob, n)
	for i := range jobs {
		def, err := workflows.MediumWorkflow().Definition()
		if err != nil {
			t.Fatalf("MediumWorkflow().Definition(): %v", err)
		}
		workflows.WithVars(def, workflows.MediumVars(httpURL, "qa-stress@example.test"))
		input := workflows.BelowThresholdInput()
		if i%2 == 0 {
			input = workflows.AboveThresholdInput()
		}
		jobs[i] = stressJob{def: def, input: input}
	}
	return jobs
}

// highStressJobs builds one definition per job so each execution inserts a row
// no other execution can collide with.
func highStressJobs(t *testing.T, n int, grpcHost, table string) []stressJob {
	t.Helper()
	jobs := make([]stressJob, n)
	for i := range jobs {
		def, err := workflows.HighWorkflow().Definition()
		if err != nil {
			t.Fatalf("HighWorkflow().Definition(): %v", err)
		}
		rowID := fmt.Sprintf("stress-row-%d", i)
		workflows.WithVars(def, workflows.HighVars(grpcHost, table, rowID))
		jobs[i] = stressJob{def: def, input: map[string]any{}}
	}
	return jobs
}

func stressDefs(jobs []stressJob) []*types.WorkflowDef {
	defs := make([]*types.WorkflowDef, 0, len(jobs))
	for _, j := range jobs {
		defs = append(defs, j.def)
	}
	return defs
}

// driveHighStress delivers the two signals the high tier waits for. It polls
// each node until it parks rather than sleeping a fixed interval, so a slow
// runner delays the job instead of failing it with a rejected signal.
func driveHighStress(h *stressHarness, id types.ExecutionID) error {
	if err := awaitNodeSuspendedStress(h, id, "gate", stressJobTimeout); err != nil {
		return err
	}
	if err := postSignalStress(h, id, workflows.HighApprovalSignal, map[string]any{
		"approver": workflows.HighApprover,
		"action":   "approve",
	}); err != nil {
		return err
	}
	// The release signal must not be sent before hold parks: the node is not
	// listening until it suspends, and an early signal would be consumed by the
	// still-open approval window instead.
	if err := awaitNodeSuspendedStress(h, id, "hold", stressJobTimeout); err != nil {
		return err
	}
	return postSignalStress(h, id, workflows.HighReleaseSignal, map[string]any{"release": true})
}

// --- harness ---

// stressHarness is the production server+runner topology under load.
type stressHarness struct {
	httpSrv *httptest.Server
	state   engine.StateStore
	runners control.RunnerDirectory
	stop    func()
}

// newStressHarness brings up the control plane and nRunners runners advertising
// the union of defs' node-type capabilities. pool and resolver are the
// production resource/credential wiring; pass nil for tiers that need neither.
func newStressHarness(t *testing.T, addr string, defs []*types.WorkflowDef, runnerPrefix string, pool types.ResourcePool, resolver func(namespace.Namespace, string) map[string]any) *stressHarness {
	t.Helper()

	b, err := distributed.New(addr, nil, distributed.WithConcurrency(8), distributed.WithConsumer(true))
	if err != nil {
		t.Fatalf("distributed.New: %v", err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	// Scoped to asynq:* so it does not disturb keys other suites hold in the
	// same Redis DB; a stale task from a crashed run would otherwise be drawn by
	// this harness's consumer and fail for a missing handler.
	flushAsynqKeysLoad(context.Background(), t, rdb)
	_ = rdb.Close()

	cp, err := control.NewControlPlane(control.Config{
		Backend:               b,
		RuntimeEvidenceBuffer: engine.NewRuntimeEvidenceBuffer(64),
	})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	srv, err := apiserver.New(apiserver.Config{}, apiserver.WithControlPlane(cp))
	if err != nil {
		t.Fatalf("apiserver.New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := srv.Start(ctx); err != nil {
		cancel()
		t.Fatalf("apiserver.Start: %v", err)
	}
	httpSrv := httptest.NewServer(srv.Handler())

	registry := execution.NewRegistry()
	caps := stressCaps(defs)
	for i := 0; i < stressRunners; i++ {
		id := fmt.Sprintf("%s-%d", runnerPrefix, i+1)
		r := runnersvc.New(
			protocol.NewClient(httpSrv.URL, httpSrv.Client()),
			registry,
			runnersvc.Config{
				RunnerID:           id,
				Concurrency:        stressRunnerConcurrency,
				Capabilities:       caps,
				PollWait:           5 * time.Millisecond,
				ResourcePool:       pool,
				CredentialResolver: resolver,
				// The medium tier's map node dispatches one synthetic batch lease
				// per item and the lease names a node with no handler, so the
				// runner needs the subgraph runtime to resolve the body's members.
				SubgraphRuntime: runnersvc.NewSubgraphRuntime(
					registry,
					runnersvc.NewPackageCache(runnersvc.PackageCacheConfig{MaxEntries: stressRunnerCacheEntries}),
				),
			},
		)
		go func() { _ = r.Run(ctx) }()
	}

	h := &stressHarness{httpSrv: httpSrv, state: b.State(), runners: cp.RunnerDirectory()}
	h.stop = func() {
		cancel()
		shutdownCtx, sc := context.WithTimeout(context.Background(), 5*time.Second)
		defer sc()
		_ = srv.Shutdown(shutdownCtx)
		httpSrv.Close()
	}
	t.Cleanup(h.stop)

	for i := 0; i < stressRunners; i++ {
		waitForRunnerLoad(t, h.runners, fmt.Sprintf("%s-%d", runnerPrefix, i+1))
	}
	return h
}

// stressCaps lists the node-type capabilities of every definition under load,
// deduplicated, which is what the runners must advertise to be assigned the
// work.
func stressCaps(defs []*types.WorkflowDef) []protocol.Capability {
	seen := make(map[string]bool)
	var caps []protocol.Capability
	for _, def := range defs {
		for nodeType := range workflows.NodeTypesIn(def) {
			if seen[nodeType] {
				continue
			}
			seen[nodeType] = true
			caps = append(caps, protocol.Capability{NodeType: nodeType})
		}
	}
	return caps
}

// --- HTTP helpers (goroutine-safe: they return errors, never call t.Fatal) ---

func submitStress(h *stressHarness, def *types.WorkflowDef, params map[string]any) (types.ExecutionID, error) {
	body := struct {
		Workflow *types.WorkflowDef `json:"workflow"`
		Params   map[string]any     `json:"params"`
	}{Workflow: def, Params: params}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		return "", fmt.Errorf("encode submit: %w", err)
	}
	resp, err := h.httpSrv.Client().Post(h.httpSrv.URL+control.SubmitWorkflowPath, "application/json", &buf)
	if err != nil {
		return "", fmt.Errorf("post submit: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var raw bytes.Buffer
		_, _ = raw.ReadFrom(resp.Body)
		return "", fmt.Errorf("submit status %d: %s", resp.StatusCode, raw.String())
	}
	// The execute route answers through the wire envelope (API-SPECIFICATION.md
	// §3), so execution_id sits under data.
	var env struct {
		Success bool            `json:"success"`
		Code    string          `json:"code"`
		Data    json.RawMessage `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		return "", fmt.Errorf("decode submit envelope: %w", err)
	}
	if !env.Success {
		return "", fmt.Errorf("submit rejected: code=%s", env.Code)
	}
	var out struct {
		ExecutionID types.ExecutionID `json:"execution_id"`
	}
	if err := json.Unmarshal(env.Data, &out); err != nil {
		return "", fmt.Errorf("decode submit data: %w", err)
	}
	if out.ExecutionID == "" {
		return "", fmt.Errorf("submit returned an empty execution_id")
	}
	return out.ExecutionID, nil
}

func postSignalStress(h *stressHarness, id types.ExecutionID, name string, data map[string]any) error {
	body, err := json.Marshal(map[string]any{"name": name, "data": data})
	if err != nil {
		return fmt.Errorf("encode signal %s: %w", name, err)
	}
	// Built from the route constant so the path is not restated here.
	url := h.httpSrv.URL + strings.Replace(apiserver.PathExecutionSignals, "{id}", string(id), 1)
	resp, err := h.httpSrv.Client().Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("post signal %s: %w", name, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var raw bytes.Buffer
		_, _ = raw.ReadFrom(resp.Body)
		return fmt.Errorf("signal %s status %d: %s", name, resp.StatusCode, raw.String())
	}
	var env struct {
		Data struct {
			Accepted bool `json:"accepted"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		return fmt.Errorf("decode signal %s: %w", name, err)
	}
	if !env.Data.Accepted {
		return fmt.Errorf("signal %s was not accepted", name)
	}
	return nil
}

// awaitNodeSuspendedStress polls a node until it parks or the execution
// terminates, so a job whose upstream already failed reports that failure
// instead of spinning to the deadline.
func awaitNodeSuspendedStress(h *stressHarness, id types.ExecutionID, name string, budget time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	ticker := time.NewTicker(stressPollEvery)
	defer ticker.Stop()

	var last types.NodeStatus
	for {
		if snap, err := h.state.GetNode(ctx, id, name); err == nil && snap != nil {
			last = snap.Status
			if snap.Status == types.NodeStatusSuspended {
				return nil
			}
		}
		if snap, err := h.state.GetExecution(ctx, id); err == nil && snap != nil && types.IsTerminalExecutionStatus(snap.Status) {
			return fmt.Errorf("execution %s reached %s before node %q suspended (node status %s, error %q)",
				id, snap.Status, name, last, snap.Error)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout after %v waiting for node %q to suspend (last status %s)", budget, name, last)
		case <-ticker.C:
		}
	}
}

// awaitTerminalStress polls until the execution reaches a terminal state. It
// returns timedOut=true when the context expires first, which the caller counts
// separately from a real failure.
func awaitTerminalStress(ctx context.Context, state engine.StateStore, id types.ExecutionID) (*engine.ExecutionSnapshot, bool) {
	ticker := time.NewTicker(stressPollEvery)
	defer ticker.Stop()
	for {
		snap, err := state.GetExecution(ctx, id)
		if err == nil && snap != nil && types.IsTerminalExecutionStatus(snap.Status) {
			return snap, false
		}
		select {
		case <-ctx.Done():
			return nil, true
		case <-ticker.C:
		}
	}
}

// --- high tier dependencies ---

// requireMySQLStress returns the MySQL DSN, skipping when the database is
// unreachable. Under XFLOW_REQUIRE_MYSQL_INTEGRATION=1 it fails instead so a
// missing dependency cannot be mistaken for a passing gate.
//
// Unlike test/integration/harness.go it carries no default password: the DSN
// arrives from XFLOW_TEST_MYSQL_DSN, or is assembled from MYSQL_ROOT_PASSWORD
// when that is set. The DSN is never echoed — it embeds the credential.
func requireMySQLStress(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("XFLOW_TEST_MYSQL_DSN")
	if dsn == "" {
		pw := os.Getenv("MYSQL_ROOT_PASSWORD")
		if pw != "" {
			port := envOrStress("MYSQL_PORT", "3306")
			db := envOrStress("MYSQL_DATABASE", "xflow")
			dsn = fmt.Sprintf("root:%s@tcp(localhost:%s)/%s?parseTime=true&multiStatements=true", pw, port, db)
		}
	}
	if dsn == "" {
		if os.Getenv("XFLOW_REQUIRE_MYSQL_INTEGRATION") == "1" {
			t.Fatal("XFLOW_REQUIRE_MYSQL_INTEGRATION=1 but neither XFLOW_TEST_MYSQL_DSN nor MYSQL_ROOT_PASSWORD is set")
		}
		t.Skip("mysql dsn unavailable: set XFLOW_TEST_MYSQL_DSN (or MYSQL_ROOT_PASSWORD)")
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open mysql: %v", err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		port := envOrStress("MYSQL_PORT", "3306")
		if os.Getenv("XFLOW_REQUIRE_MYSQL_INTEGRATION") == "1" {
			t.Fatalf("XFLOW_REQUIRE_MYSQL_INTEGRATION=1: mysql unavailable at localhost:%s: %v", port, err)
		}
		t.Skipf("mysql unavailable at localhost:%s: %v", port, err)
	}
	return dsn
}

func envOrStress(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// stressTableName returns a table name safe to splat into DDL.
func stressTableName(t *testing.T) string {
	t.Helper()
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("rand: %v", err)
	}
	return "qa_stress_tier_" + hex.EncodeToString(b[:])
}

// createStressTable creates the table the high tier inserts into. The database
// node never issues DDL, so the caller owns the schema — the same division a
// deployment has.
func createStressTable(t *testing.T, dsn, table string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open mysql: %v", err)
	}
	defer db.Close()
	stmt := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS `%s` (`id` VARCHAR(64) NOT NULL PRIMARY KEY, `qty` INT NOT NULL)",
		table,
	)
	if _, err := db.Exec(stmt); err != nil {
		t.Fatalf("create table %s: %v", table, err)
	}
}

func dropStressTable(t *testing.T, dsn, table string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Logf("drop table %s: open: %v", table, err)
		return
	}
	defer db.Close()
	if _, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", table)); err != nil {
		t.Logf("drop table %s: %v", table, err)
	}
}

// startStressGRPCServer runs a gRPC server that answers every unknown method
// with NotFound. The high tier's gRPC node answers only on its error path (see
// defs_high.go), and the unknown-service handler never unmarshals a request, so
// the status arrives without a body to decode.
func startStressGRPCServer(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer(grpc.UnknownServiceHandler(func(_ any, _ grpc.ServerStream) error {
		return status.Error(codes.NotFound, "stress: subject not registered")
	}))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}
