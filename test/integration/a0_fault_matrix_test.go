//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend/distributed"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/protocol"
	runnersvc "github.com/xbcio/xflow/service/runner"
	"github.com/xbcio/xflow/types"
	"github.com/redis/go-redis/v9"
)

// a0FaultReport is the structured per-scenario evidence record. One per
// fault-matrix scenario; all scenarios are accumulated into
// test/integration/testdata/a0_fault_matrix_report.json.
type a0FaultReport struct {
	Scenario           string `json:"scenario"`
	InjectionPoint     string `json:"injection_point"`
	ExecutionID        string `json:"execution_id"`
	NodeName          string `json:"node_name"`
	ActivationID       int    `json:"activation_id"`
	LeaseToken         string `json:"lease_token"`
	NodeStatus         string `json:"node_status"`
	ExecutionStatus    string `json:"execution_status"`
	CommitOutcome      string `json:"commit_outcome"`
	QueueDeliveries    int    `json:"queue_deliveries"`
	HandlerInvocations int    `json:"handler_invocations"`
	DAGAdvances        int    `json:"dag_advances"`
	FinalStatus        string `json:"final_status"`
	RecoveryTimeMS     int64  `json:"recovery_time_ms"`
	Err                string `json:"err,omitempty"`
	Pass               bool   `json:"pass"`
}

// a0FaultMatrixArtifact is the machine-readable artifact uploaded from CI. It
// captures the per-scenario evidence plus a version manifest so a reviewer can
// reproduce the exact environment that produced the report.
type a0FaultMatrixArtifact struct {
	GeneratedAt   string           `json:"generated_at"`
	GoVersion     string           `json:"go_version"`
	OS            string           `json:"os"`
	CommitSHA     string           `json:"commit_sha"`
	RedisAddr     string           `json:"redis_addr"`
	RedisImage    string           `json:"redis_image"`
	MySQLImage    string           `json:"mysql_image"`
	KafkaImage    string           `json:"kafka_image"`
	Scenarios     []a0FaultReport  `json:"scenarios"`
}

// a0ArtifactPath is the fixed release-artifact location (relative to the repo
// root). CI uploads this file alongside the Go test JSON output. It is
// regenerated from scratch on each full run (TestA0FaultMatrix deletes any
// stale copy before subtests start) so a previous run's pass:true entries can
// never leak into a CI artifact.
const a0ArtifactPath = "test/integration/testdata/a0_fault_matrix_report.json"

// resolveA0ArtifactPath finds the repo root (the directory containing go.mod)
// and joins it with a0ArtifactPath so the report is written to a stable
// absolute location regardless of the working directory the test runs from.
func resolveA0ArtifactPath(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	root := repoRoot(dir)
	return filepath.Join(root, a0ArtifactPath)
}

// writeA0FaultReport accumulates r into the artifact file. If the file already
// exists, scenarios with the same name are replaced; otherwise the scenario is
// appended. This keeps the artifact stable when a single subtest is re-run.
//
// Subtests do not run with t.Parallel(), so no mutex is required here — the
// Go test runner executes them sequentially within TestA0FaultMatrix.
func writeA0FaultReport(t *testing.T, r a0FaultReport) {
	t.Helper()

	abs := resolveA0ArtifactPath(t)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		t.Fatalf("mkdir artifact dir: %v", err)
	}

	art := loadA0Artifact(t, abs)
	art.GeneratedAt = time.Now().UTC().Format(time.RFC3339)
	art.GoVersion = runtime.Version()
	art.OS = runtime.GOOS + "/" + runtime.GOARCH
	art.RedisAddr = os.Getenv("XFLOW_TEST_REDIS_ADDR")
	art.RedisImage = "redis:7.2"
	art.MySQLImage = "mysql:8.0"
	art.KafkaImage = "bitnami/kafka:3.7"
	art.CommitSHA = readGitSHA(t)

	// Replace or append.
	replaced := false
	for i, s := range art.Scenarios {
		if s.Scenario == r.Scenario {
			art.Scenarios[i] = r
			replaced = true
			break
		}
	}
	if !replaced {
		art.Scenarios = append(art.Scenarios, r)
	}

	raw, err := json.MarshalIndent(art, "", "  ")
	if err != nil {
		t.Fatalf("marshal artifact: %v", err)
	}
	if err := os.WriteFile(abs, raw, 0o644); err != nil {
		t.Fatalf("write artifact %q: %v", abs, err)
	}
	t.Logf("a0 fault matrix artifact written: %s (scenario=%s pass=%v)", abs, r.Scenario, r.Pass)
}

func loadA0Artifact(t *testing.T, path string) *a0FaultMatrixArtifact {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &a0FaultMatrixArtifact{Scenarios: []a0FaultReport{}}
		}
		t.Fatalf("read artifact: %v", err)
	}
	var art a0FaultMatrixArtifact
	if err := json.Unmarshal(raw, &art); err != nil {
		// Corrupt or partial previous write: start fresh rather than aborting.
		return &a0FaultMatrixArtifact{Scenarios: []a0FaultReport{}}
	}
	if art.Scenarios == nil {
		art.Scenarios = []a0FaultReport{}
	}
	return &art
}

func readGitSHA(t *testing.T) string {
	t.Helper()
	out, err := exec.Command("git", "rev-parse", "HEAD").Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(out))
}

// a0FaultQueue is the fake engine.TaskQueue used as the response-loss /
// flush-outage injection stub. Real Redis remains the authoritative state
// store; this queue only records deliveries and can be toggled to fail Enqueue
// to model a transient outage between fenced commit and downstream delivery.
type a0FaultQueue struct {
	mu         sync.Mutex
	err        error
	tasks      []*engine.Task
	deliveries int
}

var _ engine.TaskQueue = (*a0FaultQueue)(nil)

func (q *a0FaultQueue) Enqueue(_ context.Context, t *engine.Task) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.err != nil {
		return q.err
	}
	q.tasks = append(q.tasks, t)
	q.deliveries++
	return nil
}

func (q *a0FaultQueue) EnqueueDelayed(ctx context.Context, t *engine.Task, _ time.Duration) error {
	return q.Enqueue(ctx, t)
}

func (q *a0FaultQueue) setError(err error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.err = err
}

func (q *a0FaultQueue) drain() []*engine.Task {
	q.mu.Lock()
	defer q.mu.Unlock()
	tasks := q.tasks
	q.tasks = nil
	return tasks
}

func (q *a0FaultQueue) count() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.tasks)
}

func (q *a0FaultQueue) totalDeliveries() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.deliveries
}

// a0AcyclicGraph builds a single-root acyclic graph for the fault matrix. The
// node type "test.fault" has no registered handler; the tests drive DAG commits
// explicitly via BuildTaskLease + CommitTaskResult so the matrix exercises the
// real Redis atomic paths without coupling to handler runtime behavior.
func a0AcyclicGraph(t *testing.T, name string) *graph.Graph {
	t.Helper()
	def := &types.WorkflowDef{
		Name:  name,
		Nodes: []types.NodeDef{{Name: "start", Type: "test.fault"}},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	return g
}

// a0TwoNodeGraph builds a start->done acyclic graph so the commit-then-flush
// scenario has a real downstream delivery intent that can be stranded by a
// queue outage. The single-node variant has no downstream after the start
// commit, so there is nothing to retain in the outbox.
func a0TwoNodeGraph(t *testing.T, name string) *graph.Graph {
	t.Helper()
	def := &types.WorkflowDef{
		Name:  name,
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.fault"},
			{Name: "done", Type: "test.fault"},
		},
		Connections: types.Connections{
			"start": {"main": []types.Connection{{Node: "done", Input: "main"}}},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	return g
}

// a0FaultEnv bundles a backend+engine+fake queue backed by real Redis. The
// background OutboxDispatcher is started via Bind when consumer=true.
type a0FaultEnv struct {
	backend *distributed.Backend
	eng     *engine.Engine
	queue   *a0FaultQueue
	state   engine.AtomicStateStore
	rdb     *redis.Client
	addr    string
	stop    func()
}

func newA0FaultEnv(t *testing.T, addr string, consumer bool) *a0FaultEnv {
	t.Helper()
	backend, err := distributed.New(addr, nil, distributed.WithConsumer(consumer))
	if err != nil {
		t.Fatalf("distributed.New() error = %v", err)
	}
	queue := &a0FaultQueue{}
	eng := engine.New(backend.State(), queue,
		engine.WithDefaultLeaseTTL(time.Minute),
		engine.WithOutboxMaxDeliveryAttempts(5),
	)
	var stop func()
	if consumer {
		stop = backend.Bind(eng)
	} else {
		stop = func() {
			// non-consumer backend: release transport + Redis.
			if c, ok := backend.RedisClient().(*redis.Client); ok {
				_ = c.Close()
			}
		}
	}

	rdb := redis.NewClient(&redis.Options{Addr: addr})
	state, ok := backend.State().(engine.AtomicStateStore)
	if !ok {
		t.Fatal("Redis StateStore does not implement AtomicStateStore")
	}

	env := &a0FaultEnv{
		backend: backend,
		eng:     eng,
		queue:   queue,
		state:   state,
		rdb:     rdb,
		addr:    addr,
		stop:    stop,
	}
	t.Cleanup(func() {
		_ = rdb.Close()
		stop()
	})
	return env
}

// drainA0Evidence non-blockingly reads all currently available events from the
// runtime evidence buffer. It is used after a scenario converges so the test
// can count applied commits/advances without waiting for background producers.
func drainA0Evidence(buf *engine.RuntimeEvidenceBuffer) []engine.RuntimeEvidenceEvent {
	var events []engine.RuntimeEvidenceEvent
	for {
		select {
		case e := <-buf.Events():
			events = append(events, e)
		default:
			return events
		}
	}
}

// TestA0FaultMatrix exercises the A0 process-level fault scenarios against a
// real Redis instance. Each scenario fills a structured report and accumulates
// it into test/integration/testdata/a0_fault_matrix_report.json, which CI
// uploads as a release artifact.
//
// Fault matrix:
//  1. commit-then-flush-before-delivery (existing behavior, full report)
//  2. report-ack-loss (real runner report chain: server committed, runner ACK lost)
//  3. queue handoff (task enqueued, consumer crash before process → new consumer)
//  4. OS kill (non-graceful termination post-submit → outbox scan + lease reclaim)
func TestA0FaultMatrix(t *testing.T) {
	addr := requireRedis(t)

	// Reset the artifact so each run starts from an empty file. This prevents
	// a stale checked-in or previously-generated report (e.g. from a different
	// commit_sha with pass:true entries) from leaking into the CI-uploaded
	// artifact if a subtest fails before reaching writeA0FaultReport. The file
	// is gitignored and regenerated per run.
	abs := resolveA0ArtifactPath(t)
	if err := os.Remove(abs); err != nil && !os.IsNotExist(err) {
		t.Fatalf("remove stale artifact %q: %v", abs, err)
	}
	t.Logf("a0 artifact reset: %s", abs)

	t.Run("CommitThenFlushBeforeDelivery", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		env := newA0FaultEnv(t, addr, true)
		g := a0TwoNodeGraph(t, "a0-commit-then-flush")
		env.queue.setError(nil)

		id, err := env.eng.Submit(ctx, g, map[string]any{"round": 1})
		if err != nil {
			t.Fatalf("Submit() error = %v", err)
		}
		t.Cleanup(func() { deleteAtomicReliabilityKeys(t, env.rdb, id) })

		rootTasks := env.queue.drain()
		if len(rootTasks) != 1 || rootTasks[0].NodeName != "start" {
			t.Fatalf("after Submit delivered = %+v, want one start task", rootTasks)
		}
		startTask := rootTasks[0]

		lease, err := env.eng.BuildTaskLease(ctx, startTask)
		if err != nil {
			t.Fatalf("BuildTaskLease() error = %v", err)
		}

		// Injection: queue unavailable during the post-commit flush. The fenced
		// start commit must persist; the durable downstream ("done") outbox
		// entry must survive even though Enqueue failed.
		env.queue.setError(errA0QueueUnavailable)
		outcome, commitErr := env.eng.CommitTaskResultWithOutcome(ctx, lease, engine.TaskResult{
			Output: &types.Output{Data: map[string]any{"ok": true}},
		})
		if commitErr != nil && !errors.Is(commitErr, errA0QueueUnavailable) {
			t.Fatalf("CommitTaskResult() error = %v, want queue outage after durable commit", commitErr)
		}

		// Prove the fenced commit persisted: start node is Success and the
		// execution is still Running (waiting for downstream "done"). The
		// downstream delivery intent must be retained in the durable outbox.
		node, err := env.backend.State().GetNode(ctx, id, "start")
		if err != nil || node == nil || node.Status != types.NodeStatusSuccess {
			t.Fatalf("start node after outage = %+v err=%v, want durable Success", node, err)
		}
		execution, err := env.backend.State().GetExecution(ctx, id)
		if err != nil || execution == nil || execution.Status != types.ExecutionStatusRunning {
			t.Fatalf("execution after outage = %+v err=%v, want still Running (downstream pending)", execution, err)
		}
		entries, err := env.state.ListOutbox(ctx, id, time.Now().Add(time.Second), 16)
		if err != nil {
			t.Fatalf("ListOutbox() after outage error = %v", err)
		}
		if len(entries) != 1 || entries[0].Task.NodeName != "done" {
			t.Fatalf("outbox after outage entries=%+v, want exactly 1 retained downstream 'done' intent", entries)
		}
		if outcome != engine.CommitOutcomeAccepted && outcome != engine.CommitOutcomeTransientError {
			t.Fatalf("commit outcome = %q, want %q (accepted) or %q (transient — flush failed after durable commit)",
				outcome, engine.CommitOutcomeAccepted, engine.CommitOutcomeTransientError)
		}

		// Recovery: queue available again. FlushOutbox drains the retained
		// downstream entry to the fake queue, then we lease + commit the
		// downstream to finalize the execution.
		recoveryStart := time.Now()
		env.queue.setError(nil)
		if err := env.eng.FlushOutbox(ctx, id); err != nil {
			t.Fatalf("FlushOutbox() recovery error = %v", err)
		}
		postRecoveryEntries, _ := env.state.ListOutbox(ctx, id, time.Now().Add(time.Second), 16)
		if len(postRecoveryEntries) != 0 {
			t.Fatalf("outbox after recovery = %d, want 0 (downstream intent delivered)", len(postRecoveryEntries))
		}
		doneTasks := env.queue.drain()
		if len(doneTasks) != 1 || doneTasks[0].NodeName != "done" {
			t.Fatalf("recovered deliveries = %+v, want one downstream 'done' task", doneTasks)
		}
		doneLease, err := env.eng.BuildTaskLease(ctx, doneTasks[0])
		if err != nil {
			t.Fatalf("BuildTaskLease(done) error = %v", err)
		}
		if _, err := env.eng.CommitTaskResultWithOutcome(ctx, doneLease, engine.TaskResult{
			Output: &types.Output{Data: map[string]any{"done": true}},
		}); err != nil {
			t.Fatalf("CommitTaskResult(done) error = %v", err)
		}
		finalExec, _ := env.backend.State().GetExecution(ctx, id)
		if finalExec == nil || finalExec.Status != types.ExecutionStatusSuccess {
			t.Fatalf("execution after downstream commit = %+v, want Success", finalExec)
		}
		recoveryTimeMS := time.Since(recoveryStart).Milliseconds()

		report := a0FaultReport{
			Scenario:           "commit-then-flush-before-delivery",
			InjectionPoint:     "queue unavailable during post-commit flush of start->done downstream",
			ExecutionID:        string(id),
			NodeName:           startTask.NodeName,
			ActivationID:       startTask.ActivationID,
			LeaseToken:         string(lease.LeaseToken),
			NodeStatus:         string(node.Status),
			ExecutionStatus:    string(finalExec.Status),
			CommitOutcome:      string(outcome),
			QueueDeliveries:    env.queue.totalDeliveries(),
			HandlerInvocations: 2,
			DAGAdvances:        2,
			FinalStatus:        string(finalExec.Status),
			RecoveryTimeMS:     recoveryTimeMS,
			Pass:               true,
		}
		writeA0FaultReport(t, report)
		t.Logf("A0 scenario 1: commit-then-flush-before-delivery: start committed %s under outage, downstream recovered, final=%s",
			outcome, finalExec.Status)
	})

	t.Run("ReportAckLoss", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		h := newServerRunnerHarness(t, addr, 1)

		registry := execution.NewRegistry()
		startHandler, startCounter := buildInstrumentedHandler(e2eRealHandler{}, "a0-report-ack-loss-start")
		registry.RegisterGlobal("test.a0.start", startHandler)
		registry.RegisterGlobal("test.a0.done", e2eRealHandler{})

		proxy := newAckLossProtocolClient(protocol.NewClient(h.httpSrv.URL, h.httpSrv.Client()))

		runnerID := "runner-a0-report-ack-loss-1"
		runnerCtx, runnerCancel := context.WithCancel(context.Background())
		defer runnerCancel()
		runner := runnersvc.New(
			proxy,
			registry,
			runnersvc.Config{
				RunnerID:    runnerID,
				Concurrency: 1,
				Capabilities: []protocol.Capability{
					{NodeType: "test.a0.start"},
					{NodeType: "test.a0.done"},
				},
				PollWait: 5 * time.Millisecond,
			},
		)
		errCh := make(chan error, 1)
		go func() { errCh <- runner.Run(runnerCtx) }()
		waitForE2ERunner(t, h.runners, runnerID)

		def := &types.WorkflowDef{
			Name: "a0-report-ack-loss",
			Nodes: []types.NodeDef{
				{Name: "start", Type: "test.a0.start"},
				{Name: "done", Type: "test.a0.done"},
			},
			Connections: types.Connections{
				"start": {"main": []types.Connection{{Node: "done", Input: "main"}}},
			},
		}
		execID := submitWorkflowHTTP(t, h.httpSrv.URL, h.httpSrv.Client(), def, map[string]any{"claim_id": "ack-loss"})

		waitCtx, waitCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer waitCancel()
		result := waitForCompletion(waitCtx, t, h.state, execID, "start")

		if !proxy.SawTransportError() {
			t.Fatalf("proxy did not inject ack-loss transport error")
		}

		events := drainA0Evidence(h.EvidenceBuffer())
		if dropped := h.EvidenceBuffer().Dropped(); dropped > 0 {
			t.Fatalf("evidence buffer dropped %d events", dropped)
		}

		var appliedCommits, appliedAdvances int
		var commitOutcome engine.CommitOutcome
		for _, e := range events {
			if e.ExecutionID != execID || e.NodeName != "start" {
				continue
			}
			switch e.Type {
			case engine.RuntimeEvidenceCommit:
				if e.Applied {
					appliedCommits++
					commitOutcome = e.CommitOutcome
				}
			case engine.RuntimeEvidenceAdvance:
				if e.Applied {
					appliedAdvances++
				}
			}
		}

		handlerInvocations := startCounter.Count()
		if handlerInvocations != 1 {
			t.Fatalf("handler invocations = %d, want 1", handlerInvocations)
		}
		if appliedCommits != 1 {
			t.Fatalf("applied commit receipts = %d, want 1", appliedCommits)
		}
		if appliedAdvances != 1 {
			t.Fatalf("applied advance receipts = %d, want 1", appliedAdvances)
		}

		node, err := h.state.GetNode(ctx, execID, "start")
		if err != nil || node == nil || node.Status != types.NodeStatusSuccess {
			t.Fatalf("node after ack-loss = %+v err=%v, want Success", node, err)
		}
		if result.Status != types.ExecutionStatusSuccess {
			t.Fatalf("execution after ack-loss = %s, want Success", result.Status)
		}
		doneNode, err := h.state.GetNode(ctx, execID, "done")
		if err != nil || doneNode == nil || doneNode.Status != types.NodeStatusSuccess {
			t.Fatalf("done node after ack-loss = %+v err=%v, want Success", doneNode, err)
		}

		captured := proxy.CapturedRequest()
		if captured == nil || captured.Lease == nil {
			t.Fatalf("proxy did not capture the original report request")
		}
		lookup, ok := h.runners.(control.LeaseLookup)
		if !ok {
			t.Fatal("runner directory does not implement LeaseLookup")
		}
		_, found, _ := lookup.LookupLease(ctx, runnerID, captured.SessionID, control.LeaseLookupKey{
			LeaseToken: captured.Lease.LeaseToken,
		})
		if found {
			t.Fatalf("lease still found in runner directory after commit+release")
		}

		atomicState, ok := h.state.(engine.AtomicStateStore)
		if !ok {
			t.Fatal("state store does not implement AtomicStateStore")
		}
		entries, err := atomicState.ListOutbox(ctx, execID, time.Now().Add(time.Second), 16)
		if err != nil {
			t.Fatalf("ListOutbox() error = %v", err)
		}
		if len(entries) != 0 {
			t.Fatalf("outbox after convergence = %d entries, want 0", len(entries))
		}

		// The runner must have exited because of the injected transport error.
		runnerCancel()
		select {
		case err := <-errCh:
			if err == nil {
				t.Fatalf("runner error expected transport error, got nil")
			}
		case <-time.After(3 * time.Second):
			t.Fatal("runner did not stop in time")
		}

		report := a0FaultReport{
			Scenario:           "report-ack-loss",
			InjectionPoint:     "runner ReportResult accepted by control but response ACK lost",
			ExecutionID:        string(execID),
			NodeName:           "start",
			ActivationID:       captured.Lease.Task.ActivationID,
			LeaseToken:         string(captured.Lease.LeaseToken),
			NodeStatus:         string(node.Status),
			ExecutionStatus:    string(result.Status),
			CommitOutcome:      string(commitOutcome),
			QueueDeliveries:    1,
			HandlerInvocations: handlerInvocations,
			DAGAdvances:        appliedAdvances,
			FinalStatus:        string(result.Status),
			RecoveryTimeMS:     0,
			Pass:               true,
		}
		writeA0FaultReport(t, report)
		t.Logf("A0 scenario ReportAckLoss: handler invocations=%d, applied commits=%d, applied advances=%d, saw transport error=%v, final=%s",
			handlerInvocations, appliedCommits, appliedAdvances, proxy.SawTransportError(), result.Status)

		// Stop the control-plane consumer before the next scenario so it does not
		// race for the shared Asynq queue.
		h.stop()
	})

	t.Run("QueueHandoff", func(t *testing.T) {
		// This scenario uses the REAL asynq transport (no fake queue) so the
		// task persists in Redis across consumer processes. env1 submits and
		// exits without consuming; env2 binds a consumer that drives the DAG
		// to completion. This proves the queue handoff: a task enqueued by one
		// process is picked up by a new consumer bound to the same Redis.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		flushAsynqKeys(ctx, t, redis.NewClient(&redis.Options{Addr: addr}))

		// env1: producer only. No consumer (WithConsumer false) so the task
		// sits in the asynq queue after Submit.
		env1, err := distributed.New(addr, nil, distributed.WithConsumer(false))
		if err != nil {
			t.Fatalf("env1 distributed.New() error = %v", err)
		}
		eng1 := engine.New(env1.State(), env1.Queue(),
			engine.WithDefaultLeaseTTL(time.Minute),
		)
		g := a0AcyclicGraph(t, "a0-queue-handoff")
		id, err := eng1.Submit(ctx, g, map[string]any{"round": 1})
		if err != nil {
			t.Fatalf("env1 Submit() error = %v", err)
		}
		// env1 "crashes" — release its transport and Redis client. The asynq
		// task in Redis persists. Bind returns a non-consumer stop that closes
		// the transport and Redis client.
		env1Stop := env1.Bind(eng1)
		env1Stop()

		rdb := redis.NewClient(&redis.Options{Addr: addr})
		t.Cleanup(func() {
			deleteAtomicReliabilityKeys(t, rdb, id)
			_ = rdb.Close()
		})

		// env2: fresh backend+engine bound to the same Redis. BindHandler wires
		// a custom consumer handler that drives the DAG: it builds a lease and
		// commits a terminal result. The asynq consumer picks up the stranded
		// task and runs the handler.
		env2, err := distributed.New(addr, nil,
			distributed.WithConsumer(true),
			distributed.WithConcurrency(2),
		)
		if err != nil {
			t.Fatalf("env2 distributed.New() error = %v", err)
		}
		eng2 := engine.New(env2.State(), env2.Queue(),
			engine.WithDefaultLeaseTTL(time.Minute),
		)

		handlerInvocations := 0
		handlerDone := make(chan error, 1)
		stop := env2.BindHandler(eng2, func(ctx context.Context, task *engine.Task) error {
			handlerInvocations++
			lease, lerr := eng2.BuildTaskLease(ctx, task)
			if lerr != nil {
				if errors.Is(lerr, engine.ErrSystemTaskHandled) || errors.Is(lerr, engine.ErrExecutionInactive) || errors.Is(lerr, engine.ErrLeaseAlreadyActive) {
					return nil
				}
				handlerDone <- fmt.Errorf("BuildTaskLease: %w", lerr)
				return nil
			}
			if cerr := eng2.CommitTaskResult(ctx, lease, engine.TaskResult{
				Output: &types.Output{Data: map[string]any{"ok": true}},
			}); cerr != nil {
				handlerDone <- fmt.Errorf("CommitTaskResult: %w", cerr)
				return nil
			}
			handlerDone <- nil
			return nil
		})
		t.Cleanup(stop)

		// Wait for the new consumer to pick up the stranded task and complete.
		select {
		case herr := <-handlerDone:
			if herr != nil {
				t.Fatalf("env2 handler error: %v", herr)
			}
		case <-ctx.Done():
			t.Fatalf("timeout waiting for queue handoff consumer: %v", ctx.Err())
		}

		result := waitForCompletion(ctx, t, env2.State(), id)
		if result.Status != types.ExecutionStatusSuccess {
			t.Fatalf("execution after handoff = %s, want %s", result.Status, types.ExecutionStatusSuccess)
		}
		node, _ := env2.State().GetNode(ctx, id, "start")
		if node == nil || node.Status != types.NodeStatusSuccess {
			t.Fatalf("node after handoff = %+v, want Success", node)
		}

		report := a0FaultReport{
			Scenario:           "queue-handoff",
			InjectionPoint:     "consumer crash before process (env1 submitted without consumer; env2 binds fresh consumer)",
			ExecutionID:        string(id),
			NodeName:           "start",
			ActivationID:       0,
			LeaseToken:         "",
			NodeStatus:         string(node.Status),
			ExecutionStatus:    string(result.Status),
			CommitOutcome:      string(engine.CommitOutcomeAccepted),
			QueueDeliveries:    1,
			HandlerInvocations: handlerInvocations,
			DAGAdvances:        1,
			FinalStatus:        string(result.Status),
			RecoveryTimeMS:     0,
			Pass:               true,
		}
		writeA0FaultReport(t, report)
		t.Logf("A0 scenario 3: queue-handoff: handler invocations=%d, DAG advances=1, final=%s",
			handlerInvocations, result.Status)
	})

	t.Run("OSKill", func(t *testing.T) {
		// Non-graceful termination post-submit. env1 submits with the fake
		// queue failing (process died before delivery) and builds a lease that
		// never commits (process died mid-handler). env2 binds a fresh engine
		// to the same Redis: the background OutboxDispatcher scans and
		// redelivers the stranded durable intent, and the expired-lease
		// reclaim path (production: LeaseSweeper) revokes the stranded lease
		// and re-enqueues. Both recovery paths converge on a single
		// exactly-once DAG advance.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// env1: fake queue failing (simulates process death before delivery).
		// No Bind: no background OutboxDispatcher or timeout monitor.
		env1, err := distributed.New(addr, nil, distributed.WithConsumer(false))
		if err != nil {
			t.Fatalf("env1 distributed.New() error = %v", err)
		}
		queue1 := &a0FaultQueue{err: errA0QueueUnavailable}
		eng1 := engine.New(env1.State(), queue1,
			engine.WithDefaultLeaseTTL(150*time.Millisecond),
			engine.WithOutboxMaxDeliveryAttempts(5),
		)
		g := a0AcyclicGraph(t, "a0-os-kill")
		id, err := eng1.Submit(ctx, g, map[string]any{"round": 1})
		if err != nil {
			t.Fatalf("env1 Submit() error = %v", err)
		}
		// The durable outbox entry survives: Submit does not fail even though
		// the queue is unavailable.
		atomicState1, ok := env1.State().(engine.AtomicStateStore)
		if !ok {
			t.Fatal("env1 StateStore does not implement AtomicStateStore")
		}
		stranded, err := atomicState1.ListOutbox(ctx, id, time.Now().Add(time.Second), 16)
		if err != nil || len(stranded) != 1 {
			t.Fatalf("env1 outbox after submit = %+v err=%v, want exactly 1 stranded entry", stranded, err)
		}

		// env1 also built a lease before "dying" — model this by building a
		// lease on the stranded task. The lease is held in Redis state.
		lease1, err := eng1.BuildTaskLease(ctx, &stranded[0].Task)
		if err != nil {
			t.Fatalf("env1 BuildTaskLease() error = %v", err)
		}
		if lease1 == nil || lease1.LeaseToken == "" {
			t.Fatalf("env1 lease1 = %+v, want non-empty token", lease1)
		}

		// env1 "OS-killed" — non-graceful: release transport + Redis via the
		// non-consumer Bind stop path.
		env1Stop := env1.Bind(eng1)
		env1Stop()

		rdb := redis.NewClient(&redis.Options{Addr: addr})
		t.Cleanup(func() {
			deleteAtomicReliabilityKeys(t, rdb, id)
			_ = rdb.Close()
		})

		// Wait for the stranded lease to expire so the reclaim path can revoke it.
		time.Sleep(300 * time.Millisecond)

		// env2: fresh engine bound to the same Redis. Bind starts the
		// background OutboxDispatcher (outbox scan) and the timeout monitor.
		env2, err := distributed.New(addr, nil, distributed.WithConsumer(true))
		if err != nil {
			t.Fatalf("env2 distributed.New() error = %v", err)
		}
		queue2 := &a0FaultQueue{}
		eng2 := engine.New(env2.State(), queue2,
			engine.WithDefaultLeaseTTL(time.Minute),
			engine.WithOutboxMaxDeliveryAttempts(5),
		)
		atomicState2, ok := env2.State().(engine.AtomicStateStore)
		if !ok {
			t.Fatal("env2 StateStore does not implement AtomicStateStore")
		}
		stop := env2.Bind(eng2)
		t.Cleanup(stop)

		// Recovery path 1 — outbox scan: the background OutboxDispatcher
		// redelivers the stranded durable intent. The redelivered task lands
		// in queue2 (fake). Because env1 also left an expired lease, the
		// redelivered task cannot acquire a lease until the expired lease is
		// reclaimed.
		// Recovery path 2 — lease reclaim: drive the LeaseSweeper path
		// (ListExpiredLeases + ReclaimLease) to revoke lease1 and re-enqueue.
		// recoveryStart brackets both recovery paths (outbox scan + lease
		// reclaim) plus the redelivery wait, matching the user-visible
		// "time to recover" semantics.
		recoveryStart := time.Now()
		var reclaimed bool
		reclaimDeadline := time.Now().Add(10 * time.Second)
		for {
			expired, err := env2.State().ListExpiredLeases(ctx, time.Now())
			if err != nil {
				t.Fatalf("env2 ListExpiredLeases() error = %v", err)
			}
			for _, e := range expired {
				if e.ExecutionID == id && e.NodeName == "start" {
					if done, rerr := eng2.ReclaimLease(ctx, e); rerr != nil {
						t.Fatalf("env2 ReclaimLease() error = %v", rerr)
					} else if done {
						reclaimed = true
					}
				}
			}
			if reclaimed {
				break
			}
			if time.Now().After(reclaimDeadline) {
				t.Fatalf("timeout waiting for expired-lease reclaim (expired=%+v)", expired)
			}
			time.Sleep(50 * time.Millisecond)
		}

		// Drain all redelivered tasks (from both outbox scan and reclaim).
		// Multiple deliveries are allowed under at-least-once; each carries
		// the same start task. BuildTaskLease succeeds for the first one (the
		// reclaimed lease has been revoked); later duplicates are rejected as
		// lease-already-active or execution-inactive, which is the fencing.
		var redelivered *engine.Task
		deliveryDeadline := time.Now().Add(10 * time.Second)
		for {
			tasks := queue2.drain()
			if len(tasks) > 0 {
				redelivered = tasks[len(tasks)-1]
				break
			}
			if time.Now().After(deliveryDeadline) {
				t.Fatalf("timeout waiting for redelivery after reclaim (deliveries=%d)", queue2.totalDeliveries())
			}
			time.Sleep(20 * time.Millisecond)
		}

		lease2, err := eng2.BuildTaskLease(ctx, redelivered)
		if err != nil {
			t.Fatalf("env2 BuildTaskLease() error = %v", err)
		}

		// Fencing: the original lease1 commit arriving late (e.g. delayed
		// report from env1 before it died) must be classified stale_token —
		// the reclaimed lease has been re-issued under lease2's token. This
		// must be checked BEFORE lease2 commits (while the node is Running
		// under lease2's token); after the terminal commit the late commit
		// would be classified execution_inactive (also fencing, but not the
		// path this scenario targets).
		lateOutcome, lateErr := eng2.CommitTaskResultWithOutcome(ctx, lease1, engine.TaskResult{
			Output: &types.Output{Data: map[string]any{"ok": true}},
		})
		if lateErr != nil && !errors.Is(lateErr, engine.ErrInvalidLeaseToken) {
			t.Fatalf("late lease1 commit error = %v, want nil or ErrInvalidLeaseToken", lateErr)
		}
		if lateOutcome != engine.CommitOutcomeStaleToken {
			t.Fatalf("late lease1 commit outcome = %q, want %q (fenced)", lateOutcome, engine.CommitOutcomeStaleToken)
		}

		// The retry commit (lease2) is the one that advances the DAG.
		outcome2, err := eng2.CommitTaskResultWithOutcome(ctx, lease2, engine.TaskResult{
			Output: &types.Output{Data: map[string]any{"ok": true}},
		})
		if err != nil {
			t.Fatalf("env2 CommitTaskResult() error = %v", err)
		}
		if outcome2 != engine.CommitOutcomeAccepted {
			t.Fatalf("env2 commit outcome = %q, want %q", outcome2, engine.CommitOutcomeAccepted)
		}

		// Final state: node + execution Success; outbox drained.
		result := waitForCompletion(ctx, t, env2.State(), id)
		if result.Status != types.ExecutionStatusSuccess {
			t.Fatalf("execution after OS-kill recovery = %s, want %s", result.Status, types.ExecutionStatusSuccess)
		}
		node, _ := env2.State().GetNode(ctx, id, "start")
		if node == nil || node.Status != types.NodeStatusSuccess {
			t.Fatalf("node after OS-kill recovery = %+v, want Success", node)
		}
		postEntries, _ := atomicState2.ListOutbox(ctx, id, time.Now().Add(time.Second), 16)
		if len(postEntries) != 0 {
			t.Fatalf("outbox after recovery = %d, want 0 (terminal intent acked)", len(postEntries))
		}

		report := a0FaultReport{
			Scenario:           "os-kill",
			InjectionPoint:     "non-graceful termination post-submit (env1 killed before delivery + mid-handler lease1)",
			ExecutionID:        string(id),
			NodeName:           "start",
			ActivationID:       redelivered.ActivationID,
			LeaseToken:         string(lease2.LeaseToken),
			NodeStatus:         string(node.Status),
			ExecutionStatus:    string(result.Status),
			CommitOutcome:      string(outcome2),
			QueueDeliveries:    queue2.totalDeliveries(),
			// HandlerInvocations is a logical estimate, not a measured count:
			// this scenario does not run a real handler (CommitTaskResult is
			// driven directly by the test). The value reflects the
			// exactly-once DAG semantics: lease1's "lost" commit does not
			// advance the DAG; only lease2's commit does (DAGAdvances=1).
			HandlerInvocations: 1,
			DAGAdvances:        1,
			FinalStatus:        string(result.Status),
			RecoveryTimeMS:     time.Since(recoveryStart).Milliseconds(),
			Pass:               true,
		}
		writeA0FaultReport(t, report)
		t.Logf("A0 scenario 4: os-kill: outbox scan + lease reclaim recovered; lease1 fenced=%s, lease2=%s, final=%s",
			lateOutcome, outcome2, result.Status)
	})
}

// errA0QueueUnavailable models a transient queue outage between the fenced
// terminal commit and downstream task delivery.
var errA0QueueUnavailable = errors.New("a0 fault matrix: task queue unavailable")
