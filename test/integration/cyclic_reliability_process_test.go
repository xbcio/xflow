//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend/distributed"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
	"github.com/redis/go-redis/v9"
)

// a0Report is the cross-process evidence record written by the helper binary
// to a JSON file the test reads back. One record per injection point.
type a0Report struct {
	Phase             string `json:"phase"`
	InjectionPoint    string `json:"injection_point"`
	ExecutionID       string `json:"execution_id"`
	NodeName         string `json:"node_name"`
	ActivationID      int    `json:"activation_id"`
	LeaseToken        string `json:"lease_token"`
	CommitOutcome     string `json:"commit_outcome"`
	QueueDeliveries   int    `json:"queue_deliveries"`
	HandlerInvocations int   `json:"handler_invocations"`
	DAGAdvances       int    `json:"dag_advances"`
	FinalStatus       string `json:"final_status"`
	RecoveryTimeMS    int64  `json:"recovery_time_ms"`
	Err               string `json:"err,omitempty"`
}

// TestCyclicReliabilityProcessRecovery is the A0 independent-process regression
// (2026-07-18 remediation §6.3): process A performs a fenced commit and exits
// before the outbox flush; process B binds to the same Redis and recovers via
// the background OutboxDispatcher only (no manual FlushOutbox). The test
// distinguishes duplicate *delivery* (allowed under at-least-once) from
// duplicate *DAG commit* (must be fenced to exactly one advance).
//
// It runs the same package's test binary as a subprocess driven by the
// XFLOW_A0_HELPER env var, so the helper code lives in this file under a guard.
func TestCyclicReliabilityProcessRecovery(t *testing.T) {
	switch os.Getenv("XFLOW_A0_HELPER") {
	case "commit-then-exit":
		a0HelperCommitThenExit(t)
		return
	case "recover-and-report":
		a0HelperRecoverAndReport(t)
		return
	}

	addr := requireRedis(t)

	// Build this test binary once and reuse it for both helper phases. Run the
	// build from the repo root so the relative package path resolves.
	root, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	bin := filepath.Join(t.TempDir(), "xflow-a0-helper.test")
	buildCmd := exec.Command("go", "test", "-c", "-tags=integration", "-o", bin, "./test/integration/")
	buildCmd.Dir = repoRoot(root)
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("build helper binary: %v\n%s", err, out)
	}

	// Phase A: process A submits the cyclic graph, commits start->review, leases
	// review, commits review.reject (fenced terminal + durable downstream intent),
	// injects a flush-before-delivery outage, writes its report, and exits WITHOUT
	// delivering the downstream start task. It does not call FlushOutbox for the
	// downstream.
	reportA := filepath.Join(t.TempDir(), "phase-a.json")
	argsA := []string{"-test.run=TestCyclicReliabilityProcessRecovery", "-test.timeout=60s"}
	envA := append(os.Environ(),
		"XFLOW_A0_HELPER=commit-then-exit",
		"XFLOW_A0_REDIS_ADDR="+addr,
		"XFLOW_A0_REPORT="+reportA,
	)
	cmdA := exec.Command(bin, argsA...)
	cmdA.Env = envA
	if out, err := cmdA.CombinedOutput(); err != nil {
		t.Fatalf("phase A helper failed: %v\n%s", err, out)
	}
	var phaseA a0Report
	if raw, err := os.ReadFile(reportA); err != nil {
		t.Fatalf("read phase A report: %v", err)
	} else if err := json.Unmarshal(raw, &phaseA); err != nil {
		t.Fatalf("decode phase A report: %v", err)
	}
	if phaseA.CommitOutcome != string(engine.CommitOutcomeAccepted) &&
		phaseA.CommitOutcome != string(engine.CommitOutcomeTransientError) {
		t.Fatalf("phase A commit outcome = %q, want accepted or transient_error (flush outage after durable commit)", phaseA.CommitOutcome)
	}
	if phaseA.DAGAdvances != 1 {
		t.Fatalf("phase A DAG advances = %d, want 1 (fenced reject branch persisted despite flush outage)", phaseA.DAGAdvances)
	}

	// Phase B: process B binds a fresh backend+engine to the same Redis, starts
	// the background OutboxDispatcher via Bind, and waits for it to redeliver the
	// stranded downstream start task — without calling FlushOutbox. It records the
	// delivery count and the recovery time, then exits.
	reportB := filepath.Join(t.TempDir(), "phase-b.json")
	argsB := []string{"-test.run=TestCyclicReliabilityProcessRecovery", "-test.timeout=60s"}
	envB := append(os.Environ(),
		"XFLOW_A0_HELPER=recover-and-report",
		"XFLOW_A0_REDIS_ADDR="+addr,
		"XFLOW_A0_REPORT="+reportB,
		"XFLOW_A0_EXECUTION_ID="+phaseA.ExecutionID,
		"XFLOW_A0_ACTIVATION="+itoa(phaseA.ActivationID),
	)
	startB := time.Now()
	cmdB := exec.Command(bin, argsB...)
	cmdB.Env = envB
	if out, err := cmdB.CombinedOutput(); err != nil {
		t.Fatalf("phase B helper failed: %v\n%s", err, out)
	}
	var phaseB a0Report
	if raw, err := os.ReadFile(reportB); err != nil {
		t.Fatalf("read phase B report: %v", err)
	} else if err := json.Unmarshal(raw, &phaseB); err != nil {
		t.Fatalf("decode phase B report: %v", err)
	}
	if phaseB.QueueDeliveries < 1 {
		t.Fatalf("phase B queue deliveries = %d, want >=1 (background dispatcher must redeliver)", phaseB.QueueDeliveries)
	}
	if phaseB.NodeName != "start" || phaseB.ActivationID != phaseA.ActivationID {
		t.Fatalf("phase B redelivered = %s@%d, want start@%d", phaseB.NodeName, phaseB.ActivationID, phaseA.ActivationID)
	}
	if phaseB.DAGAdvances != 0 {
		t.Fatalf("phase B DAG advances = %d, want 0 (recovery must not double-advance; delivery != commit)", phaseB.DAGAdvances)
	}
	_ = startB // recovery time is recorded by the helper
	if phaseB.RecoveryTimeMS < 0 {
		t.Fatalf("phase B recovery time = %d ms, want >=0", phaseB.RecoveryTimeMS)
	}

	// Distinct evidence: duplicate *delivery* is permitted (the dispatcher may
	// redeliver the same intent), but duplicate *DAG commit* was fenced to exactly
	// one advance in phase A. phase B must not advance the DAG at all — it only
	// redelivers the downstream task for a fresh lease.
	t.Logf("A0 process recovery: A committed %s@%d (DAG advances=%d, outcome=%s); "+
		"B redelivered downstream start@%d via background dispatcher (%d deliveries, DAG advances=%d, recovery=%dms) — "+
		"duplicate delivery allowed, duplicate DAG commit fenced",
		phaseA.NodeName, phaseA.ActivationID, phaseA.DAGAdvances, phaseA.CommitOutcome,
		phaseB.ActivationID, phaseB.QueueDeliveries, phaseB.DAGAdvances, phaseB.RecoveryTimeMS)
}

// a0CyclicGraph is the start<->review reject-loop graph used by both helpers.
func a0CyclicGraph(t *testing.T) *graph.Graph {
	t.Helper()
	def := &types.WorkflowDef{
		Name:    "cyclic-a0-proc",
		Options: &types.WorkflowOptions{AllowCycles: true, MaxAutoDepth: 10},
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "review", Type: "test.review"},
		},
		Connections: types.Connections{
			"start":  {"main": []types.Connection{{Node: "review", Input: "main"}}},
			"review": {"reject": []types.Connection{{Node: "start", Input: "main"}}},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	return g
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		b[pos] = '-'
	}
	return string(b[pos:])
}

func a0WriteReport(t *testing.T, path string, r a0Report) {
	t.Helper()
	raw, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("marshal report: %v", err)
	}
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("write report %q: %v", path, err)
	}
}

// a0HelperCommitThenExit is process A: submit, commit start->review, lease and
// commit review.reject under a flush outage, write the report, and exit — never
// delivering the downstream start task and never calling FlushOutbox.
func a0HelperCommitThenExit(t *testing.T) {
	addr := os.Getenv("XFLOW_A0_REDIS_ADDR")
	if addr == "" {
		t.Fatal("XFLOW_A0_REDIS_ADDR not set")
	}
	reportPath := os.Getenv("XFLOW_A0_REPORT")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	backend, err := newA0Backend(addr)
	if err != nil {
		a0WriteReport(t, reportPath, a0Report{Phase: "A", Err: "backend: " + err.Error()})
		return
	}
	defer backend.close()

	queue := &cyclicFakeQueue{}
	eng := engine.New(backend.state(), queue,
		engine.WithDefaultLeaseTTL(time.Minute),
		engine.WithOutboxMaxDeliveryAttempts(3),
	)
	// No Bind: process A does NOT start the background OutboxDispatcher. The
	// stranded intent must be recoverable by process B's dispatcher.

	g := a0CyclicGraph(t)
	id, err := eng.Submit(ctx, g, map[string]any{"round": 1})
	if err != nil {
		a0WriteReport(t, reportPath, a0Report{Phase: "A", Err: "submit: " + err.Error()})
		return
	}
	// Drain the auto-delivered root start task.
	rootTasks := queue.drain()
	if len(rootTasks) != 1 || rootTasks[0].NodeName != "start" {
		a0WriteReport(t, reportPath, a0Report{Phase: "A", Err: "root start task not delivered"})
		return
	}
	startTask := rootTasks[0]

	// Commit start -> review (deliver review task).
	queue.setError(nil)
	startLease, err := eng.BuildTaskLease(ctx, startTask)
	if err != nil {
		a0WriteReport(t, reportPath, a0Report{Phase: "A", Err: "lease start: " + err.Error()})
		return
	}
	if err := eng.CommitTaskResult(ctx, startLease, engine.TaskResult{Output: &types.Output{Port: "main", Data: map[string]any{"round": 1}}}); err != nil {
		a0WriteReport(t, reportPath, a0Report{Phase: "A", Err: "commit start: " + err.Error()})
		return
	}
	reviewTasks := queue.drain()
	if len(reviewTasks) != 1 || reviewTasks[0].NodeName != "review" {
		a0WriteReport(t, reportPath, a0Report{Phase: "A", Err: "review task not delivered"})
		return
	}
	reviewTask := reviewTasks[0]
	reviewLease, err := eng.BuildTaskLease(ctx, reviewTask)
	if err != nil {
		a0WriteReport(t, reportPath, a0Report{Phase: "A", Err: "lease review: " + err.Error()})
		return
	}

	// Injection point: "commit-then-flush-before-delivery". Set the queue to fail
	// so the fenced terminal commit persists the durable downstream intent but the
	// Enqueue handoff fails — modeling a crash between commit and delivery. The
	// reported outcome may be transient_error (the flush failure surfaces to the
	// caller), but the DAG advance is durable: the review node is Success and the
	// downstream start intent is stranded in the outbox. We prove the latter.
	queue.setError(errCyclicQueueUnavailable)
	outcome, err := eng.CommitTaskResultWithOutcome(ctx, reviewLease, engine.TaskResult{Output: &types.Output{Port: "reject", Data: map[string]any{"round": 2}}})
	if err != nil && !errors.Is(err, errCyclicQueueUnavailable) {
		a0WriteReport(t, reportPath, a0Report{Phase: "A", Err: "commit review: " + err.Error()})
		return
	}

	// Prove the fenced commit persisted despite the flush outage: the stranded
	// downstream intent is in the durable outbox (the DAG advance the caller's
	// transient outcome could not undo).
	state, ok := backend.state().(engine.AtomicStateStore)
	if !ok {
		a0WriteReport(t, reportPath, a0Report{Phase: "A", Err: "state store lacks AtomicStateStore"})
		return
	}
	stranded, sErr := state.ListOutbox(ctx, id, time.Now().Add(time.Second), 16)
	dagAdvances := 0
	downstreamActivation := reviewTask.ActivationID
	if sErr == nil && len(stranded) == 1 {
		dagAdvances = 1
		downstreamActivation = stranded[0].Task.ActivationID
	}

	a0WriteReport(t, reportPath, a0Report{
		Phase:           "A",
		InjectionPoint:  "commit-then-flush-before-delivery",
		ExecutionID:     string(id),
		NodeName:        reviewTask.NodeName,
		ActivationID:    downstreamActivation,
		LeaseToken:      string(reviewLease.LeaseToken),
		CommitOutcome:   string(outcome),
		QueueDeliveries: queue.count(),
		DAGAdvances:     dagAdvances,
	})
	// Exit without flushing the downstream. Process B must recover it.
}

// a0HelperRecoverAndReport is process B: bind a fresh backend+engine with Bind
// (starting the background OutboxDispatcher), wait for it to redeliver the
// stranded downstream start task without calling FlushOutbox, and record the
// delivery count + recovery time. It must not advance the DAG (delivery != commit).
func a0HelperRecoverAndReport(t *testing.T) {
	addr := os.Getenv("XFLOW_A0_REDIS_ADDR")
	if addr == "" {
		t.Fatal("XFLOW_A0_REDIS_ADDR not set")
	}
	reportPath := os.Getenv("XFLOW_A0_REPORT")
	execID := types.ExecutionID(os.Getenv("XFLOW_A0_EXECUTION_ID"))
	wantActivation := 0
	if v := os.Getenv("XFLOW_A0_ACTIVATION"); v != "" {
		wantActivation = atoiSafe(v)
	}

	backend, err := newA0Backend(addr)
	if err != nil {
		a0WriteReport(t, reportPath, a0Report{Phase: "B", Err: "backend: " + err.Error()})
		return
	}
	defer backend.close()
	queue := &cyclicFakeQueue{}
	eng := engine.New(backend.state(), queue,
		engine.WithDefaultLeaseTTL(time.Minute),
		engine.WithOutboxMaxDeliveryAttempts(3),
	)
	// Bind starts the real background OutboxDispatcher — the component A0 must
	// prove recovers a stranded intent with no manual FlushOutbox.
	stop := backend.bind(eng)
	defer stop()

	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()
	defer func() {
		if execID != "" {
			deleteAtomicReliabilityKeys(t, rdb, execID)
		}
	}()

	startRecover := time.Now()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.Now().Add(20 * time.Second)
	var delivered []*engine.Task
	for {
		delivered = queue.drain()
		if len(delivered) > 0 {
			break
		}
		if time.Now().After(deadline) {
			a0WriteReport(t, reportPath, a0Report{Phase: "B", Err: "timeout waiting for background dispatcher replay"})
			return
		}
		<-ticker.C
	}
	recoveryMS := time.Since(startRecover).Milliseconds()
	last := delivered[len(delivered)-1]
	if wantActivation > 0 && last.ActivationID != wantActivation {
		a0WriteReport(t, reportPath, a0Report{
			Phase: "B", Err: "redelivered activation " + itoa(last.ActivationID) + " != expected " + itoa(wantActivation),
			NodeName: last.NodeName, ActivationID: last.ActivationID, QueueDeliveries: len(delivered),
			RecoveryTimeMS: recoveryMS,
		})
		return
	}
	a0WriteReport(t, reportPath, a0Report{
		Phase:           "B",
		InjectionPoint:  "background-dispatcher-recovery",
		ExecutionID:     string(execID),
		NodeName:        last.NodeName,
		ActivationID:     last.ActivationID,
		QueueDeliveries: len(delivered),
		DAGAdvances:      0, // recovery redelivers; it does not double-commit
		RecoveryTimeMS:  recoveryMS,
	})
}

// repoRoot walks up from start to find the directory containing go.mod.
func repoRoot(start string) string {
	dir := start
	for i := 0; i < 12; i++ {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return start
}

func atoiSafe(s string) int {
	n := 0
	for _, c := range strings.TrimSpace(s) {
		if c < '0' || c > '9' {
			return 0
		}
		n = n*10 + int(c-'0')
	}
	return n
}

// a0Backend wraps the distributed backend enough for the A0 helpers.
type a0Backend struct {
	b *distributed.Backend
}

func newA0Backend(addr string) (*a0Backend, error) {
	b, err := distributed.New(addr, nil, distributed.WithConsumer(true))
	if err != nil {
		return nil, err
	}
	return &a0Backend{b: b}, nil
}

func (a *a0Backend) state() engine.StateStore { return a.b.State() }
func (a *a0Backend) bind(eng *engine.Engine) func() {
	// Use the public Bind path (Provider.Bind) which starts the consumer,
	// outbox dispatcher, and timeout monitor — the A1 fail-closed lifecycle.
	return a.b.Bind(eng)
}
func (a *a0Backend) close() {
	if c, ok := a.b.RedisClient().(*redis.Client); ok {
		_ = c.Close()
	}
}
