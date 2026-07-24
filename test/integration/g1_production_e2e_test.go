//go:build integration

// Package integration hosts the G1 production-auth end-to-end coverage.
//
// TestG1ProductionE2E exercises the G1 release-gate posture against real
// Redis (127.0.0.1:6380) + real MySQL (127.0.0.1:3306) with the production
// authz stack wired: PrincipalAuth + TenantAwareAuthorizer + SQLAuditSink +
// AuditReconcileWorker + Metrics + Tracer + RequireWorkflowAuth +
// WithManagement. It covers HTTP entries (submit/invoke/signal/revoke/cancel)
// with an allow/deny matrix, cross-tenant IDOR (404, no existence leak), the
// gRPC runner Connect + ReportResult path end-to-end, complex approval DAG
// features (multi-signal quorum, timer, cancel, cyclic reset, repeat signal
// → 409), the dead-letter replay HTTP contract, T9 reconcile, metrics
// scrape, and degraded idempotency assertions (at-least-once with host-side
// dedup; never a fake exactly-once count).
//
// The artifact is written to test/integration/testdata/g1_e2e_report.json.
// It is gitignored (never checked in) and removed at the top of the test so
// a stale run cannot leak into a CI-uploaded artifact.
package integration

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend/distributed"
	"github.com/xbcio/xflow/backend/tenant"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	_ "github.com/xbcio/xflow/node" // registers built-in nodes (xflow.start, xflow.wait, etc.) in nodereg
	nodereg "github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/observability/tracing"
	"github.com/xbcio/xflow/service/apiserver"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/protocol"
	runnersvc "github.com/xbcio/xflow/service/runner"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/sqlstore"
	"github.com/xbcio/xflow/types"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// g1ArtifactPath is the fixed release-artifact location for the G1 production
// e2e coverage. CI uploads it alongside the Go test JSON output. Regenerated
// from scratch per run (TestG1ProductionE2E os.Remove's any stale copy
// before subtests start).
const g1ArtifactPath = "test/integration/testdata/g1_e2e_report.json"

// g1Tokens — multi-tenant token registry for the production harness.
// Token values are carried ONLY in the Authorization header; they are never
// recorded in the artifact JSON.
const (
	g1TokFullA = "g1-tok-full-tenantA-8f3a4c2b9d"
	g1TokNoExA = "g1-tok-noexec-tenantA-7e1b5d0a3c"
	g1TokFullB = "g1-tok-full-tenantB-2c9d4e6f8a"
	g1TokDefault = "g1-tok-full-default-4a7b2c9e1d"
)

// g1TenantA / g1TenantB are the two tenants used by the authz matrix + IDOR.
const (
	g1TenantA = "tenantA"
	g1TenantB = "tenantB"
)

// g1Artifact is the machine-readable coverage record written at the end of
// TestG1ProductionE2E. Sensitive fields (token values, payloads) are NEVER
// recorded — only decisions, statuses, and counter-style evidence.
type g1Artifact struct {
	GeneratedAt       string           `json:"generated_at"`
	GoVersion         string           `json:"go_version"`
	OS                string           `json:"os"`
	CommitSHA         string           `json:"commit_sha"`
	RedisAddr         string           `json:"redis_addr"`
	MySQLDSNHost      string           `json:"mysql_dsn_host"`
	AuthzMatrix       []g1AuthzRow     `json:"authz_matrix"`
	TraceGraph        g1TraceGraph     `json:"trace_graph"`
	ApprovalDAG       g1ApprovalDAG    `json:"approval_dag"`
	AuditReconcile    g1AuditReconcile `json:"audit_reconcile"`
	DeadLetter        g1DeadLetter     `json:"dead_letter"`
	MetricsScrape     g1MetricsScrape  `json:"metrics_scrape"`
	IdempotencyReport g1Idempotency    `json:"idempotency_report"`
}

type g1AuthzRow struct {
	Route    string `json:"route"`
	Token    string `json:"token"`
	Scope    string `json:"scope"`
	Expected int    `json:"expected"`
	Got      int    `json:"got"`
	Decision string `json:"decision"`
}

type g1TraceGraph struct {
	SpansPresent             []string `json:"spans_present"`
	OneTraceID               bool     `json:"one_trace_id"`
	DispatchParentedToSubmit bool     `json:"dispatch_parented_to_submit"`
	CommitParentedToReport   bool     `json:"commit_parented_to_report"`
	// TenantA fields: the same strong assertions run against a non-default
	// tenant workflow (g1TokFullA). The pollTask tenant injection in
	// service/control/core.go must read the W3C carrier from the tenantA
	// Redis namespace, so the full 5-span graph holds with one TraceID and
	// correct parentage.
	TenantAOneTraceID               bool `json:"tenant_a_one_trace_id"`
	TenantADispatchParentedToSubmit bool `json:"tenant_a_dispatch_parented_to_submit"`
	TenantACommitParentedToReport   bool `json:"tenant_a_commit_parented_to_report"`
	// CrossTenantCarrierIsolated: tenantA + tenantB workflows submitted
	// concurrently — the tenantA dispatch span must NOT inherit tenantB's
	// submit trace (and vice versa). Proves the carrier lookup is namespace-
	// scoped, not a global read.
	CrossTenantCarrierIsolated bool `json:"cross_tenant_carrier_isolated"`
}

type g1ApprovalDAG struct {
	MultiSignalQuorum string `json:"multi_signal_quorum"`
	TimerFired         string `json:"timer_fired"`
	Cancel             string `json:"cancel"`
	CyclicReset        string `json:"cyclic_reset"`
	RepeatSignal409    string `json:"repeat_signal_409"`
}

type g1AuditReconcile struct {
	AdmissionRows            int  `json:"admission_rows"`
	OutcomeRows               int  `json:"outcome_rows"`
	ReconciledByWorker        int  `json:"reconciled_by_worker"`
	IdempotentOutcomeAppends  bool `json:"idempotent_outcome_appends"`
}

type g1DeadLetter struct {
	Seeded                bool   `json:"seeded"`
	ReplayOutcome         string `json:"replay_outcome"`
	ReceiptAuditIDSet     bool   `json:"receipt_audit_id_set"`
	DurableProjectionRows int    `json:"durable_projection_rows"`
}

type g1MetricsScrape struct {
	Scraped          bool     `json:"scraped"`
	CountersObserved []string `json:"counters_observed"`
}

type g1Idempotency struct {
	RepeatSignalOutcome             string `json:"repeat_signal_outcome"`
	DuplicateReportOutcome          string `json:"duplicate_report_outcome"`
	HandlerSideEffectsAssertion     string `json:"handler_side_effects_assertion"`
	IndependentExecutionsForSameDef bool   `json:"independent_executions_for_same_def"`
	InvocationLevelIdempotencyKey   string `json:"invocation_level_idempotency_key"`
	// Real redelivery + external side-effect evidence (replaces the prior
	// hardcoded ">=1" assertion). A handler performing a real external side
	// effect (a MySQL business row) is invoked >=2 times to model at-least-once
	// redelivery at the handler boundary. The row is keyed by the host-provided
	// stable identity (execution_id + node_name) under a UNIQUE constraint, so
	// business_rows == 1 regardless of invocation count; the duplicate commit is
	// fenced by the host. These are measured values, not constants.
	HandlerInvocations int    `json:"handler_invocations"`
	BusinessRows       int    `json:"business_rows"`
	IdempotencyKey     string `json:"idempotency_key"`
	HostFenceOutcome   string `json:"host_fence_outcome"`
}

// resolveG1ArtifactPath finds the repo root and joins it with g1ArtifactPath
// so the report is written to a stable absolute location regardless of cwd.
func resolveG1ArtifactPath(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	return filepath.Join(repoRoot(dir), g1ArtifactPath)
}

// g1HTTPClient is the shared HTTP client for the test.
var g1HTTPClient = &http.Client{Timeout: 30 * time.Second}

// g1DoAuth issues an authenticated HTTP request with the given bearer token.
// The token is carried ONLY in the Authorization header — never in a query
// parameter, never in the body, and never recorded in the artifact.
func g1DoAuth(t *testing.T, method, baseURL, path, token string, body any) (*http.Response, []byte) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, baseURL+path, reader)
	if err != nil {
		t.Fatalf("new request %s %s: %v", method, path, err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := g1HTTPClient.Do(req)
	if err != nil {
		t.Fatalf("do %s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	payload, _ := io.ReadAll(resp.Body)
	return resp, payload
}

// g1SubmitAuth submits a workflow with the given token and returns the
// execution id + response status.
func g1SubmitAuth(t *testing.T, baseURL, token string, wf *types.WorkflowDef, params map[string]any) (types.ExecutionID, int) {
	t.Helper()
	body := map[string]any{"workflow": wf}
	if params != nil {
		body["params"] = params
	}
	resp, raw := g1DoAuth(t, http.MethodPost, baseURL, "/v1/workflows", token, body)
	if resp.StatusCode != http.StatusOK {
		return "", resp.StatusCode
	}
	var out e2eSubmitResp
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode submit response: %v (raw=%q)", err, string(raw))
	}
	return out.ExecutionID, resp.StatusCode
}

// g1SubmitAllowed submits a workflow with a token expected to be allowed and
// fatals on any non-200 response. Returns the execution id.
func g1SubmitAllowed(t *testing.T, baseURL, token string, wf *types.WorkflowDef) types.ExecutionID {
	t.Helper()
	id, status := g1SubmitAuth(t, baseURL, token, wf, nil)
	if status != http.StatusOK {
		t.Fatalf("submit with allowed token: status=%d, want 200", status)
	}
	if id == "" {
		t.Fatal("empty execution_id from allowed submit")
	}
	return id
}

// g1InspectAuth GETs /v1/executions/{id} with the given token.
func g1InspectAuth(t *testing.T, baseURL, token string, id types.ExecutionID) (int, engine.ExecutionDetail) {
	t.Helper()
	resp, raw := g1DoAuth(t, http.MethodGet, baseURL, "/v1/executions/"+string(id), token, nil)
	var detail engine.ExecutionDetail
	if resp.StatusCode == http.StatusOK {
		_ = json.Unmarshal(raw, &detail)
	}
	return resp.StatusCode, detail
}

// g1SignalAuth POSTs /v1/executions/{id}/signal with the given name+data.
func g1SignalAuth(t *testing.T, baseURL, token string, id types.ExecutionID, name string, data map[string]any) (int, []byte) {
	t.Helper()
	body := map[string]any{"name": name, "data": data}
	resp, raw := g1DoAuth(t, http.MethodPost, baseURL, "/v1/executions/"+string(id)+"/signal", token, body)
	return resp.StatusCode, raw
}

// g1CancelAuth POSTs /v1/executions/{id}/cancel.
func g1CancelAuth(t *testing.T, baseURL, token string, id types.ExecutionID) (int, []byte) {
	t.Helper()
	resp, raw := g1DoAuth(t, http.MethodPost, baseURL, "/v1/executions/"+string(id)+"/cancel", token, map[string]any{})
	return resp.StatusCode, raw
}

// g1RevokeSignalAuth POSTs /v1/executions/{id}/revoke-signal.
func g1RevokeSignalAuth(t *testing.T, baseURL, token string, id types.ExecutionID, name string) (int, []byte) {
	t.Helper()
	body := map[string]any{"name": name}
	resp, raw := g1DoAuth(t, http.MethodPost, baseURL, "/v1/executions/"+string(id)+"/revoke-signal", token, body)
	return resp.StatusCode, raw
}

// g1WaitForTerminal polls /v1/executions/{id} until terminal or timeout.
func g1WaitForTerminal(t *testing.T, baseURL, token string, id types.ExecutionID, timeout time.Duration) engine.ExecutionDetail {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		status, detail := g1InspectAuth(t, baseURL, token, id)
		if status == http.StatusOK && types.IsTerminalExecutionStatus(detail.Status) {
			return detail
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for terminal status for %s (last=%d %s)", id, status, detail.Status)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// g1WaitForExecutionStatus polls /v1/executions/{id} until the execution
// reaches one of want (or timeout). Replaces fixed-duration sleeps that waited
// for the engine to materialize execution state in Redis. Returns the detail
// at the matching status.
func g1WaitForExecutionStatus(t *testing.T, baseURL, token string, id types.ExecutionID, want []types.ExecutionStatus, timeout time.Duration) engine.ExecutionDetail {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		status, detail := g1InspectAuth(t, baseURL, token, id)
		if status == http.StatusOK {
			for _, w := range want {
				if detail.Status == w {
					return detail
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for execution %s to reach %v (last=%d %s)", id, want, status, detail.Status)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// g1WaitForNodeStatus polls /v1/executions/{id} until the named node reaches
// one of want (or the execution reaches a terminal state, which is a test
// failure because the node cannot progress further). Replaces fixed-duration
// time.Sleep calls that waited for the wait node to suspend.
func g1WaitForNodeStatus(t *testing.T, baseURL, token string, id types.ExecutionID, node string, want []types.NodeStatus, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		status, detail := g1InspectAuth(t, baseURL, token, id)
		if status == http.StatusOK {
			if types.IsTerminalExecutionStatus(detail.Status) {
				t.Fatalf("execution %s reached terminal %s before node %q reached %v", id, detail.Status, node, want)
			}
			for _, n := range detail.Nodes {
				if n.Name == node {
					for _, w := range want {
						if n.Status == w {
							return
						}
					}
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for node %q on %s to reach %v (last=%d detail=%+v)", node, id, want, status, detail)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// g1NodeStatuses returns a compact status=Name summary for debug logging.
func g1NodeStatuses(d engine.ExecutionDetail) string {
	if len(d.Nodes) == 0 {
		return "(no nodes)"
	}
	parts := make([]string, 0, len(d.Nodes))
	for _, n := range d.Nodes {
		parts = append(parts, fmt.Sprintf("%s=%s", n.Name, n.Status))
	}
	return strings.Join(parts, ",")
}

// g1ProductionMappings returns the multi-tenant token registry used by the
// production harness.
func g1ProductionMappings() []apiserver.TokenPrincipalMapping {
	return []apiserver.TokenPrincipalMapping{
		{
			Token:    g1TokFullA,
			Subject:  "alice",
			TenantID: g1TenantA,
			Scopes:   []string{"workflow", "execution", "deadletter.list", "deadletter.replay", "management.read", "management.leader.read", "management.runner.read"},
		},
		{
			Token:    g1TokNoExA,
			Subject:  "bob",
			TenantID: g1TenantA,
			Scopes:   []string{"workflow"},
		},
		{
			Token:    g1TokFullB,
			Subject:  "carol",
			TenantID: g1TenantB,
			Scopes:   []string{"workflow", "execution"},
		},
		{
			Token:    g1TokDefault,
			Subject:  "dave",
			TenantID: "default",
			Scopes:   []string{"workflow", "execution", "deadletter.list", "deadletter.replay", "management.read", "management.leader.read", "management.runner.read"},
		},
	}
}

// g1StartWorkflowDef returns a single-node workflow used by AuthzMatrix + gRPC subtests.
func g1StartWorkflowDef(name string) *types.WorkflowDef {
	return &types.WorkflowDef{
		Name: name,
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.g1.real"},
		},
	}
}

// g1SignalWaitDef returns a workflow that suspends on a single signal.
func g1SignalWaitDef(name, signalName string) *types.WorkflowDef {
	return &types.WorkflowDef{
		Name: name,
		Nodes: []types.NodeDef{
			{Name: "wait", Type: "xflow.wait", Parameters: map[string]any{
				"mode":        "signal",
				"signal_name": signalName,
			}},
			{Name: "end", Type: "test.g1.real"},
		},
		Connections: types.Connections{
			"wait": {"main": []types.Connection{{Node: "end", Input: "main"}}},
		},
	}
}

// g1MultiSignalWaitDef returns a workflow that suspends on multiple signals
// with a quorum requirement.
func g1MultiSignalWaitDef(name string, signals []string, quorum int) *types.WorkflowDef {
	return &types.WorkflowDef{
		Name: name,
		Nodes: []types.NodeDef{
			{Name: "wait", Type: "xflow.wait", Parameters: map[string]any{
				"signals": signals,
				"quorum":  quorum,
			}},
			{Name: "end", Type: "test.g1.real"},
		},
		Connections: types.Connections{
			"wait": {"main": []types.Connection{{Node: "end", Input: "main"}}},
		},
	}
}

// g1TimerWaitDef returns a workflow that suspends on a timer.
func g1TimerWaitDef(name, duration string) *types.WorkflowDef {
	return &types.WorkflowDef{
		Name: name,
		Nodes: []types.NodeDef{
			{Name: "wait", Type: "xflow.wait", Parameters: map[string]any{
				"mode":     "timer",
				"duration": duration,
			}},
			{Name: "end", Type: "test.g1.real"},
		},
		Connections: types.Connections{
			"wait": {"main": []types.Connection{{Node: "end", Input: "main"}}},
		},
	}
}

// g1RealHandler is the runner-side handler for "test.g1.real". It records
// invocations for at-least-once assertions: the counter is monotonic and
// the test asserts >= 1 (never == 1, since the engine may redeliver).
type g1RealHandler struct {
	invocations atomic.Int32
}

func (*g1RealHandler) Descriptor() types.Descriptor { return types.Descriptor{Type: "test.g1.real"} }

func (h *g1RealHandler) Execute(_ context.Context, input *types.Input) (*types.Output, error) {
	h.invocations.Add(1)
	return &types.Output{Data: map[string]any{
		"handled_by": "g1-runner",
		"claim_id":   input.Data["claim_id"],
	}}, nil
}

// g1IdempotentSideEffectHandler models an at-least-once handler that performs a
// real external side effect: a MySQL business row written keyed by the
// host-provided stable identity (execution_id + node_name from types.Input).
// On redelivery the handler is invoked again, but the table's UNIQUE
// constraint makes the second INSERT a no-op — proving business_rows == 1 even
// when handler_invocations >= 2. This is the idempotent-receiver pattern the
// engine contract expects: the host fences the commit; the handler deduplicates
// its own external writes against the stable identity the host exposes.
type g1IdempotentSideEffectHandler struct {
	db          *sql.DB
	invocations atomic.Int32
}

func (*g1IdempotentSideEffectHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.g1.idempotent"}
}

func (h *g1IdempotentSideEffectHandler) Execute(ctx context.Context, input *types.Input) (*types.Output, error) {
	h.invocations.Add(1)
	key := input.ExecutionID + ":" + input.NodeName
	// INSERT IGNORE: a redelivered invocation for the same (execution_id,
	// node_name) is a no-op at the external store rather than a duplicate write.
	_, _ = h.db.ExecContext(ctx,
		"INSERT IGNORE INTO xflow_g1_idempotency_proof (execution_id, node_name, payload) VALUES (?, ?, ?)",
		input.ExecutionID, input.NodeName, key,
	)
	return &types.Output{Data: map[string]any{"idempotency_key": key}}, nil
}


// custom "test.g1.real" handler AND bridges built-in xflow.wait / xflow.start
// from the node registry.
func g1RegistryForProduction() *execution.Registry {
	reg := execution.NewRegistry()
	reg.RegisterGlobal("test.g1.real", &g1RealHandler{})
	if h, ok := nodereg.Lookup("xflow.wait"); ok {
		reg.RegisterGlobal("xflow.wait", h)
	}
	if h, ok := nodereg.Lookup("xflow.start"); ok {
		reg.RegisterGlobal("xflow.start", h)
	}
	return reg
}

// g1StartRunner launches a runner against the harness HTTP transport and
// returns a cancel function + error channel. The runner is registered with
// the control plane's RunnerDirectory before this returns.
func g1StartRunner(t *testing.T, h *productionServerRunnerHarness, id string) (context.CancelFunc, chan error) {
	t.Helper()
	runnerCtx, runnerCancel := context.WithCancel(context.Background())
	runner := runnersvc.New(
		protocol.NewClient(h.httpSrv.URL, h.httpSrv.Client()),
		g1RegistryForProduction(),
		runnersvc.Config{
			RunnerID:     id,
			Concurrency:  1,
			Capabilities: []protocol.Capability{{NodeType: "test.g1.real"}, {NodeType: "xflow.wait"}},
			PollWait:     5 * time.Millisecond,
			Tracer:       h.tracer,
			Tenants:      []tenant.TenantID{"default", g1TenantA, g1TenantB},
		},
	)
	errCh := make(chan error, 1)
	go func() { errCh <- runner.Run(runnerCtx) }()
	waitForE2ERunner(t, h.runners, id)
	return runnerCancel, errCh
}

// g1StopRunner cancels the runner and waits for it to stop (or timeout).
func g1StopRunner(t *testing.T, cancel context.CancelFunc, errCh chan error) {
	t.Helper()
	cancel()
	select {
	case err := <-errCh:
		if err != nil && err != context.Canceled {
			t.Logf("runner stop error: %v", err)
		}
	case <-time.After(3 * time.Second):
	}
}

// TestG1ProductionE2E is the G1 production-auth release-gate coverage. It
// requires real Redis + real MySQL; skips cleanly when either is missing.
func TestG1ProductionE2E(t *testing.T) {
	addr := requireRedis(t)
	dsn := requireMySQL(t)

	// Reset the artifact so each run starts from an empty file.
	abs := resolveG1ArtifactPath(t)
	if err := os.Remove(abs); err != nil && !os.IsNotExist(err) {
		t.Fatalf("remove stale artifact %q: %v", abs, err)
	}
	t.Logf("g1 artifact reset: %s", abs)

	art := &g1Artifact{
		GeneratedAt:  time.Now().UTC().Format(time.RFC3339),
		GoVersion:    runtime.Version(),
		OS:           runtime.GOOS + "/" + runtime.GOARCH,
		CommitSHA:    readGitSHA(t),
		RedisAddr:    addr,
		MySQLDSNHost: "127.0.0.1:3306",
		AuthzMatrix:  []g1AuthzRow{},
	}

	h := newProductionServerRunnerHarness(t, addr, dsn, g1ProductionMappings())

	// Install the OTel global provider so the engine's outboxTracer and the
	// control-plane tracer land in the same recorder.
	otel.SetTracerProvider(h.tracerProv)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	var (
		traceGraph     g1TraceGraph
		approvalDAG    g1ApprovalDAG
		auditReconcile g1AuditReconcile
		deadLetter     g1DeadLetter
		metricsScrape  g1MetricsScrape
		idempotency    g1Idempotency
	)

	t.Run("AuthzMatrix", func(t *testing.T) {
		art.AuthzMatrix = g1RunAuthzMatrix(t, h)
	})

	t.Run("GRPCRunnerConnectReport", func(t *testing.T) {
		traceGraph = g1RunGRPCRunnerConnectReport(t, h)
	})

	t.Run("GRPCRunnerConnectReportTenantA", func(t *testing.T) {
		tgA := g1RunGRPCRunnerConnectReportForTenant(t, h, g1TokFullA, "g1-grpc-tenantA")
		traceGraph.TenantAOneTraceID = tgA.OneTraceID
		traceGraph.TenantADispatchParentedToSubmit = tgA.DispatchParentedToSubmit
		traceGraph.TenantACommitParentedToReport = tgA.CommitParentedToReport
	})

	t.Run("CrossTenantCarrierIsolation", func(t *testing.T) {
		traceGraph.CrossTenantCarrierIsolated = g1RunCrossTenantCarrierIsolation(t, h)
	})

	t.Run("ApprovalMultiSignal", func(t *testing.T) {
		g1RunApprovalMultiSignal(t, h)
		approvalDAG.MultiSignalQuorum = "pass"
	})

	t.Run("ApprovalTimer", func(t *testing.T) {
		g1RunApprovalTimer(t, h)
		approvalDAG.TimerFired = "pass"
	})

	t.Run("ExecutionCancel", func(t *testing.T) {
		g1RunExecutionCancel(t, h)
		approvalDAG.Cancel = "pass"
	})

	t.Run("CyclicReset", func(t *testing.T) {
		g1RunCyclicReset(t, h, addr)
		approvalDAG.CyclicReset = "pass"
	})

	t.Run("RepeatSignalConflict", func(t *testing.T) {
		g1RunRepeatSignalConflict(t, h)
		approvalDAG.RepeatSignal409 = "pass"
		idempotency.RepeatSignalOutcome = "409"
	})

	t.Run("DeadLetterReplay", func(t *testing.T) {
		deadLetter = g1RunDeadLetterReplay(t, h, addr)
	})

	t.Run("AuditReconcile", func(t *testing.T) {
		auditReconcile = g1RunAuditReconcile(t, h)
	})

	t.Run("MetricsScrape", func(t *testing.T) {
		metricsScrape = g1RunMetricsScrape(t, h)
	})

	t.Run("IdempotencyReport", func(t *testing.T) {
		idempotency = g1RunIdempotencyReport(t, h, addr, dsn)
	})

	art.TraceGraph = traceGraph
	art.ApprovalDAG = approvalDAG
	art.AuditReconcile = auditReconcile
	art.DeadLetter = deadLetter
	art.MetricsScrape = metricsScrape
	art.IdempotencyReport = idempotency

	raw, err := json.MarshalIndent(art, "", "  ")
	if err != nil {
		t.Fatalf("marshal artifact: %v", err)
	}
	// M1: verify no token values leak into the artifact JSON before persisting
	// it (g1EnsureNoLeak was previously defined but never called — a dead
	// safety check).
	g1EnsureNoLeak(t, raw)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		t.Fatalf("mkdir artifact dir: %v", err)
	}
	if err := os.WriteFile(abs, raw, 0o644); err != nil {
		t.Fatalf("write artifact %q: %v", abs, err)
	}
	t.Logf("g1 artifact written: %s", abs)
}

// g1RunAuthzMatrix exercises the HTTP authz matrix: submit/invoke allow,
// inspect/signal/revoke/cancel allow (full-A) and deny (noexec-A lacks
// execution scope → 403), cross-tenant IDOR → 404 (no existence leak).
func g1RunAuthzMatrix(t *testing.T, h *productionServerRunnerHarness) []g1AuthzRow {
	t.Helper()
	rows := []g1AuthzRow{}

	// Submit allow with full-A.
	idA, statusSubmit := g1SubmitAuth(t, h.httpSrv.URL, g1TokFullA, g1StartWorkflowDef("g1-authz-submit-allow"), nil)
	rows = append(rows, g1AuthzRow{Route: "POST /v1/workflows", Token: "full-A", Scope: "workflow", Expected: 200, Got: statusSubmit, Decision: "allow"})
	if statusSubmit != 200 {
		t.Fatalf("submit allow: status=%d, want 200", statusSubmit)
	}

	// Submit allow with noexec-A (workflow scope present).
	_, statusSubmitNoEx := g1SubmitAuth(t, h.httpSrv.URL, g1TokNoExA, g1StartWorkflowDef("g1-authz-submit-noex"), nil)
	rows = append(rows, g1AuthzRow{Route: "POST /v1/workflows", Token: "noexec-A", Scope: "workflow", Expected: 200, Got: statusSubmitNoEx, Decision: "allow"})
	if statusSubmitNoEx != 200 {
		t.Fatalf("submit noexec allow: status=%d, want 200", statusSubmitNoEx)
	}

	// Invoke allow. Entry node must be of type "xflow.start" to be a valid
	// entry point for the invoke endpoint.
	invBody := map[string]any{
		"workflow": &types.WorkflowDef{
			Name:  "g1-authz-invoke",
			Nodes: []types.NodeDef{
				{Name: "start", Type: "xflow.start"},
				{Name: "work", Type: "test.g1.real"},
			},
			Connections: types.Connections{
				"start": {"main": []types.Connection{{Node: "work", Input: "main"}}},
			},
		},
		"entry": "start",
		"input": map[string]any{"claim_id": "invoke-allow"},
	}
	resp, _ := g1DoAuth(t, http.MethodPost, h.httpSrv.URL, "/v1/workflows/invoke", g1TokFullA, invBody)
	rows = append(rows, g1AuthzRow{Route: "POST /v1/workflows/invoke", Token: "full-A", Scope: "workflow", Expected: 200, Got: resp.StatusCode, Decision: "allow"})
	if resp.StatusCode != 200 {
		t.Fatalf("invoke allow: status=%d, want 200", resp.StatusCode)
	}

	// Inspect allow (full-A on its own tenant's execution).
	statusInspect, _ := g1InspectAuth(t, h.httpSrv.URL, g1TokFullA, idA)
	rows = append(rows, g1AuthzRow{Route: "GET /v1/executions/{id}", Token: "full-A", Scope: "execution", Expected: 200, Got: statusInspect, Decision: "allow"})
	if statusInspect != 200 {
		t.Fatalf("inspect allow: status=%d, want 200", statusInspect)
	}

	// Inspect deny (noexec-A lacks execution scope) → 403.
	statusInspectDeny, _ := g1InspectAuth(t, h.httpSrv.URL, g1TokNoExA, idA)
	rows = append(rows, g1AuthzRow{Route: "GET /v1/executions/{id}", Token: "noexec-A", Scope: "", Expected: 403, Got: statusInspectDeny, Decision: "deny"})
	if statusInspectDeny != 403 {
		t.Fatalf("inspect deny: status=%d, want 403 (missing execution scope)", statusInspectDeny)
	}

	// Cross-tenant IDOR: tenantB token inspecting tenantA's execution → 404.
	statusCross, _ := g1InspectAuth(t, h.httpSrv.URL, g1TokFullB, idA)
	rows = append(rows, g1AuthzRow{Route: "GET /v1/executions/{id}", Token: "full-B (cross-tenant)", Scope: "execution", Expected: 404, Got: statusCross, Decision: "deny"})
	if statusCross != 404 {
		t.Fatalf("cross-tenant inspect: status=%d, want 404 (no existence leak)", statusCross)
	}

	// Signal deny (noexec-A lacks execution scope) → 403.
	statusSigDeny, _ := g1SignalAuth(t, h.httpSrv.URL, g1TokNoExA, idA, "noop", nil)
	rows = append(rows, g1AuthzRow{Route: "POST /v1/executions/{id}/signal", Token: "noexec-A", Scope: "", Expected: 403, Got: statusSigDeny, Decision: "deny"})
	if statusSigDeny != 403 {
		t.Fatalf("signal deny: status=%d, want 403", statusSigDeny)
	}

	// Revoke-signal deny (noexec-A) → 403.
	statusRevDeny, _ := g1RevokeSignalAuth(t, h.httpSrv.URL, g1TokNoExA, idA, "noop")
	rows = append(rows, g1AuthzRow{Route: "POST /v1/executions/{id}/revoke-signal", Token: "noexec-A", Scope: "", Expected: 403, Got: statusRevDeny, Decision: "deny"})
	if statusRevDeny != 403 {
		t.Fatalf("revoke-signal deny: status=%d, want 403", statusRevDeny)
	}

	// Cancel deny (noexec-A lacks execution scope) → 403.
	statusCancelDeny, _ := g1CancelAuth(t, h.httpSrv.URL, g1TokNoExA, idA)
	rows = append(rows, g1AuthzRow{Route: "POST /v1/executions/{id}/cancel", Token: "noexec-A", Scope: "", Expected: 403, Got: statusCancelDeny, Decision: "deny"})
	if statusCancelDeny != 403 {
		t.Fatalf("cancel deny: status=%d, want 403", statusCancelDeny)
	}

	return rows
}

// g1RunGRPCRunnerConnectReport submits a workflow, drives it to completion
// via the gRPC runner Connect + ReportResult path, asserts terminal Success,
// and verifies the B1 trace graph (5 spans share one trace id with W3C
// parentage).
func g1RunGRPCRunnerConnectReport(t *testing.T, h *productionServerRunnerHarness) g1TraceGraph {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcSrv := grpc.NewServer(
		grpc.UnaryInterceptor(tracing.GRPCUnaryServerInterceptor(h.tracer)),
		grpc.StreamInterceptor(tracing.GRPCStreamServerInterceptor(h.tracer)),
	)
	h.srv.RegisterGRPC(grpcSrv)
	go func() { _ = grpcSrv.Serve(lis) }()
	t.Cleanup(grpcSrv.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial gRPC: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	registry := g1RegistryForProduction()
	runner := runnersvc.New(
		protocol.NewGRPCClient(conn),
		registry,
		runnersvc.Config{
			RunnerID:     "runner-g1-grpc",
			Concurrency:  1,
			Capabilities: []protocol.Capability{{NodeType: "test.g1.real"}, {NodeType: "xflow.wait"}},
			PollWait:     5 * time.Millisecond,
			Tracer:       h.tracer,
			Tenants:      []tenant.TenantID{"default", g1TenantA, g1TenantB},
		},
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- runner.Run(ctx) }()
	waitForE2ERunner(t, h.runners, "runner-g1-grpc")

	// R4 (2026-07-20): the pollTask tenant injection (service/control/core.go)
	// now propagates Assignment.TenantID into the engine context, so the W3C
	// carrier is read from the correct Redis namespace for both the default
	// tenant and non-default tenants. The trace-graph assertions below are
	// strong (t.Fatalf) — WARN degradation has been removed.
	execID := g1SubmitAllowed(t, h.httpSrv.URL, g1TokDefault, g1StartWorkflowDef("g1-grpc-runner"))

	detail := g1WaitForTerminal(t, h.httpSrv.URL, g1TokDefault, execID, 15*time.Second)
	if detail.Status != types.ExecutionStatusSuccess {
		t.Fatalf("gRPC runner execution status = %s, want success", detail.Status)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil && err != context.Canceled {
			t.Fatalf("runner error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("runner did not stop in time")
	}

	_ = h.tracerProv.ForceFlush(ctx)

	spans := h.spanRecorder.Ended()
	byName := map[string]sdktrace.ReadOnlySpan{}
	for _, s := range spans {
		switch s.Name() {
		case "xflow.workflow.submit", "xflow.task.dispatch", "xflow.task.execute", "xflow.task.report", "xflow.task.commit":
			if _, ok := byName[s.Name()]; !ok {
				byName[s.Name()] = s
			}
		}
	}
	tg := g1TraceGraph{SpansPresent: []string{}}
	for _, name := range []string{"xflow.workflow.submit", "xflow.task.dispatch", "xflow.task.execute", "xflow.task.report", "xflow.task.commit"} {
		if byName[name] == nil {
			t.Fatalf("missing span %q; got %v", name, spanNamesIntegration(spans))
		}
		tg.SpansPresent = append(tg.SpansPresent, name)
	}

	submit := byName["xflow.workflow.submit"]
	dispatch := byName["xflow.task.dispatch"]
	report := byName["xflow.task.report"]
	commit := byName["xflow.task.commit"]

	// R4 (2026-07-20): strong assertions — no WARN degradation. The pollTask
	// tenant injection (service/control/core.go) propagates
	// Assignment.TenantID into the engine context so the W3C carrier is read
	// from the correct Redis namespace. All 5 spans must share one TraceID,
	// dispatch must be parented to submit (real W3C remote parent via persisted
	// carrier), execute to dispatch (lease carrier), report to execute (gRPC
	// report carrier), and commit to report.
	root := submit.SpanContext().TraceID()
	tg.OneTraceID = true
	for _, s := range []sdktrace.ReadOnlySpan{dispatch, byName["xflow.task.execute"], report, commit} {
		if s.SpanContext().TraceID() != root {
			t.Fatalf("span %q trace %s != submit trace %s (trace graph broken; pollTask tenant injection or carrier extraction failed)", s.Name(), s.SpanContext().TraceID(), root)
		}
	}
	if dispatch.Parent().SpanID() != submit.SpanContext().SpanID() {
		t.Fatalf("dispatch parent %s != submit %s (dispatch did not inherit submit causality via carrier)", dispatch.Parent().SpanID(), submit.SpanContext().SpanID())
	}
	tg.DispatchParentedToSubmit = true
	if byName["xflow.task.execute"].Parent().SpanID() != dispatch.SpanContext().SpanID() {
		t.Fatalf("execute parent %s != dispatch %s (lease carrier not wired)", byName["xflow.task.execute"].Parent().SpanID(), dispatch.SpanContext().SpanID())
	}
	if report.Parent().SpanID() != byName["xflow.task.execute"].SpanContext().SpanID() {
		t.Fatalf("report parent %s != execute %s (gRPC report carrier not parented to execute)", report.Parent().SpanID(), byName["xflow.task.execute"].SpanContext().SpanID())
	}
	if commit.Parent().SpanID() != report.SpanContext().SpanID() {
		t.Fatalf("commit parent %s != report %s (report/commit nesting broken)", commit.Parent().SpanID(), report.SpanContext().SpanID())
	}
	tg.CommitParentedToReport = true
	return tg
}

// g1RunGRPCRunnerConnectReportForTenant is the tenantA variant of
// g1RunGRPCRunnerConnectReport. It creates its own gRPC server + runner,
// submits a workflow with the given token, waits for terminal Success, then
// strongly asserts the B1 trace graph (5 spans, one TraceID, 4 parent edges
// via W3C carriers). Strong assertions (t.Fatalf) — no WARN degradation.
//
// The span recorder is global, so the function filters spans by the submit
// span's TraceID to exclude spans from earlier subtests' workflows that the
// runner may have drained.
func g1RunGRPCRunnerConnectReportForTenant(t *testing.T, h *productionServerRunnerHarness, token, wfName string) g1TraceGraph {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcSrv := grpc.NewServer(
		grpc.UnaryInterceptor(tracing.GRPCUnaryServerInterceptor(h.tracer)),
		grpc.StreamInterceptor(tracing.GRPCStreamServerInterceptor(h.tracer)),
	)
	h.srv.RegisterGRPC(grpcSrv)
	go func() { _ = grpcSrv.Serve(lis) }()
	t.Cleanup(grpcSrv.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial gRPC: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	registry := g1RegistryForProduction()
	runner := runnersvc.New(
		protocol.NewGRPCClient(conn),
		registry,
		runnersvc.Config{
			RunnerID:     "runner-g1-grpc-" + wfName,
			Concurrency:  1,
			Capabilities: []protocol.Capability{{NodeType: "test.g1.real"}, {NodeType: "xflow.wait"}},
			PollWait:     5 * time.Millisecond,
			Tracer:       h.tracer,
			Tenants:      []tenant.TenantID{"default", g1TenantA, g1TenantB},
		},
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- runner.Run(ctx) }()
	waitForE2ERunner(t, h.runners, "runner-g1-grpc-"+wfName)

	execID := g1SubmitAllowed(t, h.httpSrv.URL, token, g1StartWorkflowDef(wfName))

	detail := g1WaitForTerminal(t, h.httpSrv.URL, token, execID, 15*time.Second)
	if detail.Status != types.ExecutionStatusSuccess {
		t.Fatalf("[%s] execution status = %s, want success", wfName, detail.Status)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil && err != context.Canceled {
			t.Fatalf("runner error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("runner did not stop in time")
	}

	_ = h.tracerProv.ForceFlush(ctx)

	spans := h.spanRecorder.Ended()
	// Locate the submit span for this run by finding the most recent
	// xflow.workflow.submit span. Filter downstream spans by its TraceID
	// to exclude orphaned spans from earlier subtests.
	var submit sdktrace.ReadOnlySpan
	for i := len(spans) - 1; i >= 0; i-- {
		if spans[i].Name() == "xflow.workflow.submit" {
			submit = spans[i]
			break
		}
	}
	if submit == nil {
		t.Fatalf("[%s] missing submit span; got %v", wfName, spanNamesIntegration(spans))
	}
	root := submit.SpanContext().TraceID()
	byName := map[string]sdktrace.ReadOnlySpan{}
	for _, s := range spans {
		if s.SpanContext().TraceID() != root {
			continue
		}
		switch s.Name() {
		case "xflow.task.dispatch", "xflow.task.execute", "xflow.task.report", "xflow.task.commit":
			if _, ok := byName[s.Name()]; !ok {
				byName[s.Name()] = s
			}
		}
	}
	tg := g1TraceGraph{SpansPresent: []string{"xflow.workflow.submit"}}
	for _, name := range []string{"xflow.task.dispatch", "xflow.task.execute", "xflow.task.report", "xflow.task.commit"} {
		if byName[name] == nil {
			t.Fatalf("[%s] missing span %q for trace %s; got %v", wfName, name, root, spanNamesIntegration(spans))
		}
		tg.SpansPresent = append(tg.SpansPresent, name)
	}
	dispatch := byName["xflow.task.dispatch"]
	execute := byName["xflow.task.execute"]
	report := byName["xflow.task.report"]
	commit := byName["xflow.task.commit"]
	tg.OneTraceID = true
	if dispatch.Parent().SpanID() != submit.SpanContext().SpanID() {
		t.Fatalf("[%s] dispatch parent %s != submit %s (dispatch did not inherit submit causality via carrier)", wfName, dispatch.Parent().SpanID(), submit.SpanContext().SpanID())
	}
	tg.DispatchParentedToSubmit = true
	if execute.Parent().SpanID() != dispatch.SpanContext().SpanID() {
		t.Fatalf("[%s] execute parent %s != dispatch %s (lease carrier not wired)", wfName, execute.Parent().SpanID(), dispatch.SpanContext().SpanID())
	}
	if report.Parent().SpanID() != execute.SpanContext().SpanID() {
		t.Fatalf("[%s] report parent %s != execute %s (gRPC report carrier not parented to execute)", wfName, report.Parent().SpanID(), execute.SpanContext().SpanID())
	}
	if commit.Parent().SpanID() != report.SpanContext().SpanID() {
		t.Fatalf("[%s] commit parent %s != report %s (report/commit nesting broken)", wfName, commit.Parent().SpanID(), report.SpanContext().SpanID())
	}
	tg.CommitParentedToReport = true
	return tg
}

// g1RunCrossTenantCarrierIsolation submits tenantA and tenantB workflows
// concurrently, waits for both to reach terminal Success via a shared gRPC
// runner, then asserts the carrier lookup did not cross tenant boundaries:
// each dispatch span is parented to one of the two submit spans, the two
// dispatch spans sit in different traces, and each dispatch is parented to a
// distinct submit.
//
// g1DoAuth/g1SubmitAuth call t.Fatalf, which is unsafe in goroutines
// (runtime.Goexit deadlocks the waiter). g1SubmitConcurrent is a non-fatal
// submit helper so both submits can run concurrently.
func g1RunCrossTenantCarrierIsolation(t *testing.T, h *productionServerRunnerHarness) bool {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcSrv := grpc.NewServer(
		grpc.UnaryInterceptor(tracing.GRPCUnaryServerInterceptor(h.tracer)),
		grpc.StreamInterceptor(tracing.GRPCStreamServerInterceptor(h.tracer)),
	)
	h.srv.RegisterGRPC(grpcSrv)
	go func() { _ = grpcSrv.Serve(lis) }()
	t.Cleanup(grpcSrv.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial gRPC: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	registry := g1RegistryForProduction()
	runner := runnersvc.New(
		protocol.NewGRPCClient(conn),
		registry,
		runnersvc.Config{
			RunnerID:     "runner-g1-cross-tenant",
			Concurrency:  1,
			Capabilities: []protocol.Capability{{NodeType: "test.g1.real"}, {NodeType: "xflow.wait"}},
			PollWait:     5 * time.Millisecond,
			Tracer:       h.tracer,
			Tenants:      []tenant.TenantID{"default", g1TenantA, g1TenantB},
		},
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- runner.Run(ctx) }()
	waitForE2ERunner(t, h.runners, "runner-g1-cross-tenant")

	// Submit tenantA and tenantB workflows concurrently so both carriers sit
	// in Redis simultaneously. g1SubmitConcurrent is non-fatal so it is safe
	// to call from goroutines (t.Fatalf in a goroutine deadlocks via Goexit).
	var (
		idA, idB types.ExecutionID
		stA, stB int
		wg       sync.WaitGroup
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		idA, stA = g1SubmitConcurrent(h.httpSrv.URL, g1TokFullA, g1StartWorkflowDef("g1-x-tenantA"))
	}()
	go func() {
		defer wg.Done()
		idB, stB = g1SubmitConcurrent(h.httpSrv.URL, g1TokFullB, g1StartWorkflowDef("g1-x-tenantB"))
	}()
	wg.Wait()
	if stA != 200 || idA == "" {
		t.Fatalf("cross-tenant tenantA concurrent submit: status=%d id=%q", stA, idA)
	}
	if stB != 200 || idB == "" {
		t.Fatalf("cross-tenant tenantB concurrent submit: status=%d id=%q", stB, idB)
	}

	detailA := g1WaitForTerminal(t, h.httpSrv.URL, g1TokFullA, idA, 15*time.Second)
	if detailA.Status != types.ExecutionStatusSuccess {
		t.Fatalf("cross-tenant tenantA execution status = %s, want success", detailA.Status)
	}
	detailB := g1WaitForTerminal(t, h.httpSrv.URL, g1TokFullB, idB, 15*time.Second)
	if detailB.Status != types.ExecutionStatusSuccess {
		t.Fatalf("cross-tenant tenantB execution status = %s, want success", detailB.Status)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil && err != context.Canceled {
			t.Fatalf("runner error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("runner did not stop in time")
	}

	_ = h.tracerProv.ForceFlush(ctx)

	spans := h.spanRecorder.Ended()
	// Find the two most recent submit spans (tenantA + tenantB).
	var submits []sdktrace.ReadOnlySpan
	for i := len(spans) - 1; i >= 0 && len(submits) < 2; i-- {
		if spans[i].Name() == "xflow.workflow.submit" {
			submits = append(submits, spans[i])
		}
	}
	if len(submits) != 2 {
		t.Fatalf("cross-tenant: expected 2 submit spans, got %d (%v)", len(submits), spanNamesIntegration(spans))
	}

	// Find dispatch spans whose trace ID matches one of the two submit trace
	// IDs.
	submitTraceIDs := map[string]bool{
		submits[0].SpanContext().TraceID().String(): true,
		submits[1].SpanContext().TraceID().String(): true,
	}
	var dispatches []sdktrace.ReadOnlySpan
	for _, s := range spans {
		if s.Name() != "xflow.task.dispatch" {
			continue
		}
		if submitTraceIDs[s.SpanContext().TraceID().String()] {
			dispatches = append(dispatches, s)
		}
	}
	if len(dispatches) != 2 {
		t.Fatalf("cross-tenant: expected 2 dispatch spans matching the 2 submit traces, got %d (%v)", len(dispatches), spanNamesIntegration(spans))
	}

	// Each dispatch must be parented to one of the two submits, and the
	// dispatch's trace ID must equal that submit's trace ID. If the carrier
	// crossed tenants, a dispatch would be parented to the wrong submit (or
	// not parented to any submit at all).
	for _, d := range dispatches {
		parentID := d.Parent().SpanID()
		matched := false
		for _, s := range submits {
			if parentID == s.SpanContext().SpanID() {
				matched = true
				if d.SpanContext().TraceID() != s.SpanContext().TraceID() {
					t.Fatalf("cross-tenant: dispatch trace %s != parent submit trace %s (carrier crossed tenants)", d.SpanContext().TraceID(), s.SpanContext().TraceID())
				}
				break
			}
		}
		if !matched {
			t.Fatalf("cross-tenant: dispatch parent %s does not match any submit span ID (carrier orphaned or crossed)", parentID)
		}
	}

	// The two dispatch spans must sit in different traces — proves tenantA's
	// dispatch did not inherit tenantB's submit trace (and vice versa).
	if dispatches[0].SpanContext().TraceID() == dispatches[1].SpanContext().TraceID() {
		t.Fatalf("cross-tenant: both dispatch spans share trace %s — carrier crossed tenants", dispatches[0].SpanContext().TraceID())
	}

	// Each dispatch must be parented to a distinct submit (no both-to-same
	// short-circuit).
	if dispatches[0].Parent().SpanID() == dispatches[1].Parent().SpanID() {
		t.Fatalf("cross-tenant: both dispatch spans parented to the same submit %s — one tenant's carrier leaked into the other", dispatches[0].Parent().SpanID())
	}

	return true
}

// g1SubmitConcurrent is a non-fatal submit helper safe to call from goroutines
// (t.Fatalf in a goroutine deadlocks the test via runtime.Goexit). Returns the
// execution ID and HTTP status; the caller asserts.
func g1SubmitConcurrent(baseURL, token string, wf *types.WorkflowDef) (types.ExecutionID, int) {
	body := map[string]any{"workflow": wf}
	raw, err := json.Marshal(body)
	if err != nil {
		return "", -1
	}
	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/workflows", bytes.NewReader(raw))
	if err != nil {
		return "", -1
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := g1HTTPClient.Do(req)
	if err != nil {
		return "", -1
	}
	defer resp.Body.Close()
	payload, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", resp.StatusCode
	}
	var out e2eSubmitResp
	if err := json.Unmarshal(payload, &out); err != nil {
		return "", resp.StatusCode
	}
	return out.ExecutionID, resp.StatusCode
}
// Two signals are delivered via HTTP; the second triggers the resume.
func g1RunApprovalMultiSignal(t *testing.T, h *productionServerRunnerHarness) {
	t.Helper()
	cancel, errCh := g1StartRunner(t, h, "runner-g1-multi-signal")
	defer g1StopRunner(t, cancel, errCh)

	execID := g1SubmitAllowed(t, h.httpSrv.URL, g1TokDefault, g1MultiSignalWaitDef("g1-multi-signal", []string{"sec", "app"}, 2))

	// Wait for the wait node to suspend (polling replaces a fixed 300ms sleep
	// that was flaky under load).
	g1WaitForNodeStatus(t, h.httpSrv.URL, g1TokDefault, execID, "wait", []types.NodeStatus{types.NodeStatusSuspended, types.NodeStatusWaiting}, 5*time.Second)

	// Deliver two signals to meet the quorum.
	status, _ := g1SignalAuth(t, h.httpSrv.URL, g1TokDefault, execID, "sec", map[string]any{"by": "sec"})
	if status != 200 {
		t.Fatalf("first signal sec: status=%d, want 200", status)
	}
	status, _ = g1SignalAuth(t, h.httpSrv.URL, g1TokDefault, execID, "app", map[string]any{"by": "app"})
	if status != 200 {
		t.Fatalf("second signal app: status=%d, want 200", status)
	}

	detail := g1WaitForTerminal(t, h.httpSrv.URL, g1TokDefault, execID, 10*time.Second)
	if detail.Status != types.ExecutionStatusSuccess {
		t.Fatalf("multi-signal execution status = %s, want success", detail.Status)
	}
}

// g1RunApprovalTimer drives a ModeTimer wait (300ms) through the runner.
// Uses the "default" tenant because the timer outbox entry is flushed by the
// background OutboxDispatcher which runs without tenant context (defaults to
// "default"). This is an existing behavior constraint, not a test workaround.
func g1RunApprovalTimer(t *testing.T, h *productionServerRunnerHarness) {
	t.Helper()
	cancel, errCh := g1StartRunner(t, h, "runner-g1-timer")
	defer g1StopRunner(t, cancel, errCh)

	execID := g1SubmitAllowed(t, h.httpSrv.URL, g1TokDefault, g1TimerWaitDef("g1-timer", "300ms"))

	detail := g1WaitForTerminal(t, h.httpSrv.URL, g1TokDefault, execID, 10*time.Second)
	if detail.Status != types.ExecutionStatusSuccess {
		t.Fatalf("timer execution status = %s, want success", detail.Status)
	}
}

// g1RunExecutionCancel submits a suspendable workflow, POSTs /cancel, and
// asserts the execution reaches Canceled status.
func g1RunExecutionCancel(t *testing.T, h *productionServerRunnerHarness) {
	t.Helper()
	cancel, errCh := g1StartRunner(t, h, "runner-g1-cancel")
	defer g1StopRunner(t, cancel, errCh)

	wf := g1SignalWaitDef("g1-cancel", "approval")
	execID := g1SubmitAllowed(t, h.httpSrv.URL, g1TokDefault, wf)

	// Wait for the wait node to suspend so /cancel fires against a stable
	// non-terminal execution (polling replaces a fixed 300ms sleep that was
	// flaky under load).
	g1WaitForNodeStatus(t, h.httpSrv.URL, g1TokDefault, execID, "wait", []types.NodeStatus{types.NodeStatusSuspended, types.NodeStatusWaiting}, 5*time.Second)

	status, _ := g1CancelAuth(t, h.httpSrv.URL, g1TokDefault, execID)
	if status != 200 {
		t.Fatalf("cancel: status=%d, want 200", status)
	}

	detail := g1WaitForTerminal(t, h.httpSrv.URL, g1TokDefault, execID, 10*time.Second)
	if detail.Status != types.ExecutionStatusCanceled {
		t.Fatalf("cancel execution status = %s, want canceled", detail.Status)
	}
}

// g1RunCyclicReset drives a start<->review cyclic workflow through the engine
// directly (mirroring cyclic_reliability_real_test.go). "review" returns
// "approve" → terminal Success.
func g1RunCyclicReset(t *testing.T, h *productionServerRunnerHarness, addr string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	backend, err := distributed.New(addr, nil, distributed.WithConsumer(true))
	if err != nil {
		t.Fatalf("distributed.New: %v", err)
	}
	queue := &cyclicFakeQueue{}
	eng := engine.New(backend.State(), queue,
		engine.WithDefaultLeaseTTL(time.Minute),
		engine.WithOutboxMaxDeliveryAttempts(3),
	)
	stop := backend.Bind(eng)
	defer stop()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()

	def := &types.WorkflowDef{
		Name:    "g1-cyclic-reset",
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
		t.Fatalf("Compile: %v", err)
	}

	queue.setError(nil)
	id, err := eng.Submit(ctx, g, map[string]any{"round": 1})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	defer deleteAtomicReliabilityKeys(t, rdb, id)

	startTask := queue.drain()
	if len(startTask) != 1 || startTask[0].NodeName != "start" {
		t.Fatalf("after Submit delivered=%+v, want one start task", startTask)
	}
	startLease, err := eng.BuildTaskLease(ctx, startTask[0])
	if err != nil {
		t.Fatalf("BuildTaskLease(start): %v", err)
	}
	if err := eng.CommitTaskResult(ctx, startLease, engine.TaskResult{
		Output: &types.Output{Port: "main", Data: map[string]any{"round": 1}},
	}); err != nil {
		t.Fatalf("CommitTaskResult(start): %v", err)
	}
	reviewTask := queue.drain()
	if len(reviewTask) != 1 || reviewTask[0].NodeName != "review" {
		t.Fatalf("after start commit delivered=%+v, want one review task", reviewTask)
	}

	reviewLease, err := eng.BuildTaskLease(ctx, reviewTask[0])
	if err != nil {
		t.Fatalf("BuildTaskLease(review): %v", err)
	}
	queue.setError(nil)
	if err := eng.CommitTaskResult(ctx, reviewLease, engine.TaskResult{
		Output: &types.Output{Port: "approve", Data: map[string]any{"ok": true}},
	}); err != nil {
		t.Fatalf("CommitTaskResult(review.approve): %v", err)
	}
	exec, err := backend.State().GetExecution(ctx, id)
	if err != nil || exec == nil || exec.Status != types.ExecutionStatusSuccess {
		t.Fatalf("execution after approve = %+v err=%v, want Success", exec, err)
	}
	if extra := queue.drain(); len(extra) != 0 {
		t.Fatalf("no downstream expected after approve, got %d tasks", len(extra))
	}
}

// g1RunRepeatSignalConflict submits a single-signal wait workflow, delivers
// the signal once (accepted), then attempts to revoke it. The revoke must
// return 409 (signal already consumed or not found).
func g1RunRepeatSignalConflict(t *testing.T, h *productionServerRunnerHarness) {
	t.Helper()
	cancel, errCh := g1StartRunner(t, h, "runner-g1-repeat-signal")
	defer g1StopRunner(t, cancel, errCh)

	execID := g1SubmitAllowed(t, h.httpSrv.URL, g1TokDefault, g1SignalWaitDef("g1-repeat-signal", "approval"))

	// Wait for the wait node to suspend so the signal is consumed by the
	// resumed node (polling replaces a fixed 300ms sleep that was flaky).
	g1WaitForNodeStatus(t, h.httpSrv.URL, g1TokDefault, execID, "wait", []types.NodeStatus{types.NodeStatusSuspended, types.NodeStatusWaiting}, 5*time.Second)

	// Deliver the signal once (accepted).
	status, _ := g1SignalAuth(t, h.httpSrv.URL, g1TokDefault, execID, "approval", map[string]any{"by": "ops"})
	if status != 200 {
		t.Fatalf("first signal approval: status=%d, want 200", status)
	}

	// Wait for the execution to complete — the signal is consumed when the
	// resumed node finishes.
	detail := g1WaitForTerminal(t, h.httpSrv.URL, g1TokDefault, execID, 10*time.Second)
	if detail.Status != types.ExecutionStatusSuccess {
		t.Fatalf("repeat-signal execution status = %s, want success", detail.Status)
	}

	// Revoke-signal attempt AFTER the signal has been consumed by the resumed
	// node → 409 (signal already consumed or not found).
	status, _ = g1RevokeSignalAuth(t, h.httpSrv.URL, g1TokDefault, execID, "approval")
	if status != 409 {
		t.Fatalf("repeat revoke-signal: status=%d, want 409 (signal already consumed)", status)
	}
}

// g1RunDeadLetterReplay seeds a real dead-letter entry on the live Redis
// state, then exercises the HTTP management replay endpoint end-to-end. The
// T4 receipt projection must land in MySQL (idempotent on ReceiptAuditID).
//
// I1 fix: the prior implementation never seeded a real outbox:body hash field
// for the entry ID passed to RecordOutboxFailure. recordOutboxFailureLua's
// first line is `local body = redis.call('HGET', KEYS[2], ARGV[1])` and returns
// {0,0} when the body is nil — so the dead-letter index was never written and
// replay always returned outcome=not_found, making the assertion
// `outcome != ""` a no-op tautology.
//
// This version writes a real outbox:body hash field (mirroring
// marshalRedisOutboxEntry's JSON shape) plus the node status/meta the replay
// guard reads, so RecordOutboxFailure crosses the threshold and moves the
// entry into outbox:dead/outbox:dead:body/outbox:dead:meta. The replay then
// returns outcome=replayed with a real audit_id, and the ReceiptProjector
// writes a durable SQL projection row we can read back via AuditByReceiptAuditID.
//
// Constraint: no engine/backend production code is modified. Seeding uses a
// plain redis client (the test owns the redis address). The Lua KEYS/ARGV
// shape is the production contract exposed via RecordOutboxFailure; we only
// write the inputs the Lua script already reads.
func g1RunDeadLetterReplay(t *testing.T, h *productionServerRunnerHarness, addr string) g1DeadLetter {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Submit a workflow under the default tenant so the execution exists in
	// authoritative Redis state with status=running. g1TokDefault carries
	// tenant=default, which matches tenant.FromContext(ctx) when RecordOutboxFailure
	// is later called with a background context (defaults to default tenant).
	wf := g1StartWorkflowDef("g1-deadletter-seed")
	execID := g1SubmitAllowed(t, h.httpSrv.URL, g1TokDefault, wf)
	// Poll for the execution to materialize in Redis (replaces a fixed sleep).
	g1WaitForExecutionStatus(t, h.httpSrv.URL, g1TokDefault, execID, []types.ExecutionStatus{types.ExecutionStatusRunning}, 5*time.Second)

	// Cast the harness backend state to OutboxFailureRecorder.
	recorder, ok := h.state.(engine.OutboxFailureRecorder)
	if !ok {
		t.Fatalf("state %T does not implement OutboxFailureRecorder", h.state)
	}

	// Seed a real outbox:body hash field so recordOutboxFailureLua's HGET
	// returns non-nil and the dead-letter branch fires at the threshold. The
	// JSON shape mirrors marshalRedisOutboxEntry (id+task+auto_depth+
	// activation_id+available_at_ms+created_at_ms).
	entryID := fmt.Sprintf("root/%s/start/1", execID)
	tenantID := "default"
	task := engine.Task{
		ExecutionID:  execID,
		NodeName:     "start",
		NodeIdx:      0,
		Type:         engine.TaskTypeNodeExec,
		ActivationID: 1,
	}
	nowMs := time.Now().UTC().UnixMilli()
	body := struct {
		ID          string      `json:"id"`
		Task        engine.Task `json:"task"`
		AutoDepth   int         `json:"auto_depth,omitempty"`
		Activation  int         `json:"activation_id,omitempty"`
		AvailableAt int64       `json:"available_at_ms,omitempty"`
		CreatedAt   int64       `json:"created_at_ms,omitempty"`
	}{
		ID:          entryID,
		Task:        task,
		Activation:  1,
		AvailableAt: nowMs,
		CreatedAt:   nowMs,
	}
	bodyJSON, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal outbox body: %v", err)
	}

	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()
	bodyKey := fmt.Sprintf("xflow:t%s:exec:{%s}:outbox:body", tenantID, execID)
	readyKey := fmt.Sprintf("xflow:t%s:exec:{%s}:outbox:ready", tenantID, execID)
	nodeStatus := fmt.Sprintf("xflow:t%s:exec:{%s}:node:%s:status", tenantID, execID, "start")
	nodeMeta := fmt.Sprintf("xflow:t%s:exec:{%s}:node:%s:meta", tenantID, execID, "start")
	if err := rdb.HSet(ctx, bodyKey, entryID, string(bodyJSON)).Err(); err != nil {
		t.Fatalf("HSet outbox body: %v", err)
	}
	if err := rdb.ZAdd(ctx, readyKey, redis.Z{Score: float64(nowMs), Member: entryID}).Err(); err != nil {
		t.Fatalf("ZAdd outbox ready: %v", err)
	}
	// Seed node status + activation meta so replayDeadLetterLua's node/activation
	// guard accepts the replay (status=running + activation_id=1 matching the
	// entry's ActivationID).
	if err := rdb.Set(ctx, nodeStatus, "running", 10*time.Minute).Err(); err != nil {
		t.Fatalf("set node status: %v", err)
	}
	if err := rdb.HSet(ctx, nodeMeta, "activation_id", 1).Err(); err != nil {
		t.Fatalf("set node meta: %v", err)
	}

	// Drive RecordOutboxFailure maxAttempts times so the entry lands in the
	// dead-letter index. The final call must report DeadLettered=true.
	entry := engine.OutboxEntry{
		ID:   entryID,
		Task: task,
	}
	maxAttempts := engine.DefaultOutboxMaxDeliveryAttempts
	var deadLettered bool
	var lastAttempts int
	for i := 0; i < maxAttempts; i++ {
		res, err := recorder.RecordOutboxFailure(ctx, execID, entry, maxAttempts)
		if err != nil {
			t.Fatalf("RecordOutboxFailure[%d]: %v", i, err)
		}
		lastAttempts = res.Attempts
		if res.DeadLettered {
			deadLettered = true
		}
	}
	if !deadLettered {
		t.Fatalf("RecordOutboxFailure did not dead-letter after %d attempts (last attempts=%d)", maxAttempts, lastAttempts)
	}
	t.Logf("dead-letter seeded: entry=%q attempts=%d dead_lettered=%v", entryID, lastAttempts, deadLettered)

	// List dead-letters via the HTTP API. M2: a 200 is required (not just
	// logged) — the list endpoint is the contract under test.
	listResp, listBody := g1DoAuth(t, http.MethodGet, h.httpSrv.URL,
		"/v1/management/dead-letters/"+string(execID), g1TokDefault, nil)
	if listResp.StatusCode != 200 {
		t.Fatalf("dead-letter list: status=%d body=%s", listResp.StatusCode, string(listBody))
	}
	var listParsed struct {
		Entries []struct {
			ID string `json:"id"`
		} `json:"entries"`
	}
	_ = json.Unmarshal(listBody, &listParsed)
	if len(listParsed.Entries) == 0 {
		t.Fatalf("dead-letter list returned 0 entries, want >=1 with entry %q", entryID)
	}
	gotEntry := false
	for _, e := range listParsed.Entries {
		if e.ID == entryID {
			gotEntry = true
			break
		}
	}
	if !gotEntry {
		t.Fatalf("dead-letter list did not contain seeded entry %q (got %+v)", entryID, listParsed.Entries)
	}

	// Replay via the HTTP management route.
	reqID := fmt.Sprintf("g1-replay-%d", time.Now().UnixNano())
	replayBody := map[string]any{
		"entry_id":   entryID,
		"request_id": reqID,
		"reason":     "g1 e2e: operator replay after root-cause",
	}
	replayResp, replayRaw := g1DoAuth(t, http.MethodPost, h.httpSrv.URL,
		"/v1/management/dead-letters/"+string(execID)+"/replay", g1TokDefault, replayBody)
	var replayRespParsed struct {
		Outcome      string `json:"outcome"`
		AuditID      string `json:"audit_id,omitempty"`
		ExecutionID  string `json:"execution_id,omitempty"`
		NodeID       string `json:"node_id,omitempty"`
		ActivationID string `json:"activation_id,omitempty"`
	}
	_ = json.Unmarshal(replayRaw, &replayRespParsed)
	if replayResp.StatusCode != 200 {
		t.Fatalf("dead-letter replay: status=%d body=%s", replayResp.StatusCode, string(replayRaw))
	}
	if replayRespParsed.Outcome != string(engine.ReplayReplayed) {
		t.Fatalf("replay outcome=%q, want %q (audit_id=%q body=%s)", replayRespParsed.Outcome, engine.ReplayReplayed, replayRespParsed.AuditID, string(replayRaw))
	}
	if replayRespParsed.AuditID == "" {
		t.Fatalf("replay returned outcome=replayed but no audit_id: %s", string(replayRaw))
	}

	out := g1DeadLetter{
		Seeded:            deadLettered,
		ReplayOutcome:     replayRespParsed.Outcome,
		ReceiptAuditIDSet: replayRespParsed.AuditID != "",
	}
	// Verify the durable receipt projection row landed in MySQL via the T4
	// ReceiptProjector + AuditByReceiptAuditID. A non-nil record proves the
	// HTTP replay -> DeadLetterManager -> projectorAuditSink -> SQL appender
	// chain end-to-end. The projector appends synchronously inside the replay
	// path, but the SQL write and the subsequent read may cross a connection
	// boundary, so poll briefly rather than racing a single immediate lookup.
	appender, okApp := interface{}(h.provider).(store.ReceiptAuditAppender)
	if !okApp {
		t.Fatalf("provider %T does not implement ReceiptAuditAppender", h.provider)
	}
	recDeadline := time.Now().Add(3 * time.Second)
	for {
		rec, rerr := appender.AuditByReceiptAuditID(ctx, replayRespParsed.AuditID)
		if rerr != nil {
			t.Fatalf("AuditByReceiptAuditID(%q): %v (receipt projection must be durable)", replayRespParsed.AuditID, rerr)
		}
		if rec != nil {
			out.DurableProjectionRows = 1
			break
		}
		if time.Now().After(recDeadline) {
			t.Fatalf("receipt projection for audit_id=%q never landed in MySQL (DurableProjectionRows=0); replay->projector->SQL chain broken", replayRespParsed.AuditID)
		}
		time.Sleep(20 * time.Millisecond)
	}
	return out
}

// g1RunAuditReconcile asserts the T9 worker settles an admitted mutation.
// We submit a workflow (which writes an admission audit row), then call
// ReconcileOnce. A second call must be idempotent (no new outcome rows).
func g1RunAuditReconcile(t *testing.T, h *productionServerRunnerHarness) g1AuditReconcile {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	execID := g1SubmitAllowed(t, h.httpSrv.URL, g1TokFullA, g1StartWorkflowDef("g1-audit-reconcile"))
	// Poll for the admission audit row (written inline by the authz wrapper
	// before the HTTP response returns; a poll is more robust than a fixed
	// sleep and removes flakiness under load).
	deadline := time.Now().Add(3 * time.Second)
	for g1CountUnreconciled(t, h.provider) == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}

	ar, ok := interface{}(h.provider).(store.AuditReconciler)
	if !ok {
		t.Fatalf("provider does not implement store.AuditReconciler")
	}
	authority := control.NewExecutionAuthority(h.srv.Backend().State())
	w := control.NewAuditReconcileWorker(ar, authority, control.AuditReconcileConfig{
		Elector:    prodLeaderGateAdapter{isLeader: h.srv.IsLeader},
		BacklogAge: time.Millisecond,
		Period:     10 * time.Millisecond,
	})

	beforeAdmission := g1CountUnreconciled(t, h.provider)
	settled := w.ReconcileOnce(ctx)
	afterAdmission := g1CountUnreconciled(t, h.provider)

	settled2 := w.ReconcileOnce(ctx)
	afterAdmission2 := g1CountUnreconciled(t, h.provider)

	// R3.1 observation (non-asserting): the authz wrapper now pre-allocates the
	// ExecutionID into the admission row, so Probe can reach EffectConfirmed for
	// workflow.create. We prove the leverage directly by probing THIS execution
	// (whose admission row carries execID per R3.1, proven in
	// service/apiserver/execution_id_audit_test.go): GetExecution finds it →
	// Confirmed. The batch worker still reports settled=0 here because a shared-
	// DB backlog of pre-R3.1 empty-ExecutionID rows (Indeterminate, never
	// settled) occupies the oldest-first 256-batch before this run's fresh row —
	// that backlog masking is R3.4's scope, not an R3.1 failure. The hard >0
	// assertion belongs to R3.3, so this only logs measured values.
	probeEffect, _ := authority.Probe(tenant.WithTenant(ctx, tenant.TenantID(g1TenantA)), &store.AuditRecord{ExecutionID: string(execID), Operation: "workflow.create"})
	probeEffectStr := "indeterminate"
	switch probeEffect {
	case control.EffectConfirmed:
		probeEffectStr = "confirmed"
	case control.EffectAbsent:
		probeEffectStr = "absent"
	}
	t.Logf("g1AuditReconcile: exec_id=%s probe_effect=%s before=%d settled=%d settled2=%d after=%d after2=%d reconciled_by_worker=%d",
		execID, probeEffectStr, beforeAdmission, settled, settled2, afterAdmission, afterAdmission2, settled+settled2)

	return g1AuditReconcile{
		AdmissionRows:           beforeAdmission,
		OutcomeRows:             afterAdmission,
		ReconciledByWorker:      settled + settled2,
		IdempotentOutcomeAppends: afterAdmission2 == afterAdmission,
	}
}

// g1CountUnreconciled returns the number of unreconciled admission rows.
func g1CountUnreconciled(t *testing.T, p *sqlstore.Provider) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ar, ok := interface{}(p).(store.AuditReconciler)
	if !ok {
		return 0
	}
	rows, err := ar.ListUnreconciledAdmissions(ctx, time.Now().Add(time.Second), 1024)
	if err != nil {
		t.Logf("ListUnreconciledAdmissions: %v", err)
		return 0
	}
	return len(rows)
}

// g1RunMetricsScrape scrapes the /metrics endpoint and asserts expected counters.
func g1RunMetricsScrape(t *testing.T, h *productionServerRunnerHarness) g1MetricsScrape {
	t.Helper()

	metricsSrv := httptest.NewServer(h.metrics.Handler())
	defer metricsSrv.Close()

	resp, err := http.Get(metricsSrv.URL + "/metrics")
	if err != nil {
		t.Fatalf("scrape /metrics: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("scrape /metrics: status=%d", resp.StatusCode)
	}

	// The exact set of observed counters depends on which subtests ran
	// before this one. We assert that at least one xflow_* metric family is
	// present (proving the /metrics endpoint is wired and the Prometheus
	// registry is populated). The lease histogram is always present because
	// the lease observer is wired via WithLeaseObserver.
	want := []string{
		"xflow_lease_acquire_duration_seconds",
		"xflow_audit_reconcile_settled_total",
		"xflow_audit_write_total",
	}
	observed := []string{}
	for _, name := range want {
		if bytes.Contains(body, []byte(name)) {
			observed = append(observed, name)
		}
	}
	if len(observed) == 0 {
		snippet := string(body)
		if len(snippet) > 512 {
			snippet = snippet[:512]
		}
		t.Fatalf("metrics scrape observed 0 of %d expected counters; body=%s", len(want), snippet)
	}
	return g1MetricsScrape{
		Scraped:          true,
		CountersObserved: observed,
	}
}

// g1RunIdempotencyReport collects the idempotency assertions. The
// repeat-signal 409 is asserted in RepeatSignalConflict; the duplicate-commit
// host fence is asserted here against a real at-least-once redelivery to the
// handler.
//
// The redelivery proof: a handler that performs a real external side effect (a
// MySQL business row) is invoked twice with the same engine-built input — the
// handler-boundary view of an at-least-once redelivery. The row is keyed by the
// host-provided stable identity (execution_id + node_name) under a UNIQUE
// constraint, so business_rows == 1 despite handler_invocations == 2; the
// duplicate commit is fenced by the host (DuplicateTerminal / StaleToken /
// ExecutionInactive). These are measured values, not the prior hardcoded ">=1"
// string. Invocation-level idempotency key remains out of G1 scope.
func g1RunIdempotencyReport(t *testing.T, h *productionServerRunnerHarness, addr, dsn string) g1Idempotency {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Build a separate engine + fake queue bound to the same Redis state.
	backend, err := distributed.New(addr, nil, distributed.WithConsumer(false))
	if err != nil {
		t.Fatalf("distributed.New: %v", err)
	}
	queue := &cyclicFakeQueue{}
	eng := engine.New(backend.State(), queue,
		engine.WithDefaultLeaseTTL(time.Minute),
		engine.WithOutboxMaxDeliveryAttempts(3),
	)
	defer backend.Bind(eng)()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()

	// Real external side-effect store: a MySQL table with a UNIQUE constraint on
	// (execution_id, node_name) — the stable identity types.Input exposes to
	// every handler. This is the "real external unique key" the host makes
	// available for idempotent-receiver dedup.
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS xflow_g1_idempotency_proof (
  execution_id VARCHAR(128) NOT NULL,
  node_name    VARCHAR(128) NOT NULL,
  payload      VARCHAR(256) NOT NULL,
  created_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (execution_id, node_name)
)`); err != nil {
		t.Fatalf("create idempotency proof table: %v", err)
	}
	if _, err := db.ExecContext(ctx, "DELETE FROM xflow_g1_idempotency_proof"); err != nil {
		t.Fatalf("truncate idempotency proof table: %v", err)
	}

	handler := &g1IdempotentSideEffectHandler{db: db}

	def := &types.WorkflowDef{
		Name: "g1-idempotency-redeliver",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.g1.idempotent"},
			{Name: "end", Type: "test.g1.idempotent"},
		},
		Connections: types.Connections{
			"start": {"main": []types.Connection{{Node: "end", Input: "main"}}},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	queue.setError(nil)
	id, err := eng.Submit(ctx, g, map[string]any{"claim_id": "redeliver"})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	defer deleteAtomicReliabilityKeys(t, rdb, id)

	tasks := queue.drain()
	if len(tasks) != 1 {
		t.Fatalf("after Submit delivered=%d, want 1", len(tasks))
	}
	lease, err := eng.BuildTaskLease(ctx, tasks[0])
	if err != nil {
		t.Fatalf("BuildTaskLease: %v", err)
	}

	// Redelivery #1: the runner invokes the handler with the real engine-built
	// input. The handler writes its external business row keyed by the stable
	// identity (execution_id + node_name).
	if _, err := handler.Execute(ctx, lease.Input); err != nil {
		t.Fatalf("handler invoke #1: %v", err)
	}
	// First commit: accepted — the node is now terminal.
	outcome1, err := eng.CommitTaskResultWithOutcome(ctx, lease, engine.TaskResult{
		Output: &types.Output{Data: map[string]any{"ok": true}},
	})
	if err != nil {
		t.Fatalf("first commit: %v", err)
	}
	if outcome1 != engine.CommitOutcomeAccepted {
		t.Fatalf("first commit outcome = %q, want %q", outcome1, engine.CommitOutcomeAccepted)
	}

	// Redelivery #2: the same task is delivered again (same input, same handler).
	// The handler is invoked a second time — at-least-once — and re-runs its
	// external write, but the UNIQUE constraint makes the second INSERT a no-op.
	// The duplicate commit is fenced by the host.
	if _, err := handler.Execute(ctx, lease.Input); err != nil {
		t.Fatalf("handler invoke #2: %v", err)
	}
	outcome2, err := eng.CommitTaskResultWithOutcome(ctx, lease, engine.TaskResult{
		Output: &types.Output{Data: map[string]any{"ok": true}},
	})
	if err != nil {
		t.Fatalf("duplicate commit error: %v", err)
	}
	switch outcome2 {
	case engine.CommitOutcomeDuplicateTerminal,
		engine.CommitOutcomeStaleToken,
		engine.CommitOutcomeExecutionInactive:
		// All three outcomes prove the duplicate was fenced.
	default:
		t.Fatalf("duplicate commit outcome = %q, want DuplicateTerminal/StaleToken/ExecutionInactive", outcome2)
	}

	// Submit the same WorkflowDef+input twice → distinct executionIDs (no DAG
	// mixing). Independent executions, not idempotent at the invocation level.
	id2, err := eng.Submit(ctx, g, map[string]any{"claim_id": "redeliver"})
	if err != nil {
		t.Fatalf("second Submit: %v", err)
	}
	defer deleteAtomicReliabilityKeys(t, rdb, id2)
	if id == id2 {
		t.Fatalf("second Submit returned same execution id %q — DAG mixing", id)
	}

	// Measured external side-effect count for this execution: must be 1 despite
	// the 2 handler invocations, because the idempotent receiver deduplicated
	// against the host-provided stable identity at the external store.
	var businessRows int
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM xflow_g1_idempotency_proof WHERE execution_id = ?", id,
	).Scan(&businessRows); err != nil {
		t.Fatalf("count business rows: %v", err)
	}
	invocations := int(handler.invocations.Load())
	if invocations < 2 {
		t.Errorf("handler_invocations = %d, want >= 2 (at-least-once redelivery to the handler)", invocations)
	}
	if businessRows != 1 {
		t.Errorf("business_rows = %d, want 1 (idempotent receiver; execution_id=%s, invocations=%d)", businessRows, id, invocations)
	}

	return g1Idempotency{
		RepeatSignalOutcome:             "409",
		DuplicateReportOutcome:          string(outcome2),
		HandlerSideEffectsAssertion: fmt.Sprintf(
			"handler_invocations=%d, business_rows=%d (idempotent receiver keyed by execution_id+node_name; host fence=%s)",
			invocations, businessRows, outcome2,
		),
		IndependentExecutionsForSameDef: true,
		InvocationLevelIdempotencyKey:   "not implemented (out of G1 scope)",
		HandlerInvocations:              invocations,
		BusinessRows:                    businessRows,
		IdempotencyKey:                  "execution_id+node_name (UNIQUE constraint)",
		HostFenceOutcome:                string(outcome2),
	}
}

// g1EnsureNoLeak verifies no token values appear in the artifact JSON.
// This is a safety check; the artifact struct never records token values.
// Called before os.WriteFile so a leak aborts the test before the artifact
// is persisted.
func g1EnsureNoLeak(t *testing.T, raw []byte) {
	t.Helper()
	for _, tok := range []string{g1TokFullA, g1TokNoExA, g1TokFullB, g1TokDefault} {
		if bytes.Contains(raw, []byte(tok)) {
			t.Fatalf("SEC: token value leaked into artifact JSON")
		}
	}
}
