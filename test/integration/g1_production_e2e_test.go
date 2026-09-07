//go:build integration

// Package integration hosts the G1 production-auth end-to-end coverage.
//
// TestG1ProductionE2E exercises the G1 release-gate posture against real
// Redis (127.0.0.1:6380) + real MySQL (127.0.0.1:3306) with the production
// authz stack wired: PrincipalAuth + NamespaceAwareAuthorizer + SQLAuditSink +
// AuditReconcileWorker + Metrics + Tracer + RequireWorkflowAuth +
// WithManagement. It covers HTTP entries (submit/invoke/signal/revoke/cancel)
// with an allow/deny matrix, cross-namespace IDOR (404, no existence leak), the
// gRPC runner Connect + ReportResult path end-to-end, complex approval DAG
// features (multi-signal quorum, timer, cancel, cyclic reset, repeat signal
// → 409), the dead-letter replay HTTP contract, T9 reconcile, metrics
// scrape, and degraded idempotency assertions (at-least-once with host-side
// dedup; never a fake exactly-once count).
//
// The artifact is written to XFLOW_G1_REPORT_PATH when set, otherwise to
// test/integration/testdata/g1_e2e_report.json. The default is gitignored
// (never checked in), and the selected path is removed at the top of the test
// so a stale run cannot leak into a CI-uploaded artifact.
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
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/prometheus/common/expfmt"
	prommodel "github.com/prometheus/common/model"
	"github.com/redis/go-redis/v9"
	"github.com/xbcio/xflow/backend/providers/distributed"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/namespace"
	_ "github.com/xbcio/xflow/node" // registers built-in nodes (xflow.start, xflow.wait, etc.) in nodereg
	nodereg "github.com/xbcio/xflow/node/registry"
	obsmetrics "github.com/xbcio/xflow/observability/metrics"
	"github.com/xbcio/xflow/observability/tracing"
	"github.com/xbcio/xflow/service/apiserver"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/protocol"
	runnersvc "github.com/xbcio/xflow/service/runner"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	g1ArtifactSchemaVersion = 1
	g1RequiredGoVersion     = "go1.25.0"
	g1RunIDEnv              = "XFLOW_G1_RUN_ID"
	g1CandidateSHAEnv       = "XFLOW_G1_CANDIDATE_SHA"

	// g1ArtifactPath is the default release-artifact location for the G1
	// production e2e coverage.
	g1ArtifactPath = "test/integration/testdata/g1_e2e_report.json"
	// g1ArtifactPathEnv lets the required Make target isolate the test's output
	// from the final artifact until every publication predicate has passed.
	g1ArtifactPathEnv = "XFLOW_G1_REPORT_PATH"

	g1InvocationLevelIdempotencyKey = "not implemented (out of G1 scope)"
	g1SideEffectIdempotencyKey      = "execution_id+node_name (UNIQUE constraint)"
)

// g1Tokens — multi-namespace token registry for the production harness.
// Token values are carried ONLY in the Authorization header; they are never
// recorded in the artifact JSON.
const (
	g1TokFullA   = "g1-tok-full-namespaceA-8f3a4c2b9d"
	g1TokNoExA   = "g1-tok-noexec-namespaceA-7e1b5d0a3c"
	g1TokFullB   = "g1-tok-full-namespaceB-2c9d4e6f8a"
	g1TokDefault = "g1-tok-full-default-4a7b2c9e1d"
)

// g1NamespaceA / g1NamespaceB are the two namespaces used by the authz matrix + IDOR.
const (
	g1NamespaceA = "namespaceA"
	g1NamespaceB = "namespaceB"
)

// g1Artifact is the machine-readable coverage record written at the end of
// TestG1ProductionE2E. Sensitive fields (token values, payloads) are NEVER
// recorded — only decisions, statuses, and counter-style evidence.
type g1Artifact struct {
	SchemaVersion     int               `json:"schema_version"`
	RunID             string            `json:"run_id"`
	GeneratedAt       string            `json:"generated_at"`
	GoVersion         string            `json:"go_version"`
	OS                string            `json:"os"`
	CommitSHA         string            `json:"commit_sha"`
	FullWorktreeClean bool              `json:"full_worktree_clean"`
	RedisAddr         string            `json:"redis_addr"`
	MySQLDSNHost      string            `json:"mysql_dsn_host"`
	Runtime           g1RuntimeEvidence `json:"runtime"`
	AuthzMatrix       []g1AuthzRow      `json:"authz_matrix"`
	TraceGraph        g1TraceGraph      `json:"trace_graph"`
	ApprovalDAG       g1ApprovalDAG     `json:"approval_dag"`
	AuditReconcile    g1AuditReconcile  `json:"audit_reconcile"`
	DeadLetter        g1DeadLetter      `json:"dead_letter"`
	MetricsScrape     g1MetricsScrape   `json:"metrics_scrape"`
	IdempotencyReport g1Idempotency     `json:"idempotency_report"`
}

// g1RuntimeEvidence records only non-secret connection identity and versions
// queried from the services used by this run. MySQL credentials and DSN
// parameters are deliberately absent.
type g1RuntimeEvidence struct {
	RedisEndpoint      string `json:"redis_endpoint"`
	RedisVersion       string `json:"redis_version"`
	MySQLNetwork       string `json:"mysql_network"`
	MySQLEndpoint      string `json:"mysql_endpoint"`
	MySQLDatabase      string `json:"mysql_database"`
	MySQLServerVersion string `json:"mysql_server_version"`
}

type g1MySQLTarget struct {
	Network  string
	Endpoint string
	Database string
}

type g1AuthzRow struct {
	Scenario  string `json:"scenario"`
	Operation string `json:"operation"`
	Route     string `json:"route"`
	Token     string `json:"token"`
	Scope     string `json:"scope"`
	Expected  int    `json:"expected"`
	Got       int    `json:"got"`
	Decision  string `json:"decision"`
}

type g1TraceGraph struct {
	SpansPresent             []string `json:"spans_present"`
	OneTraceID               bool     `json:"one_trace_id"`
	DispatchParentedToSubmit bool     `json:"dispatch_parented_to_submit"`
	CommitParentedToReport   bool     `json:"commit_parented_to_report"`
	// NamespaceA fields: the same strong assertions run against a non-default
	// namespace workflow (g1TokFullA). The pollTask namespace injection in
	// service/control/core.go must read the W3C carrier from the namespaceA
	// Redis namespace, so the full 5-span graph holds with one TraceID and
	// correct parentage.
	NamespaceAOneTraceID               bool `json:"namespace_a_one_trace_id"`
	NamespaceADispatchParentedToSubmit bool `json:"namespace_a_dispatch_parented_to_submit"`
	NamespaceACommitParentedToReport   bool `json:"namespace_a_commit_parented_to_report"`
	// CrossNamespaceCarrierIsolated: namespaceA + namespaceB workflows submitted
	// concurrently — the namespaceA dispatch span must NOT inherit namespaceB's
	// submit trace (and vice versa). Proves the carrier lookup is namespace-
	// scoped, not a global read.
	CrossNamespaceCarrierIsolated bool `json:"cross_namespace_carrier_isolated"`
}

type g1ApprovalDAG struct {
	MultiSignalQuorum string `json:"multi_signal_quorum"`
	TimerFired        string `json:"timer_fired"`
	Cancel            string `json:"cancel"`
	CyclicReset       string `json:"cyclic_reset"`
	RepeatSignal409   string `json:"repeat_signal_409"`
}

type g1AuditReconcile struct {
	AdmissionRows            int  `json:"admission_rows"`
	OutcomeRows              int  `json:"outcome_rows"`
	ReconciledByWorker       int  `json:"reconciled_by_worker"`
	IdempotentOutcomeAppends bool `json:"idempotent_outcome_appends"`
	SweepsToSettle           int  `json:"sweeps_to_settle"`
	FaultMatrixPass          bool `json:"fault_matrix_pass"`
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
	RequiredFamilies []string `json:"required_families"`
	ObservedFamilies []string `json:"observed_families"`
	MissingFamilies  []string `json:"missing_families"`
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

type g1RunIdentity struct {
	RunID     string
	CommitSHA string
}

// resolveG1ArtifactPath selects an environment-provided path when present. A
// relative path remains repository-relative so direct and Make-driven runs are
// stable regardless of the package test's cwd.
func resolveG1ArtifactPath(t *testing.T) string {
	t.Helper()
	configured := strings.TrimSpace(os.Getenv(g1ArtifactPathEnv))
	if configured == "" {
		configured = g1ArtifactPath
	}
	if filepath.IsAbs(configured) {
		return filepath.Clean(configured)
	}
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	return filepath.Join(repoRoot(dir), configured)
}

func g1ValidateCanonicalRunID(runID string) error {
	parsed, err := uuid.Parse(runID)
	if err != nil {
		return fmt.Errorf("must be a UUID: %w", err)
	}
	if parsed.String() != runID {
		return fmt.Errorf("must use lowercase canonical UUID form")
	}
	if parsed.Version() != uuid.Version(4) || parsed.Variant() != uuid.RFC4122 {
		return fmt.Errorf("must be an RFC 4122 UUIDv4")
	}
	return nil
}

func g1IsLowerHexCommitSHA(commitSHA string) bool {
	if len(commitSHA) != 40 {
		return false
	}
	for i := 0; i < len(commitSHA); i++ {
		if (commitSHA[i] < '0' || commitSHA[i] > '9') && (commitSHA[i] < 'a' || commitSHA[i] > 'f') {
			return false
		}
	}
	return true
}

// g1ResolveRunIdentity binds caller-issued evidence identity to the exact
// repository revision observed by the producer. It is kept free of process IO
// so the fail-closed rules can be covered without Redis, MySQL, or Git.
func g1ResolveRunIdentity(runID, candidateSHA, actualHEAD string) (g1RunIdentity, error) {
	if err := g1ValidateCanonicalRunID(runID); err != nil {
		return g1RunIdentity{}, fmt.Errorf("%s: %w", g1RunIDEnv, err)
	}
	if !g1IsLowerHexCommitSHA(candidateSHA) {
		return g1RunIdentity{}, fmt.Errorf("%s: must be exactly 40 lowercase hexadecimal characters", g1CandidateSHAEnv)
	}
	if !g1IsLowerHexCommitSHA(actualHEAD) {
		return g1RunIdentity{}, fmt.Errorf("actual HEAD: must be exactly 40 lowercase hexadecimal characters")
	}
	if candidateSHA != actualHEAD {
		return g1RunIdentity{}, fmt.Errorf("%s=%s does not match actual HEAD=%s", g1CandidateSHAEnv, candidateSHA, actualHEAD)
	}
	return g1RunIdentity{RunID: runID, CommitSHA: candidateSHA}, nil
}

func g1ReadHEAD() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("getwd: %w", err)
	}
	out, err := exec.Command("git", "-C", repoRoot(dir), "rev-parse", "--verify", "HEAD").CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("git rev-parse HEAD: %w (%s)", err, strings.TrimSpace(string(out)))
	}
	return strings.TrimSpace(string(out)), nil
}

// g1ResolveConfiguredRunIdentity separates ordinary integration runs from
// release-evidence runs. Release provenance is enabled only when both caller-
// issued values are present; a partial configuration fails closed. Ordinary
// runs still receive a valid per-run identity tied to the observed HEAD.
func g1ResolveConfiguredRunIdentity(runID, candidateSHA, actualHEAD, generatedRunID string) (g1RunIdentity, bool, error) {
	runIDConfigured := runID != ""
	candidateConfigured := candidateSHA != ""
	if runIDConfigured != candidateConfigured {
		return g1RunIdentity{}, false, fmt.Errorf("%s and %s must either both be set or both be unset", g1RunIDEnv, g1CandidateSHAEnv)
	}
	if runIDConfigured {
		identity, err := g1ResolveRunIdentity(runID, candidateSHA, actualHEAD)
		return identity, true, err
	}
	if err := g1ValidateCanonicalRunID(generatedRunID); err != nil {
		return g1RunIdentity{}, false, fmt.Errorf("generated run ID: %w", err)
	}
	if !g1IsLowerHexCommitSHA(actualHEAD) {
		return g1RunIdentity{}, false, fmt.Errorf("actual HEAD: must be exactly 40 lowercase hexadecimal characters")
	}
	return g1RunIdentity{RunID: generatedRunID, CommitSHA: actualHEAD}, false, nil
}

func g1LoadRunIdentity(generatedRunID string) (g1RunIdentity, bool, error) {
	actualHEAD, err := g1ReadHEAD()
	if err != nil {
		return g1RunIdentity{}, false, err
	}
	return g1ResolveConfiguredRunIdentity(
		os.Getenv(g1RunIDEnv),
		os.Getenv(g1CandidateSHAEnv),
		actualHEAD,
		generatedRunID,
	)
}

// g1FullWorktreeClean records full-worktree provenance. Git failures and dirty
// trees are represented as false; the artifact validator rejects either before
// publishing release evidence.
func g1FullWorktreeClean(t *testing.T) bool {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Logf("g1 full-worktree provenance: getwd: %v", err)
		return false
	}
	cmd := exec.Command("git", "-C", repoRoot(dir), "status", "--porcelain=v1", "--untracked-files=all")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("g1 full-worktree provenance: git status: %v (%s)", err, strings.TrimSpace(string(out)))
		return false
	}
	return len(bytes.TrimSpace(out)) == 0
}

// g1ParseMySQLTarget uses the driver's parser so escaped database names,
// default addresses, unix sockets, and protocol selection follow exactly the
// same rules as the real connection. Only non-secret fields are returned.
func g1ParseMySQLTarget(dsn string) (g1MySQLTarget, error) {
	cfg, err := mysqldriver.ParseDSN(dsn)
	if err != nil {
		return g1MySQLTarget{}, fmt.Errorf("parse MySQL DSN: %w", err)
	}
	if strings.TrimSpace(cfg.DBName) == "" {
		return g1MySQLTarget{}, fmt.Errorf("parse MySQL DSN: database/schema is empty")
	}
	return g1MySQLTarget{
		Network:  cfg.Net,
		Endpoint: cfg.Addr,
		Database: cfg.DBName,
	}, nil
}

func g1RedisVersionFromInfo(info string) (string, error) {
	for _, line := range strings.Split(info, "\n") {
		key, value, ok := strings.Cut(strings.TrimSpace(line), ":")
		if ok && key == "redis_version" && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value), nil
		}
	}
	return "", fmt.Errorf("redis INFO server omitted redis_version")
}

// g1CollectRuntimeEvidence queries both services rather than recording
// assumptions about the default development environment. The MySQL database
// reported by the live session must match the schema parsed from the DSN.
func g1CollectRuntimeEvidence(t *testing.T, redisEndpoint, dsn string) g1RuntimeEvidence {
	t.Helper()
	target, err := g1ParseMySQLTarget(dsn)
	if err != nil {
		t.Fatalf("G1 runtime evidence: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rdb := redis.NewClient(&redis.Options{Addr: redisEndpoint})
	defer rdb.Close()
	info, err := rdb.Info(ctx, "server").Result()
	if err != nil {
		t.Fatalf("G1 runtime evidence: query Redis INFO server at %q: %v", redisEndpoint, err)
	}
	redisVersion, err := g1RedisVersionFromInfo(info)
	if err != nil {
		t.Fatalf("G1 runtime evidence: %v", err)
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("G1 runtime evidence: open MySQL connection: %v", err)
	}
	defer db.Close()
	var mysqlVersion string
	var currentDatabase sql.NullString
	if err := db.QueryRowContext(ctx, "SELECT VERSION(), DATABASE()").Scan(&mysqlVersion, &currentDatabase); err != nil {
		t.Fatalf("G1 runtime evidence: query MySQL version/database at %s(%s): %v", target.Network, target.Endpoint, err)
	}
	if !currentDatabase.Valid || currentDatabase.String != target.Database {
		t.Fatalf("G1 runtime evidence: live MySQL database=%q, parsed DSN database=%q", currentDatabase.String, target.Database)
	}

	return g1RuntimeEvidence{
		RedisEndpoint:      rdb.Options().Addr,
		RedisVersion:       redisVersion,
		MySQLNetwork:       target.Network,
		MySQLEndpoint:      target.Endpoint,
		MySQLDatabase:      target.Database,
		MySQLServerVersion: strings.TrimSpace(mysqlVersion),
	}
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
	resp, raw := g1DoAuth(t, http.MethodPost, baseURL, "/v1/workflows/execute", token, body)
	if resp.StatusCode != http.StatusOK {
		return "", resp.StatusCode
	}
	out := decodeSubmitEnvelope(t, raw)
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
		// The inspect response is enveloped (spec §3); unwrap data before
		// decoding the typed ExecutionDetail.
		var env e2eEnvelope
		_ = json.Unmarshal(raw, &env)
		_ = json.Unmarshal(env.Data, &detail)
	}
	return resp.StatusCode, detail
}

// g1SignalAuth POSTs /v1/executions/{id}/signals (plural, spec §1.1) with the
// given name+data.
func g1SignalAuth(t *testing.T, baseURL, token string, id types.ExecutionID, name string, data map[string]any) (int, []byte) {
	t.Helper()
	body := map[string]any{"name": name, "data": data}
	resp, raw := g1DoAuth(t, http.MethodPost, baseURL, "/v1/executions/"+string(id)+"/signals", token, body)
	return resp.StatusCode, raw
}

// g1CancelAuth POSTs /v1/executions/{id}/cancel.
func g1CancelAuth(t *testing.T, baseURL, token string, id types.ExecutionID) (int, []byte) {
	t.Helper()
	resp, raw := g1DoAuth(t, http.MethodPost, baseURL, "/v1/executions/"+string(id)+"/cancel", token, map[string]any{})
	return resp.StatusCode, raw
}

// g1RevokeSignalAuth DELETEs /v1/executions/{id}/signals/{name} (spec §9.1:
// the §9.1 migration moved the signal name from the request body into the path
// segment and the verb from POST /revoke-signal to DELETE /signals/{name}).
func g1RevokeSignalAuth(t *testing.T, baseURL, token string, id types.ExecutionID, name string) (int, []byte) {
	t.Helper()
	resp, raw := g1DoAuth(t, http.MethodDelete, baseURL, "/v1/executions/"+string(id)+"/signals/"+name, token, nil)
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

// g1ProductionMappings returns the multi-namespace token registry used by the
// production harness.
func g1ProductionMappings() []apiserver.TokenPrincipalMapping {
	return []apiserver.TokenPrincipalMapping{
		{
			Token:     g1TokFullA,
			Subject:   "alice",
			Namespace: g1NamespaceA,
			Scopes:    []string{"workflow", "execution", "deadletter.list", "deadletter.replay", "management.read", "management.leader.read", "management.runner.read"},
		},
		{
			Token:     g1TokNoExA,
			Subject:   "bob",
			Namespace: g1NamespaceA,
			Scopes:    []string{"workflow"},
		},
		{
			Token:     g1TokFullB,
			Subject:   "carol",
			Namespace: g1NamespaceB,
			Scopes:    []string{"workflow", "execution"},
		},
		{
			Token:     g1TokDefault,
			Subject:   "dave",
			Namespace: "default",
			Scopes:    []string{"workflow", "execution", "deadletter.list", "deadletter.replay", "management.read", "management.leader.read", "management.runner.read"},
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
			"wait": {"main": {Targets: []types.Connection{{Node: "end", Input: "main"}}}},
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
			"wait": {"main": {Targets: []types.Connection{{Node: "end", Input: "main"}}}},
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
			"wait": {"main": {Targets: []types.Connection{{Node: "end", Input: "main"}}}},
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
			Namespaces:   []namespace.Namespace{"default", g1NamespaceA, g1NamespaceB},
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
	generatedRunID := uuid.NewString()
	runIdentity, strictEvidence, err := g1LoadRunIdentity(generatedRunID)
	if err != nil {
		t.Fatalf("G1 run identity: %v", err)
	}
	// Reset the artifact before optional dependency probes so a skipped ordinary
	// integration run cannot leave a stale report looking current.
	abs := resolveG1ArtifactPath(t)
	if err := os.Remove(abs); err != nil && !os.IsNotExist(err) {
		t.Fatalf("remove stale artifact %q: %v", abs, err)
	}
	t.Logf("g1 artifact reset: %s", abs)

	addr := requireRedis(t)
	dsn := requireMySQL(t)
	runtimeEvidence := g1CollectRuntimeEvidence(t, addr, dsn)

	art := &g1Artifact{
		SchemaVersion: g1ArtifactSchemaVersion,
		RunID:         runIdentity.RunID,
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339),
		GoVersion:     runtime.Version(),
		OS:            runtime.GOOS + "/" + runtime.GOARCH,
		CommitSHA:     runIdentity.CommitSHA,
		RedisAddr:     runtimeEvidence.RedisEndpoint,
		MySQLDSNHost:  runtimeEvidence.MySQLEndpoint,
		Runtime:       runtimeEvidence,
		AuthzMatrix:   []g1AuthzRow{},
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

	t.Run("GRPCRunnerConnectReportNamespaceA", func(t *testing.T) {
		tgA := g1RunGRPCRunnerConnectReportForNamespace(t, h, g1TokFullA, "g1-grpc-namespaceA")
		traceGraph.NamespaceAOneTraceID = tgA.OneTraceID
		traceGraph.NamespaceADispatchParentedToSubmit = tgA.DispatchParentedToSubmit
		traceGraph.NamespaceACommitParentedToReport = tgA.CommitParentedToReport
	})

	t.Run("CrossNamespaceCarrierIsolation", func(t *testing.T) {
		traceGraph.CrossNamespaceCarrierIsolated = g1RunCrossNamespaceCarrierIsolation(t, h)
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
	finalRunIdentity, finalStrictEvidence, err := g1LoadRunIdentity(generatedRunID)
	if err != nil {
		t.Fatalf("G1 run identity changed before publication: %v", err)
	}
	if finalRunIdentity != runIdentity || finalStrictEvidence != strictEvidence {
		t.Fatalf(
			"G1 run identity changed during execution: started=%+v strict=%t final=%+v strict=%t",
			runIdentity,
			strictEvidence,
			finalRunIdentity,
			finalStrictEvidence,
		)
	}
	if art.RunID != runIdentity.RunID || art.CommitSHA != runIdentity.CommitSHA {
		t.Fatalf("G1 artifact identity changed during assembly: artifact=%s/%s run=%s/%s", art.RunID, art.CommitSHA, runIdentity.RunID, runIdentity.CommitSHA)
	}
	art.FullWorktreeClean = g1FullWorktreeClean(t)
	if err := g1ValidateArtifactForMode(art, strictEvidence); err != nil {
		t.Fatalf("G1 artifact semantic validation failed: %v", err)
	}

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
	if strictEvidence {
		t.Logf("xflow-g1-evidence-run-id=%s", runIdentity.RunID)
	}
	t.Logf("g1 artifact written: %s", abs)
}

func g1RequiredAuthzMatrix() []g1AuthzRow {
	return []g1AuthzRow{
		{Scenario: "workflow_submit_allow", Operation: apiserver.OpWorkflowCreate, Route: "POST /v1/workflows/execute", Token: "full-A", Scope: "workflow", Expected: http.StatusOK, Got: http.StatusOK, Decision: "allow"},
		{Scenario: "workflow_submit_no_execution_scope_allow", Operation: apiserver.OpWorkflowCreate, Route: "POST /v1/workflows/execute", Token: "noexec-A", Scope: "workflow", Expected: http.StatusOK, Got: http.StatusOK, Decision: "allow"},
		{Scenario: "workflow_entry_invoke_allow", Operation: apiserver.OpWorkflowCreate, Route: "POST /v1/workflows/execute", Token: "full-A", Scope: "workflow", Expected: http.StatusOK, Got: http.StatusOK, Decision: "allow"},
		{Scenario: "execution_inspect_allow", Operation: apiserver.OpExecutionRead, Route: "GET /v1/executions/{id}", Token: "full-A", Scope: "execution", Expected: http.StatusOK, Got: http.StatusOK, Decision: "allow"},
		{Scenario: "execution_inspect_scope_deny", Operation: apiserver.OpExecutionRead, Route: "GET /v1/executions/{id}", Token: "noexec-A", Scope: "", Expected: http.StatusForbidden, Got: http.StatusForbidden, Decision: "deny"},
		{Scenario: "execution_inspect_cross_namespace_idor", Operation: apiserver.OpExecutionRead, Route: "GET /v1/executions/{id}", Token: "full-B (cross-namespace)", Scope: "execution", Expected: http.StatusNotFound, Got: http.StatusNotFound, Decision: "deny"},
		{Scenario: "execution_signal_allow", Operation: apiserver.OpExecutionSignal, Route: "POST /v1/executions/{id}/signals", Token: "full-A", Scope: "execution", Expected: http.StatusOK, Got: http.StatusOK, Decision: "allow"},
		{Scenario: "execution_signal_scope_deny", Operation: apiserver.OpExecutionSignal, Route: "POST /v1/executions/{id}/signals", Token: "noexec-A", Scope: "", Expected: http.StatusForbidden, Got: http.StatusForbidden, Decision: "deny"},
		{Scenario: "execution_revoke_allow", Operation: apiserver.OpExecutionRevoke, Route: "DELETE /v1/executions/{id}/signals/{name}", Token: "full-A", Scope: "execution", Expected: http.StatusOK, Got: http.StatusOK, Decision: "allow"},
		{Scenario: "execution_revoke_scope_deny", Operation: apiserver.OpExecutionRevoke, Route: "DELETE /v1/executions/{id}/signals/{name}", Token: "noexec-A", Scope: "", Expected: http.StatusForbidden, Got: http.StatusForbidden, Decision: "deny"},
		{Scenario: "execution_cancel_allow", Operation: apiserver.OpExecutionCancel, Route: "POST /v1/executions/{id}/cancel", Token: "full-A", Scope: "execution", Expected: http.StatusOK, Got: http.StatusOK, Decision: "allow"},
		{Scenario: "execution_cancel_scope_deny", Operation: apiserver.OpExecutionCancel, Route: "POST /v1/executions/{id}/cancel", Token: "noexec-A", Scope: "", Expected: http.StatusForbidden, Got: http.StatusForbidden, Decision: "deny"},
	}
}

func g1AuthzRowForScenario(scenario string, got int) (g1AuthzRow, bool) {
	for _, row := range g1RequiredAuthzMatrix() {
		if row.Scenario == scenario {
			row.Got = got
			return row, true
		}
	}
	return g1AuthzRow{}, false
}

// g1RunAuthzMatrix exercises the HTTP authz matrix: submit/invoke allow,
// inspect/signal/revoke/cancel allow (full-A) and deny (noexec-A lacks
// execution scope → 403), cross-namespace IDOR → 404 (no existence leak).
func g1RunAuthzMatrix(t *testing.T, h *productionServerRunnerHarness) []g1AuthzRow {
	t.Helper()
	rows := []g1AuthzRow{}
	record := func(scenario string, got int) {
		row, ok := g1AuthzRowForScenario(scenario, got)
		if !ok {
			t.Fatalf("authz scenario %q has no evidence definition", scenario)
		}
		rows = append(rows, row)
		if row.Got != row.Expected {
			t.Fatalf("authz %s: status=%d, want %d", scenario, row.Got, row.Expected)
		}
	}

	runnerCancel, runnerErrCh := g1StartRunner(t, h, "runner-g1-authz")
	defer g1StopRunner(t, runnerCancel, runnerErrCh)

	// Submit allow with full-A.
	idA, statusSubmit := g1SubmitAuth(t, h.httpSrv.URL, g1TokFullA, g1StartWorkflowDef("g1-authz-submit-allow"), nil)
	record("workflow_submit_allow", statusSubmit)

	// Submit allow with noexec-A (workflow scope present).
	_, statusSubmitNoEx := g1SubmitAuth(t, h.httpSrv.URL, g1TokNoExA, g1StartWorkflowDef("g1-authz-submit-noex"), nil)
	record("workflow_submit_no_execution_scope_allow", statusSubmitNoEx)

	// Invoke allow. Entry node must be of type "xflow.start" to be a valid
	// entry point for the invoke endpoint.
	invBody := map[string]any{
		"workflow": &types.WorkflowDef{
			Name: "g1-authz-invoke",
			Nodes: []types.NodeDef{
				{Name: "start", Type: "xflow.start"},
				{Name: "work", Type: "test.g1.real"},
			},
			Connections: types.Connections{
				"start": {"main": {Targets: []types.Connection{{Node: "work", Input: "main"}}}},
			},
		},
		"entry": "start",
		"input": map[string]any{"claim_id": "invoke-allow"},
	}
	resp, _ := g1DoAuth(t, http.MethodPost, h.httpSrv.URL, "/v1/workflows/execute", g1TokFullA, invBody)
	record("workflow_entry_invoke_allow", resp.StatusCode)

	// Inspect allow (full-A on its own namespace's execution).
	statusInspect, _ := g1InspectAuth(t, h.httpSrv.URL, g1TokFullA, idA)
	record("execution_inspect_allow", statusInspect)

	// Inspect deny (noexec-A lacks execution scope) → 403.
	statusInspectDeny, _ := g1InspectAuth(t, h.httpSrv.URL, g1TokNoExA, idA)
	record("execution_inspect_scope_deny", statusInspectDeny)

	// Cross-namespace IDOR: namespaceB token inspecting namespaceA's execution → 404.
	statusCross, _ := g1InspectAuth(t, h.httpSrv.URL, g1TokFullB, idA)
	record("execution_inspect_cross_namespace_idor", statusCross)

	// Signal allow uses its own suspended execution so a successful response
	// proves the authorized mutation reached a live target, not merely authz.
	signalID := g1SubmitAllowed(t, h.httpSrv.URL, g1TokFullA, g1SignalWaitDef("g1-authz-signal-allow", "approve"))
	g1WaitForNodeStatus(t, h.httpSrv.URL, g1TokFullA, signalID, "wait", []types.NodeStatus{types.NodeStatusSuspended, types.NodeStatusWaiting}, 5*time.Second)
	statusSigAllow, _ := g1SignalAuth(t, h.httpSrv.URL, g1TokFullA, signalID, "approve", map[string]any{"decision": "allow"})
	record("execution_signal_allow", statusSigAllow)
	if detail := g1WaitForTerminal(t, h.httpSrv.URL, g1TokFullA, signalID, 10*time.Second); detail.Status != types.ExecutionStatusSuccess {
		t.Fatalf("signal allow execution status=%s, want success", detail.Status)
	}

	// Signal deny (noexec-A lacks execution scope) → 403.
	statusSigDeny, _ := g1SignalAuth(t, h.httpSrv.URL, g1TokNoExA, idA, "noop", nil)
	record("execution_signal_scope_deny", statusSigDeny)

	// Revoke allow uses a second suspended execution. A non-waited signal is
	// stored without resuming the node, then the authorized DELETE revokes it.
	revokeID := g1SubmitAllowed(t, h.httpSrv.URL, g1TokFullA, g1SignalWaitDef("g1-authz-revoke-allow", "finish"))
	g1WaitForNodeStatus(t, h.httpSrv.URL, g1TokFullA, revokeID, "wait", []types.NodeStatus{types.NodeStatusSuspended, types.NodeStatusWaiting}, 5*time.Second)
	if setupStatus, _ := g1SignalAuth(t, h.httpSrv.URL, g1TokFullA, revokeID, "revocable", map[string]any{"setup": true}); setupStatus != http.StatusOK {
		t.Fatalf("revoke allow setup signal: status=%d, want %d", setupStatus, http.StatusOK)
	}
	statusRevAllow, _ := g1RevokeSignalAuth(t, h.httpSrv.URL, g1TokFullA, revokeID, "revocable")
	record("execution_revoke_allow", statusRevAllow)

	// Revoke deny (noexec-A) → 403.
	statusRevDeny, _ := g1RevokeSignalAuth(t, h.httpSrv.URL, g1TokNoExA, idA, "noop")
	record("execution_revoke_scope_deny", statusRevDeny)
	if finishStatus, _ := g1SignalAuth(t, h.httpSrv.URL, g1TokFullA, revokeID, "finish", nil); finishStatus != http.StatusOK {
		t.Fatalf("revoke allow cleanup signal: status=%d, want %d", finishStatus, http.StatusOK)
	}
	if detail := g1WaitForTerminal(t, h.httpSrv.URL, g1TokFullA, revokeID, 10*time.Second); detail.Status != types.ExecutionStatusSuccess {
		t.Fatalf("revoke allow execution status=%s, want success", detail.Status)
	}

	// Cancel allow uses a third suspended execution and verifies the mutation's
	// terminal effect after the HTTP 200.
	cancelID := g1SubmitAllowed(t, h.httpSrv.URL, g1TokFullA, g1SignalWaitDef("g1-authz-cancel-allow", "never"))
	g1WaitForNodeStatus(t, h.httpSrv.URL, g1TokFullA, cancelID, "wait", []types.NodeStatus{types.NodeStatusSuspended, types.NodeStatusWaiting}, 5*time.Second)
	statusCancelAllow, _ := g1CancelAuth(t, h.httpSrv.URL, g1TokFullA, cancelID)
	record("execution_cancel_allow", statusCancelAllow)
	if detail := g1WaitForTerminal(t, h.httpSrv.URL, g1TokFullA, cancelID, 10*time.Second); detail.Status != types.ExecutionStatusCanceled {
		t.Fatalf("cancel allow execution status=%s, want canceled", detail.Status)
	}

	// Cancel deny (noexec-A lacks execution scope) → 403.
	statusCancelDeny, _ := g1CancelAuth(t, h.httpSrv.URL, g1TokNoExA, idA)
	record("execution_cancel_scope_deny", statusCancelDeny)

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
			Namespaces:   []namespace.Namespace{"default", g1NamespaceA, g1NamespaceB},
		},
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- runner.Run(ctx) }()
	waitForE2ERunner(t, h.runners, "runner-g1-grpc")

	// R4 (2026-07-20): the pollTask namespace injection (service/control/core.go)
	// now propagates Assignment.Namespace into the engine context, so the W3C
	// carrier is read from the correct Redis namespace for both the default
	// namespace and non-default namespaces. The trace-graph assertions below are
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
		case "xflow.workflow.execute", "xflow.task.dispatch", "xflow.task.execute", "xflow.task.report", "xflow.task.commit":
			if _, ok := byName[s.Name()]; !ok {
				byName[s.Name()] = s
			}
		}
	}
	tg := g1TraceGraph{SpansPresent: []string{}}
	for _, name := range []string{"xflow.workflow.execute", "xflow.task.dispatch", "xflow.task.execute", "xflow.task.report", "xflow.task.commit"} {
		if byName[name] == nil {
			t.Fatalf("missing span %q; got %v", name, spanNamesIntegration(spans))
		}
		tg.SpansPresent = append(tg.SpansPresent, name)
	}

	submit := byName["xflow.workflow.execute"]
	dispatch := byName["xflow.task.dispatch"]
	report := byName["xflow.task.report"]
	commit := byName["xflow.task.commit"]

	// R4 (2026-07-20): strong assertions — no WARN degradation. The pollTask
	// namespace injection (service/control/core.go) propagates
	// Assignment.Namespace into the engine context so the W3C carrier is read
	// from the correct Redis namespace. All 5 spans must share one TraceID,
	// dispatch must be parented to submit (real W3C remote parent via persisted
	// carrier), execute to dispatch (lease carrier), report to execute (gRPC
	// report carrier), and commit to report.
	root := submit.SpanContext().TraceID()
	tg.OneTraceID = true
	for _, s := range []sdktrace.ReadOnlySpan{dispatch, byName["xflow.task.execute"], report, commit} {
		if s.SpanContext().TraceID() != root {
			t.Fatalf("span %q trace %s != submit trace %s (trace graph broken; pollTask namespace injection or carrier extraction failed)", s.Name(), s.SpanContext().TraceID(), root)
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

// g1RunGRPCRunnerConnectReportForNamespace is the namespaceA variant of
// g1RunGRPCRunnerConnectReport. It creates its own gRPC server + runner,
// submits a workflow with the given token, waits for terminal Success, then
// strongly asserts the B1 trace graph (5 spans, one TraceID, 4 parent edges
// via W3C carriers). Strong assertions (t.Fatalf) — no WARN degradation.
//
// The span recorder is global, so the function filters spans by the submit
// span's TraceID to exclude spans from earlier subtests' workflows that the
// runner may have drained.
func g1RunGRPCRunnerConnectReportForNamespace(t *testing.T, h *productionServerRunnerHarness, token, wfName string) g1TraceGraph {
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
			Namespaces:   []namespace.Namespace{"default", g1NamespaceA, g1NamespaceB},
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
	// xflow.workflow.execute span. Filter downstream spans by its TraceID
	// to exclude orphaned spans from earlier subtests.
	var submit sdktrace.ReadOnlySpan
	for i := len(spans) - 1; i >= 0; i-- {
		if spans[i].Name() == "xflow.workflow.execute" {
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
	tg := g1TraceGraph{SpansPresent: []string{"xflow.workflow.execute"}}
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

// g1RunCrossNamespaceCarrierIsolation submits namespaceA and namespaceB workflows
// concurrently, waits for both to reach terminal Success via a shared gRPC
// runner, then asserts the carrier lookup did not cross namespace boundaries:
// each dispatch span is parented to one of the two submit spans, the two
// dispatch spans sit in different traces, and each dispatch is parented to a
// distinct submit.
//
// g1DoAuth/g1SubmitAuth call t.Fatalf, which is unsafe in goroutines
// (runtime.Goexit deadlocks the waiter). g1SubmitConcurrent is a non-fatal
// submit helper so both submits can run concurrently.
func g1RunCrossNamespaceCarrierIsolation(t *testing.T, h *productionServerRunnerHarness) bool {
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
			RunnerID:     "runner-g1-cross-namespace",
			Concurrency:  1,
			Capabilities: []protocol.Capability{{NodeType: "test.g1.real"}, {NodeType: "xflow.wait"}},
			PollWait:     5 * time.Millisecond,
			Tracer:       h.tracer,
			Namespaces:   []namespace.Namespace{"default", g1NamespaceA, g1NamespaceB},
		},
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- runner.Run(ctx) }()
	waitForE2ERunner(t, h.runners, "runner-g1-cross-namespace")

	// Submit namespaceA and namespaceB workflows concurrently so both carriers sit
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
		idA, stA = g1SubmitConcurrent(h.httpSrv.URL, g1TokFullA, g1StartWorkflowDef("g1-x-namespaceA"))
	}()
	go func() {
		defer wg.Done()
		idB, stB = g1SubmitConcurrent(h.httpSrv.URL, g1TokFullB, g1StartWorkflowDef("g1-x-namespaceB"))
	}()
	wg.Wait()
	if stA != 200 || idA == "" {
		t.Fatalf("cross-namespace namespaceA concurrent submit: status=%d id=%q", stA, idA)
	}
	if stB != 200 || idB == "" {
		t.Fatalf("cross-namespace namespaceB concurrent submit: status=%d id=%q", stB, idB)
	}

	detailA := g1WaitForTerminal(t, h.httpSrv.URL, g1TokFullA, idA, 15*time.Second)
	if detailA.Status != types.ExecutionStatusSuccess {
		t.Fatalf("cross-namespace namespaceA execution status = %s, want success", detailA.Status)
	}
	detailB := g1WaitForTerminal(t, h.httpSrv.URL, g1TokFullB, idB, 15*time.Second)
	if detailB.Status != types.ExecutionStatusSuccess {
		t.Fatalf("cross-namespace namespaceB execution status = %s, want success", detailB.Status)
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
	// Find the two most recent submit spans (namespaceA + namespaceB).
	var submits []sdktrace.ReadOnlySpan
	for i := len(spans) - 1; i >= 0 && len(submits) < 2; i-- {
		if spans[i].Name() == "xflow.workflow.execute" {
			submits = append(submits, spans[i])
		}
	}
	if len(submits) != 2 {
		t.Fatalf("cross-namespace: expected 2 submit spans, got %d (%v)", len(submits), spanNamesIntegration(spans))
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
		t.Fatalf("cross-namespace: expected 2 dispatch spans matching the 2 submit traces, got %d (%v)", len(dispatches), spanNamesIntegration(spans))
	}

	// Each dispatch must be parented to one of the two submits, and the
	// dispatch's trace ID must equal that submit's trace ID. If the carrier
	// crossed namespaces, a dispatch would be parented to the wrong submit (or
	// not parented to any submit at all).
	for _, d := range dispatches {
		parentID := d.Parent().SpanID()
		matched := false
		for _, s := range submits {
			if parentID == s.SpanContext().SpanID() {
				matched = true
				if d.SpanContext().TraceID() != s.SpanContext().TraceID() {
					t.Fatalf("cross-namespace: dispatch trace %s != parent submit trace %s (carrier crossed namespaces)", d.SpanContext().TraceID(), s.SpanContext().TraceID())
				}
				break
			}
		}
		if !matched {
			t.Fatalf("cross-namespace: dispatch parent %s does not match any submit span ID (carrier orphaned or crossed)", parentID)
		}
	}

	// The two dispatch spans must sit in different traces — proves namespaceA's
	// dispatch did not inherit namespaceB's submit trace (and vice versa).
	if dispatches[0].SpanContext().TraceID() == dispatches[1].SpanContext().TraceID() {
		t.Fatalf("cross-namespace: both dispatch spans share trace %s — carrier crossed namespaces", dispatches[0].SpanContext().TraceID())
	}

	// Each dispatch must be parented to a distinct submit (no both-to-same
	// short-circuit).
	if dispatches[0].Parent().SpanID() == dispatches[1].Parent().SpanID() {
		t.Fatalf("cross-namespace: both dispatch spans parented to the same submit %s — one namespace's carrier leaked into the other", dispatches[0].Parent().SpanID())
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
	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/workflows/execute", bytes.NewReader(raw))
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
	var env e2eEnvelope
	if err := json.Unmarshal(payload, &env); err != nil {
		return "", resp.StatusCode
	}
	var out e2eSubmitResp
	if err := json.Unmarshal(env.Data, &out); err != nil {
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
// Uses the "default" namespace because the timer outbox entry is flushed by the
// background OutboxDispatcher which runs without namespace context (defaults to
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
			"start":  {"main": {Targets: []types.Connection{{Node: "review", Input: "main"}}}},
			"review": {"reject": {Targets: []types.Connection{{Node: "start", Input: "main"}}}},
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

	// Submit a workflow under the default namespace so the execution exists in
	// authoritative Redis state with status=running. g1TokDefault carries
	// namespace=default, which matches namespace.FromContext(ctx) when RecordOutboxFailure
	// is later called with a background context (defaults to default namespace).
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
	namespaceID := "default"
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
	bodyKey := fmt.Sprintf("xflow:ns:%s:exec:{%s}:outbox:body", namespaceID, execID)
	readyKey := fmt.Sprintf("xflow:ns:%s:exec:{%s}:outbox:ready", namespaceID, execID)
	nodeStatus := fmt.Sprintf("xflow:ns:%s:exec:{%s}:node:%s:status", namespaceID, execID, "start")
	nodeMeta := fmt.Sprintf("xflow:ns:%s:exec:{%s}:node:%s:meta", namespaceID, execID, "start")
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
	// The management dead-letter list success body is enveloped (spec §3.1,
	// Step 1 decision A of the api-specification rollout): the cursor-paginated
	// {entries,next_cursor} payload rides inside envelope.data. Unwrap data
	// before decoding the typed list shape — the cursor pagination shape itself
	// (§3.3 exception) is preserved inside data, not replaced.
	var listEnvelope e2eEnvelope
	_ = json.Unmarshal(listBody, &listEnvelope)
	var listParsed struct {
		Entries []struct {
			ID string `json:"id"`
		} `json:"entries"`
	}
	_ = json.Unmarshal(listEnvelope.Data, &listParsed)
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
	// The replay result is enveloped (spec §3.1): unwrap data before decoding
	// the typed replay response.
	var replayEnvelope e2eEnvelope
	_ = json.Unmarshal(replayRaw, &replayEnvelope)
	var replayRespParsed struct {
		Outcome      string `json:"outcome"`
		AuditID      string `json:"audit_id,omitempty"`
		ExecutionID  string `json:"execution_id,omitempty"`
		NodeID       string `json:"node_id,omitempty"`
		ActivationID string `json:"activation_id,omitempty"`
	}
	_ = json.Unmarshal(replayEnvelope.Data, &replayRespParsed)
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

// g1ReconcileBacklogAge is shared by eligibility polling, backlog counts, and
// the workers below. Keeping one cutoff rule prevents a row from being counted
// as pending before it is old enough for the worker to scan.
const g1ReconcileBacklogAge = time.Millisecond

// g1RunAuditReconcile asserts the T9 worker settles admitted mutations with
// the production authority and the default (256-row) cursor batch.
func g1RunAuditReconcile(t *testing.T, h *productionServerRunnerHarness) g1AuditReconcile {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Keep one execution alive in Redis. Both the convergence target and its
	// fillers use workflow.create against this execution, so the real authority
	// can confirm and settle every test-owned admission rather than leaving
	// permanent Indeterminate residue.
	execID := g1SubmitAllowed(t, h.httpSrv.URL, g1TokFullA, g1SignalWaitDef("g1-r33-conv-wf", "r33-conv-never"))
	tCtx := namespace.WithNamespace(ctx, namespace.Namespace(g1NamespaceA))

	appender, ok := interface{}(h.provider).(store.AuditAppender)
	if !ok {
		t.Fatalf("provider does not implement store.AuditAppender")
	}
	ar, ok := interface{}(h.provider).(store.AuditReconciler)
	if !ok {
		t.Fatalf("provider does not implement store.AuditReconciler")
	}

	nonce := fmt.Sprintf("%d", time.Now().UnixNano())
	runPrefix := "r33-conv-" + nonce
	ownedSeqIDs := make(map[string]uint64, control.DefaultReconcileBatch+1)
	fillerCount := 0
	eligibleBeforeTarget := 0

	// Fill only the current eligible-backlog gap. On a clean database this
	// injects exactly DefaultReconcileBatch rows before the target; on a dirty
	// database it avoids gratuitously adding another full batch. Re-count after
	// each top-up so a target is never appended until a full eligible page
	// actually precedes it.
	for {
		var err error
		eligibleBeforeTarget, _, err = ar.CountUnreconciledAdmissions(ctx, time.Now().Add(-g1ReconcileBacklogAge))
		if err != nil {
			t.Fatalf("count eligible admissions before convergence target: %v", err)
		}
		if eligibleBeforeTarget >= control.DefaultReconcileBatch {
			break
		}

		gap := control.DefaultReconcileBatch - eligibleBeforeTarget
		if fillerCount+gap > 2*control.DefaultReconcileBatch {
			t.Fatalf("eligible convergence backlog kept shrinking while filling: already injected=%d next_gap=%d", fillerCount, gap)
		}
		batchSeqIDs := make(map[string]uint64, gap)
		for i := 0; i < gap; i++ {
			reqID := fmt.Sprintf("%s-fill-%03d", runPrefix, fillerCount+i)
			rec := &store.AuditRecord{
				RequestID:   reqID,
				Principal:   "g1-r33-convergence-filler",
				Namespace:   g1NamespaceA,
				Operation:   "workflow.create",
				Resource:    "g1-r33-conv-wf",
				ExecutionID: string(execID),
				Decision:    "allow",
				Outcome:     store.AuditOutcomeAdmitted,
				Phase:       store.AuditPhaseAdmission,
				Timestamp:   time.Now().Add(-time.Minute),
			}
			if err := appender.AppendAudit(tCtx, rec); err != nil {
				t.Fatalf("inject convergence filler %q: %v", reqID, err)
			}
			if rec.ID == 0 {
				t.Fatalf("inject convergence filler %q returned zero SQL id", reqID)
			}
			batchSeqIDs[reqID] = rec.ID
			ownedSeqIDs[reqID] = rec.ID
		}
		fillerCount += gap
		g1WaitForEligibleAdmissions(t, ctx, ar, batchSeqIDs, g1ReconcileBacklogAge)
	}

	// Append the target only after the preceding page is demonstrably eligible.
	convReqID := runPrefix + "-target"
	convRec := &store.AuditRecord{
		RequestID:   convReqID,
		Principal:   "g1-r33-test",
		Namespace:   g1NamespaceA,
		Operation:   "workflow.create",
		Resource:    "g1-r33-conv-wf",
		ExecutionID: string(execID),
		Decision:    "allow",
		Outcome:     store.AuditOutcomeAdmitted,
		Phase:       store.AuditPhaseAdmission,
		Timestamp:   time.Now().Add(-time.Minute),
	}
	if err := appender.AppendAudit(tCtx, convRec); err != nil {
		t.Fatalf("inject convergence target admission: %v", err)
	}
	if convRec.ID == 0 {
		t.Fatalf("inject convergence target %q returned zero SQL id", convReqID)
	}
	ownedSeqIDs[convReqID] = convRec.ID

	// List polling exercises SQL's real created_at predicate. Timestamp above is
	// intentionally old but does not control SQL eligibility; created_at does.
	eligibleOwned := g1WaitForEligibleAdmissions(t, ctx, ar, ownedSeqIDs, g1ReconcileBacklogAge)
	targetSeqID := eligibleOwned[convReqID].SeqID
	for reqID, seqID := range ownedSeqIDs {
		if reqID != convReqID && seqID >= targetSeqID {
			t.Fatalf("convergence filler %q seq=%d does not precede target seq=%d", reqID, seqID, targetSeqID)
		}
	}

	initialPending, _, err := ar.CountUnreconciledAdmissions(ctx, time.Now().Add(-g1ReconcileBacklogAge))
	if err != nil {
		t.Fatalf("count eligible convergence backlog: %v", err)
	}
	if initialPending <= control.DefaultReconcileBatch {
		t.Fatalf("eligible convergence backlog=%d, want > default batch %d (target must be on a later page)",
			initialPending, control.DefaultReconcileBatch)
	}
	requiredSweeps := (initialPending-1)/control.DefaultReconcileBatch + 1
	if requiredSweeps < 2 {
		t.Fatalf("convergence sweep budget=%d, want at least 2", requiredSweeps)
	}
	const maxSweepBudget = 256
	if requiredSweeps > maxSweepBudget {
		t.Fatalf("eligible audit backlog requires %d sweeps for %d rows (batch=%d), exceeds hard limit %d",
			requiredSweeps, initialPending, control.DefaultReconcileBatch, maxSweepBudget)
	}

	trackedIDs := g1RequestIDSet(ownedSeqIDs)
	observedAudit := newG1ObservedAuditReconciler(ar, convReqID, trackedIDs)
	obs := newG1ReconcileObserver(trackedIDs)
	authority := control.NewExecutionAuthority(h.srv.Backend().State())

	// Batch is deliberately omitted: this must exercise the production default.
	w := control.NewAuditReconcileWorker(observedAudit, authority, control.AuditReconcileConfig{
		Elector:    prodLeaderGateAdapter{isLeader: h.srv.IsLeader},
		BacklogAge: g1ReconcileBacklogAge,
		Period:     10 * time.Millisecond,
		Observer:   g1FanoutReconcileObservers(obs, obsmetrics.NewReconcileMetrics(h.metrics)),
	})

	sweeps := 0
	targetSettled := false
	for sweeps < requiredSweeps {
		result := g1RunCheckedReconcileSweep(t, ctx, w, obs)
		sweeps++ // Increment only after a scan and its context/error checks pass.
		calls := observedAudit.appendObservations()[convReqID]
		if len(calls) == 0 {
			continue
		}
		if len(calls) != 1 || calls[0].err != nil || !calls[0].appended {
			t.Fatalf("convergence target append calls=%+v, want one successful real append", calls)
		}
		targetSettled = true
		t.Logf("g1AuditReconcile: target settled on sweep %d (settled_this_sweep=%d, eligible_pending=%d, fillers=%d)",
			sweeps, result.settled, initialPending, fillerCount)
		break
	}
	if !targetSettled {
		t.Fatalf("convergence target %q was not appended after %d successful sweeps (eligible_pending=%d, budget=%d)",
			convReqID, sweeps, initialPending, requiredSweeps)
	}
	if sweeps < 2 {
		t.Fatalf("convergence target settled in %d sweep(s), want at least 2 to prove cursor advancement", sweeps)
	}

	listCalls := observedAudit.listObservations()
	if len(listCalls) != sweeps {
		t.Fatalf("convergence List calls=%d, successful sweeps=%d", len(listCalls), sweeps)
	}
	first := listCalls[0]
	if first.err != nil || first.afterSeqID != 0 || first.returned != control.DefaultReconcileBatch || first.containsTarget {
		t.Fatalf("first default-batch List={after:%d returned:%d target:%t err:%v}, want {after:0 returned:%d target:false err:nil}",
			first.afterSeqID, first.returned, first.containsTarget, first.err, control.DefaultReconcileBatch)
	}
	targetListCalls := 0
	for i, call := range listCalls {
		if call.err != nil {
			t.Fatalf("convergence List call %d: %v", i+1, call.err)
		}
		if !call.containsTarget {
			continue
		}
		targetListCalls++
		if call.afterSeqID == 0 {
			t.Fatalf("convergence target appeared in List call %d with zero cursor", i+1)
		}
	}
	if targetListCalls != 1 {
		t.Fatalf("convergence target appeared in %d List calls, want exactly 1", targetListCalls)
	}
	g1AssertNoAuditCountErrors(t, observedAudit.countObservations())

	// Every owned filler and the target must have gone through the real append
	// path exactly once, and none may remain as durable pending pollution.
	expectedOwnedAppends := make(map[string]bool, len(ownedSeqIDs))
	for reqID := range ownedSeqIDs {
		expectedOwnedAppends[reqID] = true
	}
	g1AssertAuditAppends(t, observedAudit.appendObservations(), expectedOwnedAppends)
	g1AssertPendingAdmissions(t, ctx, ar, ownedSeqIDs, g1ReconcileBacklogAge, nil)

	// An extra checked sweep must neither attempt nor append any owned outcome.
	g1RunCheckedReconcileSweep(t, ctx, w, obs)
	g1AssertNoAuditCountErrors(t, observedAudit.countObservations())
	g1AssertAuditAppends(t, observedAudit.appendObservations(), expectedOwnedAppends)
	g1AssertPendingAdmissions(t, ctx, ar, ownedSeqIDs, g1ReconcileBacklogAge, nil)
	t.Logf("g1AuditReconcile: default_batch=%d sweeps=%d test_owned=%d idempotent_pass=true",
		control.DefaultReconcileBatch, sweeps, len(ownedSeqIDs))

	faultMatrixPass := g1RunAuditReconcileFaultMatrix(t, h, execID)

	return g1AuditReconcile{
		AdmissionRows:            len(ownedSeqIDs),
		OutcomeRows:              len(ownedSeqIDs),
		ReconciledByWorker:       len(ownedSeqIDs),
		IdempotentOutcomeAppends: true,
		SweepsToSettle:           sweeps,
		FaultMatrixPass:          faultMatrixPass,
	}
}

// g1WaitForEligibleAdmissions polls the real reconciler until every exact
// SQL row is visible through created_at < now-BacklogAge. It deliberately
// does not sleep for a guessed creation delay: each wait follows a failed
// visibility query and is context-cancellable.
func g1WaitForEligibleAdmissions(
	t *testing.T,
	ctx context.Context,
	ar store.AuditReconciler,
	seqByRequestID map[string]uint64,
	backlogAge time.Duration,
) map[string]*store.AuditRecord {
	t.Helper()
	for {
		if err := ctx.Err(); err != nil {
			t.Fatalf("wait for eligible audit admissions: %v", err)
		}
		rows, err := g1ListTrackedAdmissions(ctx, ar, time.Now().Add(-backlogAge), 0, len(seqByRequestID), seqByRequestID)
		if err != nil {
			t.Fatalf("list eligible audit admissions: %v", err)
		}
		found := make(map[string]*store.AuditRecord, len(rows))
		for _, rec := range rows {
			found[rec.RequestID] = rec
		}
		if len(found) == len(seqByRequestID) {
			return found
		}

		missing := g1MissingRequestIDs(seqByRequestID, found)
		timer := time.NewTimer(5 * time.Millisecond)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			t.Fatalf("wait for eligible audit admissions %v: %v", missing, ctx.Err())
		case <-timer.C:
		}
	}
}

// g1ListTrackedAdmissions scans only the SeqID window occupied by exact
// test-owned rows. It still delegates every page to the production List
// implementation, including its real SQL eligibility and NOT EXISTS logic.
func g1ListTrackedAdmissions(
	ctx context.Context,
	ar store.AuditReconciler,
	before time.Time,
	afterSeqID uint64,
	limit int,
	seqByRequestID map[string]uint64,
) ([]*store.AuditRecord, error) {
	if ar == nil {
		return nil, fmt.Errorf("list tracked admissions: nil reconciler")
	}
	minSeqID, maxSeqID, err := g1AuditSeqWindow(seqByRequestID)
	if err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = control.DefaultReconcileBatch
	}
	if afterSeqID >= maxSeqID {
		return nil, nil
	}
	cursor := afterSeqID
	if floor := minSeqID - 1; cursor < floor {
		cursor = floor
	}

	out := make([]*store.AuditRecord, 0, min(limit, len(seqByRequestID)))
	for len(out) < limit {
		page, err := ar.ListUnreconciledAdmissions(ctx, before, cursor, limit)
		if err != nil {
			return nil, err
		}
		if len(page) == 0 {
			return out, nil
		}

		lastSeqID := cursor
		for _, rec := range page {
			if rec == nil {
				return nil, fmt.Errorf("list tracked admissions: nil row after seq %d", lastSeqID)
			}
			if rec.SeqID <= lastSeqID {
				return nil, fmt.Errorf("list tracked admissions: non-advancing seq %d after %d", rec.SeqID, lastSeqID)
			}
			lastSeqID = rec.SeqID
			if rec.SeqID > maxSeqID {
				return out, nil
			}
			if expectedSeqID, ok := seqByRequestID[rec.RequestID]; ok && expectedSeqID == rec.SeqID {
				out = append(out, rec)
				if len(out) == limit {
					return out, nil
				}
			}
		}

		cursor = lastSeqID
		if cursor >= maxSeqID || len(page) < limit {
			return out, nil
		}
	}
	return out, nil
}

func g1AuditSeqWindow(seqByRequestID map[string]uint64) (uint64, uint64, error) {
	if len(seqByRequestID) == 0 {
		return 0, 0, fmt.Errorf("audit seq window: no request ids")
	}
	var minSeqID, maxSeqID uint64
	for reqID, seqID := range seqByRequestID {
		if seqID == 0 {
			return 0, 0, fmt.Errorf("audit seq window: request %q has zero seq id", reqID)
		}
		if minSeqID == 0 || seqID < minSeqID {
			minSeqID = seqID
		}
		if seqID > maxSeqID {
			maxSeqID = seqID
		}
	}
	return minSeqID, maxSeqID, nil
}

func g1RequestIDSet(seqByRequestID map[string]uint64) map[string]struct{} {
	out := make(map[string]struct{}, len(seqByRequestID))
	for reqID := range seqByRequestID {
		out[reqID] = struct{}{}
	}
	return out
}

func g1CloneRequestIDSet(in map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{}, len(in))
	for reqID := range in {
		out[reqID] = struct{}{}
	}
	return out
}

func g1CloneSeqByRequestID(in map[string]uint64) map[string]uint64 {
	out := make(map[string]uint64, len(in))
	for reqID, seqID := range in {
		out[reqID] = seqID
	}
	return out
}

func g1MissingRequestIDs(want map[string]uint64, got map[string]*store.AuditRecord) []string {
	missing := make([]string, 0)
	for reqID := range want {
		if _, ok := got[reqID]; !ok {
			missing = append(missing, reqID)
		}
	}
	sort.Strings(missing)
	return missing
}

func g1AuditRecordRequestIDs(rows []*store.AuditRecord) []string {
	ids := make([]string, 0, len(rows))
	for _, rec := range rows {
		if rec != nil {
			ids = append(ids, rec.RequestID)
		}
	}
	sort.Strings(ids)
	return ids
}

type g1AuditListObservation struct {
	afterSeqID     uint64
	returned       int
	containsTarget bool
	requestIDs     []string
	err            error
}

type g1AuditAppendObservation struct {
	appended bool
	err      error
}

type g1AuditCountObservation struct {
	before  time.Time
	pending int
	err     error
}

// g1ObservedAuditReconciler records the exact cursor calls and append results
// while delegating all behavior to the production SQL reconciler.
type g1ObservedAuditReconciler struct {
	store.AuditReconciler
	targetRequestID   string
	trackedRequestIDs map[string]struct{}

	mu      sync.Mutex
	lists   []g1AuditListObservation
	appends map[string][]g1AuditAppendObservation
	counts  []g1AuditCountObservation
}

func newG1ObservedAuditReconciler(
	ar store.AuditReconciler,
	targetRequestID string,
	trackedRequestIDs map[string]struct{},
) *g1ObservedAuditReconciler {
	return &g1ObservedAuditReconciler{
		AuditReconciler:   ar,
		targetRequestID:   targetRequestID,
		trackedRequestIDs: g1CloneRequestIDSet(trackedRequestIDs),
		appends:           make(map[string][]g1AuditAppendObservation),
	}
}

func (r *g1ObservedAuditReconciler) ListUnreconciledAdmissions(
	ctx context.Context,
	before time.Time,
	afterSeqID uint64,
	limit int,
) ([]*store.AuditRecord, error) {
	rows, err := r.AuditReconciler.ListUnreconciledAdmissions(ctx, before, afterSeqID, limit)
	observation := g1AuditListObservation{
		afterSeqID: afterSeqID,
		returned:   len(rows),
		err:        err,
	}
	for _, rec := range rows {
		if rec == nil {
			continue
		}
		observation.requestIDs = append(observation.requestIDs, rec.RequestID)
		if rec.RequestID == r.targetRequestID {
			observation.containsTarget = true
		}
	}
	r.mu.Lock()
	r.lists = append(r.lists, observation)
	r.mu.Unlock()
	return rows, err
}

func (r *g1ObservedAuditReconciler) AppendOutcomeIfAbsent(ctx context.Context, rec *store.AuditRecord) (bool, error) {
	appended, err := r.AuditReconciler.AppendOutcomeIfAbsent(ctx, rec)
	if rec != nil {
		if _, tracked := r.trackedRequestIDs[rec.RequestID]; tracked {
			r.mu.Lock()
			r.appends[rec.RequestID] = append(r.appends[rec.RequestID], g1AuditAppendObservation{appended: appended, err: err})
			r.mu.Unlock()
		}
	}
	return appended, err
}

func (r *g1ObservedAuditReconciler) CountUnreconciledAdmissions(
	ctx context.Context,
	before time.Time,
) (int, time.Time, error) {
	pending, oldest, err := r.AuditReconciler.CountUnreconciledAdmissions(ctx, before)
	r.mu.Lock()
	r.counts = append(r.counts, g1AuditCountObservation{before: before, pending: pending, err: err})
	r.mu.Unlock()
	return pending, oldest, err
}

func (r *g1ObservedAuditReconciler) listObservations() []g1AuditListObservation {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]g1AuditListObservation, len(r.lists))
	copy(out, r.lists)
	for i := range out {
		out[i].requestIDs = append([]string(nil), out[i].requestIDs...)
	}
	return out
}

func (r *g1ObservedAuditReconciler) appendObservations() map[string][]g1AuditAppendObservation {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string][]g1AuditAppendObservation, len(r.appends))
	for reqID, calls := range r.appends {
		out[reqID] = append([]g1AuditAppendObservation(nil), calls...)
	}
	return out
}

func (r *g1ObservedAuditReconciler) countObservations() []g1AuditCountObservation {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]g1AuditCountObservation(nil), r.counts...)
}

// g1ReconcileObserver treats a scan as successful only when the callback ran,
// the list succeeded, and no tracked request reported a probe/append error.
type g1ReconcileObserver struct {
	trackedRequestIDs map[string]struct{}

	mu                  sync.Mutex
	scanObserved        bool
	scanCandidates      int
	scanErr             error
	trackedErrRequestID string
	trackedErr          error
}

func newG1ReconcileObserver(trackedRequestIDs map[string]struct{}) *g1ReconcileObserver {
	return &g1ReconcileObserver{trackedRequestIDs: g1CloneRequestIDSet(trackedRequestIDs)}
}

func (o *g1ReconcileObserver) beginSweep() {
	o.mu.Lock()
	o.scanObserved = false
	o.scanCandidates = 0
	o.scanErr = nil
	o.trackedErrRequestID = ""
	o.trackedErr = nil
	o.mu.Unlock()
}

func (o *g1ReconcileObserver) sweepObservation() (bool, int, error, string, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.scanObserved, o.scanCandidates, o.scanErr, o.trackedErrRequestID, o.trackedErr
}

func (o *g1ReconcileObserver) OnReconcileScan(_ context.Context, candidates int, _ time.Duration, err error) {
	o.mu.Lock()
	o.scanObserved = true
	o.scanCandidates = candidates
	o.scanErr = err
	o.mu.Unlock()
}
func (o *g1ReconcileObserver) OnReconcileSettled(_ context.Context, _ string, _ bool, _ int64) {}
func (o *g1ReconcileObserver) OnReconcileSkipped(_ context.Context, _ string)                  {}
func (o *g1ReconcileObserver) OnReconcileError(_ context.Context, requestID string, err error) {
	if _, tracked := o.trackedRequestIDs[requestID]; !tracked {
		return
	}
	o.mu.Lock()
	if o.trackedErr == nil {
		o.trackedErrRequestID = requestID
		o.trackedErr = err
	}
	o.mu.Unlock()
}
func (o *g1ReconcileObserver) OnReconcileBacklog(_ context.Context, _ time.Duration, _ int) {}

// g1ReconcileObserverFanout keeps the test-owned sweep assertions and the
// production Prometheus observer on the exact same real worker callbacks.
type g1ReconcileObserverFanout []control.ReconcileObserver

func g1FanoutReconcileObservers(observers ...control.ReconcileObserver) g1ReconcileObserverFanout {
	return g1ReconcileObserverFanout(observers)
}

func (f g1ReconcileObserverFanout) OnReconcileScan(ctx context.Context, candidates int, elapsed time.Duration, err error) {
	for _, observer := range f {
		if observer != nil {
			observer.OnReconcileScan(ctx, candidates, elapsed, err)
		}
	}
}

func (f g1ReconcileObserverFanout) OnReconcileSettled(ctx context.Context, outcome string, appended bool, ageMs int64) {
	for _, observer := range f {
		if observer != nil {
			observer.OnReconcileSettled(ctx, outcome, appended, ageMs)
		}
	}
}

func (f g1ReconcileObserverFanout) OnReconcileSkipped(ctx context.Context, reason string) {
	for _, observer := range f {
		if observer != nil {
			observer.OnReconcileSkipped(ctx, reason)
		}
	}
}

func (f g1ReconcileObserverFanout) OnReconcileError(ctx context.Context, requestID string, err error) {
	for _, observer := range f {
		if observer != nil {
			observer.OnReconcileError(ctx, requestID, err)
		}
	}
}

func (f g1ReconcileObserverFanout) OnReconcileBacklog(ctx context.Context, oldestAge time.Duration, pending int) {
	for _, observer := range f {
		if observer != nil {
			observer.OnReconcileBacklog(ctx, oldestAge, pending)
		}
	}
}

type g1ReconcileSweepResult struct {
	settled    int
	candidates int
}

func g1RunCheckedReconcileSweep(
	t *testing.T,
	ctx context.Context,
	w *control.AuditReconcileWorker,
	obs *g1ReconcileObserver,
) g1ReconcileSweepResult {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("audit reconcile sweep context before scan: %v", err)
	}
	obs.beginSweep()
	settled := w.ReconcileOnce(ctx)
	if err := ctx.Err(); err != nil {
		t.Fatalf("audit reconcile sweep context after scan: %v", err)
	}
	scanObserved, candidates, scanErr, requestID, trackedErr := obs.sweepObservation()
	if !scanObserved {
		t.Fatalf("audit reconcile sweep completed without a scan (worker may not be leader)")
	}
	if scanErr != nil {
		t.Fatalf("audit reconcile scan: %v", scanErr)
	}
	if trackedErr != nil {
		t.Fatalf("audit reconcile tracked request %q: %v", requestID, trackedErr)
	}
	return g1ReconcileSweepResult{settled: settled, candidates: candidates}
}

func g1AssertNoAuditCountErrors(t *testing.T, observations []g1AuditCountObservation) {
	t.Helper()
	if len(observations) == 0 {
		t.Fatalf("audit reconcile worker made no CountUnreconciledAdmissions call")
	}
	for i, observation := range observations {
		if observation.err != nil {
			t.Fatalf("audit reconcile count call %d (before=%s pending=%d): %v",
				i+1, observation.before.Format(time.RFC3339Nano), observation.pending, observation.err)
		}
	}
}

func g1AssertAuditAppends(
	t *testing.T,
	got map[string][]g1AuditAppendObservation,
	want map[string]bool,
) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("tracked append request count=%d, want=%d", len(got), len(want))
	}
	for reqID, wantAppended := range want {
		calls := got[reqID]
		if len(calls) != 1 {
			t.Fatalf("tracked request %q append calls=%d, want=1", reqID, len(calls))
		}
		if calls[0].err != nil {
			t.Fatalf("tracked request %q append: %v", reqID, calls[0].err)
		}
		if calls[0].appended != wantAppended {
			t.Fatalf("tracked request %q appended=%t, want=%t", reqID, calls[0].appended, wantAppended)
		}
	}
}

func g1AssertPendingAdmissions(
	t *testing.T,
	ctx context.Context,
	ar store.AuditReconciler,
	seqByRequestID map[string]uint64,
	backlogAge time.Duration,
	want map[string]struct{},
) {
	t.Helper()
	rows, err := g1ListTrackedAdmissions(ctx, ar, time.Now().Add(-backlogAge), 0, len(seqByRequestID), seqByRequestID)
	if err != nil {
		t.Fatalf("list tracked pending admissions: %v", err)
	}
	got := make(map[string]struct{}, len(rows))
	for _, rec := range rows {
		got[rec.RequestID] = struct{}{}
	}
	if len(got) != len(want) {
		t.Fatalf("tracked pending admissions=%v, want=%v", g1AuditRecordRequestIDs(rows), g1SortedRequestIDSet(want))
	}
	for reqID := range want {
		if _, ok := got[reqID]; !ok {
			t.Fatalf("tracked pending admissions missing %q; got=%v", reqID, g1AuditRecordRequestIDs(rows))
		}
	}
}

func g1SortedRequestIDSet(ids map[string]struct{}) []string {
	out := make([]string, 0, len(ids))
	for reqID := range ids {
		out = append(out, reqID)
	}
	sort.Strings(out)
	return out
}

// g1ScopedAuditReconciler exposes only exact test-owned IDs in one SQL SeqID
// window. Count and List both call the production List implementation; append
// delegates to the production idempotent append implementation.
type g1ScopedAuditReconciler struct {
	underlying     store.AuditReconciler
	seqByRequestID map[string]uint64

	mu         sync.Mutex
	lists      []g1AuditListObservation
	appends    map[string][]g1AuditAppendObservation
	countCalls int
	countErr   error
}

func newG1ScopedAuditReconciler(
	ar store.AuditReconciler,
	seqByRequestID map[string]uint64,
) (*g1ScopedAuditReconciler, error) {
	if _, _, err := g1AuditSeqWindow(seqByRequestID); err != nil {
		return nil, err
	}
	return &g1ScopedAuditReconciler{
		underlying:     ar,
		seqByRequestID: g1CloneSeqByRequestID(seqByRequestID),
		appends:        make(map[string][]g1AuditAppendObservation),
	}, nil
}

func (r *g1ScopedAuditReconciler) beginSweep() {
	r.mu.Lock()
	r.lists = nil
	r.appends = make(map[string][]g1AuditAppendObservation)
	r.countCalls = 0
	r.countErr = nil
	r.mu.Unlock()
}

func (r *g1ScopedAuditReconciler) ListUnreconciledAdmissions(
	ctx context.Context,
	before time.Time,
	afterSeqID uint64,
	limit int,
) ([]*store.AuditRecord, error) {
	rows, err := g1ListTrackedAdmissions(ctx, r.underlying, before, afterSeqID, limit, r.seqByRequestID)
	observation := g1AuditListObservation{afterSeqID: afterSeqID, returned: len(rows), err: err}
	for _, rec := range rows {
		if rec != nil {
			observation.requestIDs = append(observation.requestIDs, rec.RequestID)
		}
	}
	r.mu.Lock()
	r.lists = append(r.lists, observation)
	r.mu.Unlock()
	return rows, err
}

func (r *g1ScopedAuditReconciler) AppendOutcomeIfAbsent(ctx context.Context, rec *store.AuditRecord) (bool, error) {
	appended, err := r.underlying.AppendOutcomeIfAbsent(ctx, rec)
	if rec != nil {
		if _, tracked := r.seqByRequestID[rec.RequestID]; tracked {
			r.mu.Lock()
			r.appends[rec.RequestID] = append(r.appends[rec.RequestID], g1AuditAppendObservation{appended: appended, err: err})
			r.mu.Unlock()
		}
	}
	return appended, err
}

func (r *g1ScopedAuditReconciler) CountUnreconciledAdmissions(
	ctx context.Context,
	before time.Time,
) (int, time.Time, error) {
	rows, err := g1ListTrackedAdmissions(ctx, r.underlying, before, 0, len(r.seqByRequestID), r.seqByRequestID)
	r.mu.Lock()
	r.countCalls++
	if err != nil && r.countErr == nil {
		r.countErr = err
	}
	r.mu.Unlock()
	// AuditRecord exposes the event timestamp, not SQL created_at. Returning a
	// zero oldest time is more honest than fabricating a backlog age in this
	// test-only scoped metrics view.
	return len(rows), time.Time{}, err
}

func (r *g1ScopedAuditReconciler) sweepObservations() (
	[]g1AuditListObservation,
	map[string][]g1AuditAppendObservation,
	int,
	error,
) {
	r.mu.Lock()
	defer r.mu.Unlock()
	lists := make([]g1AuditListObservation, len(r.lists))
	copy(lists, r.lists)
	for i := range lists {
		lists[i].requestIDs = append([]string(nil), lists[i].requestIDs...)
	}
	appends := make(map[string][]g1AuditAppendObservation, len(r.appends))
	for reqID, calls := range r.appends {
		appends[reqID] = append([]g1AuditAppendObservation(nil), calls...)
	}
	return lists, appends, r.countCalls, r.countErr
}

type g1AuthorityProbeObservation struct {
	effect control.MutationEffect
	err    error
}

// g1ObservedAdmissionAuthority records effects from the real production
// authority; it never scripts or substitutes a decision.
type g1ObservedAdmissionAuthority struct {
	underlying        control.AdmissionAuthority
	trackedRequestIDs map[string]struct{}

	mu     sync.Mutex
	probes map[string][]g1AuthorityProbeObservation
}

func newG1ObservedAdmissionAuthority(
	authority control.AdmissionAuthority,
	trackedRequestIDs map[string]struct{},
) *g1ObservedAdmissionAuthority {
	return &g1ObservedAdmissionAuthority{
		underlying:        authority,
		trackedRequestIDs: g1CloneRequestIDSet(trackedRequestIDs),
		probes:            make(map[string][]g1AuthorityProbeObservation),
	}
}

func (a *g1ObservedAdmissionAuthority) beginSweep() {
	a.mu.Lock()
	a.probes = make(map[string][]g1AuthorityProbeObservation)
	a.mu.Unlock()
}

func (a *g1ObservedAdmissionAuthority) Probe(
	ctx context.Context,
	rec *store.AuditRecord,
) (control.MutationEffect, error) {
	effect, err := a.underlying.Probe(ctx, rec)
	if rec != nil {
		if _, tracked := a.trackedRequestIDs[rec.RequestID]; tracked {
			a.mu.Lock()
			a.probes[rec.RequestID] = append(a.probes[rec.RequestID], g1AuthorityProbeObservation{effect: effect, err: err})
			a.mu.Unlock()
		}
	}
	return effect, err
}

func (a *g1ObservedAdmissionAuthority) sweepObservations() map[string][]g1AuthorityProbeObservation {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make(map[string][]g1AuthorityProbeObservation, len(a.probes))
	for reqID, calls := range a.probes {
		out[reqID] = append([]g1AuthorityProbeObservation(nil), calls...)
	}
	return out
}

func g1AssertAuthorityProbes(
	t *testing.T,
	got map[string][]g1AuthorityProbeObservation,
	want map[string]control.MutationEffect,
) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("tracked authority probe request count=%d, want=%d", len(got), len(want))
	}
	for reqID, wantEffect := range want {
		calls := got[reqID]
		if len(calls) != 1 {
			t.Fatalf("tracked request %q authority probes=%d, want=1", reqID, len(calls))
		}
		if calls[0].err != nil {
			t.Fatalf("tracked request %q authority probe: %v", reqID, calls[0].err)
		}
		if calls[0].effect != wantEffect {
			t.Fatalf("tracked request %q authority effect=%v, want=%v", reqID, calls[0].effect, wantEffect)
		}
	}
}

func g1AssertScopedSweep(
	t *testing.T,
	lists []g1AuditListObservation,
	countCalls int,
	countErr error,
	wantRequestIDs map[string]struct{},
) {
	t.Helper()
	if countCalls != 1 || countErr != nil {
		t.Fatalf("scoped Count calls=%d err=%v, want one successful call", countCalls, countErr)
	}
	if len(lists) != 1 {
		t.Fatalf("scoped worker List calls=%d, want=1", len(lists))
	}
	call := lists[0]
	if call.err != nil || call.afterSeqID != 0 || call.returned != len(wantRequestIDs) {
		t.Fatalf("scoped List={after:%d returned:%d err:%v}, want {after:0 returned:%d err:nil}",
			call.afterSeqID, call.returned, call.err, len(wantRequestIDs))
	}
	got := make(map[string]struct{}, len(call.requestIDs))
	for _, reqID := range call.requestIDs {
		got[reqID] = struct{}{}
	}
	if len(got) != len(wantRequestIDs) {
		t.Fatalf("scoped List request IDs=%v, want=%v", g1SortedRequestIDSet(got), g1SortedRequestIDSet(wantRequestIDs))
	}
	for reqID := range wantRequestIDs {
		if _, ok := got[reqID]; !ok {
			t.Fatalf("scoped List missing %q; got=%v", reqID, g1SortedRequestIDSet(got))
		}
	}
}

// g1RunAuditReconcileFaultMatrix verifies the real authority decision table
// without ever feeding shared historical backlog to Probe.
func g1RunAuditReconcileFaultMatrix(t *testing.T, h *productionServerRunnerHarness, reachableExecID types.ExecutionID) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	tCtx := namespace.WithNamespace(ctx, namespace.Namespace(g1NamespaceA))

	appender, ok := interface{}(h.provider).(store.AuditAppender)
	if !ok {
		t.Fatalf("provider does not implement store.AuditAppender")
	}
	ar, ok := interface{}(h.provider).(store.AuditReconciler)
	if !ok {
		t.Fatalf("provider does not implement store.AuditReconciler")
	}

	nonce := fmt.Sprintf("%d", time.Now().UnixNano())
	unreachableExecID := "r33-unreachable-" + nonce
	type matrixCell struct {
		operation      string
		execID         string
		reqID          string
		expectedEffect control.MutationEffect
	}
	cells := []matrixCell{
		{"workflow.create", string(reachableExecID), "r33-crash-wf-create-reach-" + nonce, control.EffectConfirmed},
		{"workflow.invoke", string(reachableExecID), "r33-crash-wf-invoke-reach-" + nonce, control.EffectConfirmed},
		{"workflow.create", unreachableExecID, "r33-crash-wf-create-unreach-" + nonce, control.EffectAbsent},
		{"workflow.invoke", unreachableExecID, "r33-crash-wf-invoke-unreach-" + nonce, control.EffectAbsent},
		{"execution.signal", string(reachableExecID), "r33-crash-ex-signal-reach-" + nonce, control.EffectConfirmed},
		{"execution.revoke", string(reachableExecID), "r33-crash-ex-revoke-reach-" + nonce, control.EffectConfirmed},
		{"execution.cancel", string(reachableExecID), "r33-crash-ex-cancel-reach-" + nonce, control.EffectConfirmed},
		{"execution.signal", unreachableExecID, "r33-crash-ex-signal-unreach-" + nonce, control.EffectIndeterminate},
		{"execution.revoke", unreachableExecID, "r33-crash-ex-revoke-unreach-" + nonce, control.EffectIndeterminate},
		{"execution.cancel", unreachableExecID, "r33-crash-ex-cancel-unreach-" + nonce, control.EffectIndeterminate},
	}

	seqByRequestID := make(map[string]uint64, len(cells))
	expectedFirstProbes := make(map[string]control.MutationEffect, len(cells))
	expectedSecondProbes := make(map[string]control.MutationEffect, 3)
	expectedFirstAppends := make(map[string]bool, 7)
	firstRequestIDs := make(map[string]struct{}, len(cells))
	secondRequestIDs := make(map[string]struct{}, 3)
	for _, cell := range cells {
		rec := &store.AuditRecord{
			RequestID:   cell.reqID,
			Principal:   "g1-r33-fault-matrix",
			Namespace:   g1NamespaceA,
			Operation:   cell.operation,
			Resource:    "r33-fault-resource",
			ExecutionID: cell.execID,
			Decision:    "allow",
			Outcome:     store.AuditOutcomeAdmitted,
			Phase:       store.AuditPhaseAdmission,
			Timestamp:   time.Now().Add(-time.Minute),
		}
		if err := appender.AppendAudit(tCtx, rec); err != nil {
			t.Fatalf("inject fault matrix row %q: %v", cell.reqID, err)
		}
		if rec.ID == 0 {
			t.Fatalf("inject fault matrix row %q returned zero SQL id", cell.reqID)
		}
		seqByRequestID[cell.reqID] = rec.ID
		expectedFirstProbes[cell.reqID] = cell.expectedEffect
		firstRequestIDs[cell.reqID] = struct{}{}
		if cell.expectedEffect == control.EffectIndeterminate {
			expectedSecondProbes[cell.reqID] = control.EffectIndeterminate
			secondRequestIDs[cell.reqID] = struct{}{}
		} else {
			expectedFirstAppends[cell.reqID] = true
		}
	}

	g1WaitForEligibleAdmissions(t, ctx, ar, seqByRequestID, g1ReconcileBacklogAge)
	scopedAudit, err := newG1ScopedAuditReconciler(ar, seqByRequestID)
	if err != nil {
		t.Fatalf("build fault-matrix audit scope: %v", err)
	}
	trackedIDs := g1RequestIDSet(seqByRequestID)
	authority := newG1ObservedAdmissionAuthority(
		control.NewExecutionAuthority(h.srv.Backend().State()),
		trackedIDs,
	)
	obs := newG1ReconcileObserver(trackedIDs)

	// One spare slot makes the first ten-row page partial, forcing cursor wrap.
	// The second sweep must therefore start at zero and expose only the three
	// rows that the first real authority pass left Indeterminate.
	matrixBatch := len(cells) + 1
	w := control.NewAuditReconcileWorker(scopedAudit, authority, control.AuditReconcileConfig{
		Elector:    prodLeaderGateAdapter{isLeader: h.srv.IsLeader},
		BacklogAge: g1ReconcileBacklogAge,
		Period:     10 * time.Millisecond,
		Batch:      matrixBatch,
		Observer:   g1FanoutReconcileObservers(obs, obsmetrics.NewReconcileMetrics(h.metrics)),
	})

	scopedAudit.beginSweep()
	authority.beginSweep()
	firstResult := g1RunCheckedReconcileSweep(t, ctx, w, obs)
	firstLists, firstAppends, firstCountCalls, firstCountErr := scopedAudit.sweepObservations()
	if firstResult.candidates != len(cells) || firstResult.settled != len(expectedFirstAppends) {
		t.Fatalf("fault-matrix first sweep candidates=%d settled=%d, want candidates=%d settled=%d",
			firstResult.candidates, firstResult.settled, len(cells), len(expectedFirstAppends))
	}
	g1AssertScopedSweep(t, firstLists, firstCountCalls, firstCountErr, firstRequestIDs)
	g1AssertAuthorityProbes(t, authority.sweepObservations(), expectedFirstProbes)
	g1AssertAuditAppends(t, firstAppends, expectedFirstAppends)
	g1AssertPendingAdmissions(t, ctx, ar, seqByRequestID, g1ReconcileBacklogAge, secondRequestIDs)

	scopedAudit.beginSweep()
	authority.beginSweep()
	secondResult := g1RunCheckedReconcileSweep(t, ctx, w, obs)
	secondLists, secondAppends, secondCountCalls, secondCountErr := scopedAudit.sweepObservations()
	if secondResult.candidates != len(secondRequestIDs) || secondResult.settled != 0 {
		t.Fatalf("fault-matrix second sweep candidates=%d settled=%d, want candidates=%d settled=0",
			secondResult.candidates, secondResult.settled, len(secondRequestIDs))
	}
	g1AssertScopedSweep(t, secondLists, secondCountCalls, secondCountErr, secondRequestIDs)
	g1AssertAuthorityProbes(t, authority.sweepObservations(), expectedSecondProbes)
	if len(secondAppends) != 0 {
		t.Fatalf("fault-matrix second sweep append calls=%d, want 0", len(secondAppends))
	}
	g1AssertPendingAdmissions(t, ctx, ar, seqByRequestID, g1ReconcileBacklogAge, secondRequestIDs)

	t.Logf("g1FaultMatrix: first page=10/11, second page=3/11; effects confirmed=5 absent=2 indeterminate=3")
	return true
}

func g1RequiredMetricFamilies() []string {
	return []string{
		"xflow_lease_acquire_duration_seconds",
		"xflow_audit_reconcile_scan_total",
		"xflow_audit_reconcile_settled_total",
	}
}

func g1MetricFamilyCoverage(body []byte, required []string) (observed, missing []string, err error) {
	parser := expfmt.NewTextParser(prommodel.LegacyValidation)
	families, err := parser.TextToMetricFamilies(bytes.NewReader(body))
	if err != nil {
		return nil, nil, fmt.Errorf("parse Prometheus metrics: %w", err)
	}
	for _, name := range required {
		if family, ok := families[name]; ok && len(family.GetMetric()) > 0 {
			observed = append(observed, name)
		} else {
			missing = append(missing, name)
		}
	}
	return observed, missing, nil
}

// g1RunMetricsScrape scrapes the /metrics endpoint and requires every declared
// family. A partial candidate match is not release evidence.
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

	required := g1RequiredMetricFamilies()
	observed, missing, err := g1MetricFamilyCoverage(body, required)
	if err != nil {
		t.Fatalf("metrics scrape: %v", err)
	}
	if len(missing) != 0 {
		snippet := string(body)
		if len(snippet) > 512 {
			snippet = snippet[:512]
		}
		t.Fatalf("metrics scrape missing required families %v (observed %v); body=%s", missing, observed, snippet)
	}
	return g1MetricsScrape{
		Scraped:          true,
		CountersObserved: observed,
		RequiredFamilies: required,
		ObservedFamilies: observed,
		MissingFamilies:  missing,
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
			"start": {"main": {Targets: []types.Connection{{Node: "end", Input: "main"}}}},
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
		HandlerSideEffectsAssertion:     g1HandlerSideEffectsAssertion(invocations, businessRows, string(outcome2)),
		IndependentExecutionsForSameDef: true,
		InvocationLevelIdempotencyKey:   g1InvocationLevelIdempotencyKey,
		HandlerInvocations:              invocations,
		BusinessRows:                    businessRows,
		IdempotencyKey:                  g1SideEffectIdempotencyKey,
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

func g1ContainsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func g1ValidateExactStringSet(got, want []string) error {
	problems := []string{}
	if len(got) != len(want) {
		problems = append(problems, fmt.Sprintf("has %d entries, want exactly %d", len(got), len(want)))
	}
	wanted := make(map[string]struct{}, len(want))
	for _, value := range want {
		wanted[value] = struct{}{}
	}
	seen := make(map[string]struct{}, len(got))
	for _, value := range got {
		if _, duplicate := seen[value]; duplicate {
			problems = append(problems, fmt.Sprintf("contains duplicate %q", value))
			continue
		}
		seen[value] = struct{}{}
		if _, ok := wanted[value]; !ok {
			problems = append(problems, fmt.Sprintf("contains unexpected %q", value))
		}
	}
	for _, value := range want {
		if _, ok := seen[value]; !ok {
			problems = append(problems, fmt.Sprintf("is missing %q", value))
		}
	}
	if len(problems) != 0 {
		return fmt.Errorf("%s", strings.Join(problems, ", "))
	}
	return nil
}

func g1HandlerSideEffectsAssertion(handlerInvocations, businessRows int, hostFenceOutcome string) string {
	return fmt.Sprintf(
		"handler_invocations=%d, business_rows=%d (idempotent receiver keyed by execution_id+node_name; host fence=%s)",
		handlerInvocations,
		businessRows,
		hostFenceOutcome,
	)
}

// g1ValidateArtifact is the final local release-evidence boundary. It validates
// the assembled object immediately before marshaling/writing, so a future
// subtest that silently leaves a zero value cannot publish a partial report.
func g1ValidateArtifact(art *g1Artifact) error {
	return g1ValidateArtifactForMode(art, true)
}

// g1ValidateArtifactForMode keeps ordinary integration runs useful on a dirty
// developer tree while preserving the clean-tree requirement for release
// evidence. Every non-provenance semantic check is identical in both modes.
func g1ValidateArtifactForMode(art *g1Artifact, strictEvidence bool) error {
	if art == nil {
		return fmt.Errorf("artifact: nil")
	}
	problems := []string{}
	problem := func(format string, args ...any) {
		problems = append(problems, fmt.Sprintf(format, args...))
	}

	if art.SchemaVersion != g1ArtifactSchemaVersion {
		problem("provenance: schema_version=%d, want %d", art.SchemaVersion, g1ArtifactSchemaVersion)
	}
	if err := g1ValidateCanonicalRunID(art.RunID); err != nil {
		problem("provenance: run_id: %v", err)
	}
	if !g1IsLowerHexCommitSHA(art.CommitSHA) {
		problem("provenance: commit_sha must be exactly 40 lowercase hexadecimal characters")
	}
	if strictEvidence && !art.FullWorktreeClean {
		problem("provenance: full_worktree_clean must be true for release evidence")
	}
	generatedAt, err := time.Parse(time.RFC3339, art.GeneratedAt)
	if err != nil {
		problem("provenance: generated_at is not RFC3339: %v", err)
	} else if generatedAt.Location() != time.UTC || generatedAt.Format(time.RFC3339) != art.GeneratedAt {
		problem("provenance: generated_at=%q must be canonical UTC RFC3339", art.GeneratedAt)
	}
	if art.GoVersion != g1RequiredGoVersion {
		problem("provenance: go_version=%q, want %q", art.GoVersion, g1RequiredGoVersion)
	}
	if strings.TrimSpace(art.OS) == "" {
		problem("provenance: os is empty")
	}
	if strings.TrimSpace(art.Runtime.RedisEndpoint) == "" || strings.TrimSpace(art.Runtime.RedisVersion) == "" {
		problem("runtime: Redis endpoint/version is incomplete")
	}
	if strings.TrimSpace(art.Runtime.MySQLNetwork) == "" || strings.TrimSpace(art.Runtime.MySQLEndpoint) == "" || strings.TrimSpace(art.Runtime.MySQLDatabase) == "" || strings.TrimSpace(art.Runtime.MySQLServerVersion) == "" {
		problem("runtime: MySQL network/endpoint/database/version is incomplete")
	}
	if art.RedisAddr != art.Runtime.RedisEndpoint {
		problem("runtime: redis_addr=%q does not match runtime endpoint=%q", art.RedisAddr, art.Runtime.RedisEndpoint)
	}
	if art.MySQLDSNHost != art.Runtime.MySQLEndpoint {
		problem("runtime: mysql_dsn_host=%q does not match runtime endpoint=%q", art.MySQLDSNHost, art.Runtime.MySQLEndpoint)
	}

	requiredAuthz := g1RequiredAuthzMatrix()
	if len(requiredAuthz) != 12 {
		problem("authz: internal required matrix has %d rows, want 12", len(requiredAuthz))
	}
	if len(art.AuthzMatrix) != len(requiredAuthz) {
		problem("authz: rows=%d, want exactly %d", len(art.AuthzMatrix), len(requiredAuthz))
	}
	requiredAuthzByScenario := make(map[string]g1AuthzRow, len(requiredAuthz))
	for _, row := range requiredAuthz {
		requiredAuthzByScenario[row.Scenario] = row
	}
	seenAuthz := make(map[string]struct{}, len(art.AuthzMatrix))
	for _, row := range art.AuthzMatrix {
		if row.Scenario == "" {
			problem("authz: row with route %q has empty scenario", row.Route)
			continue
		}
		if _, duplicate := seenAuthz[row.Scenario]; duplicate {
			problem("authz: duplicate scenario %q", row.Scenario)
			continue
		}
		seenAuthz[row.Scenario] = struct{}{}
		want, ok := requiredAuthzByScenario[row.Scenario]
		if !ok {
			problem("authz: unexpected scenario %q", row.Scenario)
			continue
		}
		if row != want {
			problem("authz: scenario %q row=%+v does not match required row=%+v", row.Scenario, row, want)
		}
	}
	for _, want := range requiredAuthz {
		if _, ok := seenAuthz[want.Scenario]; !ok {
			problem("authz: required scenario %q is missing", want.Scenario)
		}
	}

	requiredSpans := []string{"xflow.workflow.execute", "xflow.task.dispatch", "xflow.task.execute", "xflow.task.report", "xflow.task.commit"}
	if len(art.TraceGraph.SpansPresent) != len(requiredSpans) {
		problem("trace: spans_present=%v, want exactly %v", art.TraceGraph.SpansPresent, requiredSpans)
	}
	for _, span := range requiredSpans {
		if !g1ContainsString(art.TraceGraph.SpansPresent, span) {
			problem("trace: required span %q is missing", span)
		}
	}
	if !art.TraceGraph.OneTraceID || !art.TraceGraph.DispatchParentedToSubmit || !art.TraceGraph.CommitParentedToReport ||
		!art.TraceGraph.NamespaceAOneTraceID || !art.TraceGraph.NamespaceADispatchParentedToSubmit ||
		!art.TraceGraph.NamespaceACommitParentedToReport || !art.TraceGraph.CrossNamespaceCarrierIsolated {
		problem("trace: one or more trace identity/parentage/isolation assertions are false")
	}

	if art.ApprovalDAG.MultiSignalQuorum != "pass" || art.ApprovalDAG.TimerFired != "pass" ||
		art.ApprovalDAG.Cancel != "pass" || art.ApprovalDAG.CyclicReset != "pass" || art.ApprovalDAG.RepeatSignal409 != "pass" {
		problem("approval: all DAG assertions must equal pass")
	}

	if art.AuditReconcile.AdmissionRows <= 0 || art.AuditReconcile.OutcomeRows != art.AuditReconcile.AdmissionRows ||
		art.AuditReconcile.ReconciledByWorker != art.AuditReconcile.AdmissionRows || art.AuditReconcile.SweepsToSettle < 1 || art.AuditReconcile.SweepsToSettle > 256 ||
		!art.AuditReconcile.IdempotentOutcomeAppends || !art.AuditReconcile.FaultMatrixPass {
		problem("audit: reconciliation counts, idempotency, sweeps, or fault matrix are incomplete")
	}

	if !art.DeadLetter.Seeded || art.DeadLetter.ReplayOutcome != string(engine.ReplayReplayed) ||
		!art.DeadLetter.ReceiptAuditIDSet || art.DeadLetter.DurableProjectionRows != 1 {
		problem("dead-letter: seed/replay/receipt/projection evidence is incomplete")
	}

	requiredMetrics := g1RequiredMetricFamilies()
	if !art.MetricsScrape.Scraped || len(art.MetricsScrape.MissingFamilies) != 0 {
		problem("metrics: scrape failed or missing_families is non-empty: %v", art.MetricsScrape.MissingFamilies)
	}
	for _, field := range []struct {
		name   string
		values []string
	}{
		{name: "required_families", values: art.MetricsScrape.RequiredFamilies},
		{name: "observed_families", values: art.MetricsScrape.ObservedFamilies},
		{name: "counters_observed", values: art.MetricsScrape.CountersObserved},
	} {
		if err := g1ValidateExactStringSet(field.values, requiredMetrics); err != nil {
			problem("metrics: %s %v", field.name, err)
		}
	}

	validFence := art.IdempotencyReport.HostFenceOutcome == string(engine.CommitOutcomeDuplicateTerminal) ||
		art.IdempotencyReport.HostFenceOutcome == string(engine.CommitOutcomeStaleToken) ||
		art.IdempotencyReport.HostFenceOutcome == string(engine.CommitOutcomeExecutionInactive)
	if art.IdempotencyReport.RepeatSignalOutcome != "409" || !validFence ||
		art.IdempotencyReport.DuplicateReportOutcome != art.IdempotencyReport.HostFenceOutcome ||
		art.IdempotencyReport.HandlerSideEffectsAssertion != g1HandlerSideEffectsAssertion(
			art.IdempotencyReport.HandlerInvocations,
			art.IdempotencyReport.BusinessRows,
			art.IdempotencyReport.HostFenceOutcome,
		) ||
		!art.IdempotencyReport.IndependentExecutionsForSameDef || art.IdempotencyReport.HandlerInvocations < 2 ||
		art.IdempotencyReport.BusinessRows != 1 || art.IdempotencyReport.IdempotencyKey != g1SideEffectIdempotencyKey ||
		art.IdempotencyReport.InvocationLevelIdempotencyKey != g1InvocationLevelIdempotencyKey {
		problem("idempotency: repeat signal, host fence, redelivery, or unique side-effect evidence is incomplete")
	}

	if len(problems) != 0 {
		return fmt.Errorf("invalid G1 artifact: %s", strings.Join(problems, "; "))
	}
	return nil
}

func g1ValidArtifactForTest() *g1Artifact {
	requiredMetrics := g1RequiredMetricFamilies()
	return &g1Artifact{
		SchemaVersion:     g1ArtifactSchemaVersion,
		RunID:             "550e8400-e29b-41d4-a716-446655440000",
		GeneratedAt:       "2026-08-30T00:00:00Z",
		GoVersion:         g1RequiredGoVersion,
		OS:                "test/test",
		CommitSHA:         "0123456789abcdef0123456789abcdef01234567",
		FullWorktreeClean: true,
		RedisAddr:         "redis.example:6379",
		MySQLDSNHost:      "mysql.example:3306",
		Runtime: g1RuntimeEvidence{
			RedisEndpoint:      "redis.example:6379",
			RedisVersion:       "8.2.1",
			MySQLNetwork:       "tcp",
			MySQLEndpoint:      "mysql.example:3306",
			MySQLDatabase:      "xflow_evidence",
			MySQLServerVersion: "8.4.6",
		},
		AuthzMatrix: g1RequiredAuthzMatrix(),
		TraceGraph: g1TraceGraph{
			SpansPresent:                       []string{"xflow.workflow.execute", "xflow.task.dispatch", "xflow.task.execute", "xflow.task.report", "xflow.task.commit"},
			OneTraceID:                         true,
			DispatchParentedToSubmit:           true,
			CommitParentedToReport:             true,
			NamespaceAOneTraceID:               true,
			NamespaceADispatchParentedToSubmit: true,
			NamespaceACommitParentedToReport:   true,
			CrossNamespaceCarrierIsolated:      true,
		},
		ApprovalDAG: g1ApprovalDAG{MultiSignalQuorum: "pass", TimerFired: "pass", Cancel: "pass", CyclicReset: "pass", RepeatSignal409: "pass"},
		AuditReconcile: g1AuditReconcile{
			AdmissionRows: 3, OutcomeRows: 3, ReconciledByWorker: 3, IdempotentOutcomeAppends: true, SweepsToSettle: 1, FaultMatrixPass: true,
		},
		DeadLetter: g1DeadLetter{Seeded: true, ReplayOutcome: string(engine.ReplayReplayed), ReceiptAuditIDSet: true, DurableProjectionRows: 1},
		MetricsScrape: g1MetricsScrape{
			Scraped: true, CountersObserved: append([]string(nil), requiredMetrics...), RequiredFamilies: append([]string(nil), requiredMetrics...), ObservedFamilies: append([]string(nil), requiredMetrics...), MissingFamilies: []string{},
		},
		IdempotencyReport: g1Idempotency{
			RepeatSignalOutcome: "409", DuplicateReportOutcome: string(engine.CommitOutcomeDuplicateTerminal), HandlerSideEffectsAssertion: g1HandlerSideEffectsAssertion(2, 1, string(engine.CommitOutcomeDuplicateTerminal)), IndependentExecutionsForSameDef: true, InvocationLevelIdempotencyKey: g1InvocationLevelIdempotencyKey, HandlerInvocations: 2, BusinessRows: 1, IdempotencyKey: g1SideEffectIdempotencyKey, HostFenceOutcome: string(engine.CommitOutcomeDuplicateTerminal),
		},
	}
}

func TestG1ArtifactRunIdentityValidation(t *testing.T) {
	const (
		runID     = "550e8400-e29b-41d4-a716-446655440000"
		commitSHA = "0123456789abcdef0123456789abcdef01234567"
		otherSHA  = "1123456789abcdef0123456789abcdef01234567"
	)

	identity, err := g1ResolveRunIdentity(runID, commitSHA, commitSHA)
	if err != nil {
		t.Fatalf("valid run identity rejected: %v", err)
	}
	if identity.RunID != runID || identity.CommitSHA != commitSHA {
		t.Fatalf("identity=%+v, want run_id=%q commit_sha=%q", identity, runID, commitSHA)
	}

	tests := []struct {
		name         string
		runID        string
		candidateSHA string
		actualHEAD   string
		want         string
	}{
		{name: "missing run id", runID: "", candidateSHA: commitSHA, actualHEAD: commitSHA, want: g1RunIDEnv},
		{name: "uppercase run id", runID: strings.ToUpper(runID), candidateSHA: commitSHA, actualHEAD: commitSHA, want: "lowercase canonical"},
		{name: "non-v4 run id", runID: "550e8400-e29b-11d4-a716-446655440000", candidateSHA: commitSHA, actualHEAD: commitSHA, want: "UUIDv4"},
		{name: "noncanonical run id", runID: "550e8400e29b41d4a716446655440000", candidateSHA: commitSHA, actualHEAD: commitSHA, want: "lowercase canonical"},
		{name: "short candidate", runID: runID, candidateSHA: commitSHA[:39], actualHEAD: commitSHA, want: g1CandidateSHAEnv},
		{name: "uppercase candidate", runID: runID, candidateSHA: strings.ToUpper(commitSHA), actualHEAD: commitSHA, want: "lowercase hexadecimal"},
		{name: "invalid actual head", runID: runID, candidateSHA: commitSHA, actualHEAD: "unknown", want: "actual HEAD"},
		{name: "head mismatch", runID: runID, candidateSHA: commitSHA, actualHEAD: otherSHA, want: "does not match actual HEAD"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := g1ResolveRunIdentity(tc.runID, tc.candidateSHA, tc.actualHEAD)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error=%v, want substring %q", err, tc.want)
			}
		})
	}
}

func TestG1ArtifactConfiguredRunIdentityValidation(t *testing.T) {
	const (
		runID          = "550e8400-e29b-41d4-a716-446655440000"
		generatedRunID = "6ba7b810-9dad-41d1-80b4-00c04fd430c8"
		commitSHA      = "0123456789abcdef0123456789abcdef01234567"
	)

	ordinary, strict, err := g1ResolveConfiguredRunIdentity("", "", commitSHA, generatedRunID)
	if err != nil {
		t.Fatalf("ordinary integration identity rejected: %v", err)
	}
	if strict || ordinary != (g1RunIdentity{RunID: generatedRunID, CommitSHA: commitSHA}) {
		t.Fatalf("ordinary identity=%+v strict=%t", ordinary, strict)
	}

	evidence, strict, err := g1ResolveConfiguredRunIdentity(runID, commitSHA, commitSHA, generatedRunID)
	if err != nil {
		t.Fatalf("strict evidence identity rejected: %v", err)
	}
	if !strict || evidence != (g1RunIdentity{RunID: runID, CommitSHA: commitSHA}) {
		t.Fatalf("evidence identity=%+v strict=%t", evidence, strict)
	}

	tests := []struct {
		name         string
		runID        string
		candidateSHA string
		actualHEAD   string
		generatedID  string
		want         string
	}{
		{name: "run id only", runID: runID, actualHEAD: commitSHA, generatedID: generatedRunID, want: "both be set"},
		{name: "candidate only", candidateSHA: commitSHA, actualHEAD: commitSHA, generatedID: generatedRunID, want: "both be set"},
		{name: "whitespace run id only", runID: " ", actualHEAD: commitSHA, generatedID: generatedRunID, want: "both be set"},
		{name: "whitespace pair remains strict", runID: " ", candidateSHA: " ", actualHEAD: commitSHA, generatedID: generatedRunID, want: g1RunIDEnv},
		{name: "invalid generated run id", actualHEAD: commitSHA, generatedID: "not-a-uuid", want: "generated run ID"},
		{name: "invalid ordinary head", actualHEAD: "unknown", generatedID: generatedRunID, want: "actual HEAD"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := g1ResolveConfiguredRunIdentity(tc.runID, tc.candidateSHA, tc.actualHEAD, tc.generatedID)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error=%v, want substring %q", err, tc.want)
			}
		})
	}
}

func TestG1ArtifactValidationModes(t *testing.T) {
	dirty := g1ValidArtifactForTest()
	dirty.FullWorktreeClean = false
	if err := g1ValidateArtifactForMode(dirty, false); err != nil {
		t.Fatalf("ordinary integration rejected truthful dirty provenance: %v", err)
	}
	if err := g1ValidateArtifact(dirty); err == nil || !strings.Contains(err.Error(), "full_worktree_clean") {
		t.Fatalf("release validator error=%v, want dirty-worktree rejection", err)
	}

	tests := []struct {
		name   string
		mutate func(*g1Artifact)
		want   string
	}{
		{name: "schema", mutate: func(art *g1Artifact) { art.SchemaVersion++ }, want: "provenance:"},
		{name: "commit", mutate: func(art *g1Artifact) { art.CommitSHA = "unknown" }, want: "provenance:"},
		{name: "runtime", mutate: func(art *g1Artifact) { art.Runtime.RedisVersion = "" }, want: "runtime:"},
		{name: "semantic", mutate: func(art *g1Artifact) { art.DeadLetter.DurableProjectionRows = 0 }, want: "dead-letter:"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			art := g1ValidArtifactForTest()
			art.FullWorktreeClean = false
			tc.mutate(art)
			err := g1ValidateArtifactForMode(art, false)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ordinary validator error=%v, want category %q", err, tc.want)
			}
		})
	}
}

func TestG1ArtifactProvenanceValidation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*g1Artifact)
	}{
		{name: "schema", mutate: func(art *g1Artifact) { art.SchemaVersion++ }},
		{name: "run id", mutate: func(art *g1Artifact) { art.RunID = "550e8400-e29b-11d4-a716-446655440000" }},
		{name: "commit sha", mutate: func(art *g1Artifact) { art.CommitSHA = strings.ToUpper(art.CommitSHA) }},
		{name: "dirty worktree", mutate: func(art *g1Artifact) { art.FullWorktreeClean = false }},
		{name: "non-UTC generated at", mutate: func(art *g1Artifact) { art.GeneratedAt = "2026-08-30T08:00:00+08:00" }},
		{name: "Go version", mutate: func(art *g1Artifact) { art.GoVersion = "go1.25.1" }},
		{name: "OS", mutate: func(art *g1Artifact) { art.OS = " " }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			art := g1ValidArtifactForTest()
			tc.mutate(art)
			err := g1ValidateArtifact(art)
			if err == nil || !strings.Contains(err.Error(), "provenance:") {
				t.Fatalf("validation error=%v, want provenance rejection", err)
			}
		})
	}
}

func TestG1ArtifactMySQLTargetRedactsCredentials(t *testing.T) {
	dsn := "g1_secret_user:g1-secret-password@tcp(mysql.internal:4406)/evidence%2Dschema?parseTime=true"
	target, err := g1ParseMySQLTarget(dsn)
	if err != nil {
		t.Fatalf("g1ParseMySQLTarget: %v", err)
	}
	if target.Network != "tcp" || target.Endpoint != "mysql.internal:4406" || target.Database != "evidence-schema" {
		t.Fatalf("target=%+v", target)
	}
	raw, err := json.Marshal(g1RuntimeEvidence{MySQLNetwork: target.Network, MySQLEndpoint: target.Endpoint, MySQLDatabase: target.Database})
	if err != nil {
		t.Fatalf("marshal target: %v", err)
	}
	for _, secret := range []string{"g1_secret_user", "g1-secret-password"} {
		if bytes.Contains(raw, []byte(secret)) {
			t.Fatalf("credential %q leaked into runtime evidence: %s", secret, raw)
		}
	}
}

func TestG1ArtifactRedisVersionParsing(t *testing.T) {
	version, err := g1RedisVersionFromInfo("# Server\r\nredis_version:8.2.1\r\nredis_mode:standalone\r\n")
	if err != nil || version != "8.2.1" {
		t.Fatalf("version=%q err=%v", version, err)
	}
	if _, err := g1RedisVersionFromInfo("# Server\nredis_mode:standalone\n"); err == nil {
		t.Fatal("missing redis_version unexpectedly passed")
	}
}

func TestG1ArtifactMetricFamiliesRequireAll(t *testing.T) {
	required := g1RequiredMetricFamilies()
	var body strings.Builder
	for _, family := range required {
		fmt.Fprintf(&body, "# HELP %s test family\n# TYPE %s counter\n%s 1\n", family, family, family)
	}
	observed, missing, err := g1MetricFamilyCoverage([]byte(body.String()), required)
	if err != nil || len(missing) != 0 || len(observed) != len(required) {
		t.Fatalf("complete coverage: observed=%v missing=%v err=%v", observed, missing, err)
	}

	observed, missing, err = g1MetricFamilyCoverage([]byte("# HELP xflow_lease_acquire_duration_seconds test\n# TYPE xflow_lease_acquire_duration_seconds counter\nxflow_lease_acquire_duration_seconds 1\n"), required)
	if err != nil {
		t.Fatalf("partial coverage parse: %v", err)
	}
	if len(observed) != 1 || len(missing) != len(required)-1 {
		t.Fatalf("partial coverage unexpectedly passed: observed=%v missing=%v", observed, missing)
	}
}

func TestG1ArtifactReconcileMetricsFanout(t *testing.T) {
	assertionObserver := newG1ReconcileObserver(map[string]struct{}{"request-1": {}})
	assertionObserver.beginSweep()
	metricSink := obsmetrics.New()
	observer := g1FanoutReconcileObservers(assertionObserver, obsmetrics.NewReconcileMetrics(metricSink))

	ctx := namespace.WithNamespace(context.Background(), namespace.Namespace("evidence-test"))
	observer.OnReconcileScan(ctx, 1, time.Millisecond, nil)
	observer.OnReconcileSettled(ctx, store.AuditOutcomeReconciled, true, 1)

	scanObserved, candidates, scanErr, requestID, trackedErr := assertionObserver.sweepObservation()
	if !scanObserved || candidates != 1 || scanErr != nil || requestID != "" || trackedErr != nil {
		t.Fatalf("assertion observer scan=(%t, %d, %v, %q, %v)", scanObserved, candidates, scanErr, requestID, trackedErr)
	}

	recorder := httptest.NewRecorder()
	metricSink.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	required := []string{"xflow_audit_reconcile_scan_total", "xflow_audit_reconcile_settled_total"}
	observed, missing, err := g1MetricFamilyCoverage(recorder.Body.Bytes(), required)
	if err != nil || len(missing) != 0 || len(observed) != len(required) {
		t.Fatalf("reconcile metric fanout: observed=%v missing=%v err=%v", observed, missing, err)
	}
}

func TestG1ArtifactSemanticValidation(t *testing.T) {
	if err := g1ValidateArtifact(g1ValidArtifactForTest()); err != nil {
		t.Fatalf("valid artifact rejected: %v", err)
	}

	tests := []struct {
		name     string
		category string
		mutate   func(*g1Artifact)
	}{
		{name: "runtime", category: "runtime:", mutate: func(art *g1Artifact) { art.Runtime.RedisVersion = "" }},
		{name: "runtime alias", category: "runtime:", mutate: func(art *g1Artifact) { art.RedisAddr = "other.example:6379" }},
		{name: "authz", category: "authz:", mutate: func(art *g1Artifact) { art.AuthzMatrix[6].Got = http.StatusInternalServerError }},
		{name: "authz duplicate", category: "authz:", mutate: func(art *g1Artifact) { art.AuthzMatrix[11] = art.AuthzMatrix[0] }},
		{name: "authz extra", category: "authz:", mutate: func(art *g1Artifact) { art.AuthzMatrix[11].Scenario = "unexpected" }},
		{name: "trace", category: "trace:", mutate: func(art *g1Artifact) { art.TraceGraph.CrossNamespaceCarrierIsolated = false }},
		{name: "approval", category: "approval:", mutate: func(art *g1Artifact) { art.ApprovalDAG.TimerFired = "" }},
		{name: "audit", category: "audit:", mutate: func(art *g1Artifact) { art.AuditReconcile.FaultMatrixPass = false }},
		{name: "audit sweep ceiling", category: "audit:", mutate: func(art *g1Artifact) { art.AuditReconcile.SweepsToSettle = 257 }},
		{name: "dead-letter", category: "dead-letter:", mutate: func(art *g1Artifact) { art.DeadLetter.DurableProjectionRows = 0 }},
		{name: "dead-letter extra projection", category: "dead-letter:", mutate: func(art *g1Artifact) { art.DeadLetter.DurableProjectionRows = 2 }},
		{name: "metrics", category: "metrics:", mutate: func(art *g1Artifact) { art.MetricsScrape.MissingFamilies = []string{g1RequiredMetricFamilies()[0]} }},
		{name: "metrics required duplicate", category: "metrics:", mutate: func(art *g1Artifact) { art.MetricsScrape.RequiredFamilies[2] = art.MetricsScrape.RequiredFamilies[0] }},
		{name: "metrics observed extra", category: "metrics:", mutate: func(art *g1Artifact) { art.MetricsScrape.ObservedFamilies[2] = "xflow_unexpected_total" }},
		{name: "metrics counter duplicate", category: "metrics:", mutate: func(art *g1Artifact) { art.MetricsScrape.CountersObserved[2] = art.MetricsScrape.CountersObserved[0] }},
		{name: "idempotency", category: "idempotency:", mutate: func(art *g1Artifact) { art.IdempotencyReport.BusinessRows = 2 }},
		{name: "idempotency side-effect key", category: "idempotency:", mutate: func(art *g1Artifact) { art.IdempotencyReport.IdempotencyKey = "execution_id" }},
		{name: "idempotency invocation declaration", category: "idempotency:", mutate: func(art *g1Artifact) { art.IdempotencyReport.InvocationLevelIdempotencyKey = "implemented" }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			art := g1ValidArtifactForTest()
			tc.mutate(art)
			err := g1ValidateArtifact(art)
			if err == nil || !strings.Contains(err.Error(), tc.category) {
				t.Fatalf("validation error=%v, want category %q", err, tc.category)
			}
		})
	}
}
