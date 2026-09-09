//go:build integration

// Package integration hosts the R8 real-binary end-to-end coverage.
//
// TestR8BinaryProcessE2E is the R8 release-gate evidence: it builds the
// production `bin/server` and `bin/runner` binaries, starts them as real OS
// processes against real Redis + MySQL (production mode, fail-closed auth,
// durable audit + reconciler), and proves the runner process can be SIGKILLed
// and restarted with recovery via the durable Redis state store + asynq
// outbox. Every other e2e in this package runs the server/runner in-process;
// this test is the only one that exercises the actual built binaries.
package integration

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// r8Token is a test-only bearer token written to the production auth-tokens
// file. The server runs in production mode (fail-closed); this token + the
// tokens file satisfy PrincipalAuth. The token is scoped to the default
// namespace with workflow/execution/management.read scopes.
const r8Token = "r8-prod-token-015a3e7f9c"

// r8HTTPClient is the shared client for R8 API calls.
var r8HTTPClient = &http.Client{Timeout: 30 * time.Second}

// safeBuffer is a mutex-guarded bytes.Buffer safe for use as cmd.Stdout/Stderr
// (the exec package copies the child pipe from a goroutine while the test may
// read concurrently).
type safeBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *safeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *safeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// repoRootR8 walks up from start until it finds go.mod. Mirrors the
// cyclic_reliability_process_test.go repoRoot helper so this file stays
// self-contained.
func repoRootR8(start string) string {
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

// buildR8Binaries builds the production server + runner binaries into a temp
// dir and returns their paths. Built fresh each run (never reuses stale bin/)
// so the test exercises current code. Uses `go build` (faster than go test -c
// and yields the real production binary, not a test binary).
func buildR8Binaries(t *testing.T) (serverBin, runnerBin string) {
	t.Helper()
	root := repoRootR8(func() string {
		d, err := os.Getwd()
		if err != nil {
			t.Fatalf("getwd: %v", err)
		}
		return d
	}())
	out := t.TempDir()
	serverBin = filepath.Join(out, "xflow-server")
	runnerBin = filepath.Join(out, "xflow-runner")
	for _, target := range []struct{ path, out string }{
		{"./cmd/server", serverBin},
		{"./cmd/runner", runnerBin},
	} {
		cmd := exec.Command("go", "build", "-o", target.out, target.path)
		cmd.Dir = root
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("go build %s: %v\n%s", target.path, err, out)
		}
	}
	return serverBin, runnerBin
}

// writeR8TokensFile writes the production auth-tokens file (0600) binding
// r8Token to the default namespace with workflow/execution/management scopes.
func writeR8TokensFile(t *testing.T) string {
	t.Helper()
	mappings := []map[string]any{{
		"token":     r8Token,
		"subject":   "r8",
		"namespace": "default",
		"scopes":    []string{"workflow", "execution", "management.read", "management.runner.read"},
	}}
	raw, err := json.Marshal(mappings)
	if err != nil {
		t.Fatalf("marshal tokens: %v", err)
	}
	path := filepath.Join(t.TempDir(), "r8-tokens.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write tokens file: %v", err)
	}
	return path
}

// freeAddr returns a "127.0.0.1:<port>" by opening an ephemeral listener,
// reading its address, then closing it. There is a small TOCTOU window before
// the server binds; startR8Server detects a bind failure via /readyz timeout
// and the caller can retry.
func freeAddr(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen ephemeral: %v", err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()
	return addr
}

// r8MasterKeyFile writes a 0600 file holding a base64 32-byte master key.
//
// Production mode refuses to start without one (cmd/server/main.go's
// requireProduction), so every real-binary e2e needs it. The key is a fixed
// test-only value rather than a random one so a failing run is reproducible; it
// protects nothing but this test's own throwaway MySQL rows.
func r8MasterKeyFile(t *testing.T) string {
	t.Helper()
	// 32 bytes of 0x2a, base64-encoded. Deliberately not derived from anything.
	key := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x2a}, 32))
	path := filepath.Join(t.TempDir(), "r8-master-key")
	if err := os.WriteFile(path, []byte(key), 0o600); err != nil {
		t.Fatalf("write master key file: %v", err)
	}
	return path
}

// r8RunnerToken is the bearer token every e2e runner presents to the server's
// runner protocol. Fixed rather than random so a failing run is reproducible;
// it authorizes nothing beyond the throwaway server the test just started.
//
// Note this is NOT the same credential as writeR8TokensFile's: that one is API
// auth (--auth-tokens-file, for callers of the HTTP API), this one is
// runner-protocol auth (--auth-policy, for processes claiming work). Production
// mode requires both, and conflating them is what left these tests red.
const r8RunnerToken = "r8-e2e-runner-token"

// r8RunnerPolicyFile writes the runners.yaml that --auth-policy loads.
//
// Production mode is unsatisfiable without it: apiserver's gate accepts either
// control.IsConfigured(Auth) or an enrollment declaration, and a server started
// with neither refuses to boot with "runner-auth ... set: --auth-policy or
// --enroll". Enrollment is the heavier of the two — it would make every e2e
// mint a registration code before it could start a runner — so these tests take
// the policy path and hand each runner a static token.
//
// id_prefix is a strings.HasPrefix match and cannot be empty, so every prefix
// the e2e runners use needs an entry. An e2e that introduces a new prefix and
// forgets to add it here fails at runner registration with an ID-prefix denial,
// NOT at server startup — the server comes up fine and the runner never claims
// work, which reads like a dispatch bug.
//
// allowed_node_types is "*" because the caps vary per test and none of them is
// what these tests are about. allowed_namespaces is left empty, which the
// policy layer defines as "the default namespace only" — exactly what the
// submitter token is bound to.
func r8RunnerPolicyFile(t *testing.T) string {
	t.Helper()
	const policy = `version: 1
runners:
  - name: r8-e2e
    id_prefix: "r8-"
    token: "` + r8RunnerToken + `"
    allowed_node_types: ["*"]
  - name: metrics-e2e
    id_prefix: "runner-"
    token: "` + r8RunnerToken + `"
    allowed_node_types: ["*"]
  - name: subgraph-e2e
    id_prefix: "sg-"
    token: "` + r8RunnerToken + `"
    allowed_node_types: ["*"]
`
	path := filepath.Join(t.TempDir(), "runners.yaml")
	// 0600: the store refuses to load a world/group-readable policy file
	// because it may hold plaintext tokens.
	if err := os.WriteFile(path, []byte(policy), 0o600); err != nil {
		t.Fatalf("write runner policy file: %v", err)
	}
	return path
}

// startR8Server builds and starts the production server binary, polling
// /readyz until it is ready (or fails fast). Returns the HTTP base URL, a
// captured output buffer, and a stop function.
func startR8Server(t *testing.T, serverBin, addr, redisAddr, dsn, tokensFile string) (httpURL string, out *safeBuffer, stop func()) {
	t.Helper()
	out = &safeBuffer{}
	cmd := exec.Command(serverBin,
		"-addr", addr,
		"-redis", redisAddr,
		"-mysql-dsn", dsn,
		"-auth-tokens-file", tokensFile,
		"-auth-policy", r8RunnerPolicyFile(t),
		"-master-key-file", r8MasterKeyFile(t),
		"-require-api-auth",
		"-management",
		"-mode", "production",
		"-log-format", "json",
	)
	cmd.Stdout = out
	cmd.Stderr = out
	if err := cmd.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	httpURL = "http://" + addr
	ready := false
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		// If the process has already exited, fail immediately with logs.
		if cmd.ProcessState != nil {
			t.Fatalf("server exited early:\n%s", out.String())
		}
		resp, err := r8HTTPClient.Get(httpURL + "/readyz")
		if err == nil {
			ready = resp.StatusCode == http.StatusOK
			resp.Body.Close()
			if ready {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !ready {
		_ = cmd.Process.Signal(syscall.SIGKILL)
		_, _ = cmd.Process.Wait()
		t.Fatalf("server /readyz never returned 200:\n%s", out.String())
	}
	return httpURL, out, func() {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		done := make(chan struct{})
		go func() { _, _ = cmd.Process.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			_ = cmd.Process.Signal(syscall.SIGKILL)
			<-done
		}
	}
}

// r8Process wraps a child process with a captured output buffer so failures can
// print the child's stderr/stdout for diagnosis.
type r8Process struct {
	cmd *exec.Cmd
	out *safeBuffer
}

// startR8Runner starts the production runner binary against the server. It
// presents r8RunnerToken, which the --auth-policy the server loaded binds to the
// "r8-" id prefix and the default namespace — matching the submitter token's
// namespace binding.
//
// This used to say runner-protocol auth was DisabledAuthenticator and no token
// was needed. That stopped being true when production mode began requiring an
// explicit runner-auth posture; the comment outlived the premise and the test
// went red at server startup, not here.
// --allow-plaintext is load-bearing, not boilerplate. validateTransportSecurity
// refuses to start a standalone runner that would ship its bearer token over an
// unencrypted link, and every process e2e here dials 127.0.0.1 over plain http.
// Without the opt-out the runner exits before it registers, and the test spends
// its whole timeout waiting for a process that is already dead -- which is how
// this was found. The token never leaves the loopback interface, so accepting
// the risk is honest here; giving these tests TLS material would test the
// harness instead of the feature.
//
// TestRunnerRefusesPlaintextByDefault is what keeps the gate itself covered on a
// real binary once all three harnesses opt out.
func startR8Runner(t *testing.T, runnerBin, httpURL, id string) *r8Process {
	t.Helper()
	out := &safeBuffer{}
	cmd := exec.Command(runnerBin, "run",
		"--server", httpURL,
		"--transport", "http",
		"--id", id,
		"--token", r8RunnerToken,
		"--cap", "xflow.function,xflow.http,xflow.script",
		"--poll-wait", "50ms",
		"--concurrency", "1",
		"--allow-plaintext",
	)
	cmd.Stdout = out
	cmd.Stderr = out
	if err := cmd.Start(); err != nil {
		t.Fatalf("start runner: %v", err)
	}
	return trackR8Process(t, cmd, out, id)
}

// trackR8Process registers an already-started child so the test framework owns
// its lifetime.
//
// The cleanup terminates the child, it does not merely log it. Several subtests
// reclaim their runner with an explicit kill/stop placed *after* a wait helper
// that can t.Fatal; on that path the runner would outlive `go test` entirely and
// keep polling the shared Redis for tasks, stealing work from later runs and
// making a healthy tree read as a regression. stop and kill are both nil-safe
// and tolerate an already-reaped child, so the explicit calls stay harmless.
func trackR8Process(t *testing.T, cmd *exec.Cmd, out *safeBuffer, id string) *r8Process {
	t.Helper()
	p := &r8Process{cmd: cmd, out: out}
	t.Cleanup(func() {
		p.stop(t)
		if t.Failed() {
			t.Logf("runner %s logs:\n%s", id, out.String())
		}
	})
	return p
}

// kill sends an uncatchable SIGKILL and waits for the process to exit. Returns
// whether the exit indicates a signal kill (non-zero, non-graceful).
func (p *r8Process) kill(t *testing.T) {
	t.Helper()
	if p == nil || p.cmd == nil || p.cmd.Process == nil {
		return
	}
	_ = p.cmd.Process.Signal(syscall.SIGKILL)
	_ = p.cmd.Wait()
}

// stop sends SIGTERM and waits briefly for graceful shutdown, then SIGKILL.
func (p *r8Process) stop(t *testing.T) {
	t.Helper()
	if p == nil || p.cmd == nil || p.cmd.Process == nil {
		return
	}
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	done := make(chan struct{})
	go func() { _ = p.cmd.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(8 * time.Second):
		_ = p.cmd.Process.Signal(syscall.SIGKILL)
		<-done
	}
}

// r8Workflow builds a single-node xflow.function workflow whose inline Expr
// code evaluates to the given string. xflow.function is a built-in node
// auto-registered by the runner's _ import of the node package; no external
// function registration is needed.
func r8Workflow(name, code string) *types.WorkflowDef {
	return &types.WorkflowDef{
		Name: name,
		Nodes: []types.NodeDef{
			{Name: "fn", Type: "xflow.function", Parameters: map[string]any{"code": code}},
		},
	}
}

// r8MidFlightBarrier is a test-owned HTTP endpoint that proves the first
// runner entered a real node handler before the test kills it. NodeStatusRunning
// alone is not sufficient: BuildTaskLease commits that status before the poll
// response is finalized and delivered, so killing on Running can cancel the
// dispatch handshake and let the replacement runner execute attempt 1.
type r8MidFlightBarrier struct {
	server      *httptest.Server
	started     chan struct{}
	release     chan struct{}
	releaseOnce sync.Once
	requests    atomic.Int32
}

func newR8MidFlightBarrier(t *testing.T) *r8MidFlightBarrier {
	t.Helper()
	b := &r8MidFlightBarrier{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	b.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if b.requests.Add(1) == 1 {
			close(b.started)
			select {
			case <-b.release:
			case <-r.Context().Done():
			}
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(func() {
		b.releaseFirst()
		b.server.Close()
	})
	return b
}

func (b *r8MidFlightBarrier) releaseFirst() {
	b.releaseOnce.Do(func() { close(b.release) })
}

// r8BlockingHTTPWorkflow runs a single HTTP node against the barrier. The first
// request blocks until its runner is killed; the recovered attempt gets an
// immediate 200 response and can complete normally.
func r8BlockingHTTPWorkflow(name, rawURL string) *types.WorkflowDef {
	return &types.WorkflowDef{
		Name: name,
		Nodes: []types.NodeDef{
			{Name: "busy", Type: "xflow.http", Parameters: map[string]any{
				"method":  http.MethodGet,
				"url":     rawURL,
				"options": map[string]any{"timeout": "30s"},
			}},
		},
	}
}

// r8Submit submits a workflow with the test token and returns the execution id.
func r8Submit(t *testing.T, baseURL string, wf *types.WorkflowDef) types.ExecutionID {
	t.Helper()
	body := map[string]any{"workflow": wf}
	resp, raw := g1DoAuth(t, http.MethodPost, baseURL, "/v1/workflows/execute", r8Token, body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("r8 submit: status=%d, want 200 (body=%s)", resp.StatusCode, string(raw))
	}
	out := decodeSubmitEnvelope(t, raw)
	return out.ExecutionID
}

// r8Flush wipes asynq + xflow keys so a subtest starts from a clean slate.
func r8Flush(t *testing.T, addr string) {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()
	ctx := context.Background()
	flushAsynqKeys(ctx, t, rdb)
	flushXflowKeys(ctx, t, rdb)
}

// r8Server is a shared server fixture for a subtest: the running server plus
// its base URL and a Redis client for direct state verification.
type r8Server struct {
	httpURL string
	out     *safeBuffer
	stop    func()
	rdb     *redis.Client
}

// newR8Server builds the tokens file, flushes Redis, starts the server, and
// registers cleanup. Returns the ready server fixture.
func newR8Server(t *testing.T, serverBin, addr, redisAddr, dsn string) *r8Server {
	t.Helper()
	tokensFile := writeR8TokensFile(t)
	r8Flush(t, redisAddr)
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	httpURL, out, stop := startR8Server(t, serverBin, addr, redisAddr, dsn, tokensFile)
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("server logs:\n%s", out.String())
		}
		stop()
		_ = rdb.Close()
	})
	return &r8Server{httpURL: httpURL, out: out, stop: stop, rdb: rdb}
}

func TestR8BinaryProcessE2E(t *testing.T) {
	addr := requireRedis(t)
	dsn := requireMySQL(t)
	serverBin, runnerBin := buildR8Binaries(t)

	t.Run("HappyPath", func(t *testing.T) {
		r8HappyPath(t, serverBin, runnerBin, addr, dsn)
	})
	t.Run("KillRunnerWhilePendingThenRestart", func(t *testing.T) {
		r8KillPendingRestart(t, serverBin, runnerBin, addr, dsn)
	})
	t.Run("KillRunnerMidFlightThenRestart", func(t *testing.T) {
		r8KillMidFlightRestart(t, serverBin, runnerBin, addr, dsn)
	})
}

// r8HappyPath proves the real bin/server + bin/runner happy path: a submitted
// xflow.function workflow reaches terminal Success via the actual built
// binaries over HTTP + real Redis/MySQL.
func r8HappyPath(t *testing.T, serverBin, runnerBin, addr, dsn string) {
	srv := newR8Server(t, serverBin, freeAddr(t), addr, dsn)
	runner := startR8Runner(t, runnerBin, srv.httpURL, "r8-runner-happy")
	t.Cleanup(func() { runner.stop(t) })

	id := r8Submit(t, srv.httpURL, r8Workflow("r8-happy", "\"warmup\""))
	t.Cleanup(func() { deleteAtomicReliabilityKeys(t, srv.rdb, id) })

	detail := g1WaitForTerminal(t, srv.httpURL, r8Token, id, 30*time.Second)
	if detail.Status != types.ExecutionStatusSuccess {
		t.Fatalf("happy path: execution status=%s, want %s (runner out=%s)",
			detail.Status, types.ExecutionStatusSuccess, runner.out.String())
	}
	t.Logf("R8 happy path: real bin/server+bin/runner, execution %s -> Success", id)
}

// r8KillPendingRestart proves recovery across runner process death: SIGKILL the
// runner while no runner is alive, submit a workflow (it must stay pending —
// proving nothing processes it), then restart a fresh runner process and verify
// the workflow completes. The durable Redis state store + asynq queue survive
// the kill; the new runner process drains the queued task.
func r8KillPendingRestart(t *testing.T, serverBin, runnerBin, addr, dsn string) {
	srv := newR8Server(t, serverBin, freeAddr(t), addr, dsn)
	runner := startR8Runner(t, runnerBin, srv.httpURL, "r8-runner-pending-1")

	// Kill the runner (uncatchable SIGKILL).
	runner.kill(t)
	t.Logf("R8: runner SIGKILLed before submitting pending workflow")

	// Submit while no runner is alive. The task is enqueued durably to asynq
	// but must NOT be processed (no consumer).
	id := r8Submit(t, srv.httpURL, r8Workflow("r8-pending", "\"recover\""))
	t.Cleanup(func() { deleteAtomicReliabilityKeys(t, srv.rdb, id) })

	// Within a short window the execution must remain non-terminal — proof no
	// runner is processing it. (It may briefly be Pending/Running as the engine
	// materializes state, but must not reach terminal.)
	deadline := time.Now().Add(1500 * time.Millisecond)
	stayedNonTerminal := true
	for time.Now().Before(deadline) {
		_, detail := g1InspectAuth(t, srv.httpURL, r8Token, id)
		if types.IsTerminalExecutionStatus(detail.Status) {
			stayedNonTerminal = false
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !stayedNonTerminal {
		t.Fatalf("execution %s reached terminal while no runner was alive — recovery invariant violated", id)
	}

	// Restart a fresh runner process. It must drain the queued task from asynq
	// and complete the execution (at-least-once across process death).
	runner2 := startR8Runner(t, runnerBin, srv.httpURL, "r8-runner-pending-2")
	t.Cleanup(func() { runner2.stop(t) })

	detail := g1WaitForTerminal(t, srv.httpURL, r8Token, id, 30*time.Second)
	if detail.Status != types.ExecutionStatusSuccess {
		t.Fatalf("recovery: execution %s status=%s, want %s (runner out=%s)",
			id, detail.Status, types.ExecutionStatusSuccess, runner2.out.String())
	}
	t.Logf("R8 recovery: runner killed+restarted, pending execution %s -> Success", id)
}

// r8KillMidFlightRestart proves at-least-once recovery when the runner is
// SIGKILLed *while* a node is executing. The node's lease expires with no owner
// alive to renew it; xflow's own LeaseSweeper reclaims it and re-enqueues the
// task; the restarted runner re-executes it (at-least-once) and the execution
// succeeds. This is the strong cross-process-death evidence.
//
// Recovery latency comes from xflow's LeaseSweeper, NOT asynq's recoverer:
// engine.DefaultLeaseTTL is 60s (engine/engine.go) and
// control.DefaultSweepPeriod is 10s (service/control/lease_sweeper.go), so
// convergence lands around 60-70s; measured runs converge at ~79s. Neither
// binary exposes a lease-TTL flag, so this floor cannot be shortened from a
// real-binary test — hence the wide window below. An earlier revision waited
// only 30s, which is under the lease TTL itself, so the subtest could never
// pass and always degraded to a skip.
func r8KillMidFlightRestart(t *testing.T, serverBin, runnerBin, addr, dsn string) {
	barrier := newR8MidFlightBarrier(t)
	srv := newR8Server(t, serverBin, freeAddr(t), addr, dsn)
	runner := startR8Runner(t, runnerBin, srv.httpURL, "r8-runner-mid-1")

	id := r8Submit(t, srv.httpURL, r8BlockingHTTPWorkflow("r8-midflight", barrier.server.URL))
	t.Cleanup(func() { deleteAtomicReliabilityKeys(t, srv.rdb, id) })

	// The barrier is reached from inside xflow.http.Execute, after the poll
	// response has reached this runner. Unlike NodeStatusRunning, this cannot be
	// observed in the server-side dispatch-before-delivery window.
	select {
	case <-barrier.started:
	case <-time.After(15 * time.Second):
		status, detail := g1InspectAuth(t, srv.httpURL, r8Token, id)
		t.Fatalf("R8 mid-flight: timeout waiting for runner to enter HTTP barrier "+
			"(inspect status=%d detail=%+v; runner out=%s)", status, detail, runner.out.String())
	}
	t.Logf("R8: node busy entered HTTP handler; SIGKILL runner mid-execution")
	runner.kill(t)
	barrier.releaseFirst()

	// Restart a fresh runner and wait out the lease-sweep window.
	runner2 := startR8Runner(t, runnerBin, srv.httpURL, "r8-runner-mid-2")
	t.Cleanup(func() { runner2.stop(t) })

	start := time.Now()
	detail := g1WaitForTerminal(t, srv.httpURL, r8Token, id, 150*time.Second)
	if detail.Status != types.ExecutionStatusSuccess {
		t.Fatalf("R8 mid-flight: execution %s converged to %s in %v, want %s; nodes=%+v (runner2 out=%s)",
			id, detail.Status, time.Since(start), types.ExecutionStatusSuccess, detail.Nodes, runner2.out.String())
	}
	// Attempt >= 2 is the recovery proof: attempt 1 died with the SIGKILLed
	// runner, so a success on attempt 1 would mean the kill never landed
	// mid-flight and the test verified nothing.
	var busy *engine.NodeDetail
	for i := range detail.Nodes {
		if detail.Nodes[i].Name == "busy" {
			busy = &detail.Nodes[i]
			break
		}
	}
	if busy == nil {
		t.Fatalf("R8 mid-flight: execution %s has no node named busy; nodes=%+v", id, detail.Nodes)
	}
	if busy.Attempt < 2 {
		t.Fatalf("R8 mid-flight: node busy succeeded on attempt %d, want >= 2 — the SIGKILL did not "+
			"interrupt an in-flight execution, so lease-sweep recovery was never exercised; node=%+v",
			busy.Attempt, *busy)
	}
	if requests := barrier.requests.Load(); requests < 2 {
		t.Fatalf("R8 mid-flight: barrier received %d request(s), want >= 2 (interrupted attempt + recovery)", requests)
	}
	t.Logf("R8 mid-flight recovery: execution %s -> Success on attempt %d in %v after runner SIGKILL+restart",
		id, busy.Attempt, time.Since(start))
}
