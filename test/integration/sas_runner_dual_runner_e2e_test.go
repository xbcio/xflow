//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/sdk/xflow"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

const sasDualRunnerSinkType = "xflow.sas.sink"

// The two functions below are the single source of truth for each runner's
// node-type set. They exist because this test pins the set TWICE — once in the
// policy fixture that the runners must satisfy, and once in the registration
// assertion — and the two drifting apart is precisely how this test broke when
// the standalone runner grew its ULP capabilities: a policy that lists fewer
// types than the process declares answers 403, so the process never registers
// and the failure surfaces far from the cause.
//
// They return fresh slices rather than exposing package-level vars because
// callers hand them to stores and assertions that may append.
//
// sasDualRunnerLocalNodeTypes is the embedded local runner's set: the SAS sink
// plus the engine's group node. The local runner advertises nothing else, which
// is the invariant that lets a local-only deployment avoid carrying the
// collection and ULP nodes.
func sasDualRunnerLocalNodeTypes() []string {
	return []string{sasDualRunnerSinkType, engine.GroupNodeType}
}

// sasDualRunnerStandaloneNodeTypes mirrors FixedCapabilities in the SAS
// repository (asop/sas-runner/cmd/sas-runner/main.go), which carries a comment
// pointing back at this contract. Keep the two in lockstep: the middleware
// compares the node-type SET for exact equality and answers 403 on any
// difference, so a missing entry here is a hard failure, not a tolerance.
func sasDualRunnerStandaloneNodeTypes() []string {
	return []string{
		// Cross-environment traffic collection.
		"xflow.trigger.kafka",
		"xflow.map",
		"xflow.script",
		// ULP remote login.
		"xflow.http",
		"xflow.browser.cdp",
		"xflow.if",
		// Added by the runner assembly on top of the profile's
		// FixedCapabilities, so the registered set is one larger than the
		// profile declares.
		engine.GroupNodeType,
	}
}

// TestSASRunnerDualRunnerE2E proves the intended split between an embedded SAS
// runner and the standalone sas-runner process. The test deliberately builds
// the sibling process against this checkout through an ephemeral GOWORK: the
// production module files remain untouched, while the binary exercises the
// current runner profile and SDK assembly rather than a released xflow
// version from the module cache.
func TestSASRunnerDualRunnerE2E(t *testing.T) {
	xflowRoot := sasDualRunnerRepoRoot(t)
	sasRunnerDir := sasDualRunnerSourceDir(t, xflowRoot)
	sasRunnerBin := sasDualRunnerBuildBinary(t, xflowRoot, sasRunnerDir)

	runID := strconv.FormatInt(time.Now().UnixNano(), 36)
	localRunnerID := "sas-local-e2e-" + runID
	standaloneRunnerID := "sas-runner-e2e-" + runID
	localToken := "sas-local-e2e-token-" + runID
	standaloneToken := "sas-runner-e2e-token-" + runID

	runnerAuth, err := control.NewFilePolicyStoreFromConfig(control.PolicyConfig{
		Version: 1,
		Runners: []control.PolicyEntry{
			{
				Name:              "sas-local",
				IDPrefix:          "sas-local-e2e-",
				Token:             localToken,
				AllowedNodeTypes:  sasDualRunnerLocalNodeTypes(),
				AllowedNamespaces: []string{string(namespace.Default)},
			},
			{
				Name:              "sas-runner",
				IDPrefix:          "sas-runner-",
				Token:             standaloneToken,
				AllowedNodeTypes:  sasDualRunnerStandaloneNodeTypes(),
				AllowedNamespaces: []string{string(namespace.Default)},
			},
		},
	}, false)
	if err != nil {
		t.Fatalf("create runner auth policy: %v", err)
	}

	server, err := xflow.NewServer(xflow.ServerConfig{}, xflow.WithServerAuth(runnerAuth))
	if err != nil {
		t.Fatalf("create embedded control plane: %v", err)
	}
	serverCtx, cancelServer := context.WithCancel(context.Background())
	if err := server.Start(serverCtx); err != nil {
		cancelServer()
		t.Fatalf("start embedded control plane: %v", err)
	}
	// Register this cleanup before the HTTP server and runners so the latter can
	// finish their graceful shutdown traffic while the control plane is alive.
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			t.Errorf("shutdown embedded control plane: %v", err)
		}
		cancelServer()
	})

	registrations := &sasDualRunnerRegistrationRecorder{}
	httpServer := httptest.NewServer(sasDualRunnerCaptureRegistrations(server.Handler(), registrations))
	t.Cleanup(httpServer.Close)

	sink := newSASDualRunnerSink()
	localRunner := sasDualRunnerStartLocalRunner(t, httpServer.URL, localRunnerID, localToken, sink)
	standaloneRunner := sasDualRunnerStartProcess(t, sasRunnerBin, httpServer.URL, standaloneRunnerID, standaloneToken)

	localRegistration, standaloneRegistration := sasDualRunnerWaitForRegistrations(
		t, registrations, localRunner, standaloneRunner, localRunnerID, standaloneRunnerID,
	)
	sasDualRunnerAssertRegistration(t, "local SAS runner", localRegistration,
		map[string]string{"workload": "local"},
		sasDualRunnerLocalNodeTypes(),
	)
	sasDualRunnerAssertRegistration(t, "standalone sas-runner", standaloneRegistration,
		map[string]string{"workload": "sas-runner"},
		sasDualRunnerStandaloneNodeTypes(),
	)

	// The local registry below contains only xflow.sas.sink. xflow's built-in
	// registry is process-global, but the local runner neither advertises
	// xflow.script nor satisfies the script node's required selector; successful
	// execution therefore proves the actual standalone process claimed it.
	workflow := &types.WorkflowDef{
		Namespace: string(namespace.Default),
		Name:      "sas-dual-runner-e2e-" + runID,
		Nodes: []types.NodeDef{
			{
				Name: "generic-script",
				Type: "xflow.script",
				Kind: types.NodeKindAction,
				Parameters: map[string]any{
					"language": "js",
					"runtime":  "goja",
					"code":     `({routed_by: "sas-runner"})`,
				},
				RunnerSelector: &types.RunnerSelector{
					MatchLabels: map[string]string{
						"workload": "sas-runner",
					},
				},
			},
			{
				Name: "sas-sink",
				Type: sasDualRunnerSinkType,
				Kind: types.NodeKindAction,
				RunnerSelector: &types.RunnerSelector{
					MatchLabels: map[string]string{
						"workload": "local",
					},
				},
			},
		},
		Connections: types.Connections{
			"generic-script": {
				"main": {Targets: []types.Connection{{Node: "sas-sink", Input: "main"}}},
			},
		},
	}

	executionID := sasDualRunnerSubmitWorkflow(t, httpServer.URL, httpServer.Client(), workflow, nil)
	detail := sasDualRunnerWaitForTerminal(t, httpServer.URL, httpServer.Client(), executionID, localRunner, standaloneRunner)
	if detail.Status != types.ExecutionStatusSuccess {
		t.Fatalf("execution status = %s, want %s (error=%q)", detail.Status, types.ExecutionStatusSuccess, detail.Error)
	}

	generic := sasDualRunnerNode(t, detail, "generic-script")
	if generic.Status != types.NodeStatusSuccess {
		t.Fatalf("generic script status = %s, want %s (error=%q)", generic.Status, types.NodeStatusSuccess, generic.Error)
	}
	sinkNode := sasDualRunnerNode(t, detail, "sas-sink")
	if sinkNode.Status != types.NodeStatusSuccess {
		t.Fatalf("SAS sink status = %s, want %s (error=%q)", sinkNode.Status, types.NodeStatusSuccess, sinkNode.Error)
	}
	if got := sinkNode.Output["handled_by"]; got != "sas-local" {
		t.Fatalf("SAS sink output handled_by = %#v, want sas-local (output=%#v)", got, sinkNode.Output)
	}
	if got := sinkNode.Output["routed_by"]; got != "sas-runner" {
		t.Fatalf("SAS sink output routed_by = %#v, want sas-runner (output=%#v)", got, sinkNode.Output)
	}

	select {
	case received := <-sink.received:
		if got := received["routed_by"]; got != "sas-runner" {
			t.Fatalf("local sink received routed_by = %#v, want sas-runner (input=%#v)", got, received)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("local SAS sink never received the output from the standalone script runner")
	}
	if got := sink.count.Load(); got != 1 {
		t.Fatalf("local SAS sink invocation count = %d, want 1", got)
	}
	if exited, err := standaloneRunner.exited(); exited {
		t.Fatalf("standalone sas-runner exited before test cleanup: %v\nlogs:\n%s", err, standaloneRunner.output.String())
	}
}

// sasDualRunnerSink is intentionally test-local business behavior. It is
// registered only with the in-process SAS runner's isolated Registry, never
// with xflow's global built-in registry or the standalone process.
type sasDualRunnerSink struct {
	received chan map[string]any
	count    atomic.Int32
}

func newSASDualRunnerSink() *sasDualRunnerSink {
	return &sasDualRunnerSink{received: make(chan map[string]any, 1)}
}

func (h *sasDualRunnerSink) Descriptor() types.Descriptor {
	return types.Descriptor{Type: sasDualRunnerSinkType, Kind: types.NodeKindAction}
}

func (h *sasDualRunnerSink) Execute(_ context.Context, input *types.Input) (*types.Output, error) {
	h.count.Add(1)
	data := make(map[string]any, len(input.Data))
	for key, value := range input.Data {
		data[key] = value
	}
	select {
	case h.received <- data:
	default:
	}
	return &types.Output{Data: map[string]any{
		"handled_by": "sas-local",
		"routed_by":  data["routed_by"],
	}}, nil
}

// sasDualRunnerLocal owns an in-process xflow.Runner and makes its Run
// goroutine observable without consuming its completion result. A closed done
// channel remains safe to observe from both failure paths and t.Cleanup.
type sasDualRunnerLocal struct {
	runner *xflow.Runner
	cancel context.CancelFunc
	done   chan struct{}
	logs   *sasDualRunnerBuffer

	mu     sync.Mutex
	runErr error
}

func sasDualRunnerStartLocalRunner(t *testing.T, baseURL, runnerID, token string, sink *sasDualRunnerSink) *sasDualRunnerLocal {
	t.Helper()
	registry := execution.NewRegistry()
	registry.RegisterGlobal(sasDualRunnerSinkType, sink)
	logs := &sasDualRunnerBuffer{}
	runner, err := xflow.NewRunner(
		xflow.RunnerConfig{
			ServerURL:         baseURL,
			Transport:         xflow.RunnerTransportHTTP,
			RunnerID:          runnerID,
			Token:             token,
			Concurrency:       1,
			PollWait:          10 * time.Millisecond,
			HeartbeatInterval: 100 * time.Millisecond,
			Labels:            map[string]string{"workload": "local"},
			Capabilities:      []string{sasDualRunnerSinkType},
		},
		xflow.WithRunnerNodeRegistry(registry),
		xflow.WithRunnerLogger(slog.New(slog.NewTextHandler(logs, nil))),
	)
	if err != nil {
		t.Fatalf("create local SAS runner: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	local := &sasDualRunnerLocal{
		runner: runner,
		cancel: cancel,
		done:   make(chan struct{}),
		logs:   logs,
	}
	go func() {
		err := runner.Run(ctx)
		local.mu.Lock()
		local.runErr = err
		local.mu.Unlock()
		close(local.done)
	}()
	t.Cleanup(func() {
		local.stop(t)
		if t.Failed() {
			t.Logf("local SAS runner logs:\n%s", local.logs.String())
		}
	})
	return local
}

func (r *sasDualRunnerLocal) exited() (bool, error) {
	select {
	case <-r.done:
		r.mu.Lock()
		defer r.mu.Unlock()
		return true, r.runErr
	default:
		return false, nil
	}
}

func (r *sasDualRunnerLocal) stop(t *testing.T) {
	t.Helper()
	if r == nil {
		return
	}
	r.cancel()
	select {
	case <-r.done:
		r.mu.Lock()
		err := r.runErr
		r.mu.Unlock()
		if err != nil {
			t.Errorf("local SAS runner returned while stopping: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Error("local SAS runner did not stop within 10s of context cancellation")
	}
	if err := r.runner.Close(); err != nil {
		t.Errorf("close local SAS runner: %v", err)
	}
}

// sasDualRunnerBuffer is safe for concurrent child-process pipe writes and
// diagnostics from the test goroutine.
type sasDualRunnerBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *sasDualRunnerBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *sasDualRunnerBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// sasDualRunnerProcess owns Wait in one goroutine. done is separate from the
// error value so observing an early exit never races or steals Cleanup's signal.
type sasDualRunnerProcess struct {
	cmd    *exec.Cmd
	output *sasDualRunnerBuffer
	done   chan struct{}

	mu      sync.Mutex
	waitErr error
}

func sasDualRunnerStartProcess(t *testing.T, binary, baseURL, runnerID, token string) *sasDualRunnerProcess {
	t.Helper()
	output := &sasDualRunnerBuffer{}
	cmd := exec.Command(binary,
		"run",
		"--server", baseURL,
		"--transport", "http",
		"--id", runnerID,
		"--token", token,
		"--poll-wait", "10ms",
		"--concurrency", "1",
		"--allow-plaintext",
	)
	// A developer's runner config/environment must not accidentally override
	// the sibling profile under test. The binary receives every required value
	// through arguments, while its profile supplies labels and capabilities.
	cmd.Env = sasDualRunnerWithoutPrefix(os.Environ(), "XFLOW_RUNNER_")
	cmd.Stdout = output
	cmd.Stderr = output
	if err := cmd.Start(); err != nil {
		t.Fatalf("start standalone sas-runner: %v", err)
	}

	process := &sasDualRunnerProcess{cmd: cmd, output: output, done: make(chan struct{})}
	go func() {
		err := cmd.Wait()
		process.mu.Lock()
		process.waitErr = err
		process.mu.Unlock()
		close(process.done)
	}()
	t.Cleanup(func() {
		process.stop(t)
		if t.Failed() {
			t.Logf("standalone sas-runner logs:\n%s", output.String())
		}
	})
	return process
}

func (p *sasDualRunnerProcess) exited() (bool, error) {
	select {
	case <-p.done:
		p.mu.Lock()
		defer p.mu.Unlock()
		return true, p.waitErr
	default:
		return false, nil
	}
}

// stop asks the child to exit normally first. If SIGTERM is ignored or the
// runtime is stuck, SIGKILL is the bounded fallback so no runner survives a
// failed E2E test and steals work from a later test process.
func (p *sasDualRunnerProcess) stop(t *testing.T) {
	t.Helper()
	if p == nil || p.cmd == nil || p.cmd.Process == nil {
		return
	}
	if exited, _ := p.exited(); exited {
		return
	}

	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		select {
		case <-p.done:
			return
		case <-time.After(time.Second):
			t.Errorf("send SIGTERM to standalone sas-runner: %v", err)
			return
		}
	}
	select {
	case <-p.done:
		return
	case <-time.After(5 * time.Second):
	}

	if err := p.cmd.Process.Signal(syscall.SIGKILL); err != nil {
		select {
		case <-p.done:
			return
		default:
			t.Errorf("send SIGKILL to standalone sas-runner: %v", err)
			return
		}
	}
	select {
	case <-p.done:
	case <-time.After(5 * time.Second):
		t.Error("standalone sas-runner did not exit after SIGKILL")
	}
}

// sasDualRunnerRegistrationRecorder captures exactly what each runner sent to
// the real register endpoint, while still forwarding the untouched request to
// the embedded control plane for authentication and routing.
type sasDualRunnerRegistrationRecorder struct {
	mu            sync.Mutex
	registrations map[string]protocol.RegisterRunnerRequest
	err           error
}

func sasDualRunnerCaptureRegistrations(next http.Handler, recorder *sasDualRunnerRegistrationRecorder) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == protocol.RegisterRunnerPath {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				recorder.recordError(fmt.Errorf("read registration request: %w", err))
				http.Error(w, "failed to read registration request", http.StatusInternalServerError)
				return
			}
			_ = r.Body.Close()
			r.Body = io.NopCloser(bytes.NewReader(body))
			recorder.record(body)
		}
		next.ServeHTTP(w, r)
	})
}

func (r *sasDualRunnerRegistrationRecorder) record(body []byte) {
	var request protocol.RegisterRunnerRequest
	if err := json.Unmarshal(body, &request); err != nil {
		r.recordError(fmt.Errorf("decode registration request: %w", err))
		return
	}
	// Do not retain an authentication secret merely for profile assertions.
	request.AuthToken = ""
	request.Labels = sasDualRunnerCloneLabels(request.Labels)
	request.Capabilities = append([]protocol.Capability(nil), request.Capabilities...)

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.registrations == nil {
		r.registrations = make(map[string]protocol.RegisterRunnerRequest)
	}
	r.registrations[request.RunnerID] = request
}

func (r *sasDualRunnerRegistrationRecorder) recordError(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err == nil {
		r.err = err
	}
}

func (r *sasDualRunnerRegistrationRecorder) snapshot(runnerID string) (protocol.RegisterRunnerRequest, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return protocol.RegisterRunnerRequest{}, false, r.err
	}
	request, ok := r.registrations[runnerID]
	return request, ok, nil
}

func sasDualRunnerWaitForRegistrations(
	t *testing.T,
	recorder *sasDualRunnerRegistrationRecorder,
	local *sasDualRunnerLocal,
	standalone *sasDualRunnerProcess,
	localRunnerID, standaloneRunnerID string,
) (protocol.RegisterRunnerRequest, protocol.RegisterRunnerRequest) {
	t.Helper()
	deadline := time.NewTimer(20 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		localRegistration, localOK, err := recorder.snapshot(localRunnerID)
		if err != nil {
			t.Fatalf("capture local runner registration: %v", err)
		}
		standaloneRegistration, standaloneOK, err := recorder.snapshot(standaloneRunnerID)
		if err != nil {
			t.Fatalf("capture standalone runner registration: %v", err)
		}
		if localOK && standaloneOK {
			return localRegistration, standaloneRegistration
		}
		if exited, err := local.exited(); exited {
			t.Fatalf("local SAS runner exited before registration: %v\nlogs:\n%s", err, local.logs.String())
		}
		if exited, err := standalone.exited(); exited {
			t.Fatalf("standalone sas-runner exited before registration: %v\nlogs:\n%s", err, standalone.output.String())
		}

		select {
		case <-deadline.C:
			t.Fatalf("timeout waiting for runner registrations (local=%t standalone=%t)\nstandalone logs:\n%s",
				localOK, standaloneOK, standalone.output.String())
		case <-ticker.C:
		}
	}
}

func sasDualRunnerAssertRegistration(
	t *testing.T,
	name string,
	request protocol.RegisterRunnerRequest,
	wantLabels map[string]string,
	wantCapabilities []string,
) {
	t.Helper()
	if len(request.Labels) != len(wantLabels) {
		t.Fatalf("%s labels = %#v, want %#v", name, request.Labels, wantLabels)
	}
	for key, want := range wantLabels {
		if got := request.Labels[key]; got != want {
			t.Fatalf("%s label %q = %q, want %q (labels=%#v)", name, key, got, want, request.Labels)
		}
	}

	gotCapabilities := make(map[string]protocol.Capability, len(request.Capabilities))
	for _, capability := range request.Capabilities {
		if _, duplicate := gotCapabilities[capability.NodeType]; duplicate {
			t.Fatalf("%s registered duplicate capability %q: %#v", name, capability.NodeType, request.Capabilities)
		}
		gotCapabilities[capability.NodeType] = capability
	}
	if len(gotCapabilities) != len(wantCapabilities) {
		t.Fatalf("%s capabilities = %#v, want node types %#v", name, request.Capabilities, wantCapabilities)
	}
	for _, want := range wantCapabilities {
		capability, ok := gotCapabilities[want]
		if !ok {
			t.Fatalf("%s did not register capability %q: %#v", name, want, request.Capabilities)
		}
		if want == engine.GroupNodeType && !sasDualRunnerHasString(capability.Features, engine.FeatureGroupExecV1) {
			t.Fatalf("%s xflow.group features = %#v, want %q", name, capability.Features, engine.FeatureGroupExecV1)
		}
	}
}

func sasDualRunnerHasString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

type sasDualRunnerEnvelope struct {
	Success bool            `json:"success"`
	Code    string          `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
}

type sasDualRunnerExecuteRequest struct {
	Workflow *types.WorkflowDef `json:"workflow"`
	Params   map[string]any     `json:"params"`
}

type sasDualRunnerExecuteResponse struct {
	ExecutionID types.ExecutionID `json:"execution_id"`
}

// sasDualRunnerSubmitWorkflow keeps submit diagnostics local to this E2E: if
// graph compilation or admission fails, the server's response envelope is
// included in the failure rather than being discarded by a shared test helper.
func sasDualRunnerSubmitWorkflow(
	t *testing.T,
	baseURL string,
	client *http.Client,
	workflow *types.WorkflowDef,
	params map[string]any,
) types.ExecutionID {
	t.Helper()
	var payload bytes.Buffer
	if err := json.NewEncoder(&payload).Encode(sasDualRunnerExecuteRequest{Workflow: workflow, Params: params}); err != nil {
		t.Fatalf("encode workflow submission: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, baseURL+control.SubmitWorkflowPath, &payload)
	if err != nil {
		t.Fatalf("create workflow submission request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("submit workflow: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read workflow submission response: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("submit workflow status = %d, want %d; response=%s", resp.StatusCode, http.StatusOK, strings.TrimSpace(string(body)))
	}

	var envelope sasDualRunnerEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("decode workflow submission envelope: %v (response=%q)", err, string(body))
	}
	if !envelope.Success {
		t.Fatalf("submit workflow unsuccessful: code=%q message=%q response=%s", envelope.Code, envelope.Message, strings.TrimSpace(string(body)))
	}
	var result sasDualRunnerExecuteResponse
	if err := json.Unmarshal(envelope.Data, &result); err != nil {
		t.Fatalf("decode workflow submission data: %v (data=%s)", err, envelope.Data)
	}
	if result.ExecutionID == "" {
		t.Fatalf("submit workflow returned empty execution_id (response=%s)", strings.TrimSpace(string(body)))
	}
	return result.ExecutionID
}

func sasDualRunnerWaitForTerminal(
	t *testing.T,
	baseURL string,
	client *http.Client,
	executionID types.ExecutionID,
	local *sasDualRunnerLocal,
	standalone *sasDualRunnerProcess,
) engine.ExecutionDetail {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	ticker := time.NewTicker(30 * time.Millisecond)
	defer ticker.Stop()

	for {
		if exited, err := local.exited(); exited {
			t.Fatalf("local SAS runner exited while waiting for execution %s: %v\nlogs:\n%s", executionID, err, local.logs.String())
		}
		if exited, err := standalone.exited(); exited {
			t.Fatalf("standalone sas-runner exited while waiting for execution %s: %v\nlogs:\n%s", executionID, err, standalone.output.String())
		}

		detail, err := sasDualRunnerInspect(ctx, baseURL, client, executionID)
		if err != nil {
			if ctx.Err() != nil {
				t.Fatalf("timeout waiting for execution %s: %v\nstandalone logs:\n%s", executionID, ctx.Err(), standalone.output.String())
			}
			t.Fatalf("inspect execution %s: %v", executionID, err)
		}
		if types.IsTerminalExecutionStatus(detail.Status) {
			return detail
		}

		select {
		case <-ctx.Done():
			t.Fatalf("timeout waiting for execution %s: %v\nstandalone logs:\n%s", executionID, ctx.Err(), standalone.output.String())
		case <-ticker.C:
		}
	}
}

func sasDualRunnerInspect(ctx context.Context, baseURL string, client *http.Client, executionID types.ExecutionID) (engine.ExecutionDetail, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/v1/executions/"+string(executionID), nil)
	if err != nil {
		return engine.ExecutionDetail{}, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return engine.ExecutionDetail{}, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return engine.ExecutionDetail{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return engine.ExecutionDetail{}, fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var envelope sasDualRunnerEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil {
		return engine.ExecutionDetail{}, fmt.Errorf("decode response envelope: %w (body=%q)", err, string(body))
	}
	if !envelope.Success {
		return engine.ExecutionDetail{}, fmt.Errorf("unsuccessful response code=%q message=%q", envelope.Code, envelope.Message)
	}
	var detail engine.ExecutionDetail
	if err := json.Unmarshal(envelope.Data, &detail); err != nil {
		return engine.ExecutionDetail{}, fmt.Errorf("decode execution detail: %w (data=%s)", err, envelope.Data)
	}
	return detail, nil
}

func sasDualRunnerNode(t *testing.T, detail engine.ExecutionDetail, name string) engine.NodeDetail {
	t.Helper()
	for _, node := range detail.Nodes {
		if node.Name == name {
			return node
		}
	}
	t.Fatalf("execution %s has no node %q: %#v", detail.ExecutionID, name, detail.Nodes)
	return engine.NodeDetail{}
}

func sasDualRunnerRepoRoot(t *testing.T) string {
	t.Helper()
	start, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working directory: %v", err)
	}
	for dir := start; ; dir = filepath.Dir(dir) {
		if info, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil && !info.IsDir() {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("could not find xflow go.mod above %q", start)
		}
	}
}

// sasDualRunnerSourceDir resolves the explicit override first. Absence of a
// sibling source checkout is an environment precondition, not an XFlow test
// failure, so it skips with a precise way to supply one.
func sasDualRunnerSourceDir(t *testing.T, xflowRoot string) string {
	t.Helper()
	dir := strings.TrimSpace(os.Getenv("XFLOW_SAS_RUNNER_DIR"))
	if dir == "" {
		dir = filepath.Join(filepath.Dir(xflowRoot), "sas-runner")
	}
	absolute, err := filepath.Abs(dir)
	if err != nil {
		t.Skipf("sas-runner source is unavailable: resolve %q: %v; set XFLOW_SAS_RUNNER_DIR", dir, err)
	}
	if info, err := os.Stat(absolute); err != nil || !info.IsDir() {
		t.Skipf("sas-runner source is unavailable at %q; set XFLOW_SAS_RUNNER_DIR", absolute)
	}
	for _, required := range []string{"go.mod", filepath.Join("cmd", "sas-runner", "main.go")} {
		path := filepath.Join(absolute, required)
		info, err := os.Stat(path)
		if err != nil || info.IsDir() {
			t.Skipf("sas-runner source at %q lacks %s; set XFLOW_SAS_RUNNER_DIR to a compatible checkout", absolute, required)
		}
	}
	return absolute
}

// sasDualRunnerBuildBinary builds the sibling module using a disposable
// workspace. No go.mod, go.work, replace directive, or downloaded source in
// either checkout is mutated by this operation.
func sasDualRunnerBuildBinary(t *testing.T, xflowRoot, sasRunnerDir string) string {
	t.Helper()
	if _, err := exec.LookPath("go"); err != nil {
		t.Skipf("Go toolchain is unavailable for sas-runner build: %v", err)
	}

	temporary := t.TempDir()
	workspace := filepath.Join(temporary, "go.work")
	contents := "go 1.25.0\n\nuse (\n\t" + strconv.Quote(filepath.ToSlash(xflowRoot)) + "\n\t" + strconv.Quote(filepath.ToSlash(sasRunnerDir)) + "\n)\n"
	if err := os.WriteFile(workspace, []byte(contents), 0o600); err != nil {
		t.Fatalf("write temporary GOWORK: %v", err)
	}

	binary := filepath.Join(temporary, "sas-runner")
	cmd := exec.Command("go", "build", "-mod=readonly", "-o", binary, "./cmd/sas-runner")
	cmd.Dir = sasRunnerDir
	cmd.Env = sasDualRunnerSetEnv(os.Environ(), "GOWORK", workspace)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build sibling sas-runner through temporary GOWORK: %v\n%s", err, output)
	}
	return binary
}

func sasDualRunnerCloneLabels(labels map[string]string) map[string]string {
	if len(labels) == 0 {
		return nil
	}
	clone := make(map[string]string, len(labels))
	for key, value := range labels {
		clone[key] = value
	}
	return clone
}

func sasDualRunnerSetEnv(environment []string, key, value string) []string {
	prefix := key + "="
	filtered := make([]string, 0, len(environment)+1)
	for _, entry := range environment {
		if !strings.HasPrefix(entry, prefix) {
			filtered = append(filtered, entry)
		}
	}
	return append(filtered, prefix+value)
}

func sasDualRunnerWithoutPrefix(environment []string, prefix string) []string {
	filtered := make([]string, 0, len(environment))
	for _, entry := range environment {
		key, _, _ := strings.Cut(entry, "=")
		if !strings.HasPrefix(key, prefix) {
			filtered = append(filtered, entry)
		}
	}
	return filtered
}
