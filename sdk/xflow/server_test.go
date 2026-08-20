package xflow

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/protocol"
	runnersvc "github.com/xbcio/xflow/service/runner"
	"github.com/xbcio/xflow/store/memstore"
	"github.com/xbcio/xflow/types"
)

// sdkEnvelope is the wire envelope (API-SPECIFICATION.md §3) the user-facing
// executions endpoints return. The SDK tests unwrap the data field to decode
// the typed payload, mirroring the integration package's e2eEnvelope.
type sdkEnvelope struct {
	Success bool            `json:"success"`
	Code    string          `json:"code"`
	Data    json.RawMessage `json:"data"`
}

func TestNewServerMemoryBackendServesHandler(t *testing.T) {
	srv, err := NewServer(ServerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := srv.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srv.Shutdown(context.Background()) }()

	req := httptest.NewRequest(http.MethodPost, control.SubmitWorkflowPath, nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code == http.StatusNotFound {
		t.Fatalf("Handler() did not route %s, got 404", control.SubmitWorkflowPath)
	}
}

func TestNewServerShutdownIsGraceful(t *testing.T) {
	srv, err := NewServer(ServerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
}

func TestNewServerMountableOnHostMux(t *testing.T) {
	srv, err := NewServer(ServerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srv.Shutdown(context.Background()) }()

	mux := http.NewServeMux()
	mux.Handle("/xflow/", http.StripPrefix("/xflow", srv.Handler()))

	req := httptest.NewRequest(http.MethodPost, "/xflow"+control.SubmitWorkflowPath, nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code == http.StatusNotFound {
		t.Fatal("host mux did not route through mounted xflow handler")
	}
}

func TestServerIsLeader(t *testing.T) {
	srv, err := NewServer(ServerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if !srv.IsLeader() {
		t.Fatal("IsLeader() = false for memory backend, want true (AlwaysLeader)")
	}
}

// TestNewServerRedisBackendDispatchesTaskToRunner guards against a regression
// where NewServer's Redis branch passed distributed.WithConsumer(false),
// which disables the Asynq subscription entirely (no mux registration, no
// timeout monitor, no consumer goroutine — see (*distributed.Backend).BindHandler).
// That left submitted workflows enqueued forever with nothing ever consuming
// the queue and dispatching to a registered runner. This test exercises the
// full path against real (miniredis) Redis: submit a workflow through the
// HTTP handler, run a real runner against it over the Runner Protocol, and
// confirm the execution actually reaches a terminal state — which is only
// possible if the Task Dispatcher is actively consuming the Asynq queue.
func TestNewServerRedisBackendDispatchesTaskToRunner(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	srv, err := NewServer(ServerConfig{RedisAddr: mr.Addr()})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := srv.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srv.Shutdown(context.Background()) }()

	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	registry := execution.NewRegistry()
	registry.RegisterGlobal("test.sdk-e2e", sdkE2EHandler{})

	runnerCtx, runnerCancel := context.WithCancel(context.Background())
	defer runnerCancel()
	runner := runnersvc.New(protocol.NewClient(httpSrv.URL, httpSrv.Client()), registry, runnersvc.Config{
		RunnerID:     "sdk-runner-1",
		Concurrency:  1,
		Capabilities: []protocol.Capability{{NodeType: "test.sdk-e2e"}},
		PollWait:     5 * time.Millisecond,
	})
	errCh := make(chan error, 1)
	go func() { errCh <- runner.Run(runnerCtx) }()

	execID := submitSDKWorkflow(t, httpSrv.URL, &types.WorkflowDef{
		Name: "sdk-redis-dispatch-e2e",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.sdk-e2e"},
		},
	}, map[string]any{"claim_id": "sdk-c-1"})

	status := waitForExecutionStatus(t, httpSrv.URL, execID, 5*time.Second)
	if status != types.ExecutionStatusSuccess {
		t.Fatalf("execution status = %s, want success", status)
	}

	runnerCancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("runner error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("runner did not stop")
	}
}

type sdkE2EHandler struct{}

func (sdkE2EHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.sdk-e2e"}
}

func (sdkE2EHandler) Execute(_ context.Context, input *types.Input) (*types.Output, error) {
	return &types.Output{Data: map[string]any{
		"handled_by": "runner",
		"claim_id":   input.Data["claim_id"],
	}}, nil
}

func submitSDKWorkflow(t *testing.T, baseURL string, wf *types.WorkflowDef, params map[string]any) types.ExecutionID {
	t.Helper()
	body := struct {
		Workflow *types.WorkflowDef `json:"workflow"`
		Params   map[string]any     `json:"params,omitempty"`
	}{Workflow: wf, Params: params}
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(baseURL+control.SubmitWorkflowPath, "application/json", &buf)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("submit status = %d, want 200", resp.StatusCode)
	}
	var env struct {
		Data struct {
			ExecutionID types.ExecutionID `json:"execution_id"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		t.Fatal(err)
	}
	if env.Data.ExecutionID == "" {
		t.Fatal("empty execution_id")
	}
	return env.Data.ExecutionID
}

func waitForExecutionStatus(t *testing.T, baseURL string, execID types.ExecutionID, timeout time.Duration) types.ExecutionStatus {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		resp, err := http.Get(baseURL + "/v1/executions/" + string(execID))
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		// The inspect response is enveloped (spec §3); unwrap data before
		// decoding the typed ExecutionDetail.
		var detail engine.ExecutionDetail
		decodeErr := json.Unmarshal(extractSDKData(t, body), &detail)
		if resp.StatusCode == http.StatusOK && decodeErr == nil && types.IsTerminalExecutionStatus(detail.Status) {
			return detail.Status
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for execution %q to reach terminal status, last status %q", execID, detail.Status)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// extractSDKData unwraps an enveloped response body and returns the raw data
// field. The user-facing endpoints return the §3 envelope; the SDK tests decode
// the typed payload from data.
func extractSDKData(t *testing.T, body []byte) []byte {
	t.Helper()
	var env sdkEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		t.Fatalf("decode envelope: %v (raw=%q)", err, string(body))
	}
	return env.Data
}

func TestServerUpdateSupply(t *testing.T) {
	ms := memstore.New()
	srv, err := NewServer(ServerConfig{Store: ms})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := srv.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srv.Shutdown(ctx) }()

	content := []byte(`{"sasl_mechanism":"scram-sha-256","sasl_username":"user","sasl_password":"pass"}`)
	if err := srv.UpdateSupply(ctx, "", "kafka-creds", content); err != nil {
		t.Fatalf("UpdateSupply() error = %v", err)
	}

	// Verify content was persisted via the store.
	rec, err := ms.GetSupply(ctx, "default", "kafka-creds")
	if err != nil {
		t.Fatalf("GetSupply() error = %v", err)
	}
	if !bytes.Equal(rec.Content, content) {
		t.Fatalf("stored content = %s, want %s", rec.Content, content)
	}
	if rec.ContentHash == "" {
		t.Fatal("content hash is empty")
	}
	if rec.Revision == 0 {
		t.Fatal("revision not bumped")
	}

	// Update again — unconditional write, revision bumps.
	content2 := []byte(`{"sasl_mechanism":"plain","sasl_username":"u2","sasl_password":"p2"}`)
	if err := srv.UpdateSupply(ctx, "", "kafka-creds", content2); err != nil {
		t.Fatalf("UpdateSupply() second call error = %v", err)
	}
	rec2, err := ms.GetSupply(ctx, "default", "kafka-creds")
	if err != nil {
		t.Fatal(err)
	}
	if rec2.Revision <= rec.Revision {
		t.Fatalf("revision did not bump: %d <= %d", rec2.Revision, rec.Revision)
	}
	if !bytes.Equal(rec2.Content, content2) {
		t.Fatalf("stored content after update = %s, want %s", rec2.Content, content2)
	}
}

func TestServerUpdateSupplyNoStore(t *testing.T) {
	srv, err := NewServer(ServerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.UpdateSupply(context.Background(), "", "x", []byte("y")); err == nil {
		t.Fatal("expected error when Store is nil")
	}
}

func TestServerUpdateSupplyEmptyName(t *testing.T) {
	ms := memstore.New()
	srv, err := NewServer(ServerConfig{Store: ms})
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.UpdateSupply(context.Background(), "", "", []byte("y")); err == nil {
		t.Fatal("expected error for empty name")
	}
}

// TestServerUpdateSupplyEmptyNSRoundTrip verifies that writing with ns=""
// (which normalises to "default") is readable by reading with ns="" via the
// store directly. This is the asymmetric-namespace regression test.
func TestServerUpdateSupplyEmptyNSRoundTrip(t *testing.T) {
	ms := memstore.New()
	srv, err := NewServer(ServerConfig{Store: ms})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := srv.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srv.Shutdown(ctx) }()

	content := []byte(`{"probe":true}`)
	if err := srv.UpdateSupply(ctx, "", "probe-supply", content); err != nil {
		t.Fatalf("UpdateSupply() error = %v", err)
	}

	// Read back via the store with empty namespace — must find the record.
	rec, err := ms.GetSupply(ctx, "", "probe-supply")
	if err != nil {
		t.Fatalf("GetSupply(\"\", ...) error = %v; want the record written via UpdateSupply(\"\", ...)", err)
	}
	if !bytes.Equal(rec.Content, content) {
		t.Fatalf("content mismatch: got %s, want %s", rec.Content, content)
	}
}
