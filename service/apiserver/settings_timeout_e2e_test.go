package apiserver

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/service/protocol"
	runnersvc "github.com/xbcio/xflow/service/runner"
	"github.com/xbcio/xflow/types"
)

// blockingHandler holds a node open until the runner cancels it or the test
// ends. It stands in for a node that runs longer than the workflow's budget.
type blockingHandler struct {
	release <-chan struct{}
}

func (blockingHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.block"}
}

func (h blockingHandler) Execute(ctx context.Context, _ *types.Input) (*types.Output, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-h.release:
		return &types.Output{Data: map[string]any{"released": true}}, nil
	}
}

// registerWorkflowE2E registers a definition over POST /v1/workflows and returns
// the server-assigned id.
func registerWorkflowE2E(t *testing.T, baseURL string, wf *types.WorkflowDef) types.WorkflowID {
	t.Helper()
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(wf); err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(baseURL+PathWorkflows, "application/json", &buf)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	bodyBytes, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("register status = %d, want 201 (body=%s)", resp.StatusCode, bodyBytes)
	}
	var out registerWorkflowResponse
	if err := json.Unmarshal(extractData(t, bodyBytes), &out); err != nil {
		t.Fatal(err)
	}
	if out.WorkflowID == "" {
		t.Fatal("empty workflow_id")
	}
	return out.WorkflowID
}

// executeRegisteredE2E runs a registered workflow over
// POST /v1/workflows/{id}/execute with no body.
func executeRegisteredE2E(t *testing.T, baseURL string, id types.WorkflowID) types.ExecutionID {
	t.Helper()
	resp, err := http.Post(baseURL+"/v1/workflows/"+string(id)+"/execute", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	bodyBytes, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("execute status = %d, want 200 (body=%s)", resp.StatusCode, bodyBytes)
	}
	var out executeWorkflowResponse
	if err := json.Unmarshal(extractData(t, bodyBytes), &out); err != nil {
		t.Fatal(err)
	}
	if out.ExecutionID == "" {
		t.Fatal("empty execution_id")
	}
	return out.ExecutionID
}

// TestRegisteredWorkflowWithTimeoutExpiresOverHTTP is the end-to-end half of
// settings.timeout: the budget travels as integer nanoseconds in the
// registration body, is stored with the definition, becomes the execution's
// deadline when the registered workflow is run, and a node that outlives it ends
// the execution as timeout (reason "execution deadline exceeded") rather than
// failed.
func TestRegisteredWorkflowWithTimeoutExpiresOverHTTP(t *testing.T) {
	srv, err := New(Config{Concurrency: 1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = srv.Shutdown(context.Background()) }()

	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	release := make(chan struct{})
	defer close(release)
	registry := execution.NewRegistry()
	registry.RegisterGlobal("test.block", blockingHandler{release: release})
	runner := runnersvc.New(protocol.NewClient(httpSrv.URL, httpSrv.Client()), registry, runnersvc.Config{
		RunnerID:     "runner-timeout",
		Concurrency:  1,
		Capabilities: []protocol.Capability{{NodeType: "test.block"}},
		PollWait:     5 * time.Millisecond,
	})
	errCh := make(chan error, 1)
	go func() { errCh <- runner.Run(ctx) }()

	workflowID := registerWorkflowE2E(t, httpSrv.URL, &types.WorkflowDef{
		Name:     "http-timeout",
		Nodes:    []types.NodeDef{{Name: "slow", Type: "test.block"}},
		Settings: &types.WorkflowSettings{Timeout: 300 * time.Millisecond},
	})
	execID := executeRegisteredE2E(t, httpSrv.URL, workflowID)

	detail := waitExecutionE2E(t, httpSrv.URL, execID, 10*time.Second)
	if detail.Status != types.ExecutionStatusTimeout {
		t.Fatalf("status = %s (error %q), want %s", detail.Status, detail.Error, types.ExecutionStatusTimeout)
	}
	if detail.Error != engine.ExecutionDeadlineExceededError {
		t.Fatalf("error = %q, want %q", detail.Error, engine.ExecutionDeadlineExceededError)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("runner error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runner did not stop")
	}
}
