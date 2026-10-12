package apiserver

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// concurrencyLimitedWorkflow is a definition that admits one execution at a
// time and refuses the rest.
//
// The mode is explicit: a bare int means QUEUE mode, and queueing is not
// implemented by the local store, so a bare `concurrency: 1` here would be
// refused for a different reason than the one this file is about.
func concurrencyLimitedWorkflow() *types.WorkflowDef {
	return &types.WorkflowDef{
		Name: "concurrency-refused",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.echo"},
		},
		Settings: &types.WorkflowSettings{
			Concurrency: &types.ConcurrencySettings{
				Limit: 1,
				Mode:  types.ConcurrencyModeReject,
			},
		},
	}
}

// TestExecuteWorkflowRefusesOverLimitWith429 is the end-to-end half of the
// status mapping: a workflow that is already running its one allowed execution
// answers the next submission with 429, not 500.
//
// Nothing has to run for this: the first submission holds its slot until it
// reaches a terminal status, and with no runner polling, it never does.
func TestExecuteWorkflowRefusesOverLimitWith429(t *testing.T) {
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

	wf := concurrencyLimitedWorkflow()
	submitWorkflowE2E(t, httpSrv.URL, wf, nil)

	body := executeWorkflowRequest{Workflow: wf}
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(httpSrv.URL+"/v1/workflows/execute", "application/json", &buf)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("second submit status = %d, want 429", resp.StatusCode)
	}
	// Retry-After is a floor rather than a promise (no slot-release time is
	// knowable), but it has to be present and parseable: a client that only
	// reads the status has nothing to back off against.
	if got := resp.Header.Get("Retry-After"); got != "1" {
		t.Fatalf("Retry-After = %q, want \"1\"", got)
	}
	env := decodeEnvelope(t, resp, nil)
	if env.Code != "workflow_concurrency_limit" {
		t.Fatalf("code = %q, want workflow_concurrency_limit", env.Code)
	}
	if env.Success {
		t.Fatal("envelope reports success on a refused submission")
	}
}

// TestExecuteWorkflowEntryPathMapsConcurrencyRefusalTo429 covers the Invoke
// branch of the same handler, which the end-to-end case does not reach: it
// submits with no entry, so only Submit's mapping is exercised there.
func TestExecuteWorkflowEntryPathMapsConcurrencyRefusalTo429(t *testing.T) {
	f := &fakeControlFacade{invokeErr: engine.ErrConcurrencyLimit}
	mux := newControlMux(f)

	resp := doJSON(t, mux, http.MethodPost, "/v1/workflows/execute", executeWorkflowRequest{
		Workflow: validWorkflow(),
		Entry:    "start",
	})
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", resp.StatusCode)
	}
	if got := resp.Header.Get("Retry-After"); got != "1" {
		t.Fatalf("Retry-After = %q, want \"1\"", got)
	}
	if env := decodeEnvelope(t, resp, nil); env.Code != "workflow_concurrency_limit" {
		t.Fatalf("code = %q, want workflow_concurrency_limit", env.Code)
	}
}
