package xflow

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/service/protocol"
	runnersvc "github.com/xbcio/xflow/service/runner"
	"github.com/xbcio/xflow/types"
)

// This file is the end-to-end proof for U-9: a node's structured
// ClassifiedError.Details must be readable by a consumer of the execution API
// AFTER the execution has been read back, not merely present in the process
// that produced it.
//
// The distinction matters because Details already survived the runner→server
// wire (service/protocol.MarshalTaskResult) before any of this existed. The
// loss was at the persistence/projection layer: engine.EffectiveClassification
// dropped the map, so nothing was ever stored or served. A test that only
// exercised MarshalTaskResult/UnmarshalTaskResult would have passed against the
// broken code.
//
// Three read surfaces are asserted, because they are three different readers of
// the same projection and only the last one crosses a process boundary that
// could silently drop a field:
//
//	Engine.Wait    -> types.Result      (what most callers use)
//	Engine.Inspect -> engine.ExecutionDetail
//	GET /v1/executions/{id} -> the JSON body of ExecutionDetail
const (
	errorDetailsNodeType   = "test.error-details"
	errorNoDetailsNodeType = "test.error-no-details"
)

// errorDetailsNodeHandler fails the way a policy-denied browser navigation
// does: a fixed message, plus a Details map naming which of several sources
// made the decision. Details is the ONLY place that distinction lives once the
// message has been rendered.
var errorDetailsNodeHandler = node.Define(
	errorDetailsNodeType,
	func(_ context.Context, _ *types.Input) (*types.Output, error) {
		return nil, &types.ClassifiedError{
			Kind:      types.ErrorKindPermanent,
			Code:      "browser.host_denied",
			Message:   "browser destination is denied by policy",
			Permanent: true,
			Details: map[string]any{
				"source":       "endpoint_allowlist",
				"rejected_url": "https://denied.test/path",
			},
		}
	},
)

// errorNoDetailsNodeHandler is the control: same shape of failure, same
// classification, no Details. It pins that the new field is additive — a
// detail-free error must keep serializing exactly as it did before, with no
// empty "error_details" object appearing on the surface.
var errorNoDetailsNodeHandler = node.Define(
	errorNoDetailsNodeType,
	func(_ context.Context, _ *types.Input) (*types.Output, error) {
		return nil, &types.ClassifiedError{
			Kind:      types.ErrorKindPermanent,
			Code:      "browser.unavailable",
			Message:   "remote browser is unavailable",
			Permanent: true,
		}
	},
)

// TestExecutionErrorDetailsReachConsumerSurfaces is the primary U-9 test.
func TestExecutionErrorDetailsReachConsumerSurfaces(t *testing.T) {
	facade, httpSrv := newErrorDetailsHarness(t)

	detailsID := invokeFailingNode(t, facade, "error-details-wf", errorDetailsNodeHandler)
	plainID := invokeFailingNode(t, facade, "error-no-details-wf", errorNoDetailsNodeHandler)

	t.Run("Details are visible on Inspect", func(t *testing.T) {
		detail := inspectWorkflow(t, facade, detailsID)
		nodeDetail := findNodeDetail(t, detail, "work")

		if nodeDetail.ErrorDetails == nil {
			t.Fatalf("Inspect() node %q ErrorDetails = nil, want the node's structured detail; "+
				"the failure message alone is %q", "work", nodeDetail.Error)
		}
		if got, want := nodeDetail.ErrorDetails["source"], "endpoint_allowlist"; got != want {
			t.Errorf("ErrorDetails[source] = %#v, want %#v", got, want)
		}
		if got, want := nodeDetail.ErrorDetails["rejected_url"], "https://denied.test/path"; got != want {
			t.Errorf("ErrorDetails[rejected_url] = %#v, want %#v", got, want)
		}
	})

	t.Run("Details are visible in the HTTP inspect body", func(t *testing.T) {
		body := getInspectBody(t, httpSrv, detailsID)
		t.Logf("GET /v1/executions/%s body = %s", detailsID, body)

		for _, want := range []string{`"error_details"`, `"source":"endpoint_allowlist"`, `"rejected_url":"https://denied.test/path"`} {
			if !strings.Contains(body, want) {
				t.Errorf("inspect body is missing %s; body = %s", want, body)
			}
		}
	})

	t.Run("Wait reports the failure but carries no per-node detail", func(t *testing.T) {
		result := waitWorkflow(t, facade, detailsID)
		encoded, err := json.Marshal(result)
		if err != nil {
			t.Fatalf("marshal Result: %v", err)
		}
		t.Logf("Wait() result JSON = %s", encoded)

		if result.Status != types.ExecutionStatusFailed {
			t.Fatalf("Wait() status = %q, want %q", result.Status, types.ExecutionStatusFailed)
		}
		// Result is an execution-level view: it carries the rendered reason and
		// no per-node structure. Pinned deliberately rather than left implicit,
		// so that if Wait ever starts carrying details the assertion is
		// revisited instead of silently going stale.
		if strings.Contains(string(encoded), "error_details") {
			t.Errorf("Wait() result unexpectedly carries per-node error details: %s", encoded)
		}
		if !strings.Contains(result.Error, "browser.host_denied") {
			t.Errorf("Wait() error = %q, want the classified failure reason", result.Error)
		}
	})

	t.Run("an error with no Details stays byte-clean", func(t *testing.T) {
		detail := inspectWorkflow(t, facade, plainID)
		nodeDetail := findNodeDetail(t, detail, "work")

		if len(nodeDetail.ErrorDetails) != 0 {
			t.Fatalf("Inspect() ErrorDetails = %#v, want empty for an error that carried none", nodeDetail.ErrorDetails)
		}
		if nodeDetail.Error == "" {
			t.Fatal("Inspect() Error is empty: the control case must still report the plain message")
		}

		body := getInspectBody(t, httpSrv, plainID)
		t.Logf("control GET /v1/executions/%s body = %s", plainID, body)
		if strings.Contains(body, "error_details") {
			t.Errorf("control inspect body contains error_details for a detail-free error: %s", body)
		}
		if !strings.Contains(body, "browser.unavailable") {
			t.Errorf("control inspect body is missing the plain failure text: %s", body)
		}
	})
}

// TestPrivateOutputWithholdsErrorDetails is the U-5 guard for the new field.
//
// engine/inspect.go fails closed for a node whose graph policy marks its output
// private, and a new field on NodeDetail is exactly the kind of change that can
// quietly become a second egress for data the first one refuses to serve.
// ErrorDetails is therefore projected AFTER that fail-closed return.
//
// The assertion is paired with a positive control in the same test: an
// identical non-private node DOES surface its details. Without the control the
// redaction assertion would pass if ErrorDetails were simply never populated,
// which is the bug this whole change exists to fix.
func TestPrivateOutputWithholdsErrorDetails(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	eng, err := NewLocal()
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	defer eng.Stop()

	wf := Workflow("private-error-details")
	start := wf.Node("start", node.Start())
	// Both nodes are non-fatal. On the default "stop" strategy the FIRST
	// failure aborts the execution and the second node never runs, so the
	// public control would be inspected while still pending and the assertion
	// on it would fail for a reason that has nothing to do with redaction.
	private := wf.LocalNode("private", errorDetailsNodeHandler).PrivateOutput().OnError(types.OnErrorContinue)
	public := wf.LocalNode("public", errorDetailsNodeHandler).OnError(types.OnErrorContinue)
	wf.Connect(start, private)
	wf.Connect(private, public)

	workflowID, err := eng.AddWorkflow(ctx, wf)
	if err != nil {
		t.Fatalf("AddWorkflow() error = %v", err)
	}
	executionID, err := eng.Invoke(ctx, workflowID, Start(), nil)
	if err != nil {
		t.Fatalf("Invoke() error = %v", err)
	}
	if _, err := eng.Wait(ctx, executionID); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}

	detail, err := eng.Inspect(ctx, executionID)
	if err != nil {
		t.Fatalf("Inspect() error = %v", err)
	}

	privateDetail := findNodeDetail(t, detail, "private")
	if len(privateDetail.ErrorDetails) != 0 {
		t.Fatalf("private node ErrorDetails = %#v, want empty: the detail map is a "+
			"runner-controlled channel and must fail closed with the private output",
			privateDetail.ErrorDetails)
	}
	if privateDetail.Error == "" {
		t.Error("private node Error is empty: the rendered reason must still be reported, " +
			"otherwise the failure becomes undiagnosable rather than redacted")
	}
	if privateDetail.Output != nil {
		t.Errorf("private node Output = %#v, want nil", privateDetail.Output)
	}

	// Positive control on the same run: the identical handler on a public node
	// must still expose Details, proving the redaction above is specific to the
	// private policy rather than a dead field.
	publicDetail := findNodeDetail(t, detail, "public")
	if got, want := publicDetail.ErrorDetails["source"], "endpoint_allowlist"; got != want {
		t.Fatalf("public node ErrorDetails[source] = %#v, want %#v — the control case must "+
			"surface details, or the private-node assertion above proves nothing",
			got, want)
	}
}

// newErrorDetailsHarness stands up the in-process server + real runner pair
// used by server_engine_test.go, and returns the consumer facade plus a client
// for the control HTTP API.
//
// Both node types are declared in the runner's Capabilities. Without that the
// dispatcher never matches a capability for the node and the execution simply
// hangs until the test context expires — a 15s timeout that reads like a
// product bug rather than a harness omission.
func newErrorDetailsHarness(t *testing.T) (*Engine, *httptest.Server) {
	t.Helper()

	srv, err := NewServer(ServerConfig{}, WithServerInsecureNoRunnerAuth())
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			t.Errorf("Shutdown() error = %v", err)
		}
	})

	httpSrv := httptest.NewServer(srv.Handler())
	t.Cleanup(httpSrv.Close)

	registry := execution.NewRegistry()
	registry.RegisterGlobal(errorDetailsNodeType, errorDetailsNodeHandler)
	registry.RegisterGlobal(errorNoDetailsNodeType, errorNoDetailsNodeHandler)

	runnerCtx, runnerCancel := context.WithCancel(context.Background())
	runner := runnersvc.New(protocol.NewClient(httpSrv.URL, httpSrv.Client()), registry, runnersvc.Config{
		RunnerID:    "error-details-runner",
		Concurrency: 1,
		Capabilities: []protocol.Capability{
			{NodeType: "xflow.start"},
			{NodeType: errorDetailsNodeType},
			{NodeType: errorNoDetailsNodeType},
		},
		PollWait: 5 * time.Millisecond,
	})
	runnerErr := make(chan error, 1)
	go func() { runnerErr <- runner.Run(runnerCtx) }()
	t.Cleanup(func() {
		runnerCancel()
		select {
		case err := <-runnerErr:
			if err != nil {
				t.Errorf("runner error = %v", err)
			}
		case <-time.After(time.Second):
			t.Error("runner did not stop")
		}
	})

	return srv.Engine(), httpSrv
}

// invokeFailingNode runs start -> work, where work always fails, and returns
// the execution id once the engine has accepted the invocation.
func invokeFailingNode(t *testing.T, facade *Engine, workflowName string, handler *node.Definition) types.ExecutionID {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	wf := Workflow(workflowName)
	start := wf.Node("start", node.Start())
	work := wf.Node("work", handler.New(nil))
	wf.Connect(start, work)

	workflowID, err := facade.AddWorkflow(ctx, wf)
	if err != nil {
		t.Fatalf("AddWorkflow(%s) error = %v", workflowName, err)
	}
	executionID, err := facade.Invoke(ctx, workflowID, Start(), nil)
	if err != nil {
		t.Fatalf("Invoke(%s) error = %v", workflowName, err)
	}
	// Wait for terminal state so the failed commit has certainly landed before
	// any surface is read.
	waitWorkflow(t, facade, executionID)
	return executionID
}

func waitWorkflow(t *testing.T, facade *Engine, id types.ExecutionID) types.Result {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	result, err := facade.Wait(ctx, id)
	if err != nil {
		t.Fatalf("Wait(%s) error = %v", id, err)
	}
	return result
}

func inspectWorkflow(t *testing.T, facade *Engine, id types.ExecutionID) engine.ExecutionDetail {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	detail, err := facade.Inspect(ctx, id)
	if err != nil {
		t.Fatalf("Inspect(%s) error = %v", id, err)
	}
	encoded, err := json.Marshal(detail)
	if err != nil {
		t.Fatalf("marshal ExecutionDetail: %v", err)
	}
	t.Logf("Inspect(%s) JSON = %s", id, encoded)
	return detail
}

// findNodeDetail returns the audit detail for one node, failing the test when
// the node is absent rather than returning a zero value that would let an
// assertion pass vacuously.
func findNodeDetail(t *testing.T, detail engine.ExecutionDetail, nodeName string) engine.NodeDetail {
	t.Helper()
	for _, n := range detail.Nodes {
		if n.Name == nodeName {
			return n
		}
	}
	t.Fatalf("Inspect() has no node %q; got %+v", nodeName, detail.Nodes)
	return engine.NodeDetail{}
}

// getInspectBody reads GET /v1/executions/{id} and returns the raw JSON body.
// Asserting on the raw body rather than a decoded envelope keeps the check
// independent of the response-envelope shape (spec §3), which is not what this
// test is about.
func getInspectBody(t *testing.T, httpSrv *httptest.Server, id types.ExecutionID) string {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, httpSrv.URL+"/v1/executions/"+string(id), nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := httpSrv.Client().Do(req)
	if err != nil {
		t.Fatalf("GET /v1/executions/%s: %v", id, err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read inspect body: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/executions/%s = %d, want 200; body = %s", id, resp.StatusCode, body)
	}
	return string(body)
}
