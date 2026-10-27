package apiserver

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/observability/tracing"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// installAPIServerSpanRecorder returns a tracer backed by an in-memory
// recorder. The apiserver module and the control plane take the tracer
// explicitly (unlike the engine, which resolves the global provider), so this
// test does not have to swap process globals. The W3C propagator IS global:
// carrier injection and extraction both read it, and without it every carrier
// in these tests would be empty.
func installAPIServerSpanRecorder(t *testing.T) (*tracetest.SpanRecorder, tracing.Tracer) {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(rec),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	prevPropagator := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTextMapPropagator(prevPropagator)
		_ = tp.Shutdown(context.Background())
	})
	return rec, tracing.NewOTelTracer(tp.Tracer("xflow-apiserver-test"))
}

func endedSpan(t *testing.T, rec *tracetest.SpanRecorder, name string) sdktrace.ReadOnlySpan {
	t.Helper()
	var found sdktrace.ReadOnlySpan
	for _, span := range rec.Ended() {
		if span.Name() == name {
			if found != nil {
				t.Fatalf("found more than one %q span", name)
			}
			found = span
		}
	}
	if found == nil {
		names := make([]string, 0, len(rec.Ended()))
		for _, span := range rec.Ended() {
			names = append(names, span.Name())
		}
		t.Fatalf("missing span %q; ended spans: %v", name, names)
	}
	return found
}

func spantributeString(span sdktrace.ReadOnlySpan, key string) (string, bool) {
	for _, kv := range span.Attributes() {
		if string(kv.Key) == key {
			return kv.Value.AsString(), true
		}
	}
	return "", false
}

// traceTestWorkflow is a single entry node: the dispatch test only needs a
// claimable task, not a handler to run.
func traceTestWorkflow() *types.WorkflowDef {
	return &types.WorkflowDef{
		Name:    "span-by-id",
		Version: "1",
		Nodes:   []types.NodeDef{{Name: "start", Type: "xflow.start"}},
	}
}

// readExecutionCarrier returns the W3C carrier persisted on the execution
// snapshot. It is the value the asynchronous dispatch span extracts, so
// asserting on it is asserting on what dispatch will parent to.
func readExecutionCarrier(t *testing.T, cp *control.ControlPlane, id types.ExecutionID) map[string]string {
	t.Helper()
	carrier, err := cp.SchedulingCore().ExecutionTraceCarrier(context.Background(), id)
	if err != nil {
		t.Fatalf("ExecutionTraceCarrier(%s) error = %v", id, err)
	}
	if len(carrier) == 0 {
		t.Fatalf("execution %s has no trace carrier: the submit span was not persisted", id)
	}
	return carrier
}

// TestExecuteWorkflowByIDStartsExecutionTrace pins the by-id route's half of
// the submit→dispatch causality contract: it must open the same
// xflow.workflow.execute span the inline route opens, tag it with the registry
// id, and persist its SpanContext on the execution snapshot.
func TestExecuteWorkflowByIDStartsExecutionTrace(t *testing.T) {
	rec, tracer := installAPIServerSpanRecorder(t)
	be := local.New()
	cp, err := control.NewControlPlane(control.Config{Backend: be, Tracer: tracer})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	m := &workflowControlModule{cp: cp, eng: cp.Engine(), tracer: tracer}
	mux := http.NewServeMux()
	m.RegisterHTTP(mux)

	def := traceTestWorkflow()
	registerSeedWorkflowDef(t, be, def)

	resp := doJSON(t, mux, http.MethodPost, "/v1/workflows/"+def.Name+"/execute", executeRegisteredRequest{})
	out := executionData(t, resp)
	if out.ExecutionID == "" {
		t.Fatal("by-id execute returned no execution id")
	}

	span := endedSpan(t, rec, "xflow.workflow.execute")
	if entry, ok := spantributeString(span, "entry"); !ok || entry != "" {
		t.Fatalf("entry attribute = %q, present=%t; want an empty entry for a params submission", entry, ok)
	}
	if id, ok := spantributeString(span, "workflow_id"); !ok || id != def.Name {
		t.Fatalf("workflow_id attribute = %q, present=%t; want %q", id, ok, def.Name)
	}
	// The route is the root of the execution's trace: nothing upstream started a
	// span (the unit mux has no HTTP middleware), so the span must be a root,
	// not a child of some other request span.
	if span.Parent().IsValid() {
		t.Fatalf("by-id execute span has parent %s, want a root span", span.Parent().SpanID())
	}

	// The persisted carrier must be exactly this span's context: that is what
	// makes the later asynchronous dispatch a child of the submit span.
	carrier := readExecutionCarrier(t, cp, out.ExecutionID)
	persisted := oteltrace.SpanContextFromContext(tracing.ExtractCarrier(context.Background(), carrier))
	if persisted.TraceID() != span.SpanContext().TraceID() || persisted.SpanID() != span.SpanContext().SpanID() {
		t.Fatalf("persisted carrier = trace %s span %s, want submit span %s/%s",
			persisted.TraceID(), persisted.SpanID(), span.SpanContext().TraceID(), span.SpanContext().SpanID())
	}
}

// TestExecuteWorkflowInlineKeepsExecuteSpanName is the regression guard on the
// shared name: the inline route's span is the one the G1 trace graph and the
// integration trace tests match on, so the by-id route must have reused it
// rather than introduced a second name, and the inline route must still emit
// it. It also pins that workflow_id is the by-id route's addition, not
// something the inline route started emitting.
func TestExecuteWorkflowInlineKeepsExecuteSpanName(t *testing.T) {
	rec, tracer := installAPIServerSpanRecorder(t)
	be := local.New()
	cp, err := control.NewControlPlane(control.Config{Backend: be})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	m := &workflowControlModule{cp: cp, eng: cp.Engine(), tracer: tracer}
	mux := http.NewServeMux()
	m.RegisterHTTP(mux)

	resp := doJSON(t, mux, http.MethodPost, PathWorkflowExecute, executeWorkflowRequest{
		Workflow: traceTestWorkflow(),
	})
	out := executionData(t, resp)
	if out.ExecutionID == "" {
		t.Fatal("inline execute returned no execution id")
	}

	span := endedSpan(t, rec, "xflow.workflow.execute")
	if _, ok := spantributeString(span, "workflow_id"); ok {
		t.Fatal("inline execute span carries workflow_id; only the by-id route has a registry id")
	}
	if span.Parent().IsValid() {
		t.Fatalf("inline execute span has parent %s, want a root span", span.Parent().SpanID())
	}
}

// TestExecuteWorkflowByIDDispatchParentedToExecuteSpan is the end-to-end half:
// the by-id route's root span must parent the control plane's dispatch span,
// via the persisted carrier (a real W3C remote parent, not a string-faked one).
func TestExecuteWorkflowByIDDispatchParentedToExecuteSpan(t *testing.T) {
	rec, tracer := installAPIServerSpanRecorder(t)
	be := local.New()
	cp, err := control.NewControlPlane(control.Config{Backend: be, Tracer: tracer})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	m := &workflowControlModule{cp: cp, eng: cp.Engine(), tracer: tracer}
	mux := http.NewServeMux()
	m.RegisterHTTP(mux)
	protocol.RegisterRunnerRoutes(mux, cp.RunnerHTTPHandler())
	httpSrv := httptest.NewServer(mux)
	defer httpSrv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := cp.Start(ctx); err != nil {
		t.Fatalf("ControlPlane.Start: %v", err)
	}
	t.Cleanup(func() { _ = cp.Shutdown(context.Background()) })

	// Register before submitting: the dispatcher must find a runner on its first
	// attempt, or an earlier failed dispatch would end its own span and the
	// unique-span assertion below would be reading the wrong one.
	session := registerTraceRunner(t, httpSrv.URL)

	def := traceTestWorkflow()
	registerSeedWorkflowDef(t, be, def)
	resp := doJSON(t, mux, http.MethodPost, "/v1/workflows/"+def.Name+"/execute", executeRegisteredRequest{})
	out := executionData(t, resp)
	if out.ExecutionID == "" {
		t.Fatal("by-id execute returned no execution id")
	}
	executeSpan := endedSpan(t, rec, "xflow.workflow.execute")

	lease := pollForLease(t, httpSrv.URL, session)
	if lease == nil {
		t.Fatal("poll returned no lease: the task was never dispatched")
	}

	dispatchSpan := endedSpan(t, rec, "xflow.task.dispatch")
	if dispatchSpan.SpanContext().TraceID() != executeSpan.SpanContext().TraceID() {
		t.Fatalf("dispatch trace = %s, want the execute trace %s",
			dispatchSpan.SpanContext().TraceID(), executeSpan.SpanContext().TraceID())
	}
	if dispatchSpan.Parent().SpanID() != executeSpan.SpanContext().SpanID() {
		t.Fatalf("dispatch parent = %s, want the execute span %s",
			dispatchSpan.Parent().SpanID(), executeSpan.SpanContext().SpanID())
	}
	if !dispatchSpan.Parent().IsRemote() {
		t.Fatal("dispatch parent is not remote: the persisted carrier must be re-extracted, not string-matched")
	}
	// The lease the runner receives carries the same trace, so the runner's
	// xflow.task.execute lands in the execution trace as well.
	leaseParent := oteltrace.SpanContextFromContext(tracing.ExtractCarrier(context.Background(), lease.TraceCarrier))
	if leaseParent.TraceID() != executeSpan.SpanContext().TraceID() {
		t.Fatalf("lease carrier trace = %s, want the execute trace %s",
			leaseParent.TraceID(), executeSpan.SpanContext().TraceID())
	}
}

const traceRunnerID = "runner-execute-span"

// registerTraceRunner registers a runner that accepts the trace test workflow's
// only node type and returns its session id.
func registerTraceRunner(t *testing.T, baseURL string) string {
	t.Helper()
	body := protocol.RegisterRunnerRequest{
		RunnerID:     traceRunnerID,
		InstanceUID:  "instance-execute-span",
		Concurrency:  1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.start"}},
	}
	var out protocol.RegisterRunnerResponse
	postTraceJSON(t, baseURL+protocol.RegisterRunnerPath, body, &out)
	if out.SessionID == "" {
		t.Fatal("runner registration returned no session id")
	}
	return out.SessionID
}

// pollForLease polls until the control plane hands back a lease or the retry
// budget runs out. Dispatch is asynchronous (submit enqueues, a queue worker
// claims), so a single poll can legitimately return nothing.
func pollForLease(t *testing.T, baseURL, sessionID string) *engine.TaskLease {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var out protocol.PollTaskResponse
		postTraceJSON(t, baseURL+protocol.PollTaskPath, protocol.PollTaskRequest{
			RunnerID:       traceRunnerID,
			SessionID:      sessionID,
			ActiveLeaseIDs: []string{},
		}, &out)
		if out.Lease != nil {
			return out.Lease
		}
		time.Sleep(10 * time.Millisecond)
	}
	return nil
}

func postTraceJSON(t *testing.T, url string, body, out any) {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	resp, err := http.Post(url, "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST %s status = %d", url, resp.StatusCode)
	}
	if out == nil {
		return
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		t.Fatalf("decode %s response: %v", url, err)
	}
}
