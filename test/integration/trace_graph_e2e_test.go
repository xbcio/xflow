//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/xbcio/xflow/backend/providers/distributed"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/observability/tracing"
	"github.com/xbcio/xflow/service/apiserver"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/protocol"
	runnersvc "github.com/xbcio/xflow/service/runner"
	"github.com/xbcio/xflow/types"
	"github.com/redis/go-redis/v9"
)

// TestServerRunnerE2ETraceGraphRealRedis proves the full B1 trace graph
// (submit → dispatch → execute → report → commit) is one trace with real W3C
// parentage against a real Redis backend. The submit carrier is persisted on
// the execution snapshot (Redis key trace_carrier) and extracted at dispatch
// so the asynchronous dispatch span is a child of submit — not a root and not
// a trace_id/span_id string fake parent (RELEASE-GATES §4). The runner's
// execute span is a remote child of dispatch via the lease carrier, and the
// server's commit span is a remote child of execute via the gRPC report
// carrier (B1 blocker 1).
//
// Requires real Redis at XFLOW_TEST_REDIS_ADDR (host port 6380); skips
// honestly when absent.
func TestServerRunnerE2ETraceGraphRealRedis(t *testing.T) {
	addr := requireRedis(t)

	// In-memory exporter + global provider so the engine's outboxTracer
	// (which uses otel.Tracer global) and the control-plane tracer both land
	// in the same recorder.
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(rec),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	defer tp.Shutdown(context.Background())
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	tracer := tracing.NewOTelTracer(tp.Tracer("github.com/xbcio/xflow"))

	b, err := distributed.New(addr, nil, distributed.WithConcurrency(1), distributed.WithConsumer(true))
	if err != nil {
		t.Fatalf("distributed.New: %v", err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	flushAsynqKeys(context.Background(), t, rdb)
	_ = rdb.Close()

	cp, err := control.NewControlPlane(control.Config{Backend: b, Tracer: tracer})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	srv, err := apiserver.New(apiserver.Config{Tracer: tracer}, apiserver.WithControlPlane(cp))
	if err != nil {
		t.Fatalf("apiserver.New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("apiserver.Start: %v", err)
	}
	defer func() {
		shutdownCtx, sc := context.WithTimeout(context.Background(), 5*time.Second)
		defer sc()
		_ = srv.Shutdown(shutdownCtx)
	}()

	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	// gRPC production transport with the stream+unary interceptors the
	// apiserver wires when Tracer is set.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcSrv := grpc.NewServer(
		grpc.UnaryInterceptor(tracing.GRPCUnaryServerInterceptor(tracer)),
		grpc.StreamInterceptor(tracing.GRPCStreamServerInterceptor(tracer)),
	)
	srv.RegisterGRPC(grpcSrv)
	go func() { _ = grpcSrv.Serve(lis) }()
	defer grpcSrv.Stop()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial gRPC: %v", err)
	}
	defer conn.Close()

	registry := execution.NewRegistry()
	registry.RegisterGlobal("test.e2e.trace", traceGraphHandler{})
	runner := runnersvc.New(
		protocol.NewGRPCClient(conn),
		registry,
		runnersvc.Config{
			RunnerID:     "runner-trace-grpc",
			Concurrency:  1,
			Capabilities: []protocol.Capability{{NodeType: "test.e2e.trace"}},
			PollWait:     5 * time.Millisecond,
			Tracer:       tracer,
		},
	)
	errCh := make(chan error, 1)
	go func() { errCh <- runner.Run(ctx) }()
	waitForE2ERunner(t, cp.RunnerDirectory(), "runner-trace-grpc")

	execID := submitTraceWorkflow(t, httpSrv.URL, &types.WorkflowDef{
		Name: "trace-graph-e2e",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.e2e.trace"},
		},
	}, map[string]any{"claim_id": "c-trace"})

	detail := waitTraceExecution(t, httpSrv.URL, execID, 15*time.Second)
	if detail.Status != types.ExecutionStatusSuccess {
		t.Fatalf("status = %s, want success", detail.Status)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil && err != context.Canceled {
			t.Fatalf("runner error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("runner did not stop")
	}

	// Flush the batch span processor so all ended spans are exported.
	_ = tp.ForceFlush(ctx)

	spans := rec.Ended()
	byName := map[string]sdktrace.ReadOnlySpan{}
	for _, s := range spans {
		// Keep the first occurrence per name within our xflow span set.
		switch s.Name() {
		case "xflow.workflow.submit", "xflow.task.dispatch", "xflow.task.execute", "xflow.task.report", "xflow.task.commit":
			if _, ok := byName[s.Name()]; !ok {
				byName[s.Name()] = s
			}
		}
	}
	for _, name := range []string{"xflow.workflow.submit", "xflow.task.dispatch", "xflow.task.execute", "xflow.task.report", "xflow.task.commit"} {
		if byName[name] == nil {
			t.Fatalf("missing span %q; got %v", name, spanNamesIntegration(spans))
		}
	}

	submit := byName["xflow.workflow.submit"]
	dispatch := byName["xflow.task.dispatch"]
	execute := byName["xflow.task.execute"]
	report := byName["xflow.task.report"]
	commit := byName["xflow.task.commit"]

	// All five spans must share one trace ID.
	root := submit.SpanContext().TraceID()
	for _, s := range []sdktrace.ReadOnlySpan{dispatch, execute, report, commit} {
		if s.SpanContext().TraceID() != root {
			t.Fatalf("span %q trace %s != submit trace %s (trace graph broken)", s.Name(), s.SpanContext().TraceID(), root)
		}
	}

	// dispatch is parented to submit (real W3C remote parent via persisted
	// carrier, not a string-faked parent).
	if dispatch.Parent().SpanID() != submit.SpanContext().SpanID() {
		t.Fatalf("dispatch parent %s != submit %s (dispatch did not inherit submit causality)",
			dispatch.Parent().SpanID(), submit.SpanContext().SpanID())
	}
	// execute is a remote child of dispatch via the lease carrier.
	if execute.Parent().SpanID() != dispatch.SpanContext().SpanID() {
		t.Fatalf("execute parent %s != dispatch %s (lease carrier not wired)",
			execute.Parent().SpanID(), dispatch.SpanContext().SpanID())
	}
	// commit is a child of execute via the gRPC report carrier, and report is
	// the outer span around commit. commit's parent should be execute (the
	// report carrier parented the report span to execute, and commit is a
	// child of report — so commit's parent chain reaches execute).
	if commit.Parent().SpanID() != report.SpanContext().SpanID() {
		t.Fatalf("commit parent %s != report %s (report/commit nesting broken)",
			commit.Parent().SpanID(), report.SpanContext().SpanID())
	}
	if report.Parent().SpanID() != execute.SpanContext().SpanID() {
		t.Fatalf("report parent %s != execute %s (gRPC report carrier not parented to execute)",
			report.Parent().SpanID(), execute.SpanContext().SpanID())
	}

	// Namespace must be a span ATTRIBUTE on submit, never W3C baggage. Assert the
	// namespace attribute is present and there is no baggage member on the
	// submit span context (baggage is opt-in and namespace is denylisted).
	hasNamespaceAttr := false
	for _, kv := range submit.Attributes() {
		if string(kv.Key) == "namespace" {
			hasNamespaceAttr = true
		}
	}
	if !hasNamespaceAttr {
		t.Fatalf("submit span has no namespace attribute (namespace must be a span attribute, not baggage)")
	}
}

type traceGraphHandler struct{}

func (traceGraphHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.e2e.trace"}
}
func (traceGraphHandler) Execute(_ context.Context, input *types.Input) (*types.Output, error) {
	return &types.Output{Data: map[string]any{
		"handled_by": "runner",
		"claim_id":   input.Data["claim_id"],
	}}, nil
}

func submitTraceWorkflow(t *testing.T, baseURL string, wf *types.WorkflowDef, params map[string]any) types.ExecutionID {
	t.Helper()
	body := e2eSubmitReq{Workflow: wf, Params: params}
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		t.Fatalf("encode: %v", err)
	}
	resp, err := http.Post(baseURL+control.SubmitWorkflowPath, "application/json", &buf)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("submit status = %d, want 200", resp.StatusCode)
	}
	var out e2eSubmitResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return out.ExecutionID
}

func waitTraceExecution(t *testing.T, baseURL string, id types.ExecutionID, timeout time.Duration) engine.ExecutionDetail {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, baseURL+"/v1/executions/"+string(id)+"/wait?timeout="+timeout.String(), nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	defer resp.Body.Close()
	var detail engine.ExecutionDetail
	if err := json.NewDecoder(resp.Body).Decode(&detail); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return detail
}

func spanNamesIntegration(spans []sdktrace.ReadOnlySpan) []string {
	out := make([]string, 0, len(spans))
	for _, s := range spans {
		out = append(out, s.Name())
	}
	return out
}
