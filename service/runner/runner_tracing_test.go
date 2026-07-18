package runner

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/observability/tracing"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// capturingClient is a fakeProtocolClient that records the report request so the
// test can assert the runner injected a report carrier parented to its execute span.
type capturingClient struct {
	lease    *engine.TaskLease
	reported protocol.ReportResultRequest
	got      bool
	cancel   context.CancelFunc
}

func (c *capturingClient) Register(context.Context, protocol.RegisterRunnerRequest) (protocol.RegisterRunnerResponse, error) {
	return protocol.RegisterRunnerResponse{SessionID: "session-1"}, nil
}
func (c *capturingClient) Heartbeat(context.Context, protocol.HeartbeatRequest) (protocol.HeartbeatResponse, error) {
	return protocol.HeartbeatResponse{}, nil
}
func (c *capturingClient) Poll(ctx context.Context, _ protocol.PollTaskRequest) (protocol.PollTaskResponse, error) {
	if c.got {
		// Already delivered the lease; wait for the test to cancel so the poll
		// loop doesn't spin.
		<-ctx.Done()
		return protocol.PollTaskResponse{}, ctx.Err()
	}
	c.got = true
	return protocol.PollTaskResponse{Lease: c.lease}, nil
}
func (c *capturingClient) ReportResult(_ context.Context, req protocol.ReportResultRequest) (protocol.ReportResultResponse, error) {
	c.reported = req
	// Stop the runner after the report is captured.
	if c.cancel != nil {
		c.cancel()
	}
	return protocol.ReportResultResponse{Accepted: true}, nil
}

// TestRunnerExecuteSpanParentedToDispatchCarrier verifies the B1 contract: the
// runner extracts the remote parent from the lease's TraceCarrier, starts an
// xflow.task.execute span in the SAME trace, and injects a report carrier whose
// traceparent references the execute span. A bare context.Background() report
// would have lost the trace linkage.
func TestRunnerExecuteSpanParentedToDispatchCarrier(t *testing.T) {
	// Set up a real OTel provider with an in-memory recorder so we can inspect
	// the span graph the runner produces.
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(rec),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	dispatchTracer := tracing.NewOTelTracer(tp.Tracer("xflow-test"))

	// Server-side: simulate a dispatch span and inject its carrier into the lease.
	dispatchCtx, dispatchSpan := dispatchTracer.Start(context.Background(), "xflow.task.dispatch",
		"execution_id", "exec-trace-1",
	)
	carrier := tracing.InjectCarrier(dispatchCtx)
	dispatchSpan.End()

	lease := &engine.TaskLease{
		LeaseID:      engine.LeaseID("lease-trace"),
		Task:         engine.Task{ExecutionID: types.ExecutionID("exec-trace-1"), NodeName: "start"},
		Input:        &types.Input{Data: map[string]any{"claim_id": "c-trace"}},
		NodeType:     "test.function",
		TraceCarrier:  carrier,
	}
	client := &capturingClient{lease: lease}
	registry := execution.NewRegistry()
	registry.RegisterGlobal("test.function", functionHandler{})

	r := New(client, registry, Config{
		RunnerID:          "runner-trace",
		Concurrency:       1,
		Capabilities:      []protocol.Capability{{NodeType: "test.function"}},
		HeartbeatInterval: time.Hour,
		PollWait:          time.Millisecond,
		Tracer:            dispatchTracer,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	client.cancel = cancel
	defer cancel()
	if err := r.Run(ctx); err != nil && err != context.Canceled {
		t.Fatalf("Run() error = %v", err)
	}

	// The runner must have injected a report carrier.
	if len(client.reported.TraceCarrier) == 0 {
		t.Fatal("runner did not inject a report carrier")
	}

	// The report carrier's traceparent must share the dispatch trace ID.
	ended := rec.Ended()
	var executeSpan, foundDispatch sdktrace.ReadOnlySpan
	for _, s := range ended {
		switch s.Name() {
		case "xflow.task.execute":
			executeSpan = s
		case "xflow.task.dispatch":
			foundDispatch = s
		}
	}
	if foundDispatch == nil {
		t.Fatal("dispatch span not recorded")
	}
	if executeSpan == nil {
		t.Fatalf("xflow.task.execute span not recorded; spans=%v", spanNames(ended))
	}
	if executeSpan.SpanContext().TraceID() != foundDispatch.SpanContext().TraceID() {
		t.Fatalf("execute trace %s != dispatch trace %s (trace graph broken)",
			executeSpan.SpanContext().TraceID(), foundDispatch.SpanContext().TraceID())
	}
	// The execute span's parent must be the dispatch span's span ID (remote parent).
	if executeSpan.Parent().SpanID() != foundDispatch.SpanContext().SpanID() {
		t.Fatalf("execute parent %s != dispatch span %s (remote parent not linked)",
			executeSpan.Parent().SpanID(), foundDispatch.SpanContext().SpanID())
	}

	// The report carrier, when extracted, must carry the execute span context so
	// the server's commit span links to the execute span — not a fresh root.
	reportCtx := tracing.ExtractCarrier(context.Background(), client.reported.TraceCarrier)
	sc := oteltrace.SpanFromContext(reportCtx).SpanContext()
	if !sc.IsValid() {
		t.Fatal("report carrier did not carry a valid SpanContext")
	}
	if sc.TraceID() != executeSpan.SpanContext().TraceID() {
		t.Fatalf("report trace %s != execute trace %s", sc.TraceID(), executeSpan.SpanContext().TraceID())
	}
	if sc.SpanID() != executeSpan.SpanContext().SpanID() {
		t.Fatalf("report span %s != execute span %s (commit would not be parented to execute)",
			sc.SpanID(), executeSpan.SpanContext().SpanID())
	}
}

func spanNames(spans []sdktrace.ReadOnlySpan) []string {
	out := make([]string, 0, len(spans))
	for _, s := range spans {
		out = append(out, s.Name())
	}
	return out
}
