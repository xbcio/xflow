package tracing

import (
	"context"
	"errors"
	"testing"

	"github.com/xbcio/xflow/namespace"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
)

func TestOTelTracerRecordsSpanWithAttributesAndErrors(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	tracer := NewOTelTracer(provider.Tracer("xflow-test"))

	ctx := context.Background()
	ctx, span := tracer.Start(ctx, "dispatch", "execution_id", "e1")
	if got := SpanFromContext(ctx); got != span {
		t.Fatalf("SpanFromContext() = %v, want started span", got)
	}
	if !oteltrace.SpanFromContext(ctx).SpanContext().IsValid() {
		t.Fatal("OpenTelemetry span was not stored in context")
	}

	span.Set("node", "approve")
	span.RecordError(errors.New("boom"))
	span.End()

	ended := recorder.Ended()
	if len(ended) != 1 {
		t.Fatalf("ended span count = %d, want 1", len(ended))
	}
	if ended[0].Name() != "dispatch" {
		t.Fatalf("span name = %q, want dispatch", ended[0].Name())
	}
	if !spanHasAttribute(ended[0], attribute.String("execution_id", "e1")) {
		t.Fatalf("span attributes = %#v, want execution_id=e1", ended[0].Attributes())
	}
	if !spanHasAttribute(ended[0], attribute.String("node", "approve")) {
		t.Fatalf("span attributes = %#v, want node=approve", ended[0].Attributes())
	}
	if !spanHasAttribute(ended[0], attribute.String("namespace", "default")) {
		t.Fatalf("span attributes = %#v, want namespace=default", ended[0].Attributes())
	}
	if len(ended[0].Events()) == 0 {
		t.Fatal("RecordError did not add an OpenTelemetry span event")
	}
}

func TestOTelTracerReadsNamespaceFromContext(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	tracer := NewOTelTracer(provider.Tracer("xflow-test"))

	ctx := namespace.WithNamespace(context.Background(), namespace.Namespace("namespace-x"))
	_, span := tracer.Start(ctx, "execute")
	span.End()

	ended := recorder.Ended()
	if len(ended) != 1 {
		t.Fatalf("ended span count = %d, want 1", len(ended))
	}
	if !spanHasAttribute(ended[0], attribute.String("namespace", "namespace-x")) {
		t.Fatalf("span attributes = %#v, want namespace=namespace-x", ended[0].Attributes())
	}
}

func TestNoopTracerReturnsUsableSpan(t *testing.T) {
	ctx, span := NoopTracer{}.Start(context.Background(), "noop")
	if got := SpanFromContext(ctx); got != span {
		t.Fatalf("SpanFromContext() = %v, want noop span", got)
	}
	span.Set("key", "value")
	span.RecordError(errors.New("boom"))
	span.End()
}

func spanHasAttribute(span sdktrace.ReadOnlySpan, want attribute.KeyValue) bool {
	for _, got := range span.Attributes() {
		if got.Key == want.Key && got.Value.AsString() == want.Value.AsString() {
			return true
		}
	}
	return false
}

// TestTraceIDFromContextReturnsTheTraceIDNotTheSpanID pins the one line
// TraceIDFromContext exists for. Nothing in the repo called this function from
// a test: provider_test.go:97-100 only names it in a comment while asserting on
// oteltrace.SpanContextFromContext directly, engine/submission_context_test.go
// exercises a different same-named function in package engine, and the
// apiserver's own coverage never looks at the value —
// service/apiserver/envelope_test.go:32 checks only that the "trace_id" KEY is
// present, and module_control_workflows_test.go:790 t.Logf's (does not fail) an
// empty TraceID, calling that acceptable. test/integration/trace_graph_e2e_test.go
// reads the recorded OTel span directly and never routes through this function.
//
// So `return sc.TraceID().String()` could be `sc.SpanID().String()` and the
// whole suite stays green: both are well-formed lowercase hex, just different
// lengths. The consumers are service/apiserver/envelope.go:98 (the trace_id in
// every API response envelope) and authz_wrap.go:83,194,233 (AuditEvent.TraceID
// on every audited request). A span ID there silently breaks every
// audit-row-to-trace correlation — the value still looks like an ID, so nobody
// notices until someone tries to look a trace up and finds nothing.
//
// The IDs below are distinct constants rather than values read back from the
// span context, so the assertion is an independent statement of what the
// function must return.
func TestTraceIDFromContextReturnsTheTraceIDNotTheSpanID(t *testing.T) {
	traceID, err := oteltrace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	if err != nil {
		t.Fatal(err)
	}
	spanID, err := oteltrace.SpanIDFromHex("00f067aa0ba902b7")
	if err != nil {
		t.Fatal(err)
	}
	ctx := oteltrace.ContextWithSpanContext(context.Background(),
		oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
			TraceID:    traceID,
			SpanID:     spanID,
			TraceFlags: oteltrace.FlagsSampled,
			Remote:     true,
		}))

	got := TraceIDFromContext(ctx)
	if got != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Fatalf("TraceIDFromContext() = %q, want the trace id "+
			"4bf92f3577b34da6a3ce929d0e0e4736", got)
	}
	// Stated separately because it is the specific confusion the doc comment
	// warns against ("Never returns a span id or baggage").
	if got == "00f067aa0ba902b7" {
		t.Fatal("TraceIDFromContext() returned the SPAN id: audit rows and " +
			"response envelopes would carry an id that resolves to no trace")
	}
}

// TestTraceIDFromContextWithoutASpanIsEmpty covers the two early returns, which
// no test reached either. An invalid span context must yield "" rather than the
// all-zero trace id: service/apiserver/authz_wrap.go writes the result straight
// into AuditEvent.TraceID, and "00000000000000000000000000000000" there reads as
// a real correlation id that leads nowhere, whereas "" reads as "not traced".
func TestTraceIDFromContextWithoutASpanIsEmpty(t *testing.T) {
	if got := TraceIDFromContext(context.Background()); got != "" {
		t.Fatalf("TraceIDFromContext(background) = %q, want empty", got)
	}
	// A nil context is exactly the guard under test; held in a variable so the
	// linters that ban a literal nil argument do not flag the call.
	var nilCtx context.Context
	if got := TraceIDFromContext(nilCtx); got != "" {
		t.Fatalf("TraceIDFromContext(nil) = %q, want empty", got)
	}
}
