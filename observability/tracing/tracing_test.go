package tracing

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
)

func TestOTelTracerRecordsSpanWithAttributesAndErrors(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	tracer := NewOTelTracer(provider.Tracer("xflow-test"))

	ctx, span := tracer.Start(context.Background(), "dispatch", "execution_id", "e1")
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
	if len(ended[0].Events()) == 0 {
		t.Fatal("RecordError did not add an OpenTelemetry span event")
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
