package tracing

import (
	"context"
	"fmt"

	"github.com/xbcio/xflow/backend/tenant"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	oteltrace "go.opentelemetry.io/otel/trace"
)

const instrumentationName = "github.com/xbcio/xflow"

type spanContextKey struct{}

// Tracer starts named spans and attaches the active span to the returned
// context.
type Tracer interface {
	Start(ctx context.Context, name string, attrs ...any) (context.Context, Span)
}

// Span records attributes and errors for a scoped operation.
type Span interface {
	Set(args ...any)
	RecordError(err error)
	End()
}

// SpanFromContext returns the active span previously attached by Tracer.Start.
func SpanFromContext(ctx context.Context) Span {
	if ctx == nil {
		return nil
	}
	span, _ := ctx.Value(spanContextKey{}).(Span)
	return span
}

// TraceIDFromContext returns the W3C trace ID of the active OTel span in
// ctx, or "" when no span is active or tracing is disabled. It reads the
// OpenTelemetry span context directly (not the xflow Span wrapper) so it
// works for any span attached by the tracing middleware, including ones
// extracted from an inbound traceparent. Used by the audit path to correlate
// audit rows with the request's distributed trace (T7 span context → T9
// audit trace_id). Never returns a span id or baggage — only the trace id.
func TraceIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	sc := oteltrace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return ""
	}
	return sc.TraceID().String()
}

// NoopTracer is a tracing implementation that records nothing.
type NoopTracer struct{}

// Start returns a no-op span and stores it in the returned context.
func (NoopTracer) Start(ctx context.Context, _ string, _ ...any) (context.Context, Span) {
	span := noopSpan{}
	return context.WithValue(ctx, spanContextKey{}, span), span
}

type noopSpan struct{}

func (noopSpan) Set(...any)        {}
func (noopSpan) RecordError(error) {}
func (noopSpan) End()              {}

// OTelTracer records spans through OpenTelemetry.
type OTelTracer struct {
	tracer oteltrace.Tracer
}

// NewOTelTracer returns an OpenTelemetry-backed tracer. A nil tracer falls
// back to otel.Tracer with xflow's instrumentation name.
func NewOTelTracer(tracer oteltrace.Tracer) OTelTracer {
	if tracer == nil {
		tracer = otel.Tracer(instrumentationName)
	}
	return OTelTracer{tracer: tracer}
}

// Start starts an OpenTelemetry span and stores both the OTel span and xflow
// Span adapter in the returned context. The span is automatically tagged with
// the tenant carried by ctx (from tenant.FromContext) so every trace has a
// tenant dimension for multi-tenant observability. Tenant is stored as a span
// attribute, never as W3C baggage, to avoid cross-tenant leakage.
func (t OTelTracer) Start(ctx context.Context, name string, attrs ...any) (context.Context, Span) {
	if ctx == nil {
		ctx = context.Background()
	}
	tracer := t.tracer
	if tracer == nil {
		tracer = otel.Tracer(instrumentationName)
	}
	// Always attach the tenant from context. DefaultTenant is emitted explicitly
	// so single-tenant deployments still have a stable tenant attribute.
	attrs = append(attrs, "tenant", string(tenant.FromContext(ctx)))
	ctx, span := tracer.Start(ctx, name, oteltrace.WithAttributes(attributes(attrs...)...))
	wrapped := otelSpan{span: span}
	return context.WithValue(ctx, spanContextKey{}, wrapped), wrapped
}

type otelSpan struct {
	span oteltrace.Span
}

func (s otelSpan) Set(args ...any) {
	if s.span == nil {
		return
	}
	s.span.SetAttributes(attributes(args...)...)
}

func (s otelSpan) RecordError(err error) {
	if s.span == nil || err == nil {
		return
	}
	s.span.RecordError(err)
}

func (s otelSpan) End() {
	if s.span == nil {
		return
	}
	s.span.End()
}

func attributes(args ...any) []attribute.KeyValue {
	if len(args) == 0 {
		return nil
	}
	attrs := make([]attribute.KeyValue, 0, (len(args)+1)/2)
	for i := 0; i < len(args); i += 2 {
		key := fmt.Sprint(args[i])
		if key == "" {
			continue
		}
		if i+1 >= len(args) {
			attrs = append(attrs, attribute.String(key, ""))
			continue
		}
		attrs = append(attrs, attributeValue(key, args[i+1]))
	}
	return attrs
}

func attributeValue(key string, value any) attribute.KeyValue {
	switch v := value.(type) {
	case string:
		return attribute.String(key, v)
	case bool:
		return attribute.Bool(key, v)
	case int:
		return attribute.Int(key, v)
	case int64:
		return attribute.Int64(key, v)
	case float64:
		return attribute.Float64(key, v)
	case fmt.Stringer:
		return attribute.String(key, v.String())
	default:
		return attribute.String(key, fmt.Sprint(v))
	}
}
