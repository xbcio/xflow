package tracing

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// TestMiddlewareExtractsInboundTraceparent proves Middleware extracts the W3C
// traceparent header from the inbound request before starting the server
// span, so the span it creates is parented to the caller's trace instead of
// becoming a root. Dropping the otel.GetTextMapPropagator().Extract call
// (using r.Context() directly) would make every HTTP-triggered span in the
// system a root span, silently severing cross-service traces (workflow
// submit → runner dispatch → commit) at every hop that crosses an HTTP
// boundary. This file previously had no test at all.
func TestMiddlewareExtractsInboundTraceparent(t *testing.T) {
	// Caller side: start a parent span and inject its traceparent into an
	// outbound HTTP request's headers.
	callerTP := sdktrace.NewTracerProvider()
	defer func() { _ = callerTP.Shutdown(context.Background()) }()
	callerTracer := callerTP.Tracer("caller")
	ctx, parentSpan := callerTracer.Start(context.Background(), "caller.request")
	defer parentSpan.End()

	// The propagator is global process state, so it has to be restored: leaving
	// TraceContext installed would make a later test in this package extract
	// headers it was written assuming nothing extracts.
	prevPropagator := otel.GetTextMapPropagator()
	t.Cleanup(func() { otel.SetTextMapPropagator(prevPropagator) })
	otel.SetTextMapPropagator(propagation.TraceContext{})
	req := httptest.NewRequest(http.MethodGet, "/v1/workflows", nil)
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
	if req.Header.Get("traceparent") == "" {
		t.Fatal("setup: traceparent header was not injected into the request")
	}

	// Server side: a fresh provider + recorder so the only span recorded is
	// the one Middleware starts (proving it inherited the caller's parent).
	serverTP, sr := testTracerProvider()
	defer func() { _ = serverTP.Shutdown(context.Background()) }()
	serverTracer := NewOTelTracer(serverTP.Tracer("xflow-server"))

	handlerCalled := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerCalled = true
	})
	Middleware(serverTracer, next).ServeHTTP(httptest.NewRecorder(), req)

	if !handlerCalled {
		t.Fatal("wrapped handler was never invoked")
	}
	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("recorded spans = %d, want 1 (the request span)", len(spans))
	}
	got := spans[0]
	if !got.Parent().IsValid() {
		t.Fatalf("request span has no valid parent; traceparent header was not extracted (span=%+v)", got)
	}
	if got.Parent().SpanID() != parentSpan.SpanContext().SpanID() {
		t.Fatalf("request span parent = %v, want caller %v", got.Parent().SpanID(), parentSpan.SpanContext().SpanID())
	}
	if got.Parent().TraceID() != parentSpan.SpanContext().TraceID() {
		t.Fatalf("request span trace id = %v, want caller trace id %v", got.Parent().TraceID(), parentSpan.SpanContext().TraceID())
	}
	if want := "GET /v1/workflows"; got.Name() != want {
		t.Fatalf("span name = %q, want %q", got.Name(), want)
	}
}

// TestMiddlewarePropagatesContextToHandler proves the span-carrying context
// Middleware builds is the one the wrapped handler actually receives (via
// r.WithContext), not just used internally to start-and-discard a span. If
// Middleware started a span but forgot to call r.WithContext(ctx), downstream
// handlers would run with the original request context and SpanFromContext
// would find nothing.
func TestMiddlewarePropagatesContextToHandler(t *testing.T) {
	tp, _ := testTracerProvider()
	defer func() { _ = tp.Shutdown(context.Background()) }()
	tracer := NewOTelTracer(tp.Tracer("xflow-server"))

	var gotSpan Span
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotSpan = SpanFromContext(r.Context())
	})

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	Middleware(tracer, next).ServeHTTP(httptest.NewRecorder(), req)

	if gotSpan == nil {
		t.Fatal("handler's request context carries no Span; Middleware did not propagate its context via r.WithContext")
	}
}
