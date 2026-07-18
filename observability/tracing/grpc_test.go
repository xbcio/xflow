package tracing

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// TestGRPCUnaryInterceptorExtractsRemoteParent proves that a traceparent
// injected into outgoing gRPC metadata is extracted by the interceptor so the
// server span is parented to the caller's trace (not a root span). This is the
// gRPC-path analogue of the HTTP Middleware's W3C extraction.
func TestGRPCUnaryInterceptorExtractsRemoteParent(t *testing.T) {
	// Caller-side: start a parent span and inject traceparent into metadata.
	tp := sdktrace.NewTracerProvider()
	defer tp.Shutdown(context.Background())
	callerTracer := tp.Tracer("caller")
	ctx, parentSpan := callerTracer.Start(context.Background(), "caller.rpc")
	defer parentSpan.End()

	otel.SetTextMapPropagator(propagation.TraceContext{})
	md := metadata.New(nil)
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	for k, v := range carrier {
		md.Set(k, v)
	}
	injectCtx := metadata.NewOutgoingContext(context.Background(), md)

	// Server-side: a fresh provider + recorder so the only span it records is
	// the interceptor's server span (proving it inherited the parent).
	serverTP, serverSR := testTracerProvider()
	defer serverTP.Shutdown(context.Background())
	serverTracer := NewOTelTracer(serverTP.Tracer("xflow-server"))
	interceptor := GRPCUnaryServerInterceptor(serverTracer)

	// Run the interceptor with an incoming context that carries the metadata.
	// metadata.FromIncomingContext reads the incoming metadata block.
	incomingCtx := metadata.NewIncomingContext(context.Background(), md)
	var handlerCtx context.Context
	_, err := interceptor(incomingCtx, struct{}{}, &grpc.UnaryServerInfo{FullMethod: "/xflow/PollTask"},
		func(c context.Context, req any) (any, error) {
			handlerCtx = c
			return nil, nil
		})
	if err != nil {
		t.Fatalf("interceptor: %v", err)
	}

	spans := serverSR.Ended()
	if len(spans) != 1 {
		t.Fatalf("recorded spans = %d, want 1 (the server span)", len(spans))
	}
	serverSpan := spans[0]
	if !serverSpan.Parent().IsValid() {
		t.Fatalf("server span has no valid parent; tracecontext was not extracted from gRPC metadata (span = %+v)", serverSpan)
	}
	if serverSpan.Parent().SpanID() != parentSpan.SpanContext().SpanID() {
		t.Fatalf("server span parent = %v, want caller %v", serverSpan.Parent().SpanID(), parentSpan.SpanContext().SpanID())
	}
	if serverSpan.Name() != "grpc./xflow/PollTask" {
		t.Fatalf("span name = %q, want grpc./xflow/PollTask", serverSpan.Name())
	}
	_ = handlerCtx
	_ = injectCtx
	_ = callerTracer
}

// TestGRPCUnaryInterceptorNilTracerIsPassthrough proves that with no Tracer
// configured the interceptor runs the handler unchanged.
func TestGRPCUnaryInterceptorNilTracerIsPassthrough(t *testing.T) {
	interceptor := GRPCUnaryServerInterceptor(nil)
	called := false
	_, err := interceptor(context.Background(), struct{}{}, &grpc.UnaryServerInfo{FullMethod: "/xflow/Foo"},
		func(ctx context.Context, req any) (any, error) {
			called = true
			return "ok", nil
		})
	if err != nil {
		t.Fatalf("interceptor: %v", err)
	}
	if !called {
		t.Fatal("handler not called")
	}
}

// TestGRPCUnaryInterceptorRecordsHandlerError proves the span records an error
// when the handler returns one.
func TestGRPCUnaryInterceptorRecordsHandlerError(t *testing.T) {
	tp, sr := testTracerProvider()
	defer tp.Shutdown(context.Background())
	tracer := NewOTelTracer(tp.Tracer("xflow-server"))
	interceptor := GRPCUnaryServerInterceptor(tracer)

	handlerErr := context.DeadlineExceeded
	_, err := interceptor(context.Background(), struct{}{}, &grpc.UnaryServerInfo{FullMethod: "/xflow/Bar"},
		func(ctx context.Context, req any) (any, error) {
			return nil, handlerErr
		})
	if err != handlerErr {
		t.Fatalf("interceptor returned %v, want %v (must propagate)", err, handlerErr)
	}
	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("recorded spans = %d, want 1", len(spans))
	}
	// RecordError adds an "exception" event; OTel does not auto-set span
	// status from RecordError (that is the application's call). Assert the
	// error was recorded as an event rather than requiring a status code.
	hasErrEvent := false
	for _, ev := range spans[0].Events() {
		if ev.Name == "exception" {
			hasErrEvent = true
		}
	}
	if !hasErrEvent {
		t.Fatalf("span has no exception event; RecordError did not record the handler error")
	}
}

func testTracerProvider() (*sdktrace.TracerProvider, *tracetest.SpanRecorder) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	return tp, sr
}
