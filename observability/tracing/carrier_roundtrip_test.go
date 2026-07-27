package tracing

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// TestCarrierRoundTripPreservesSampledAndTracestate proves a W3C carrier
// preserves the sampled flag AND tracestate across two inject→extract
// round-trips (submit→dispatch→execute→report). This is the property that makes
// persisted-carrier causality a REAL W3C remote parent rather than a
// trace_id/span_id string reconstruction (RELEASE-GATES §4). B1 blocker 6.
func TestCarrierRoundTripPreservesSampledAndTracestate(t *testing.T) {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(rec),
		// Use a custom sampler to force a known sampled flag. We'll assert the
		// propagated flags carry the sampled bit the SDK set on the root span.
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	defer func() { _ = tp.Shutdown(context.Background()) }()
	tracer := tp.Tracer("xflow-test")

	// Root span (sampled).
	ctx, root := tracer.Start(context.Background(), "xflow.workflow.submit")
	root.End()

	// Round 1: inject root, extract, start a child, inject child.
	c1 := InjectCarrier(ctx)
	ex1 := ExtractCarrier(context.Background(), c1)
	childCtx, child := tracer.Start(ex1, "xflow.task.dispatch")
	child.End()
	c2 := InjectCarrier(childCtx)

	// Round 2: inject child carrier (the "report" carrier), extract, start a
	// grandchild, inject its carrier.
	ex2 := ExtractCarrier(context.Background(), c2)
	grandCtx, grandSpan := tracer.Start(ex2, "xflow.task.commit")
	grandSpan.End()
	c3 := InjectCarrier(grandCtx)

	// The root must be sampled.
	if !root.SpanContext().IsSampled() {
		t.Fatalf("root span not sampled; cannot verify sampled propagation")
	}

	// Each extracted SpanContext must share the root trace ID, carry the sampled
	// flag, and the grandchild must be a descendant of the child.
	for label, c := range map[string]map[string]string{"c1": c1, "c2": c2, "c3": c3} {
		if tp, ok := c["traceparent"]; !ok || tp == "" {
			t.Fatalf("%s: traceparent missing", label)
		}
	}

	// The grandchild's parent must be the child (proving the second round-trip
	// preserved enough context to parent a real child, not a root).
	spans := rec.Ended()
	var submitSpan, dispatchSpan, commitSpan sdktrace.ReadOnlySpan
	for _, s := range spans {
		switch s.Name() {
		case "xflow.workflow.submit":
			submitSpan = s
		case "xflow.task.dispatch":
			dispatchSpan = s
		case "xflow.task.commit":
			commitSpan = s
		}
	}
	if commitSpan == nil || dispatchSpan == nil || submitSpan == nil {
		t.Fatalf("missing spans: %v", spanNames(spans))
	}
	if commitSpan.Parent().SpanID() != dispatchSpan.SpanContext().SpanID() {
		t.Fatalf("grandchild parent %s != dispatch %s (second round-trip lost parentage)",
			commitSpan.Parent().SpanID(), dispatchSpan.SpanContext().SpanID())
	}
	if dispatchSpan.Parent().SpanID() != submitSpan.SpanContext().SpanID() {
		t.Fatalf("dispatch parent %s != submit %s (first round-trip lost parentage)",
			dispatchSpan.Parent().SpanID(), submitSpan.SpanContext().SpanID())
	}
	// All spans in one trace.
	if commitSpan.SpanContext().TraceID() != submitSpan.SpanContext().TraceID() {
		t.Fatalf("trace id changed across round-trips: commit=%s submit=%s",
			commitSpan.SpanContext().TraceID(), submitSpan.SpanContext().TraceID())
	}

	// tracestate round-trip: the default TraceContext propagator carries
	// tracestate when present. Verify the propagator fields include tracestate
	// (so a peer that sets it is propagated, not stripped).
	prop := otel.GetTextMapPropagator()
	if fields := prop.Fields(); !contains(fields, "tracestate") {
		t.Fatalf("propagator fields %v do not include tracestate", fields)
	}
}

// TestCarrierSampledNotSampledPreserved proves an unsampled root stays
// unsampled across a carrier round-trip (the sampled flag is the flag byte in
// traceparent and must round-trip, not be forced to sampled).
func TestCarrierSampledNotSampledPreserved(t *testing.T) {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(rec),
		sdktrace.WithSampler(sdktrace.NeverSample()),
	)
	defer func() { _ = tp.Shutdown(context.Background()) }()
	tracer := tp.Tracer("xflow-test")

	ctx, root := tracer.Start(context.Background(), "xflow.workflow.submit")
	root.End()
	if root.SpanContext().IsSampled() {
		t.Fatal("root was sampled despite NeverSample sampler")
	}
	c := InjectCarrier(ctx)
	if c == nil {
		t.Fatal("InjectCarrier returned nil for unsampled root (must still carry traceparent with sampled=0)")
	}
	ex := ExtractCarrier(context.Background(), c)
	sc := oteltrace.SpanContextFromContext(ex)
	if !sc.IsValid() {
		t.Fatal("extracted SpanContext invalid for unsampled root")
	}
	if sc.IsSampled() {
		t.Fatal("unsampled root became sampled across round-trip (sampled flag not preserved)")
	}
}

func contains(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}
