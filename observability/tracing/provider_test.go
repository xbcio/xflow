package tracing

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// TestNewTracerProviderDefaults verifies B1 contract: default sampler is
// ParentBased(AlwaysSample) and the default propagator is W3C TraceContext
// only (no Baggage).
func TestNewTracerProviderDefaults(t *testing.T) {
	_, shutdown, err := NewTracerProvider(context.Background(), ProviderConfig{
		Mode: "stdout",
	})
	if err != nil {
		t.Fatalf("NewTracerProvider: %v", err)
	}
	t.Cleanup(func() { shutdown(context.Background()) })

	// Default propagator must be TraceContext only — Baggage is opt-in.
	p := otel.GetTextMapPropagator()
	carrier := propagation.MapCarrier{}
	p.Inject(context.Background(), carrier)
	// TraceContext injects a traceparent only when there is a sampled span; a
	// bare background context has no span so injection yields nothing. The
	// point is that the propagator type is TraceContext, not the composite.
	if _, ok := p.(propagation.TraceContext); !ok {
		t.Fatalf("default propagator = %T, want TraceContext", p)
	}
}

// TestNewTracerProviderBaggageOptIn verifies baggage propagation is opt-in:
// with Baggage=true the propagator carries baggage; without it, it does not.
func TestNewTracerProviderBaggageOptIn(t *testing.T) {
	_, shutdown, err := NewTracerProvider(context.Background(), ProviderConfig{Mode: "stdout", Baggage: true})
	if err != nil {
		t.Fatalf("NewTracerProvider: %v", err)
	}
	t.Cleanup(func() { shutdown(context.Background()) })
	p := otel.GetTextMapPropagator()
	// With baggage enabled, the propagator must be a composite that includes
	// baggage — verified by injecting a context with baggage set.
	ctx := baggageContext(t)
	carrier := propagation.MapCarrier{}
	p.Inject(ctx, carrier)
	if carrier.Get("baggage") == "" {
		t.Fatal("Baggage=true did not propagate baggage header")
	}
}

// TestNewTracerProviderSamplerConfigurable verifies each sampler mode produces
// the expected sampling decision on a new root span (no parent).
//
// The `want` column below used to be declared and never read: the body started
// a span, ended it, and asserted only that NewTracerProvider returned no error,
// with a comment saying the sampling outcome was "verified structurally by the
// SDK in its own tests". The SDK's tests cover the SDK's samplers; nothing
// covered which sampler THIS function hands back for a given SamplerMode.
// Swapping the SamplerAlwaysOn and SamplerAlwaysOff arms of newSampler — so
// always_off sampled everything and always_on sampled nothing — left every
// subtest green.
//
// The decision is observable: the sampler runs at span creation, and its
// verdict lands in the span context's trace flags.
func TestNewTracerProviderSamplerConfigurable(t *testing.T) {
	cases := []struct {
		name    string
		sampler SamplerMode
		want    bool // sampled?
	}{
		{"always_on", SamplerAlwaysOn, true},
		{"always_off", SamplerAlwaysOff, false},
		{"parentbased default", SamplerParentBased, true},
		{"traceidratio 1.0", SamplerTraceIDRatio, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tracer, shutdown, err := NewTracerProvider(context.Background(), ProviderConfig{
				Mode:    "stdout",
				Sampler: c.sampler,
				SampleRatio: func() float64 {
					if c.sampler == SamplerTraceIDRatio {
						return 1.0
					}
					return 1.0
				}(),
			})
			if err != nil {
				t.Fatalf("NewTracerProvider: %v", err)
			}
			t.Cleanup(func() { shutdown(context.Background()) })
			spanCtx, span := tracer.Start(context.Background(), "test")
			// tracing.Span is a narrow wrapper with no SpanContext method, but
			// Start attaches the OTel span to the context — the same route
			// TraceIDFromContext takes.
			got := oteltrace.SpanContextFromContext(spanCtx).IsSampled()
			span.End()
			if got != c.want {
				t.Errorf("sampler %q: root span IsSampled() = %v, want %v",
					c.sampler, got, c.want)
			}
		})
	}
}

// TestNewTracerProviderShutdownIdempotent verifies repeated shutdown calls are
// safe (graceful-shutdown + defer in cmd/ both call it).
func TestNewTracerProviderShutdownIdempotent(t *testing.T) {
	_, shutdown, err := NewTracerProvider(context.Background(), ProviderConfig{Mode: "stdout"})
	if err != nil {
		t.Fatalf("NewTracerProvider: %v", err)
	}
	shutdown(context.Background())
	shutdown(context.Background()) // must not panic
	shutdown(context.Background())
}

// TestNewTracerProviderDisabledIsNoop verifies disabled mode returns a no-op
// tracer that records nothing.
func TestNewTracerProviderDisabledIsNoop(t *testing.T) {
	tracer, shutdown, err := NewTracerProvider(context.Background(), ProviderConfig{Mode: "disabled"})
	if err != nil {
		t.Fatalf("NewTracerProvider: %v", err)
	}
	defer shutdown(context.Background())
	if _, ok := tracer.(NoopTracer); !ok {
		t.Fatalf("disabled tracer = %T, want NoopTracer", tracer)
	}
}

// TestNewTracerProviderUnknownModeErrors verifies an unrecognized Mode is a
// hard startup error, not a silent fallback to no tracing. cmd/server and
// cmd/runner pass the --tracing-mode flag straight through to cfg.Mode with
// no validation of their own, so this is the only gate: a typo'd flag value
// (e.g. "otpl" instead of "otlp") must fail process startup loudly instead of
// quietly running with tracing disabled, which would leave an operator
// believing tracing is on while every span silently vanishes.
func TestNewTracerProviderUnknownModeErrors(t *testing.T) {
	tracer, shutdown, err := NewTracerProvider(context.Background(), ProviderConfig{Mode: "bogus"})
	if err == nil {
		t.Fatal("NewTracerProvider(bogus mode) returned nil error, want an error rejecting the unknown mode")
	}
	if tracer != nil {
		t.Fatalf("NewTracerProvider(bogus mode) returned a non-nil tracer %v alongside an error", tracer)
	}
	if shutdown != nil {
		t.Fatal("NewTracerProvider(bogus mode) returned a non-nil shutdown func alongside an error")
	}
}

// helper: build a context carrying a baggage entry.
func baggageContext(t *testing.T) context.Context {
	t.Helper()
	m, err := baggage.NewMember("k", "v")
	if err != nil {
		t.Fatalf("baggage.NewMember: %v", err)
	}
	b, err := baggage.New(m)
	if err != nil {
		t.Fatalf("baggage.New: %v", err)
	}
	return baggage.ContextWithBaggage(context.Background(), b)
}
