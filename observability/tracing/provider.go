package tracing

import (
	"context"
	"fmt"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// SamplerMode selects the OTel sampler. Default is ParentBased(AlwaysSample),
// which honors an upstream sampling decision and samples at 100% when there is
// no parent (the typical xflow case: a workflow submit or runner poll with no
// inbound trace).
type SamplerMode string

const (
	// SamplerParentBased (default) — ParentBased(AlwaysSample). Honors an
	// upstream sampled flag; samples new roots at 100%.
	SamplerParentBased SamplerMode = "parentbased"
	// SamplerAlwaysOn — AlwaysSample. Every span is sampled.
	SamplerAlwaysOn SamplerMode = "always_on"
	// SamplerAlwaysOff — NeverSample. No spans are sampled (cost control).
	SamplerAlwaysOff SamplerMode = "always_off"
	// SamplerTraceIDRatio — probabilistic sampling. Ratio in [0,1].
	SamplerTraceIDRatio SamplerMode = "traceidratio"
)

// ProviderConfig configures a TracerProvider.
type ProviderConfig struct {
	// Mode is one of "disabled", "stdout", or "otlp". Empty means "disabled".
	Mode string
	// Endpoint is the OTLP collector gRPC endpoint for "otlp" mode, e.g. "localhost:4317".
	Endpoint string
	// Insecure disables TLS verification for the OTLP connection.
	Insecure bool
	// ServiceName is the OTel resource service.name attribute.
	ServiceName string
	// ServiceVersion is the OTel resource service.version attribute.
	ServiceVersion string
	// Sampler selects the sampling strategy. Empty means ParentBased(AlwaysSample).
	Sampler SamplerMode
	// SampleRatio is the ratio for SamplerTraceIDRatio, in [0,1]. Ignored
	// for other sampler modes.
	SampleRatio float64
	// Baggage, when true, enables W3C Baggage propagation in addition to
	// TraceContext. Default false: baggage is opt-in because it can carry
	// unbounded/cross-tenant data. When enabled, callers should bound the keys
	// and values they accept.
	Baggage bool
}

// NewTracerProvider creates a TracerProvider from cfg, registers it as the
// global OTel provider, and installs the configured text-map propagator
// (W3C TraceContext by default; Baggage opt-in via cfg.Baggage). Returns a
// Tracer and an idempotent shutdown func that must be called on process exit
// to flush pending spans.
//
// Mode "disabled" or "" returns a NoopTracer and a no-op (idempotent) shutdown,
// safe to use as the production default when tracing is not configured.
//
// Default sampler is ParentBased(AlwaysSample): it honors an upstream sampled
// flag (so a not-sampled inbound request produces no downstream spans) and
// samples new roots at 100%. This is the safe default for a workflow engine
// whose traces originate at operator-facing API calls.
func NewTracerProvider(ctx context.Context, cfg ProviderConfig) (Tracer, func(context.Context), error) {
	if cfg.Mode == "" || cfg.Mode == "disabled" {
		return NoopTracer{}, idempotentShutdown(func(context.Context) {}), nil
	}

	res, err := sdkresource.New(ctx,
		sdkresource.WithAttributes(
			semconv.ServiceName(cfg.ServiceName),
			semconv.ServiceVersion(cfg.ServiceVersion),
		),
		sdkresource.WithProcess(),
		sdkresource.WithHost(),
	)
	if err != nil {
		// Non-fatal: fall back to the minimal default resource.
		res = sdkresource.Default()
	}

	var exporter sdktrace.SpanExporter
	switch cfg.Mode {
	case "stdout":
		exporter, err = stdouttrace.New(stdouttrace.WithPrettyPrint())
		if err != nil {
			return nil, nil, fmt.Errorf("tracing: stdout exporter: %w", err)
		}
	case "otlp":
		opts := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(cfg.Endpoint)}
		if cfg.Insecure {
			opts = append(opts, otlptracegrpc.WithInsecure())
		}
		exporter, err = otlptracegrpc.New(ctx, opts...)
		if err != nil {
			return nil, nil, fmt.Errorf("tracing: otlp exporter to %q: %w", cfg.Endpoint, err)
		}
	default:
		return nil, nil, fmt.Errorf("tracing: unknown mode %q (want: disabled|stdout|otlp)", cfg.Mode)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(newSampler(cfg)),
	)

	otel.SetTracerProvider(tp)
	// Default propagator is W3C TraceContext only. Baggage is opt-in: it can
	// carry unbounded cross-tenant data, so callers must explicitly enable it
	// and bound accepted keys/values.
	var prop propagation.TextMapPropagator = propagation.TraceContext{}
	if cfg.Baggage {
		prop = propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		)
	}
	otel.SetTextMapPropagator(prop)

	tracer := NewOTelTracer(tp.Tracer(instrumentationName))
	return tracer, idempotentShutdown(func(ctx context.Context) { _ = tp.Shutdown(ctx) }), nil
}

// newSampler builds the SDK sampler from cfg, defaulting to
// ParentBased(AlwaysSample).
func newSampler(cfg ProviderConfig) sdktrace.Sampler {
	switch cfg.Sampler {
	case SamplerAlwaysOn:
		return sdktrace.AlwaysSample()
	case SamplerAlwaysOff:
		return sdktrace.NeverSample()
	case SamplerTraceIDRatio:
		ratio := cfg.SampleRatio
		if ratio < 0 {
			ratio = 0
		}
		if ratio > 1 {
			ratio = 1
		}
		return sdktrace.TraceIDRatioBased(ratio)
	case SamplerParentBased, "":
		fallthrough
	default:
		return sdktrace.ParentBased(sdktrace.AlwaysSample())
	}
}

// idempotentShutdown wraps a shutdown func so repeated calls (e.g. from both
// graceful-shutdown and a defer in cmd/) are safe and only flush once.
func idempotentShutdown(fn func(context.Context)) func(context.Context) {
	var once sync.Once
	return func(ctx context.Context) {
		once.Do(func() { fn(ctx) })
	}
}
