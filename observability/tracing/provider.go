package tracing

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
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
}

// NewTracerProvider creates a TracerProvider from cfg, registers it as the
// global OTel provider, and installs W3C TraceContext + Baggage as the global
// text-map propagator. Returns a Tracer and a shutdown func that must be called
// on process exit to flush pending spans.
//
// Mode "disabled" or "" returns a NoopTracer and a no-op shutdown, safe to use
// as the production default when tracing is not configured.
func NewTracerProvider(ctx context.Context, cfg ProviderConfig) (Tracer, func(context.Context), error) {
	if cfg.Mode == "" || cfg.Mode == "disabled" {
		return NoopTracer{}, func(context.Context) {}, nil
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
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	tracer := NewOTelTracer(tp.Tracer(instrumentationName))
	shutdown := func(ctx context.Context) { _ = tp.Shutdown(ctx) }
	return tracer, shutdown, nil
}
