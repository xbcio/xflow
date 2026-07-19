package engine

import (
	"go.opentelemetry.io/otel"
	"github.com/xbcio/xflow/observability/tracing"
)

// engineInstrumentationName is the OTel tracer name for engine-owned spans.
const engineInstrumentationName = "github.com/xbcio/xflow"

// outboxTracer returns the Tracer used for the engine-owned outbox spans
// (xflow.outbox.flush, xflow.outbox.replay). It is backed by the process-wide
// OTel global provider (registered by observability/tracing.NewTracerProvider),
// resolved lazily on each call so a provider installed after engine init is
// picked up. It is a real tracer in production and a no-op in tests / when
// tracing is disabled. The engine stays free of an explicit Tracer constructor
// option: these spans surface only when a host program has installed a provider.
func outboxTracer() tracing.Tracer {
	return tracing.NewOTelTracer(otel.Tracer(engineInstrumentationName))
}
