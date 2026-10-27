package engine

import (
	"context"

	"github.com/xbcio/xflow/observability/tracing"
	"github.com/xbcio/xflow/types"
	"go.opentelemetry.io/otel"
)

// engineInstrumentationName is the OTel tracer name for engine-owned spans.
const engineInstrumentationName = "github.com/xbcio/xflow"

// engineTracer returns the Tracer used for the engine-owned spans
// (xflow.outbox.*, xflow.engine.*, xflow.signal.*, xflow.map.batch). It is
// backed by the process-wide OTel global provider (registered by
// observability/tracing.NewTracerProvider), resolved lazily on each call so a
// provider installed after engine init is picked up. It is a real tracer in
// production and a no-op in tests / when tracing is disabled. The engine stays
// free of an explicit Tracer constructor option: these spans surface only when
// a host program has installed a provider.
func engineTracer() tracing.Tracer {
	return tracing.NewOTelTracer(otel.Tracer(engineInstrumentationName))
}

// executionTraceContext re-anchors a context that carries no span of its own to
// the W3C carrier captured at submission and persisted on the execution
// snapshot. Without this, every engine-owned span opened on a background path
// is a detached root: the outbox dispatcher, the queue's worker goroutines and
// the resume delivery path all start from a context that is not a descendant of
// the submit/invoke context that created the execution.
//
// A context that already carries a span is left alone. That is what keeps the
// inline path honest: an advance flushed from the commit that caused it is a
// child of that commit, not a sibling re-parented to the submission root. The
// carrier round-trip is a real W3C ExtractCarrier (it preserves tracestate and
// the sampled flag), never a trace_id/span_id string reconstruction.
func (e *Engine) executionTraceContext(ctx context.Context, id types.ExecutionID) context.Context {
	if e == nil || e.state == nil || tracing.TraceIDFromContext(ctx) != "" {
		return ctx
	}
	carrier, err := e.ExecutionTraceCarrier(ctx, id)
	if err != nil || len(carrier) == 0 {
		return ctx
	}
	return tracing.ExtractCarrier(ctx, carrier)
}

// startExecutionSpan opens an engine-owned span for one unit of work belonging
// to the execution id, parented to the submission trace when ctx has no span of
// its own (see executionTraceContext).
func (e *Engine) startExecutionSpan(ctx context.Context, id types.ExecutionID, name string, attrs ...any) (context.Context, tracing.Span) {
	return engineTracer().Start(e.executionTraceContext(ctx, id), name, attrs...)
}
