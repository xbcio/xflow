package subgraph

import (
	"context"

	"github.com/xbcio/xflow/observability/tracing"
	"go.opentelemetry.io/otel"
)

// subgraphInstrumentationName is the OTel tracer name for subgraph-owned spans.
const subgraphInstrumentationName = "github.com/xbcio/xflow"

// subgraphTracer returns the Tracer used for xflow.subgraph.execute and
// xflow.map.body. It is backed by the process-wide OTel global provider
// (registered by observability/tracing.NewTracerProvider), resolved lazily on
// each call so a provider installed after this package is initialised is
// picked up. It is a real tracer in production and a no-op in tests / when
// tracing is disabled — the same pattern engine/tracing.go uses, chosen because
// a sub-graph execution can be started by the engine, by a runner, or by the
// SDK's in-process map body, and none of those constructors carries a tracer.
func subgraphTracer() tracing.Tracer {
	return tracing.NewOTelTracer(otel.Tracer(subgraphInstrumentationName))
}

// suppressedSubgraphSpanKey marks a context in which Executor.Execute must not
// open its own xflow.subgraph.execute span.
type suppressedSubgraphSpanKey struct{}

// withoutSubgraphSpan marks ctx as "the caller already opened the span that
// covers this execution". MapBodyExecutor sets it for the item loop: a map
// body runs Executor.Execute once per item, so without this mark a map over N
// items would emit N subgraph spans — one per item, the exact fan-out shape
// the span budget forbids. The batch-level xflow.map.body span covers those
// executions instead.
func withoutSubgraphSpan(ctx context.Context) context.Context {
	return context.WithValue(ctx, suppressedSubgraphSpanKey{}, true)
}

func subgraphSpanSuppressed(ctx context.Context) bool {
	suppressed, _ := ctx.Value(suppressedSubgraphSpanKey{}).(bool)
	return suppressed
}
