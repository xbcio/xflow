package tracing

import (
	"context"
	"net/http"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// Middleware wraps an http.Handler to extract W3C traceparent/tracestate from
// inbound requests, start a server span for the request lifetime, and propagate
// the span context to downstream handlers via the returned context.
func Middleware(tracer Tracer, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		ctx, span := tracer.Start(ctx, r.Method+" "+r.URL.Path)
		defer span.End()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// InjectCarrier serializes the active OTel SpanContext from ctx into a string
// map using the global W3C propagator (traceparent + tracestate). Returns nil
// when no active trace is present (e.g. tracing is disabled or unsampled).
func InjectCarrier(ctx context.Context) map[string]string {
	m := make(propagation.MapCarrier)
	otel.GetTextMapPropagator().Inject(ctx, m)
	if len(m) == 0 {
		return nil
	}
	return map[string]string(m)
}

// ExtractCarrier restores a SpanContext from a W3C propagation map into ctx so
// downstream code can create properly-parented child spans. Returns ctx
// unchanged when carrier is nil or empty.
//
// A malformed traceparent (present but unparseable) does not panic: the W3C
// propagator returns an invalid SpanContext, which we surface via the
// CarrierParseFailures counter so operators can detect broken propagation
// without a process crash.
func ExtractCarrier(ctx context.Context, carrier map[string]string) context.Context {
	if len(carrier) == 0 {
		return ctx
	}
	extracted := otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(carrier))
	if _, hasTP := carrier["traceparent"]; hasTP {
		if sc := oteltrace.SpanFromContext(extracted).SpanContext(); !sc.IsValid() {
			recordCarrierParseFailure()
		}
	}
	return extracted
}
