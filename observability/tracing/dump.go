package tracing

import (
	"fmt"
	"strings"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// DumpSpans formats a slice of ended spans as an indented, human-readable trace
// graph (name → trace/span/parent). It is the local-test analogue of a
// collector artifact: tests and the T6/T10 CI-artifact step can call it on the
// in-memory exporter's Ended() output to produce a reviewable graph dump
// without an external collector. The format is stable enough for golden-style
// assertions and contains no secrets (only span names and IDs).
func DumpSpans(spans []sdktrace.ReadOnlySpan) string {
	if len(spans) == 0 {
		return "(no spans)"
	}
	var b strings.Builder
	for i, s := range spans {
		if i > 0 {
			b.WriteString("\n")
		}
		fmt.Fprintf(&b, "- %s  trace=%s span=%s parent=%s",
			s.Name(),
			s.SpanContext().TraceID(),
			s.SpanContext().SpanID(),
			s.Parent().SpanID(),
		)
		if !s.Parent().IsValid() {
			b.WriteString(" (root)")
		}
	}
	return b.String()
}
