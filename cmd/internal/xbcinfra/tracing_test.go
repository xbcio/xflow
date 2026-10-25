package xbcinfra

import (
	"context"
	"errors"
	"testing"

	xbctracing "github.com/xbcio/xbc/extensions/observability/tracing"

	xflowtracing "github.com/xbcio/xflow/observability/tracing"
)

func TestNewTracerStartsSpansAndShutsDown(t *testing.T) {
	cfg := xbctracing.DefaultConfig()
	// Sampling nothing keeps the test hermetic: the span never records and the
	// batch processor has nothing to export when shutdown flushes.
	cfg.SampleRatio = 0

	tracer, shutdown, err := NewTracer(cfg)
	if err != nil {
		t.Fatalf("NewTracer() = %v", err)
	}
	if tracer == nil || shutdown == nil {
		t.Fatal("NewTracer() returned a nil tracer or shutdown")
	}

	ctx, span := tracer.Start(context.Background(), "xbc.integration")
	if span == nil {
		t.Fatal("Start() returned a nil span")
	}
	if xflowtracing.SpanFromContext(ctx) == nil {
		t.Fatal("Start() did not attach the span to the returned context")
	}
	span.Set("key", "value")
	span.RecordError(errors.New("test error"))
	span.End()

	shutdown(context.Background())
	shutdown(context.Background())
}
