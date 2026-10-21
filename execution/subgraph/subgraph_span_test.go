package subgraph

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/observability/tracing"
	"github.com/xbcio/xflow/types"
)

// installSubgraphSpanRecorder sets the process-global OTel provider: the
// subgraph tracer resolves it lazily (subgraphTracer), so a recorder installed
// after package init is the only way to observe these spans.
func installSubgraphSpanRecorder(t *testing.T) (*tracetest.SpanRecorder, tracing.Tracer) {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(rec),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	prevProvider := otel.GetTracerProvider()
	prevPropagator := otel.GetTextMapPropagator()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(prevProvider)
		otel.SetTextMapPropagator(prevPropagator)
		_ = tp.Shutdown(context.Background())
	})
	return rec, tracing.NewOTelTracer(tp.Tracer("xflow-subgraph-test"))
}

func subgraphSpans(rec *tracetest.SpanRecorder) []sdktrace.ReadOnlySpan {
	var out []sdktrace.ReadOnlySpan
	for _, span := range rec.Ended() {
		if span.Name() == "xflow.subgraph.execute" {
			out = append(out, span)
		}
	}
	return out
}

func subgraphSpanAttr(span sdktrace.ReadOnlySpan, key string) (string, bool) {
	for _, kv := range span.Attributes() {
		if string(kv.Key) == key {
			return kv.Value.Emit(), true
		}
	}
	return "", false
}

// TestExecutorExecuteOpensOneSubgraphSpan pins the group/subflow boundary: one
// execution, one xflow.subgraph.execute, parented by the caller and describing
// the package it ran (hash + member count), never its payload.
func TestExecutorExecuteOpensOneSubgraphSpan(t *testing.T) {
	rec, tracer := installSubgraphSpanRecorder(t)
	pkg := buildTwoNodeChainPackage(t)
	reg := testRegistry(t)
	ex := NewExecutor(reg, NewPackageCache(PackageCacheConfig{
		MaxEntries: 4, MaxPackageBytes: 1 << 20,
	}), func() Backend { return local.New(local.WithRegistry(reg), local.WithConcurrency(1)) })

	hash, err := graph.ComputePackageHash(pkg)
	if err != nil {
		t.Fatalf("compute package hash: %v", err)
	}

	callerCtx, callerSpan := tracer.Start(context.Background(), "caller.group")
	callerSC := oteltrace.SpanContextFromContext(callerCtx)
	res, err := ex.Execute(callerCtx, Request{
		Package:     pkg,
		PackageHash: hash,
		Input:       &types.Input{Data: map[string]any{"seed": 1, "token": "sensitive-subgraph-payload-must-not-export"}},
	})
	callerSpan.End()
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if res.Outcome != OutcomeSuccess {
		t.Fatalf("outcome = %v, want success (error: %s)", res.Outcome, res.Error)
	}

	spans := subgraphSpans(rec)
	if len(spans) != 1 {
		t.Fatalf("found %d xflow.subgraph.execute spans, want 1", len(spans))
	}
	span := spans[0]
	if got := span.SpanContext().TraceID(); got != callerSC.TraceID() {
		t.Fatalf("subgraph span trace = %s, want caller trace %s", got, callerSC.TraceID())
	}
	if got := span.Parent().SpanID(); got != callerSC.SpanID() {
		t.Fatalf("subgraph span parent = %s, want caller span %s", got, callerSC.SpanID())
	}
	if got, ok := subgraphSpanAttr(span, "package_hash"); !ok || got != hash {
		t.Fatalf("package_hash = %q, present=%t; want %q", got, ok, hash)
	}
	if got, ok := subgraphSpanAttr(span, "member_count"); !ok || got == "" || got == "0" {
		t.Fatalf("member_count = %q, present=%t; want the package's node count", got, ok)
	}
	for _, kv := range span.Attributes() {
		if key := string(kv.Key); key == "payload" || key == "params" || key == "token" {
			t.Fatalf("subgraph span carries sensitive attribute %q", key)
		}
		if kv.Value.Emit() == "sensitive-subgraph-payload-must-not-export" {
			t.Fatalf("subgraph span leaks the input payload in attribute %q", kv.Key)
		}
	}
}

// TestMapBodyStyleSuppressionSkipsTheSubgraphSpan pins the other half of the
// rule: when the caller marks the context (MapBodyExecutor does this for every
// item), no per-item span is opened even though the execution is unchanged.
func TestMapBodyStyleSuppressionSkipsTheSubgraphSpan(t *testing.T) {
	rec, _ := installSubgraphSpanRecorder(t)
	pkg := buildTwoNodeChainPackage(t)
	reg := testRegistry(t)
	ex := NewExecutor(reg, NewPackageCache(PackageCacheConfig{
		MaxEntries: 4, MaxPackageBytes: 1 << 20,
	}), func() Backend { return local.New(local.WithRegistry(reg), local.WithConcurrency(1)) })

	hash, err := graph.ComputePackageHash(pkg)
	if err != nil {
		t.Fatalf("compute package hash: %v", err)
	}
	res, err := ex.Execute(withoutSubgraphSpan(context.Background()), Request{
		Package:     pkg,
		PackageHash: hash,
		Input:       &types.Input{Data: map[string]any{"seed": 1}},
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if res.Outcome != OutcomeSuccess {
		t.Fatalf("outcome = %v, want success (error: %s)", res.Outcome, res.Error)
	}
	if spans := subgraphSpans(rec); len(spans) != 0 {
		t.Fatalf("found %d xflow.subgraph.execute spans for a suppressed execution, want 0", len(spans))
	}
}
