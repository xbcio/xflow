package xflow

import (
	"context"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"

	enginecore "github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/observability/tracing"
	"github.com/xbcio/xflow/types"
)

// traceMapBodyHandler is the map body's only node. Its presence proves the body
// ran; the span shape around it is what this test is about.
type traceMapBodyHandler struct{}

func (*traceMapBodyHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.trace_map_body"}
}

func (*traceMapBodyHandler) Execute(_ context.Context, in *types.Input) (*types.Output, error) {
	return &types.Output{Data: map[string]any{"item": in.Data["$item"]}}, nil
}

func endedNamed(rec *tracetest.SpanRecorder, name string) []sdktrace.ReadOnlySpan {
	var out []sdktrace.ReadOnlySpan
	for _, span := range rec.Ended() {
		if span.Name() == name {
			out = append(out, span)
		}
	}
	return out
}

func spanIntValue(span sdktrace.ReadOnlySpan, key string) (int64, bool) {
	for _, kv := range span.Attributes() {
		if string(kv.Key) == key {
			return kv.Value.AsInt64(), true
		}
	}
	return 0, false
}

// waitForMapSpans waits until the batch and body spans have all ended and
// together account for every item. Both are opened and closed on queue/body
// goroutines, so Wait returning on the terminal execution state can precede the
// last End by a moment.
func waitForMapSpans(t *testing.T, rec *tracetest.SpanRecorder, wantItems int) (body, batch []sdktrace.ReadOnlySpan) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		body = endedNamed(rec, "xflow.map.body")
		batch = endedNamed(rec, "xflow.map.batch")
		var seen int64
		for _, span := range body {
			n, _ := spanIntValue(span, "item_count")
			seen += n
		}
		// One body per batch is the shape under test, so wait for both counts to
		// agree rather than for a threshold: a body span without its batch span
		// (or the reverse) is exactly the defect this test exists to catch.
		if len(body) > 0 && len(body) == len(batch) && seen == int64(wantItems) {
			return body, batch
		}
		if time.Now().After(deadline) {
			t.Fatalf("map spans never settled: %d body, %d batch, %d/%d items accounted for",
				len(body), len(batch), seen, wantItems)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestLocalMapEmitsOneSpanPerBatchNeverPerItem pins the span budget of the
// fan-out paths: one xflow.map.batch per batch, one xflow.map.body per batch,
// and NO xflow.subgraph.execute per item — a map over N items must not multiply
// the trace by N. The spans must also join the submission trace through the
// persisted carrier, since batches are delivered by the outbox dispatcher on a
// background context.
func TestLocalMapEmitsOneSpanPerBatchNeverPerItem(t *testing.T) {
	rec, tracer := installLocalSpanRecorder(t)
	eng, err := NewLocal(WithTracer(tracer))
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	defer eng.Stop()

	body := Workflow("trace-double-body")
	body.LocalNode("double", &traceMapBodyHandler{})
	wf := Workflow("map-trace")
	start := wf.Node("start", node.Start())
	mapNode := wf.Node("m", node.Map("$input.ids", 2))
	mapNode.Body(body)
	wf.Connect(start, mapNode)

	ctx := context.Background()
	workflowID, err := eng.AddWorkflow(ctx, wf)
	if err != nil {
		t.Fatalf("AddWorkflow() error = %v", err)
	}

	parentCtx, parentSpan := tracer.Start(ctx, "caller.map")
	parentSC := oteltrace.SpanContextFromContext(parentCtx)
	submissionCtx := enginecore.WithTraceCarrier(parentCtx, tracing.InjectCarrier(parentCtx))
	parentSpan.End()

	execID, err := eng.Invoke(submissionCtx, workflowID, Start(), map[string]any{"ids": []any{1, 2, 3}})
	if err != nil {
		t.Fatalf("Invoke() error = %v", err)
	}
	if _, err := eng.Wait(ctx, execID); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}

	bodies, batches := waitForMapSpans(t, rec, 3)
	for _, span := range batches {
		if got := span.SpanContext().TraceID(); got != parentSC.TraceID() {
			t.Errorf("xflow.map.batch trace = %s, want submission trace %s", got, parentSC.TraceID())
		}
		if got := span.Parent().SpanID(); got != parentSC.SpanID() {
			t.Errorf("xflow.map.batch parent = %s, want submission span %s", got, parentSC.SpanID())
		}
		if name, ok := spanStringValue(span, "node_name"); !ok || name != "m" {
			t.Errorf("xflow.map.batch node_name = %q, present=%t; want m", name, ok)
		}
		if n, ok := spanIntValue(span, "item_count"); !ok || n <= 0 {
			t.Errorf("xflow.map.batch item_count = %d, present=%t; want a positive count", n, ok)
		}
		if n, ok := spanIntValue(span, "batch_count"); !ok || n < 1 {
			t.Errorf("xflow.map.batch batch_count = %d, present=%t; want at least 1", n, ok)
		}
	}
	// Pair every body span with THE batch span that ran it, by batch_index. A
	// set-membership check would also pass if a body were parented to some OTHER
	// batch's span, which is the defect this pairing rules out.
	batchSpanByIndex := make(map[int64]oteltrace.SpanID, len(batches))
	for _, span := range batches {
		index, ok := spanIntValue(span, "batch_index")
		if !ok {
			t.Errorf("xflow.map.batch missing batch_index attribute")
			continue
		}
		if _, dup := batchSpanByIndex[index]; dup {
			t.Errorf("two xflow.map.batch spans share batch_index %d", index)
			continue
		}
		batchSpanByIndex[index] = span.SpanContext().SpanID()
	}
	paired := make(map[int64]struct{}, len(bodies))
	for _, span := range bodies {
		if got := span.SpanContext().TraceID(); got != parentSC.TraceID() {
			t.Errorf("xflow.map.body trace = %s, want submission trace %s", got, parentSC.TraceID())
		}
		index, ok := spanIntValue(span, "batch_index")
		if !ok {
			t.Errorf("xflow.map.body missing batch_index attribute")
			continue
		}
		// The body runs inside the batch's ExecuteBatch, on the batch span's
		// context: the body with batch_index i must be batch span i's child, not
		// a sibling root that merely shares the trace, and not another batch's
		// child.
		want, ok := batchSpanByIndex[index]
		if !ok {
			t.Errorf("xflow.map.body batch_index %d has no matching xflow.map.batch span", index)
			continue
		}
		if _, dup := paired[index]; dup {
			t.Errorf("two xflow.map.body spans share batch_index %d", index)
			continue
		}
		paired[index] = struct{}{}
		if got := span.Parent().SpanID(); got != want {
			t.Errorf("xflow.map.body batch_index %d parent = %s, want batch span %s", index, got, want)
		}
		if span.Parent().IsRemote() {
			t.Errorf("xflow.map.body parent is remote; the enclosing batch span is local to this process")
		}
	}
	for index := range batchSpanByIndex {
		if _, ok := paired[index]; !ok {
			t.Errorf("xflow.map.batch batch_index %d has no xflow.map.body span", index)
		}
	}
	// The item executions go through subgraph.Executor.Execute; the body marks
	// their contexts as suppressed precisely so this count stays zero.
	if got := endedNamed(rec, "xflow.subgraph.execute"); len(got) != 0 {
		t.Errorf("map fan-out produced %d xflow.subgraph.execute spans, want 0 (one per ITEM is the forbidden shape)", len(got))
	}
}
