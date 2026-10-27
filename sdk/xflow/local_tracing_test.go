package xflow

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"

	enginecore "github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/observability/tracing"
	"github.com/xbcio/xflow/types"
)

// installLocalSpanRecorder installs the W3C propagator (the lease carrier and
// the SDK both go through the global one) and an in-memory provider as the
// process global, restoring both on cleanup. The global provider is what the
// engine- and subgraph-owned spans resolve; the returned explicit tracer (passed
// to NewLocal via WithTracer) records the same provider, so one recorder sees
// the whole local trace. No test in this package runs in parallel, so the
// global swap cannot race another test's assertions.
func installLocalSpanRecorder(t *testing.T) (*tracetest.SpanRecorder, tracing.Tracer) {
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
	return rec, tracing.NewOTelTracer(tp.Tracer("xflow-local-test"))
}

// waitForEndedSpans polls until at least want spans of the given name have
// ended. The task spans end on the queue worker's goroutine, so a Wait that
// returns on the committed terminal state can still be a moment ahead of the
// last span's End.
func waitForEndedSpans(t *testing.T, rec *tracetest.SpanRecorder, name string, want int) []sdktrace.ReadOnlySpan {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var out []sdktrace.ReadOnlySpan
		for _, span := range rec.Ended() {
			if span.Name() == name {
				out = append(out, span)
			}
		}
		if len(out) >= want {
			return out
		}
		if time.Now().After(deadline) {
			names := make([]string, 0, len(rec.Ended()))
			for _, span := range rec.Ended() {
				names = append(names, span.Name())
			}
			t.Fatalf("found %d %q spans, want at least %d; ended spans: %v", len(out), name, want, names)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// assertLocalSpanParentedTo pins that every local task span belongs to the
// caller's trace and is its direct child. The only path that can produce a
// remote parent here is the lease carrier stamped by BuildTaskLease, so this
// also proves the carrier round-trip rather than a coincidental trace id.
func assertLocalSpanParentedTo(t *testing.T, span sdktrace.ReadOnlySpan, parent oteltrace.SpanContext) {
	t.Helper()
	if got := span.SpanContext().TraceID(); got != parent.TraceID() {
		t.Fatalf("%s trace id = %s, want caller trace %s", span.Name(), got, parent.TraceID())
	}
	if got := span.Parent().SpanID(); got != parent.SpanID() {
		t.Fatalf("%s parent span = %s, want caller span %s", span.Name(), got, parent.SpanID())
	}
	if !span.Parent().IsRemote() {
		t.Fatalf("%s parent is not remote: the lease carrier must be re-extracted", span.Name())
	}
}

func spanStringValue(span sdktrace.ReadOnlySpan, key string) (string, bool) {
	for _, kv := range span.Attributes() {
		if string(kv.Key) == key {
			return kv.Value.AsString(), true
		}
	}
	return "", false
}

// assertNoPayloadSpanAttributes is the local-mode leak guard: a task span may
// describe the attempt, never the business payload it carried.
func assertNoPayloadSpanAttributes(t *testing.T, span sdktrace.ReadOnlySpan, forbidden ...string) {
	t.Helper()
	for _, kv := range span.Attributes() {
		key := strings.ToLower(string(kv.Key))
		for _, banned := range []string{"payload", "params", "token", "secret", "password", "credential", "output", "input.data"} {
			if key == banned || strings.HasSuffix(key, "."+banned) || strings.HasPrefix(key, banned+"_") {
				t.Errorf("%s span carries sensitive attribute key %q", span.Name(), kv.Key)
			}
		}
		for _, value := range forbidden {
			if value != "" && strings.Contains(kv.Value.String(), value) {
				t.Errorf("%s span attribute %q leaks value %q", span.Name(), kv.Key, value)
			}
		}
	}
}

// TestLocalEngineTaskSpanSharesSubmissionTrace is the embedded-mode half of
// the trace contract: with WithTracer installed, the node attempt reports
// xflow.local.task in the SUBMISSION trace even though the queue worker starts
// from context.Background(). The submission carrier is the only thing that can
// connect the two.
func TestLocalEngineTaskSpanSharesSubmissionTrace(t *testing.T) {
	rec, tracer := installLocalSpanRecorder(t)
	const secret = "sensitive-local-payload-must-not-export"

	eng, err := NewLocal(WithTracer(tracer))
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	defer eng.Stop()

	wf := Workflow("local-trace")
	start := wf.Node("start", node.Start())
	work := wf.Node("work", node.Function("return input"))
	wf.Connect(start, work)
	workflowID, err := eng.AddWorkflow(context.Background(), wf)
	if err != nil {
		t.Fatalf("AddWorkflow() error = %v", err)
	}

	parentCtx, parentSpan := tracer.Start(context.Background(), "caller.submit")
	parentSC := oteltrace.SpanContextFromContext(parentCtx)
	submissionCtx := enginecore.WithTraceCarrier(parentCtx, tracing.InjectCarrier(parentCtx))
	parentSpan.End()

	execID, err := eng.Invoke(submissionCtx, workflowID, Start(), map[string]any{"token": secret})
	if err != nil {
		t.Fatalf("Invoke() error = %v", err)
	}
	if _, err := eng.Wait(context.Background(), execID); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}

	spans := waitForEndedSpans(t, rec, "xflow.local.task", 1)
	for _, span := range spans {
		assertLocalSpanParentedTo(t, span, parentSC)
		assertNoPayloadSpanAttributes(t, span, secret)
	}
}

// traceFAFHandler signals when its action has run so the FAF goroutine can be
// waited on without polling execution state (FAF persists none).
type traceFAFHandler struct {
	nodeType string
	ran      chan struct{}
	once     sync.Once
}

func (h *traceFAFHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: h.nodeType, Kind: types.NodeKindAction}
}

func (h *traceFAFHandler) Execute(context.Context, *types.Input) (*types.Output, error) {
	h.once.Do(func() { close(h.ran) })
	return &types.Output{}, nil
}

// TestFAFTaskSpanSharesCallerTrace covers the direct-dispatch core path: FAF
// persists no execution state, so the caller's carrier travels on the lease
// and the ad-hoc runner must be given the tracer for the asynchronous action
// to stay in the caller's trace.
func TestFAFTaskSpanSharesCallerTrace(t *testing.T) {
	rec, tracer := installLocalSpanRecorder(t)
	const secret = "sensitive-faf-payload-must-not-export"

	eng, err := NewLocal(WithTracer(tracer))
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	defer eng.Stop()

	handler := &traceFAFHandler{nodeType: "test.faf.trace", ran: make(chan struct{})}
	wf := Workflow("faf-trace").FAF()
	wf.LocalNode("only", handler)
	workflowID, err := eng.AddWorkflow(context.Background(), wf)
	if err != nil {
		t.Fatalf("AddWorkflow() error = %v", err)
	}

	parentCtx, parentSpan := tracer.Start(context.Background(), "caller.faf")
	parentSC := oteltrace.SpanContextFromContext(parentCtx)
	if err := eng.FireAndForget(parentCtx, workflowID, map[string]any{"token": secret}); err != nil {
		t.Fatalf("FireAndForget() error = %v", err)
	}
	parentSpan.End()

	select {
	case <-handler.ran:
	case <-time.After(5 * time.Second):
		t.Fatal("FAF handler never ran")
	}

	spans := waitForEndedSpans(t, rec, "xflow.local.task", 1)
	for _, span := range spans {
		assertLocalSpanParentedTo(t, span, parentSC)
		assertNoPayloadSpanAttributes(t, span, secret)
	}
}
