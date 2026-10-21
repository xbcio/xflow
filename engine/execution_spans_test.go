package engine

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/observability/tracing"
	"github.com/xbcio/xflow/types"
)

// carrierFakeState is a fakeState that also implements the narrow
// ExecutionTraceCarrierReader capability, which is what makes BuildTaskLease
// stamp the submission carrier onto a lease and what the engine's background
// paths read to re-anchor their spans.
type carrierFakeState struct {
	*fakeState
	carrier map[string]string
}

func (s *carrierFakeState) GetExecutionTraceCarrier(context.Context, types.ExecutionID) (map[string]string, bool, error) {
	if len(s.carrier) == 0 {
		return nil, false, nil
	}
	return s.carrier, true, nil
}

// errOutboxListDeferred is the injected store outage deferredFlushState reports
// once, standing in for the transient list failure that leaves an appended
// intent for the outbox dispatcher's next tick.
var errOutboxListDeferred = errors.New("outbox list unavailable (deferred flush)")

// deferredFlushState defers exactly one outbox flush: it fails the next
// ListOutbox so the commit's inline FlushOutbox reports the delivery failure
// without consuming the intent the commit appended. That is the transient
// outage shape the OutboxDispatcher exists for — the terminal transition has
// applied, the advance intent is durable, and the next flush is its only
// consumer.
//
// It is deliberately a non-folding backend: a folding commit schedules
// downstream work inside the terminal transition itself, so no advance intent
// exists and the outbox-dispatcher path would not be reached at all.
type deferredFlushState struct {
	*carrierFakeState
	deferNextFlush bool
}

func (s *deferredFlushState) FoldsArrivalsIntoCommit() bool { return false }

func (s *deferredFlushState) ListOutbox(ctx context.Context, id types.ExecutionID, before time.Time, limit int) ([]OutboxEntry, error) {
	if s.deferNextFlush {
		s.deferNextFlush = false
		return nil, errOutboxListDeferred
	}
	return s.carrierFakeState.ListOutbox(ctx, id, before, limit)
}

// installEngineSpanRecorder installs an in-memory OTel provider as the process
// global and restores the previous provider/propagator on cleanup. The engine
// resolves its tracer lazily from the global provider (engine/tracing.go), so a
// test that wants engine spans must install one here. No test in this package
// runs in parallel, so the global swap cannot race another test's assertions.
func installEngineSpanRecorder(t *testing.T) (*tracetest.SpanRecorder, tracing.Tracer) {
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
	return rec, tracing.NewOTelTracer(tp.Tracer("xflow-engine-test"))
}

// submitUnderParentSpan submits g while a parent span is active and returns the
// execution id plus that parent's SpanContext. The submission context carries
// the parent's W3C carrier, which is exactly what the API server does — the
// engine's background paths are expected to recover it.
func submitUnderParentSpan(t *testing.T, eng *Engine, g *graph.Graph, tracer tracing.Tracer) (types.ExecutionID, oteltrace.SpanContext, map[string]string) {
	t.Helper()
	parentCtx, parentSpan := tracer.Start(context.Background(), "xflow.workflow.execute")
	parentSC := oteltrace.SpanContextFromContext(parentCtx)
	carrier := tracing.InjectCarrier(parentCtx)
	parentSpan.End()
	id, err := eng.Submit(WithTraceCarrier(context.Background(), carrier), g, nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	return id, parentSC, carrier
}

func findEndedSpan(t *testing.T, rec *tracetest.SpanRecorder, name string) sdktrace.ReadOnlySpan {
	t.Helper()
	var found sdktrace.ReadOnlySpan
	for _, span := range rec.Ended() {
		if span.Name() == name {
			if found != nil {
				t.Fatalf("found more than one %q span", name)
			}
			found = span
		}
	}
	if found == nil {
		names := make([]string, 0, len(rec.Ended()))
		for _, span := range rec.Ended() {
			names = append(names, span.Name())
		}
		t.Fatalf("missing span %q; ended spans: %v", name, names)
	}
	return found
}

func spanAttribute(span sdktrace.ReadOnlySpan, key string) (attribute.KeyValue, bool) {
	for _, kv := range span.Attributes() {
		if string(kv.Key) == key {
			return kv, true
		}
	}
	return attribute.KeyValue{}, false
}

// assertSpanParent pins the remote-parent contract: the span must live in the
// submission trace and be a direct child of the submit span recovered from the
// carrier, not a detached root and not a string-reconstructed look-alike.
func assertSpanParent(t *testing.T, span sdktrace.ReadOnlySpan, parent oteltrace.SpanContext) {
	t.Helper()
	if got := span.SpanContext().TraceID(); got != parent.TraceID() {
		t.Fatalf("%s trace id = %s, want submit trace %s", span.Name(), got, parent.TraceID())
	}
	if got := span.Parent().SpanID(); got != parent.SpanID() {
		t.Fatalf("%s parent span = %s, want submit span %s", span.Name(), got, parent.SpanID())
	}
	if !span.Parent().IsRemote() {
		t.Fatalf("%s parent is not remote: the carrier round-trip must restore a remote parent", span.Name())
	}
}

// assertNoSensitiveSpanAttributes is the security half of the span budget: a
// span may name WHAT happened, never carry the business payload. forbidden are
// concrete values that must not appear anywhere (attribute values or string
// forms), so a future attribute that smuggles a payload fails even if it does
// not use one of the denied key names.
func assertNoSensitiveSpanAttributes(t *testing.T, span sdktrace.ReadOnlySpan, forbidden ...string) {
	t.Helper()
	for _, kv := range span.Attributes() {
		key := strings.ToLower(string(kv.Key))
		for _, banned := range []string{"payload", "params", "token", "secret", "password", "credential", "output", "input.data"} {
			if key == banned || strings.HasSuffix(key, "."+banned) || strings.HasPrefix(key, banned+"_") {
				t.Errorf("%s span carries sensitive attribute key %q", span.Name(), kv.Key)
			}
		}
		for _, value := range forbidden {
			if value != "" && strings.Contains(kv.Value.Emit(), value) {
				t.Errorf("%s span attribute %q leaks value %q", span.Name(), kv.Key, value)
			}
		}
	}
}

// advanceGraph is a two-node acyclic graph: start → end on the default port.
func advanceGraph() *graph.Graph {
	wf := &types.WorkflowDef{
		Name: "span-advance",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.echo"},
			{Name: "end", Type: "test.echo"},
		},
		Connections: types.Connections{
			"start": {"default": {Targets: []types.Connection{{Node: "end", Input: "default"}}}},
		},
	}
	g, err := graph.Compile(wf)
	if err != nil {
		panic(err)
	}
	return g
}

// TestEngineAdvanceSpanParentedToSubmissionTrace covers the queue-consumer /
// legacy direct-consume path: HandleSystemTask is called on a context with no
// span of its own, so the only way xflow.engine.advance can land in the
// workflow's trace is the submission carrier persisted on the execution
// snapshot. It does NOT cover the outbox dispatcher: FlushOutbox opens spans of
// its own around the inline consumption, and that composition is pinned
// separately by TestEngineBackgroundFlushAdvanceSpanParentedToSubmissionTrace.
func TestEngineAdvanceSpanParentedToSubmissionTrace(t *testing.T) {
	rec, tracer := installEngineSpanRecorder(t)
	state := &carrierFakeState{fakeState: newFakeState()}
	eng := New(state, &fakeQueue{})
	id, parentSC, carrier := submitUnderParentSpan(t, eng, advanceGraph(), tracer)
	state.carrier = carrier

	// Make the source node terminal so the advance transition applies: this is
	// the state a commit leaves behind, without exercising the commit path the
	// span is not about.
	if err := state.UpsertNode(context.Background(), &NodeSnapshot{
		ExecutionID:  id,
		Name:         "start",
		Status:       types.NodeStatusSuccess,
		ActivationID: 0,
		Port:         "default",
	}); err != nil {
		t.Fatalf("UpsertNode() error = %v", err)
	}
	port := "default"
	handled, err := eng.HandleSystemTask(context.Background(), &Task{
		ExecutionID:  id,
		NodeName:     "start",
		NodeIdx:      0,
		Type:         TaskTypeNodeAdvance,
		ActivationID: 0,
		Port:         &port,
	})
	if err != nil || !handled {
		t.Fatalf("HandleSystemTask(advance) = %v, %v; want handled, nil", handled, err)
	}

	span := findEndedSpan(t, rec, "xflow.engine.advance")
	assertSpanParent(t, span, parentSC)
	if kv, ok := spanAttribute(span, "execution_id"); !ok || kv.Value.AsString() != string(id) {
		t.Errorf("execution_id attribute = %v, want %q", kv.Value.Emit(), id)
	}
	if kv, ok := spanAttribute(span, "node_name"); !ok || kv.Value.AsString() != "start" {
		t.Errorf("node_name attribute = %v, want start", kv.Value.Emit())
	}
	if kv, ok := spanAttribute(span, "applied"); !ok || !kv.Value.AsBool() {
		t.Errorf("applied attribute = %v, want true", kv.Value.Emit())
	}
	if kv, ok := spanAttribute(span, "skipped"); !ok || kv.Value.AsInt64() != 0 {
		t.Errorf("skipped attribute = %v, want 0", kv.Value.Emit())
	}
}

// TestEngineBackgroundFlushAdvanceSpanParentedToSubmissionTrace covers the real
// outbox-dispatcher composition end to end: Submit under a parent span, a
// terminal commit appends an advance intent, the commit's own flush fails (a
// transient list outage), and the next FlushOutbox — called on a bare
// background context, exactly as the dispatcher calls it — is the only consumer
// of that intent.
//
// Both spans the background flush opens must join the submission trace through
// the persisted carrier. The advance in particular must be a remote child of
// the submission span, not a descendant of an operational flush span: the flush
// has its own lifecycle, and on a background context that is not re-anchored
// first it becomes a detached root whose trace the advance inherits.
func TestEngineBackgroundFlushAdvanceSpanParentedToSubmissionTrace(t *testing.T) {
	rec, tracer := installEngineSpanRecorder(t)
	state := &deferredFlushState{carrierFakeState: &carrierFakeState{fakeState: newFakeState()}}

	parentCtx, parentSpan := tracer.Start(context.Background(), "xflow.workflow.execute")
	parentSC := oteltrace.SpanContextFromContext(parentCtx)
	carrier := tracing.InjectCarrier(parentCtx)
	parentSpan.End()
	// The carrier is what both BuildTaskLease and the engine's background
	// re-anchoring read; stamp it before submission, as the snapshot write does.
	state.carrier = carrier

	ctx := context.Background()
	queue := &fakeQueue{}
	eng := New(state, queue)
	id, err := eng.Submit(WithTraceCarrier(ctx, carrier), advanceGraph(), nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	tasks := queue.Drain()
	if len(tasks) != 1 || tasks[0].NodeName != "start" {
		t.Fatalf("initial delivery = %+v, want one task for start", tasks)
	}
	lease, err := eng.BuildTaskLease(ctx, tasks[0])
	if err != nil || lease == nil {
		t.Fatalf("BuildTaskLease() = %v, %v", lease, err)
	}

	// A transient outbox outage on the commit's own delivery attempt: the
	// terminal transition applies, the advance intent stays durable, and the
	// flush that would have consumed it reports the failure. This is the state
	// the dispatcher is expected to recover from.
	state.deferNextFlush = true
	err = eng.CommitTaskResult(ctx, lease, TaskResult{Output: &types.Output{
		Port: "default",
		Data: map[string]any{"out": 1},
	}})
	if !errors.Is(err, errOutboxListDeferred) {
		t.Fatalf("CommitTaskResult() error = %v, want the deferred outbox list failure", err)
	}

	before := len(rec.Ended())
	if err := eng.FlushOutbox(context.Background(), id); err != nil {
		t.Fatalf("FlushOutbox() error = %v", err)
	}
	delivered := queue.Drain()
	if len(delivered) != 1 || delivered[0].NodeName != "end" || delivered[0].Type != TaskTypeNodeExec {
		t.Fatalf("delivered after background flush = %+v, want one exec task for end; "+
			"the flush must actually consume the deferred advance intent, not just open spans", delivered)
	}

	var flushSpan, advanceSpan sdktrace.ReadOnlySpan
	for _, span := range rec.Ended()[before:] {
		switch span.Name() {
		case "xflow.outbox.flush":
			flushSpan = span
		case "xflow.engine.advance":
			advanceSpan = span
		}
	}
	if flushSpan == nil || advanceSpan == nil {
		t.Fatalf("background flush ended %d spans without both xflow.outbox.flush and xflow.engine.advance", len(rec.Ended())-before)
	}
	// Delivery itself must re-join the workflow's trace: the flush span's parent
	// is the submission span recovered from the carrier, remote because it came
	// through a real W3C ExtractCarrier round-trip.
	assertSpanParent(t, flushSpan, parentSC)
	// The advance is execution work and hangs off the submission span, not off
	// the operational flush span.
	assertSpanParent(t, advanceSpan, parentSC)
	if got := advanceSpan.Parent().SpanID(); got == flushSpan.SpanContext().SpanID() {
		t.Fatalf("advance parent = flush span %s; the background path must not nest execution work under the delivery span", got)
	}
	if kv, ok := spanAttribute(advanceSpan, "applied"); !ok || !kv.Value.AsBool() {
		t.Errorf("applied attribute = %v, want true (the background flush applied the deferred advance)", kv.Value.Emit())
	}
}

// TestEngineRetrySpanParentedToSubmissionTrace covers the same recovery on the
// retry scheduler: a node failure is committed from a span-carrying commit in
// production, but scheduleRetry is also reached from recovery paths whose ctx
// has no span.
func TestEngineRetrySpanParentedToSubmissionTrace(t *testing.T) {
	rec, tracer := installEngineSpanRecorder(t)
	state := &carrierFakeState{fakeState: newFakeState()}
	eng := New(state, &fakeQueue{})
	id, parentSC, carrier := submitUnderParentSpan(t, eng, advanceGraph(), tracer)
	state.carrier = carrier

	task := &Task{
		ExecutionID:  id,
		NodeName:     "start",
		NodeIdx:      0,
		Type:         TaskTypeNodeExec,
		ActivationID: 0,
	}
	lease, err := eng.BuildTaskLease(context.Background(), task)
	if err != nil || lease == nil {
		t.Fatalf("BuildTaskLease() = %v, %v", lease, err)
	}
	settings := &types.RetrySettings{Strategy: types.RetryStrategyFixed, InitialInterval: 250}
	if _, err := eng.scheduleRetry(context.Background(), task, 1, settings, lease.LeaseToken); err != nil {
		t.Fatalf("scheduleRetry() error = %v", err)
	}

	span := findEndedSpan(t, rec, "xflow.engine.retry")
	assertSpanParent(t, span, parentSC)
	if kv, ok := spanAttribute(span, "execution_id"); !ok || kv.Value.AsString() != string(id) {
		t.Errorf("execution_id attribute = %v, want %q", kv.Value.Emit(), id)
	}
	if kv, ok := spanAttribute(span, "node_name"); !ok || kv.Value.AsString() != "start" {
		t.Errorf("node_name attribute = %v, want start", kv.Value.Emit())
	}
	if kv, ok := spanAttribute(span, "attempt"); !ok || kv.Value.AsInt64() != 1 {
		t.Errorf("attempt attribute = %v, want 1", kv.Value.Emit())
	}
	if kv, ok := spanAttribute(span, "delay_ms"); !ok || kv.Value.AsInt64() <= 0 {
		t.Errorf("delay_ms attribute = %v, want a positive delay", kv.Value.Emit())
	}
	// The retry settings themselves (intervals, strategy) are configuration, not
	// payload; nothing else about the failing node may appear.
	assertNoSensitiveSpanAttributes(t, span)
}

// TestEngineSignalSpanCarriesNoPayload is the leak guard for the one span whose
// input is entirely caller-supplied business data. DeliverSignal must report the
// signal's NAME and delivery state, never the data map it carries.
func TestEngineSignalSpanCarriesNoPayload(t *testing.T) {
	rec, tracer := installEngineSpanRecorder(t)
	state := &carrierFakeState{fakeState: newFakeState()}
	eng := New(state, &fakeQueue{})
	id, parentSC, carrier := submitUnderParentSpan(t, eng, advanceGraph(), tracer)
	state.carrier = carrier

	const secret = "sensitive-payload-must-not-export"
	// No suspended node exists, so this stores the signal and resumes nothing —
	// the span must exist and describe exactly that.
	if err := eng.DeliverSignal(context.Background(), id, "approve", map[string]any{
		"token":   secret,
		"payload": map[string]any{"password": secret},
	}); err != nil {
		t.Fatalf("DeliverSignal() error = %v", err)
	}

	span := findEndedSpan(t, rec, "xflow.signal.deliver")
	assertSpanParent(t, span, parentSC)
	if kv, ok := spanAttribute(span, "signal_name"); !ok || kv.Value.AsString() != "approve" {
		t.Errorf("signal_name attribute = %v, want approve", kv.Value.Emit())
	}
	if kv, ok := spanAttribute(span, "durable"); !ok || kv.Value.AsBool() {
		// fakeState has no DurableSignalDeliverer: the legacy path is reported.
		t.Errorf("durable attribute = %v, want false (legacy path)", kv.Value.Emit())
	}
	if kv, ok := spanAttribute(span, "resumed"); !ok || kv.Value.AsBool() {
		t.Errorf("resumed attribute = %v, want false (nothing suspended)", kv.Value.Emit())
	}
	assertNoSensitiveSpanAttributes(t, span, secret)
}
