package metrics

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// TestOnTriggerFireDropped_KeepsReasonsApart pins the label this counter exists
// for. The reasons call for different responses — refused is the workflow
// declining its own trigger traffic, overlap is the cron policy holding a firing
// back, transport and emit_error are the trigger failing to submit at all — so a
// counter that put them in one series would report a working throttle and a broken
// control plane as the same number.
//
// It also pins the label VALUES, which are the constants the triggers pass. A
// rename there would leave every alert and dashboard written against the old
// spelling matching nothing, with no build failure and no test failure anywhere
// else in the tree.
func TestOnTriggerFireDropped_KeepsReasonsApart(t *testing.T) {
	m := New()
	tm := TriggerMetrics{Metrics: m}
	ctx := context.Background()

	tm.OnTriggerFireDropped(ctx, types.TriggerFireDroppedRefused, nil)
	tm.OnTriggerFireDropped(ctx, types.TriggerFireDroppedRefused, nil)
	tm.OnTriggerFireDropped(ctx, types.TriggerFireDroppedOverlap, nil)

	byReason := map[string]float64{}
	for _, metric := range gatherMetricFamily(t, m, "xflow_trigger_fire_dropped_total").GetMetric() {
		byReason[labelValue(metric, "reason")] += metric.GetCounter().GetValue()
	}
	if byReason["refused"] != 2 || byReason["overlap"] != 1 {
		t.Fatalf("dropped firings by reason = %v, want refused=2 overlap=1 "+
			"(the vocabulary is %q/%q/%q/%q)",
			byReason,
			types.TriggerFireDroppedRefused, types.TriggerFireDroppedOverlap,
			types.TriggerFireDroppedTransport, types.TriggerFireDroppedEmitError)
	}
}

// TestOnTriggerFireDropped_NarrowsTheConcurrencyLimit is the reason the observer
// receives the error at all. A trigger reports every embedded-engine rejection as
// emit_error, because the sentinel that identifies the workflow's own concurrency
// limit lives in package engine and no production code under node/ may import
// that package. Reporting the two as one series would be the exact confusion this
// counter was added to remove: a schedule being throttled by its own workflow
// would be indistinguishable from an engine refusing submissions, and only one of
// those is a fault.
func TestOnTriggerFireDropped_NarrowsTheConcurrencyLimit(t *testing.T) {
	m := New()
	tm := TriggerMetrics{Metrics: m}
	ctx := context.Background()

	// The three shapes the narrowing has to get right: the sentinel on its own, a
	// sentinel wrapped by the caller that produced it (which is how it arrives —
	// errors.Is is the only comparison that survives that), and a failure that is
	// not the sentinel and must stay where the trigger put it.
	tm.OnTriggerFireDropped(ctx, types.TriggerFireDroppedEmitError, engine.ErrConcurrencyLimit)
	tm.OnTriggerFireDropped(ctx, types.TriggerFireDroppedEmitError, fmt.Errorf("admit: %w", engine.ErrConcurrencyLimit))
	tm.OnTriggerFireDropped(ctx, types.TriggerFireDroppedEmitError, errors.New("engine is closed"))

	byReason := map[string]float64{}
	for _, metric := range gatherMetricFamily(t, m, "xflow_trigger_fire_dropped_total").GetMetric() {
		byReason[labelValue(metric, "reason")] += metric.GetCounter().GetValue()
	}
	if byReason["refused"] != 2 || byReason["emit_error"] != 1 {
		t.Fatalf("dropped firings by reason = %v, want refused=2 (the concurrency "+
			"limit, wrapped or not) and emit_error=1 (anything else the engine "+
			"rejected): the two call for opposite responses — capacity versus a "+
			"broken submission", byReason)
	}
}

// TestOnTriggerFireDropped_NarrowsOnlyTheEmitReason guards the narrowing's blast
// radius. An error can accompany a report for reasons that are already decided —
// a transport failure carries the dial error, an overlap drop carries none — and
// rewriting those from the error would relabel a control plane outage as the
// workflow's own throttling, which is the one reading that must never be wrong.
func TestOnTriggerFireDropped_NarrowsOnlyTheEmitReason(t *testing.T) {
	m := New()
	tm := TriggerMetrics{Metrics: m}
	ctx := context.Background()

	tm.OnTriggerFireDropped(ctx, types.TriggerFireDroppedTransport, engine.ErrConcurrencyLimit)
	tm.OnTriggerFireDropped(ctx, types.TriggerFireDroppedOverlap, engine.ErrConcurrencyLimit)

	byReason := map[string]float64{}
	for _, metric := range gatherMetricFamily(t, m, "xflow_trigger_fire_dropped_total").GetMetric() {
		byReason[labelValue(metric, "reason")] += metric.GetCounter().GetValue()
	}
	if byReason[types.TriggerFireDroppedTransport] != 1 || byReason[types.TriggerFireDroppedOverlap] != 1 {
		t.Fatalf("dropped firings by reason = %v, want transport=1 overlap=1: the "+
			"narrowing rewrote a decision the trigger had already made, so an "+
			"unreachable control plane now reads as the workflow throttling itself",
			byReason)
	}
}

// TestTriggerFireDroppedCarriesTheNamespace asserts what the counter does not
// carry: the workflow and the node. Those are definition data — unbounded in a
// multi-tenant deployment and cheap to reach from the trigger's own log line —
// while namespace is the label the whole registry is scoped by, and a series that
// dropped it would mix one namespace's throttling into another's.
func TestTriggerFireDroppedCarriesTheNamespace(t *testing.T) {
	m := New()
	tm := TriggerMetrics{Metrics: m}

	tm.OnTriggerFireDropped(namespace.WithNamespace(context.Background(), "ns-a"), types.TriggerFireDroppedRefused, nil)
	tm.OnTriggerFireDropped(namespace.WithNamespace(context.Background(), "ns-b"), types.TriggerFireDroppedRefused, nil)

	seen := map[string]float64{}
	for _, metric := range gatherMetricFamily(t, m, "xflow_trigger_fire_dropped_total").GetMetric() {
		seen[labelValue(metric, "namespace")] = metric.GetCounter().GetValue()
	}
	if len(seen) != 2 || seen["ns-a"] != 1 || seen["ns-b"] != 1 {
		t.Fatalf("dropped firings by namespace = %v, want ns-a=1 ns-b=1: the counter is "+
			"per-namespace like every other xflow series", seen)
	}
}

// TestTriggerMetricsIsATriggerFireObserver asserts the interface the trigger
// packages install this type through. The triggers hold a
// types.TriggerFireObserver slot, so a signature drift here would not fail to
// compile in the trigger packages' tests — it would fail at the SetObserver call
// in sdk/xflow, which is the process builder and is where an operator would first
// find out.
func TestTriggerMetricsIsATriggerFireObserver(t *testing.T) {
	var observer types.TriggerFireObserver = TriggerMetrics{Metrics: New()}
	observer.OnTriggerFireDropped(context.Background(), types.TriggerFireDroppedTransport, errors.New("dial tcp: connection refused"))
}
