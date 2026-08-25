package metrics

import (
	"context"
	"time"

	"github.com/xbcio/xflow/node/supply"
)

// Supply and wasm reactor metric names.
const (
	metricSupplyAgeSeconds         = "xflow_supply_age_seconds"
	metricSupplyFetchTotal         = "xflow_supply_fetch_total"
	metricSupplyNotReady           = "xflow_supply_not_ready"
	metricSupplyUnavailableServing = "xflow_supply_unavailable_serving"
	metricSupplyConsumers          = "xflow_supply_consumers"
	metricWasmConfigRuleCount      = "xflow_wasm_config_rule_count"
	metricWasmConfigGeneration     = "xflow_wasm_config_generation"
	metricWasmPoolSwapTotal        = "xflow_wasm_pool_swap_total"
	metricWasmPoolSwapDuration     = "xflow_wasm_pool_swap_duration_seconds"
	metricWasmInstanceTotal        = "xflow_wasm_instance_total"
	metricWasmInstanceRecycled     = "xflow_wasm_instance_recycled_total"
	metricWasmPoolBorrowWait       = "xflow_wasm_pool_borrow_wait_seconds"
	metricWasmEvalStdinBytes       = "xflow_wasm_eval_stdin_bytes"
	metricWasmEvalDuration         = "xflow_wasm_eval_duration_seconds"
	metricWasmModuleCompileTotal   = "xflow_wasm_module_compile_total"
)

// SupplyMetrics observes supply distribution and wasm reactor pool activity.
//
// Every label here is low-cardinality: supply name, workflow ID, and fixed
// result/state/cause enums. Content, content hashes, and execution IDs are
// deliberately absent — the first two are not an operator's to read from a
// metric, and the third would be unbounded.
type SupplyMetrics struct {
	Metrics *Metrics
}

func NewSupplyMetrics(m *Metrics) SupplyMetrics { return SupplyMetrics{Metrics: m} }

// --- wasm reactor observer surface ---

// OnPoolSwap records a config application: its outcome, the RULE COUNT (never
// the content), the content revision, and how long the rebuild took.
//
// Both gauges are host-wide aggregates supplied by the caller — see
// wasm.Observer. Like OnInstanceCount below, they Set a series keyed only by
// namespace, so each call REPLACES the previous value; with more than one wasm
// module resident, a per-engine report meant "whichever module swapped last".
//
// Rule count and generation are published only for an applied swap. Rejected
// content never became active, and while the caller now passes the still-serving
// state rather than the rejected config's, re-Setting an unchanged value on a
// failure path only invites the reading that a rejection published something.
// rules < 0 (ruleCount's "shape not recognized" signal, poisoned across the
// whole host so one unreadable module cannot hide inside a sum) also skips the
// rule-count gauge, for the same reason a negative count is never a real
// quantity.
func (s SupplyMetrics) OnPoolSwap(ctx context.Context, result string, rules int, revision uint64, d time.Duration) {
	s.Metrics.Inc(metricWasmPoolSwapTotal, withNamespace(ctx, map[string]string{"result": result}))
	s.Metrics.Observe(metricWasmPoolSwapDuration, withNamespace(ctx, nil), d)
	if result != "applied" {
		return
	}
	if rules >= 0 {
		s.Metrics.Set(metricWasmConfigRuleCount, withNamespace(ctx, nil), float64(rules))
	}
	s.Metrics.Set(metricWasmConfigGeneration, withNamespace(ctx, nil), float64(revision))
}

// OnConfigAge records how long the STALEST active content across every wasm
// engine has been in service. This is the single most important signal in this
// file: a source that stopped updating leaves every version counter frozen and
// therefore looks healthy — only elapsed time reveals it.
//
// The max is the caller's job for the same reason the sum is in OnInstanceCount:
// this Sets a series keyed only by namespace, with no module identity, so each
// call replaces the previous value. Reporting per-engine let a module that
// refreshed a second ago overwrite a sibling frozen for hours, and with traffic
// interleaved across both the gauge alternated between them — an alert on it
// flaps, and a flapping alert gets muted.
func (s SupplyMetrics) OnConfigAge(ctx context.Context, age time.Duration) {
	s.Metrics.Set(metricSupplyAgeSeconds, withNamespace(ctx, nil), age.Seconds())
}

// OnInstanceCount records total resident pool instances across all engines.
// state is "ready"; see wasm.Observer for why there is no "doomed" value and
// why the caller must sum rather than report per-engine. This Sets a series
// keyed only by state, so each call REPLACES the previous value — that is
// exactly why summing is the caller's job.
func (s SupplyMetrics) OnInstanceCount(ctx context.Context, state string, n int) {
	s.Metrics.Set(metricWasmInstanceTotal, withNamespace(ctx, map[string]string{"state": state}), float64(n))
}

// OnInstanceRecycled records an instance teardown. cause is one of "timeout",
// "eval_error", "memory_high_water", "max_evals", "shutdown", "pool_swapped",
// "rebuild_failed". The last one is not a teardown but a failed replacement: the
// pool is permanently one instance narrower, since nothing retries the rebuild.
//
// memory_high_water is the planned recycle that fires on real traffic: the
// guest's linear memory crossed the threshold that precedes an out-of-memory
// trap. A rising rate is normal on large records and is what keeps them from
// failing; max_evals firing instead means the guest's memory never climbed.
func (s SupplyMetrics) OnInstanceRecycled(ctx context.Context, cause string) {
	s.Metrics.Inc(metricWasmInstanceRecycled, withNamespace(ctx, map[string]string{"cause": cause}))
}

// OnBorrowWait records how long a caller waited for a free pool instance.
func (s SupplyMetrics) OnBorrowWait(ctx context.Context, d time.Duration) {
	s.Metrics.Observe(metricWasmPoolBorrowWait, withNamespace(ctx, nil), d)
}

// OnEval records one eval's stdin size and duration, attributed to the node
// that ran it.
//
// Size and duration together answer a question neither answers alone: whether a
// rising eval cost is the engine getting slower or the payload getting bigger.
// Eval is dominated by the guest re-parsing stdin inside the sandbox, so the two
// series normally track each other; a duration that climbs while size holds flat
// is contention or oversubscription, and a size that climbs on its own is a
// redundant copy leaking into the payload.
//
// workflow/node answer the next question: WHICH node. A runner hosting several
// script nodes merges them into one series without these, so the node that
// regressed is the one that cannot be named — and in a collection pipeline the
// nodes do not cost remotely the same. Cardinality is bounded by the deployed
// workflow definitions; neither value is derived from a message.
//
// Both labels are emitted even when empty. An eval that reached the engine
// without going through the node layer has no node to name, and dropping the
// labels for it would mean a second vec under the same metric name — which
// prometheus refuses to register, so those evals would vanish from /metrics with
// only a log line to say so. node="" is the honest encoding of "unattributed".
//
// The size histogram is also the only way the TAIL is visible. Every other
// signal here reports a mean, and a payload distribution with a p99 at 17x the
// mean spends most of its cost on records the mean never shows.
func (s SupplyMetrics) OnEval(ctx context.Context, workflow, node string, stdinBytes int, d time.Duration) {
	labels := withNamespace(ctx, map[string]string{"workflow": workflow, "node": node})
	s.Metrics.ObserveBytes(metricWasmEvalStdinBytes, labels, stdinBytes)
	s.Metrics.Observe(metricWasmEvalDuration, labels, d)
}

// OnModuleCompile records module compilation cache outcome: "hit" or "miss".
func (s SupplyMetrics) OnModuleCompile(ctx context.Context, result string) {
	s.Metrics.Inc(metricWasmModuleCompileTotal, withNamespace(ctx, map[string]string{"result": result}))
}

// --- supply gate observer surface ---

// OnSupplyFetch records one content fetch attempt. result is "ok" or "error".
func (s SupplyMetrics) OnSupplyFetch(ctx context.Context, name, result string) {
	s.Metrics.Inc(metricSupplyFetchTotal, withNamespace(ctx, map[string]string{
		"name": name, "result": result,
	}))
}

// OnSupplyNotReady reports whether an activation is currently being declined
// for a missing supply. Paired with Kafka consumer lag it is what lets an
// operator tell "lag because a supply never arrived" from "lag because we
// cannot keep up" — the two need completely different responses.
//
// The gauge must be set both ways: 1 while declining, 0 once admitted.
// Setting only 1 would leave an alert stuck forever after the condition
// clears.
func (s SupplyMetrics) OnSupplyNotReady(ctx context.Context, workflow, supplyName string, notReady bool) {
	v := 0.0
	if notReady {
		v = 1
	}
	s.Metrics.Set(metricSupplyNotReady, withNamespace(ctx, map[string]string{
		"workflow": workflow, "supply": supplyName,
	}), v)
}

// OnSupplyServingUnavailable reports that traffic is being served with
// content that was never successfully fetched (require_ready:false).
//
// This is the half that config_rule_count cannot express: rule_count == 0 is
// true both for "the source says there are no rules" (legal) and for "we
// never got any content" (an incident). Without this gauge the second one is
// a silent data-quality failure — traffic flows through untagged and nobody
// knows. Like OnSupplyNotReady, this must be set both ways.
func (s SupplyMetrics) OnSupplyServingUnavailable(ctx context.Context, name string, serving bool) {
	v := 0.0
	if serving {
		v = 1
	}
	s.Metrics.Set(metricSupplyUnavailableServing, withNamespace(ctx, map[string]string{"name": name}), v)
}

// OnConsumerCount reports how many in-process consumers a supply has. Named
// to satisfy supply.ConsumerCountObserver directly (see var _ assertion
// below) rather than through a separate adapter type.
func (s SupplyMetrics) OnConsumerCount(ctx context.Context, name string, n int) {
	s.Metrics.Set(metricSupplyConsumers, withNamespace(ctx, map[string]string{"name": name}), float64(n))
}

var _ supply.ConsumerCountObserver = SupplyMetrics{}
