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
// the content), the content revision, and how long the rebuild took. Rule
// count and generation are published only for an applied swap: content that
// was rejected never became active, so attributing a count/generation to it
// would misrepresent what is actually serving. rules < 0 (ruleCount's
// "shape not recognized" signal) also skips the rule-count gauge for the same
// reason a negative count is never a real quantity.
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

// OnConfigAge records how long the active content has been in service. This is
// the single most important signal in this file: a source that stopped
// updating leaves every version counter frozen and therefore looks healthy —
// only elapsed time reveals it.
func (s SupplyMetrics) OnConfigAge(ctx context.Context, age time.Duration) {
	s.Metrics.Set(metricSupplyAgeSeconds, withNamespace(ctx, nil), age.Seconds())
}

// OnInstanceCount records pool occupancy. state is "ready" or "doomed".
func (s SupplyMetrics) OnInstanceCount(ctx context.Context, state string, n int) {
	s.Metrics.Set(metricWasmInstanceTotal, withNamespace(ctx, map[string]string{"state": state}), float64(n))
}

// OnInstanceRecycled records an instance teardown. cause is one of "timeout",
// "eval_error", "shutdown", "pool_swapped".
func (s SupplyMetrics) OnInstanceRecycled(ctx context.Context, cause string) {
	s.Metrics.Inc(metricWasmInstanceRecycled, withNamespace(ctx, map[string]string{"cause": cause}))
}

// OnBorrowWait records how long a caller waited for a free pool instance.
func (s SupplyMetrics) OnBorrowWait(ctx context.Context, d time.Duration) {
	s.Metrics.Observe(metricWasmPoolBorrowWait, withNamespace(ctx, nil), d)
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
