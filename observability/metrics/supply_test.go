package metrics

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestSupplyMetricsOnPoolSwapAppliedRecordsRuleCountAndGeneration(t *testing.T) {
	m := New()
	s := NewSupplyMetrics(m)
	ctx := context.Background()

	s.OnPoolSwap(ctx, "applied", 3, 7, 15*time.Millisecond)

	body := gatherMetricsBody(t, m)
	for _, want := range []string{
		`xflow_wasm_pool_swap_total{namespace="default",result="applied"} 1`,
		`xflow_wasm_pool_swap_duration_seconds_count{namespace="default"} 1`,
		`xflow_wasm_config_rule_count{namespace="default"} 3`,
		`xflow_wasm_config_generation{namespace="default"} 7`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics body missing %q:\n%s", want, body)
		}
	}
}

// A rejected swap must record the swap counter/duration, but must NOT publish
// rule count or generation for content that never became active — reporting
// those would attribute a count to content that is not actually serving.
func TestSupplyMetricsOnPoolSwapRejectedSkipsRuleCountAndGeneration(t *testing.T) {
	m := New()
	s := NewSupplyMetrics(m)
	ctx := context.Background()

	s.OnPoolSwap(ctx, "rejected", 5, 9, time.Millisecond)

	body := gatherMetricsBody(t, m)
	if strings.Contains(body, "xflow_wasm_config_rule_count") {
		t.Fatalf("rejected swap must not publish rule count:\n%s", body)
	}
	if strings.Contains(body, "xflow_wasm_config_generation") {
		t.Fatalf("rejected swap must not publish generation:\n%s", body)
	}
	if !strings.Contains(body, `xflow_wasm_pool_swap_total{namespace="default",result="rejected"} 1`) {
		t.Fatalf("expected rejected swap counter:\n%s", body)
	}
}

// A negative rule count (ruleCount's "shape not recognized" signal) must not
// be published as a gauge value either: -1 rules served is nonsensical and
// would corrupt the gauge for legitimate zero-rule content.
func TestSupplyMetricsOnPoolSwapNegativeRuleCountSkipsGauge(t *testing.T) {
	m := New()
	s := NewSupplyMetrics(m)
	ctx := context.Background()

	s.OnPoolSwap(ctx, "applied", -1, 4, time.Millisecond)

	body := gatherMetricsBody(t, m)
	if strings.Contains(body, "xflow_wasm_config_rule_count") {
		t.Fatalf("negative rule count must not publish the gauge:\n%s", body)
	}
	if !strings.Contains(body, `xflow_wasm_config_generation{namespace="default"} 4`) {
		t.Fatalf("generation must still publish even when rule count is unrecognized:\n%s", body)
	}
}

func TestSupplyMetricsOnConfigAge(t *testing.T) {
	m := New()
	s := NewSupplyMetrics(m)
	s.OnConfigAge(context.Background(), 90*time.Second)

	body := gatherMetricsBody(t, m)
	if !strings.Contains(body, `xflow_supply_age_seconds{namespace="default"} 90`) {
		t.Fatalf("metrics body missing supply_age_seconds:\n%s", body)
	}
}

func TestSupplyMetricsOnInstanceCountAndRecycled(t *testing.T) {
	m := New()
	s := NewSupplyMetrics(m)
	ctx := context.Background()

	s.OnInstanceCount(ctx, "ready", 4)
	s.OnInstanceCount(ctx, "doomed", 1)
	s.OnInstanceRecycled(ctx, "timeout")
	s.OnInstanceRecycled(ctx, "eval_error")

	body := gatherMetricsBody(t, m)
	for _, want := range []string{
		`xflow_wasm_instance_total{namespace="default",state="ready"} 4`,
		`xflow_wasm_instance_total{namespace="default",state="doomed"} 1`,
		`xflow_wasm_instance_recycled_total{cause="timeout",namespace="default"} 1`,
		`xflow_wasm_instance_recycled_total{cause="eval_error",namespace="default"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics body missing %q:\n%s", want, body)
		}
	}
}

func TestSupplyMetricsOnBorrowWaitAndModuleCompile(t *testing.T) {
	m := New()
	s := NewSupplyMetrics(m)
	ctx := context.Background()

	s.OnBorrowWait(ctx, 2*time.Millisecond)
	s.OnModuleCompile(ctx, "hit")
	s.OnModuleCompile(ctx, "miss")

	body := gatherMetricsBody(t, m)
	for _, want := range []string{
		`xflow_wasm_pool_borrow_wait_seconds_count{namespace="default"} 1`,
		`xflow_wasm_module_compile_total{namespace="default",result="hit"} 1`,
		`xflow_wasm_module_compile_total{namespace="default",result="miss"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics body missing %q:\n%s", want, body)
		}
	}
}

func TestSupplyMetricsOnSupplyFetch(t *testing.T) {
	m := New()
	s := NewSupplyMetrics(m)
	ctx := context.Background()

	s.OnSupplyFetch(ctx, "rules", "ok")
	s.OnSupplyFetch(ctx, "rules", "error")

	body := gatherMetricsBody(t, m)
	for _, want := range []string{
		`xflow_supply_fetch_total{name="rules",namespace="default",result="ok"} 1`,
		`xflow_supply_fetch_total{name="rules",namespace="default",result="error"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics body missing %q:\n%s", want, body)
		}
	}
}

// The gauge must go both ways: set to 1 while declining, and back to 0 once
// admitted. A metric that only ever sets 1 would leave an alert stuck forever
// after the underlying condition clears.
func TestSupplyMetricsOnSupplyNotReadyToggles(t *testing.T) {
	m := New()
	s := NewSupplyMetrics(m)
	ctx := context.Background()

	s.OnSupplyNotReady(ctx, "wf1", "rules", true)
	body := gatherMetricsBody(t, m)
	if !strings.Contains(body, `xflow_supply_not_ready{namespace="default",supply="rules",workflow="wf1"} 1`) {
		t.Fatalf("expected not_ready=1:\n%s", body)
	}

	s.OnSupplyNotReady(ctx, "wf1", "rules", false)
	body = gatherMetricsBody(t, m)
	if !strings.Contains(body, `xflow_supply_not_ready{namespace="default",supply="rules",workflow="wf1"} 0`) {
		t.Fatalf("expected not_ready reset to 0:\n%s", body)
	}
}

// Same both-directions requirement for the serving-unavailable gauge: it must
// clear once content actually arrives.
func TestSupplyMetricsOnSupplyServingUnavailableToggles(t *testing.T) {
	m := New()
	s := NewSupplyMetrics(m)
	ctx := context.Background()

	s.OnSupplyServingUnavailable(ctx, "rules", true)
	body := gatherMetricsBody(t, m)
	if !strings.Contains(body, `xflow_supply_unavailable_serving{name="rules",namespace="default"} 1`) {
		t.Fatalf("expected unavailable_serving=1:\n%s", body)
	}

	s.OnSupplyServingUnavailable(ctx, "rules", false)
	body = gatherMetricsBody(t, m)
	if !strings.Contains(body, `xflow_supply_unavailable_serving{name="rules",namespace="default"} 0`) {
		t.Fatalf("expected unavailable_serving reset to 0:\n%s", body)
	}
}

func TestSupplyMetricsOnConsumerCount(t *testing.T) {
	m := New()
	s := NewSupplyMetrics(m)
	ctx := context.Background()

	s.OnConsumerCount(ctx, "rules", 2)

	body := gatherMetricsBody(t, m)
	if !strings.Contains(body, `xflow_supply_consumers{name="rules",namespace="default"} 2`) {
		t.Fatalf("metrics body missing supply_consumers:\n%s", body)
	}
}
