package metrics

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/namespace"
)

// The namespace and cause labels on this counter are string literals chosen in
// this package, and the two failure shapes they separate are not cosmetic:
//
//	OnSupplyHintReadError(ctx, ns, "decrypt") -> an at-rest key is missing from
//	    this replica's keyring (or the envelope is damaged)
//	OnSupplyHintReadError(ctx, ns, "other")   -> everything else, including a
//	    content-hash mismatch, which has no sentinel to be told apart by
//
// service/control's tests assert on the observer, not on the registry, so if the
// mapping here were wrong — both causes collapsed into one label, or the
// namespace dropped — nothing downstream would notice. The series would simply
// say "supply reads are failing" with no way to tell a keyring problem from a
// corrupt row, which is the distinction the metric exists to make.
func TestSupplyHintReadErrorsSeparateCauseAndNamespace(t *testing.T) {
	m := New()
	s := NewSupplyHintMetrics(m)
	ctx := context.Background()

	s.OnSupplyHintReadError(ctx, "team-a", "decrypt")
	s.OnSupplyHintReadError(ctx, "team-a", "decrypt")
	s.OnSupplyHintReadError(ctx, "team-b", "other")

	type key struct{ ns, cause string }
	byKey := map[key]float64{}
	for _, metric := range gatherMetricFamily(t, m, "xflow_supply_hint_read_errors_total").GetMetric() {
		byKey[key{labelValue(metric, "namespace"), labelValue(metric, "cause")}] += metric.GetCounter().GetValue()
	}

	want := map[key]float64{
		{"team-a", "decrypt"}: 2,
		{"team-b", "other"}:   1,
	}
	if len(byKey) != len(want) {
		t.Fatalf("series = %v, want exactly %v", byKey, want)
	}
	for k, n := range want {
		if byKey[k] != n {
			t.Errorf("namespace=%q cause=%q counted %v, want %v (all series: %v)", k.ns, k.cause, byKey[k], n, byKey)
		}
	}
}

// The namespace label must be the one the caller passed, NOT the one on ctx.
//
// HintsForRunner iterates its own namespace list and issues a read per
// namespace, so the failing namespace is not necessarily the heartbeat's scope.
// Every other method in control.go labels from ctx via withNamespace, and
// copying that habit here would silently attribute every failure to whatever
// the heartbeat happened to carry — typically nothing at all. That is worse than
// no label: an operator would go look at the wrong tenant.
//
// The ctx here carries a DIFFERENT, real namespace (via namespace.WithNamespace,
// the same mechanism withNamespace reads), so this test fails if the label ever
// starts coming from ctx. A ctx that merely lacked a namespace would pass either
// way and lock nothing in.
func TestSupplyHintReadErrorUsesTheArgumentNamespaceNotTheContextOne(t *testing.T) {
	m := New()
	s := NewSupplyHintMetrics(m)

	ctx := namespace.WithNamespace(context.Background(), "heartbeat-scope-tenant")

	s.OnSupplyHintReadError(ctx, "real-tenant", "decrypt")

	family := gatherMetricFamily(t, m, "xflow_supply_hint_read_errors_total")
	if len(family.GetMetric()) != 1 {
		t.Fatalf("series = %d, want exactly 1", len(family.GetMetric()))
	}
	if got := labelValue(family.GetMetric()[0], "namespace"); got != "real-tenant" {
		t.Errorf("namespace label = %q, want %q: the label must come from the "+
			"argument, not from ctx", got, "real-tenant")
	}
}
