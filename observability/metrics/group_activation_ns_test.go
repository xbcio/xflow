package metrics

import (
	"sort"
	"strings"
	"testing"

	dto "github.com/prometheus/client_model/go"

	"github.com/xbcio/xflow/namespace"
)

// seriesByLabels indexes a family's series by "namespace[/action]" and returns
// the sorted label-name set of every series, so a test can assert both the
// per-namespace values and that no series carries a different label shape.
func seriesByLabels(t *testing.T, fam *dto.MetricFamily, value func(*dto.Metric) float64) (map[string]float64, []string) {
	t.Helper()
	got := make(map[string]float64, len(fam.GetMetric()))
	shapes := make(map[string]struct{})
	for _, m := range fam.GetMetric() {
		names := make([]string, 0, len(m.GetLabel()))
		for _, l := range m.GetLabel() {
			names = append(names, l.GetName())
		}
		sort.Strings(names)
		shapes[strings.Join(names, ",")] = struct{}{}

		key := labelValue(m, "namespace")
		if action := labelValue(m, "action"); action != "" {
			key += "/" + action
		}
		got[key] = value(m)
	}
	out := make([]string, 0, len(shapes))
	for s := range shapes {
		out = append(out, s)
	}
	sort.Strings(out)
	return got, out
}

func counterValue(m *dto.Metric) float64 { return m.GetCounter().GetValue() }
func gaugeValue(m *dto.Metric) float64   { return m.GetGauge().GetValue() }

// Each family must expose one series per namespace with an identical label
// shape: the registry keys series by (name, sorted label names) and silently
// drops a registration whose label set conflicts with an earlier one, so a
// single emitter that omitted or renamed the label would vanish without an
// error.
// Mutation: removing the "namespace" entry from any of the five emitters in
// group.go collapses that family to one unlabelled series and turns this red.
func TestGroupActivationFamiliesCarryNamespaceLabel(t *testing.T) {
	const a, b namespace.Namespace = "ns-a", "ns-b"

	tests := []struct {
		family    string
		emit      func(g *GroupMetrics)
		value     func(*dto.Metric) float64
		wantShape string
		want      map[string]float64
	}{
		{
			family: "xflow_group_activation_total",
			emit: func(g *GroupMetrics) {
				g.OnGroupActivation(a, "activate")
				g.OnGroupActivation(a, "activate")
				g.OnGroupActivation(a, "deactivate")
				g.OnGroupActivation(b, "activate")
			},
			value:     counterValue,
			wantShape: "action,namespace",
			want:      map[string]float64{"ns-a/activate": 2, "ns-a/deactivate": 1, "ns-b/activate": 1},
		},
		{
			family: "xflow_group_activation_generation_fenced_total",
			emit: func(g *GroupMetrics) {
				g.OnGroupActivationFenced(a)
				g.OnGroupActivationFenced(a)
				g.OnGroupActivationFenced(b)
			},
			value:     counterValue,
			wantShape: "namespace",
			want:      map[string]float64{"ns-a": 2, "ns-b": 1},
		},
		{
			family: "xflow_group_activation_redelivered_total",
			emit: func(g *GroupMetrics) {
				g.OnGroupActivationRedelivered(a, "activate")
				g.OnGroupActivationRedelivered(b, "deactivate")
				g.OnGroupActivationRedelivered(b, "deactivate")
			},
			value:     counterValue,
			wantShape: "action,namespace",
			want:      map[string]float64{"ns-a/activate": 1, "ns-b/deactivate": 2},
		},
		{
			family: "xflow_group_selector_fallback_total",
			emit: func(g *GroupMetrics) {
				g.OnGroupSelectorFallback(b)
				g.OnGroupSelectorFallback(b)
				g.OnGroupSelectorFallback(b)
				g.OnGroupSelectorFallback(a)
			},
			value:     counterValue,
			wantShape: "namespace",
			want:      map[string]float64{"ns-a": 1, "ns-b": 3},
		},
		{
			// An explicit 0 must survive as its own series: it is what tells a
			// drained namespace apart from one that was never written.
			family: "xflow_group_activation_active",
			emit: func(g *GroupMetrics) {
				g.SetGroupActivationActive(a, 3)
				g.SetGroupActivationActive(b, 0)
			},
			value:     gaugeValue,
			wantShape: "namespace",
			want:      map[string]float64{"ns-a": 3, "ns-b": 0},
		},
	}
	for _, tc := range tests {
		t.Run(tc.family, func(t *testing.T) {
			m := New()
			tc.emit(NewGroupMetrics(m))

			got, shapes := seriesByLabels(t, gatherMetricFamily(t, m, tc.family), tc.value)
			if len(shapes) != 1 || shapes[0] != tc.wantShape {
				t.Fatalf("label shapes = %q, want exactly [%q]", shapes, tc.wantShape)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("series = %v, want %v", got, tc.want)
			}
			for k, want := range tc.want {
				if v, ok := got[k]; !ok || v != want {
					t.Fatalf("series %q = %v (present=%v), want %v; all series: %v", k, v, ok, want, got)
				}
			}
		})
	}
}

// A drained namespace keeps exporting its last value unless the reconciler
// rewrites it, so the gauge setter must overwrite rather than accumulate, and
// must not disturb the other namespace's series.
// Mutation: switching Set to Add/Inc in SetGroupActivationActive makes the
// second write accumulate (2 + 0 stays 2, then 5 becomes 7).
func TestGroupActivationActiveGaugeOverwritesPerNamespace(t *testing.T) {
	m := New()
	g := NewGroupMetrics(m)

	g.SetGroupActivationActive("ns-a", 2)
	g.SetGroupActivationActive("ns-b", 5)
	g.SetGroupActivationActive("ns-a", 0)

	got, _ := seriesByLabels(t, gatherMetricFamily(t, m, "xflow_group_activation_active"), gaugeValue)
	if got["ns-a"] != 0 {
		t.Fatalf("ns-a gauge = %v, want 0 after being rewritten", got["ns-a"])
	}
	if got["ns-b"] != 5 {
		t.Fatalf("ns-b gauge = %v, want 5 untouched by ns-a's write", got["ns-b"])
	}
}

// The help text is what an operator reads next to the series; it must say that
// the family is per namespace and how to recover the deployment-wide figure,
// otherwise the label change silently invalidates unlabelled dashboards.
// Mutation: reverting any of the five metricHelp entries to its pre-label
// wording turns this red.
func TestGroupActivationHelpDescribesNamespaceLabel(t *testing.T) {
	for _, family := range []string{
		"xflow_group_activation_total",
		"xflow_group_activation_generation_fenced_total",
		"xflow_group_activation_redelivered_total",
		"xflow_group_activation_active",
		"xflow_group_selector_fallback_total",
	} {
		help, ok := metricHelp[family]
		if !ok {
			t.Fatalf("no metricHelp entry for %s", family)
		}
		if !strings.Contains(help, "namespace") {
			t.Fatalf("%s help does not mention the namespace label: %q", family, help)
		}
		if !strings.Contains(help, "sum without(namespace)") {
			t.Fatalf("%s help does not give the deployment-wide aggregation: %q", family, help)
		}
	}
	if help := metricHelp["xflow_group_activation_active"]; !strings.Contains(help, "max by (namespace)") {
		t.Fatalf("xflow_group_activation_active help lacks the failover guidance: %q", help)
	}
}
