package metrics

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/xbcio/xflow/namespace"
)

// adapterSeries indexes a family's series by "namespace[/other label values]"
// and returns the distinct label-name shapes, so a test can assert the per
// namespace values and that every series of a family has the same label set.
func adapterSeries(fam *dto.MetricFamily, value func(*dto.Metric) float64) (map[string]float64, []string) {
	got := make(map[string]float64, len(fam.GetMetric()))
	shapes := make(map[string]struct{})
	for _, m := range fam.GetMetric() {
		names := make([]string, 0, len(m.GetLabel()))
		var others []string
		for _, l := range m.GetLabel() {
			names = append(names, l.GetName())
			if l.GetName() != "namespace" {
				others = append(others, l.GetValue())
			}
		}
		sort.Strings(names)
		sort.Strings(others)
		shapes[strings.Join(names, ",")] = struct{}{}

		key := labelValue(m, "namespace")
		if len(others) > 0 {
			key += "/" + strings.Join(others, "/")
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

func histogramCount(m *dto.Metric) float64 { return float64(m.GetHistogram().GetSampleCount()) }

// Every hook of the adapter must report under the namespace its ctx carries.
// The counts differ per namespace so a swapped or collapsed bucket cannot hide
// behind symmetric totals, and a ctx without a namespace must land on "default"
// rather than on an empty label value.
// Mutation: replacing namespace.FromContext(ctx) with namespace.Default in any
// adapter method moves that hook's series to "default"; dropping the namespace
// argument from a GroupMetrics call in the adapter collapses its family.
func TestGroupObserverAdapterLabelsEveryHookByCtxNamespace(t *testing.T) {
	a := namespace.WithNamespace(context.Background(), "ns-a")
	b := namespace.WithNamespace(context.Background(), "ns-b")
	bare := context.Background()
	d := 5 * time.Millisecond

	tests := []struct {
		family    string
		emit      func(o *GroupObserverAdapter)
		value     func(*dto.Metric) float64
		wantShape string
		want      map[string]float64
	}{
		{
			family: "xflow_group_lease_acquired_total",
			emit: func(o *GroupObserverAdapter) {
				o.OnGroupLeaseAcquired(a)
				o.OnGroupLeaseAcquired(a)
				o.OnGroupLeaseAcquired(b)
				o.OnGroupLeaseAcquired(bare)
			},
			value: counterValue, wantShape: "namespace",
			want: map[string]float64{"ns-a": 2, "ns-b": 1, "default": 1},
		},
		{
			family: "xflow_group_lease_expired_total",
			emit: func(o *GroupObserverAdapter) {
				o.OnGroupLeaseExpired(b)
				o.OnGroupLeaseExpired(b)
				o.OnGroupLeaseExpired(a)
			},
			value: counterValue, wantShape: "namespace",
			want: map[string]float64{"ns-a": 1, "ns-b": 2},
		},
		{
			family: "xflow_group_lease_renew_total",
			emit: func(o *GroupObserverAdapter) {
				o.OnGroupLeaseRenew(a, "ok", d)
				o.OnGroupLeaseRenew(a, "not_renewed", d)
				o.OnGroupLeaseRenew(b, "ok", d)
				o.OnGroupLeaseRenew(b, "ok", d)
			},
			value: counterValue, wantShape: "namespace,result",
			want: map[string]float64{"ns-a/ok": 1, "ns-a/not_renewed": 1, "ns-b/ok": 2},
		},
		{
			family: "xflow_group_lease_renew_duration_seconds",
			emit: func(o *GroupObserverAdapter) {
				o.OnGroupLeaseRenew(a, "ok", d)
				o.OnGroupLeaseRenew(b, "ok", d)
				o.OnGroupLeaseRenew(b, "error", d)
			},
			value: histogramCount, wantShape: "namespace",
			want: map[string]float64{"ns-a": 1, "ns-b": 2},
		},
		{
			family: "xflow_group_commit_total",
			emit: func(o *GroupObserverAdapter) {
				o.OnGroupCommit(a, "success", d)
				o.OnGroupCommit(b, "success", d)
				o.OnGroupCommit(b, "failed_fatal", d)
			},
			value: counterValue, wantShape: "namespace,outcome",
			want: map[string]float64{"ns-a/success": 1, "ns-b/success": 1, "ns-b/failed_fatal": 1},
		},
		{
			family: "xflow_group_exec_duration_seconds",
			emit: func(o *GroupObserverAdapter) {
				o.OnGroupCommit(a, "success", d)
				o.OnGroupCommit(a, "failed_tolerated", d)
				o.OnGroupCommit(b, "success", d)
			},
			value: histogramCount, wantShape: "namespace",
			want: map[string]float64{"ns-a": 2, "ns-b": 1},
		},
		{
			family: "xflow_group_admission_total",
			emit: func(o *GroupObserverAdapter) {
				o.OnGroupAdmission(a, "accepted", d)
				o.OnGroupAdmission(b, "accepted", d)
				o.OnGroupAdmission(b, "duplicate", d)
			},
			value: counterValue, wantShape: "namespace,outcome",
			want: map[string]float64{"ns-a/accepted": 1, "ns-b/accepted": 1, "ns-b/duplicate": 1},
		},
		{
			family: "xflow_group_admission_duration_seconds",
			emit: func(o *GroupObserverAdapter) {
				o.OnGroupAdmission(b, "accepted", d)
				o.OnGroupAdmission(b, "conflict", d)
				o.OnGroupAdmission(a, "error", d)
			},
			value: histogramCount, wantShape: "namespace",
			want: map[string]float64{"ns-a": 1, "ns-b": 2},
		},
	}
	for _, tc := range tests {
		t.Run(tc.family, func(t *testing.T) {
			m := New()
			tc.emit(NewGroupObserver(m))

			got, shapes := adapterSeries(gatherMetricFamily(t, m, tc.family), tc.value)
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

// Mutation: dropping the sentence from any of the eight entries turns this red.
func TestGroupLeaseCommitAdmissionHelpDescribesNamespaceLabel(t *testing.T) {
	for _, family := range []string{
		"xflow_group_lease_acquired_total",
		"xflow_group_lease_expired_total",
		"xflow_group_lease_renew_total",
		"xflow_group_lease_renew_duration_seconds",
		"xflow_group_commit_total",
		"xflow_group_exec_duration_seconds",
		"xflow_group_admission_total",
		"xflow_group_admission_duration_seconds",
	} {
		help, ok := metricHelp[family]
		if !ok {
			t.Fatalf("no metricHelp entry for %s", family)
		}
		if !strings.Contains(help, "namespace label") || !strings.Contains(help, "sum without(namespace)") {
			t.Fatalf("%s help does not describe the namespace label and its aggregation: %q", family, help)
		}
	}
}
