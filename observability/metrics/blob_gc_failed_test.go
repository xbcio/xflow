package metrics

import (
	"strings"
	"testing"
)

// A failed pass adds its total; zero and negative values add nothing and create
// no series, matching OnGC's lazy registration. Mutation: emitting on zero turns
// the first check red; dropping the > 0 guard on negative turns the second red.
func TestBlobMetricsGCFailedAddsPositiveAndSkipsZero(t *testing.T) {
	m := New()
	b := NewBlobMetrics(m)

	b.OnGCFailed(0)
	b.OnGCFailed(-2)
	if got := blobFamilies(t, m, metricBlobGCFailed); len(got) != 0 {
		t.Fatalf("zero or negative failures created the series: %v", got)
	}
	if body := blobMetricsBody(m); strings.Contains(body, metricBlobGCFailed) {
		t.Fatalf("/metrics exports %s before any failure:\n%s", metricBlobGCFailed, body)
	}

	b.OnGCFailed(2)
	b.OnGCFailed(3)
	got := blobCounts(t, m, metricBlobGCFailed, "")
	if len(got) != 1 || got[""] != 5 {
		t.Fatalf("%s = %v, want a single unlabelled series at 5", metricBlobGCFailed, got)
	}
}

// The help-text guard scans this package's consts and would fail on a missing
// entry; this pins the key the guard expects.
func TestBlobMetricsGCFailedHasHelpText(t *testing.T) {
	if _, ok := metricHelp[metricBlobGCFailed]; !ok {
		t.Fatalf("no metricHelp entry for %s", metricBlobGCFailed)
	}
}

// A nil *Metrics is the "metrics disabled" spelling every adapter here accepts.
func TestBlobMetricsGCFailedNilMetricsIsInert(t *testing.T) {
	NewBlobMetrics(nil).OnGCFailed(3)
}
