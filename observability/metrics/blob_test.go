package metrics

import (
	"context"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	dto "github.com/prometheus/client_model/go"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
)

// blobFamilies gathers the registry and returns the families of the given names
// that exist. Gathering is the only view that also shows which series exist, so a
// test can tell "counted zero" from "never created".
func blobFamilies(t *testing.T, m *Metrics, names ...string) map[string]*dto.MetricFamily {
	t.Helper()
	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	want := map[string]bool{}
	for _, name := range names {
		want[name] = true
	}
	out := map[string]*dto.MetricFamily{}
	for _, family := range families {
		if want[family.GetName()] {
			out[family.GetName()] = family
		}
	}
	return out
}

// blobCounts reads a counter family back as label value -> count, keyed by the
// given label ("" for an unlabelled family). It fails when the family is not a
// counter or a series carries other labels than the one expected.
func blobCounts(t *testing.T, m *Metrics, name, label string) map[string]float64 {
	t.Helper()
	family, ok := blobFamilies(t, m, name)[name]
	out := map[string]float64{}
	if !ok {
		return out
	}
	if family.GetType() != dto.MetricType_COUNTER {
		t.Fatalf("%s type = %s, want COUNTER", name, family.GetType())
	}
	wantLabels := 0
	if label != "" {
		wantLabels = 1
	}
	for _, metric := range family.GetMetric() {
		if len(metric.GetLabel()) != wantLabels {
			t.Errorf("%s series has labels %v, want %d", name, metric.GetLabel(), wantLabels)
		}
		out[labelValue(metric, label)] = metric.GetCounter().GetValue()
	}
	return out
}

// blobMetricsBody renders /metrics the way a scrape sees it.
func blobMetricsBody(m *Metrics) string {
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	return rec.Body.String()
}

var blobSeriesNames = []string{
	metricBlobPut,
	metricBlobGet,
	metricBlobGCDeleted,
	metricBlobGCSkippedGeneration,
	metricOutboxDecodeFailures,
}

// Constructing the adapter, and observing nothing, must leave /metrics free of
// every blob series and of the decode-failure counter: a deployment without blob
// storage must look exactly as it did before. Mutation: registering any of them
// at construction turns this red.
func TestBlobMetricsLazyRegistration(t *testing.T) {
	m := New()
	_ = NewBlobMetrics(m)
	_ = NewOutboxMetrics(m)

	if got := blobFamilies(t, m, blobSeriesNames...); len(got) != 0 {
		t.Fatalf("series exist before any observation: %v", got)
	}
	body := blobMetricsBody(m)
	for _, name := range blobSeriesNames {
		if strings.Contains(body, name) {
			t.Errorf("/metrics exports %s before any observation:\n%s", name, body)
		}
	}
}

// Each result is counted under its own label value, and the two counters do not
// share series. Mutation: swapping the metric names, or counting every call under
// one label, turns the values red.
func TestBlobMetricsCountsPutAndGetPerResult(t *testing.T) {
	m := New()
	b := NewBlobMetrics(m)

	for range 3 {
		b.OnPut("ok")
	}
	b.OnPut("too_large")
	b.OnPut("error")
	b.OnPut("error")
	b.OnGet("ok")
	b.OnGet("missing")
	b.OnGet("corrupt")
	b.OnGet("namespace")
	b.OnGet("error")
	b.OnGet("error")

	wantPut := map[string]float64{"ok": 3, "too_large": 1, "error": 2}
	wantGet := map[string]float64{"ok": 1, "missing": 1, "corrupt": 1, "namespace": 1, "error": 2}
	for name, want := range map[string]map[string]float64{metricBlobPut: wantPut, metricBlobGet: wantGet} {
		got := blobCounts(t, m, name, "result")
		if len(got) != len(want) {
			t.Fatalf("%s series = %v, want %v", name, got, want)
		}
		for result, n := range want {
			if got[result] != n {
				t.Errorf("%s result %q = %v, want %v (all: %v)", name, result, got[result], n, got)
			}
		}
	}
}

// The result labels' cardinality is fixed by the tables, whatever the caller
// sends: anything outside them collapses into one "unknown" series, so a future
// or hostile value cannot mint new ones. Mutation: using the raw result as the
// label turns the series count red.
func TestBlobMetricsUnknownResultCollapses(t *testing.T) {
	m := New()
	b := NewBlobMetrics(m)

	for _, result := range []string{"", "OK", "Too_Large", "open /var/blobs/x: permission denied", "x"} {
		b.OnPut(result)
		b.OnGet(result)
	}
	for i := range 200 {
		b.OnPut(strings.Repeat("p", i+1))
		b.OnGet(strings.Repeat("g", i+1))
	}

	for _, name := range []string{metricBlobPut, metricBlobGet} {
		got := blobCounts(t, m, name, "result")
		if len(got) != 1 || got[BlobResultUnknown] != 205 {
			t.Errorf("%s series = %v, want a single unknown series at 205", name, got)
		}
	}
	// The exported lists are the recognised results; unknown is not among them,
	// and every recognised result maps to itself rather than to unknown.
	for _, result := range BlobPutResults() {
		if result == BlobResultUnknown {
			t.Errorf("BlobPutResults() lists %q", result)
		}
	}
	for _, result := range BlobGetResults() {
		if result == BlobResultUnknown {
			t.Errorf("BlobGetResults() lists %q", result)
		}
	}
	if len(BlobPutResults()) != 3 || len(BlobGetResults()) != 5 {
		t.Errorf("BlobPutResults() = %v, BlobGetResults() = %v, want 3 and 5 results", BlobPutResults(), BlobGetResults())
	}
}

// Every result the adapter lists must be counted under its own name, not folded
// into unknown. Mutation: dropping a result from the lookup table turns this red.
func TestBlobMetricsEveryListedResultIsRecognised(t *testing.T) {
	m := New()
	b := NewBlobMetrics(m)
	for _, result := range BlobPutResults() {
		b.OnPut(result)
	}
	for _, result := range BlobGetResults() {
		b.OnGet(result)
	}

	for name, results := range map[string][]string{metricBlobPut: BlobPutResults(), metricBlobGet: BlobGetResults()} {
		got := blobCounts(t, m, name, "result")
		if len(got) != len(results) {
			t.Errorf("%s series = %v, want one per %v", name, got, results)
		}
		for _, result := range results {
			if got[result] != 1 {
				t.Errorf("%s result %q = %v, want 1 (all: %v)", name, result, got[result], got)
			}
		}
		if _, ok := got[BlobResultUnknown]; ok {
			t.Errorf("%s has an unknown series though every result was a listed one: %v", name, got)
		}
	}
}

// A collector pass adds its totals; a zero adds nothing and creates no series.
// Mutation: emitting on zero turns the lazy checks red; replacing Add by Inc
// turns the totals red.
func TestBlobMetricsGCAddsTotalsAndSkipsZero(t *testing.T) {
	m := New()
	b := NewBlobMetrics(m)

	b.OnGC(0, 0)
	if got := blobFamilies(t, m, blobSeriesNames...); len(got) != 0 {
		t.Fatalf("a pass that did nothing created series: %v", got)
	}
	if body := blobMetricsBody(m); strings.Contains(body, "xflow_blob_gc") {
		t.Fatalf("/metrics exports a gc series after an empty pass:\n%s", body)
	}

	b.OnGC(5, 0)
	if got := blobFamilies(t, m, metricBlobGCSkippedGeneration); len(got) != 0 {
		t.Fatalf("a pass with nothing fenced created the skipped series: %v", got)
	}
	b.OnGC(0, 2)
	b.OnGC(7, 3)

	if got := blobCounts(t, m, metricBlobGCDeleted, ""); len(got) != 1 || got[""] != 12 {
		t.Errorf("%s = %v, want a single unlabelled series at 12", metricBlobGCDeleted, got)
	}
	if got := blobCounts(t, m, metricBlobGCSkippedGeneration, ""); len(got) != 1 || got[""] != 5 {
		t.Errorf("%s = %v, want a single unlabelled series at 5", metricBlobGCSkippedGeneration, got)
	}
}

// A negative count is not a count of anything; it must neither panic the
// counter nor move it.
func TestBlobMetricsGCIgnoresNegative(t *testing.T) {
	m := New()
	b := NewBlobMetrics(m)
	b.OnGC(-1, -4)
	if got := blobFamilies(t, m, blobSeriesNames...); len(got) != 0 {
		t.Fatalf("negative counts created series: %v", got)
	}
}

// A nil *Metrics is the "metrics disabled" spelling every adapter here accepts.
func TestBlobMetricsNilMetricsIsInert(t *testing.T) {
	b := NewBlobMetrics(nil)
	b.OnPut("ok")
	b.OnGet("missing")
	b.OnGC(1, 1)
}

// The adapter is shared by every request goroutine and the collector loop, so
// concurrent use must neither race nor lose counts. Run with -race.
func TestBlobMetricsConcurrentUse(t *testing.T) {
	m := New()
	b := NewBlobMetrics(m)
	const workers, each = 8, 250

	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range each {
				b.OnPut("ok")
				b.OnGet("ok")
				b.OnGC(1, 1)
			}
		}()
	}
	wg.Wait()

	const total = workers * each
	if got := blobCounts(t, m, metricBlobPut, "result"); got["ok"] != total {
		t.Errorf("put ok = %v, want %d", got["ok"], total)
	}
	if got := blobCounts(t, m, metricBlobGet, "result"); got["ok"] != total {
		t.Errorf("get ok = %v, want %d", got["ok"], total)
	}
	if got := blobCounts(t, m, metricBlobGCDeleted, ""); got[""] != total {
		t.Errorf("gc deleted = %v, want %d", got[""], total)
	}
	if got := blobCounts(t, m, metricBlobGCSkippedGeneration, ""); got[""] != total {
		t.Errorf("gc skipped = %v, want %d", got[""], total)
	}
}

// decodeFailureCount reads xflow_outbox_metrics_decode_failures_total back as
// namespace -> count, so a test can tell "counted zero" from "never created".
func decodeFailureCount(t *testing.T, m *Metrics) map[string]float64 {
	t.Helper()
	return blobCounts(t, m, metricOutboxDecodeFailures, "namespace")
}

func backlog(failures int) engine.OutboxMetricsSnapshot {
	return engine.OutboxMetricsSnapshot{Pending: 10, Ready: 1, DecodeFailures: failures}
}

// A backlog scan that saw no undecodable entry registers nothing, however many
// times it runs. Mutation: emitting on zero (or registering at construction)
// turns this red.
func TestOutboxDecodeFailuresZeroDoesNotRegister(t *testing.T) {
	m := New()
	ctx := context.Background()
	outbox := NewOutboxMetrics(m)

	for range 5 {
		outbox.OnOutboxBacklog(ctx, backlog(0))
	}

	if got := blobFamilies(t, m, metricOutboxDecodeFailures); len(got) != 0 {
		t.Fatalf("%s exists though no scan saw a failure: %v", metricOutboxDecodeFailures, got)
	}
	if body := blobMetricsBody(m); strings.Contains(body, metricOutboxDecodeFailures) {
		t.Fatalf("/metrics exports %s though no scan saw a failure:\n%s", metricOutboxDecodeFailures, body)
	}
	// The ready gauge still comes through: the new counter must not displace it.
	if got := gatherMetricFamily(t, m, metricOutboxReady).GetMetric()[0].GetGauge().GetValue(); got != 1 {
		t.Errorf("%s = %v, want 1", metricOutboxReady, got)
	}
}

// The same undecodable entry is met again by every scan, so the counter must not
// grow while the reading stays put. Mutation: adding each reading to the counter
// (the sum of readings) turns the steady-state value red: 3 scans of 3 entries
// would read 9.
func TestOutboxDecodeFailuresCountsGrowthNotRepeatedReadings(t *testing.T) {
	m := New()
	ctx := context.Background()
	outbox := NewOutboxMetrics(m)

	outbox.OnOutboxBacklog(ctx, backlog(3))
	outbox.OnOutboxBacklog(ctx, backlog(3))
	outbox.OnOutboxBacklog(ctx, backlog(3))
	if got := decodeFailureCount(t, m)["default"]; got != 3 {
		t.Fatalf("after three scans of the same 3 bad entries counter = %v, want 3", got)
	}

	outbox.OnOutboxBacklog(ctx, backlog(5)) // two new bad entries appeared
	outbox.OnOutboxBacklog(ctx, backlog(5))
	if got := decodeFailureCount(t, m)["default"]; got != 5 {
		t.Fatalf("after growth to 5 counter = %v, want 5", got)
	}
}

// A reading that falls lowers the baseline without moving the counter, and a
// later rise counts again from the lower baseline. Mutation: never lowering the
// baseline leaves the new entries uncounted (counter stays 5); resetting the
// baseline only when the reading hits zero leaves 5 as well.
func TestOutboxDecodeFailuresFallThenRiseCountsAgain(t *testing.T) {
	m := New()
	ctx := context.Background()
	outbox := NewOutboxMetrics(m)

	outbox.OnOutboxBacklog(ctx, backlog(5))
	outbox.OnOutboxBacklog(ctx, backlog(2)) // three bad entries were drained
	if got := decodeFailureCount(t, m)["default"]; got != 5 {
		t.Fatalf("a falling reading moved the counter to %v, want it to stay 5", got)
	}
	outbox.OnOutboxBacklog(ctx, backlog(4)) // two new ones since the low point
	if got := decodeFailureCount(t, m)["default"]; got != 7 {
		t.Fatalf("a rise from 2 to 4 left the counter at %v, want 7", got)
	}

	outbox.OnOutboxBacklog(ctx, backlog(0)) // all gone
	outbox.OnOutboxBacklog(ctx, backlog(1))
	if got := decodeFailureCount(t, m)["default"]; got != 8 {
		t.Fatalf("a rise from 0 to 1 left the counter at %v, want 8", got)
	}
}

// The baseline is per namespace: two namespaces with the same reading are two
// problems, and one namespace's drain must not mask the other's growth.
// Mutation: a single shared baseline turns the per-namespace values red.
func TestOutboxDecodeFailuresBaselineIsPerNamespace(t *testing.T) {
	m := New()
	outbox := NewOutboxMetrics(m)
	alpha := namespace.WithNamespace(context.Background(), namespace.Namespace("alpha"))
	beta := namespace.WithNamespace(context.Background(), namespace.Namespace("beta"))

	outbox.OnOutboxBacklog(alpha, backlog(2))
	outbox.OnOutboxBacklog(beta, backlog(2))
	outbox.OnOutboxBacklog(alpha, backlog(2))
	outbox.OnOutboxBacklog(beta, backlog(3))

	got := decodeFailureCount(t, m)
	if len(got) != 2 || got["alpha"] != 2 || got["beta"] != 3 {
		t.Fatalf("counter by namespace = %v, want alpha=2 beta=3", got)
	}
}

// Copies of the observer, and two observers built over the same Metrics, share
// one baseline: the value type is copied freely by its holders. Different
// Metrics do not share it. Mutation: keeping the baseline on the observer value
// turns the shared case red.
func TestOutboxDecodeFailuresBaselineFollowsMetricsNotTheObserver(t *testing.T) {
	ctx := context.Background()
	m := New()
	first, second := NewOutboxMetrics(m), NewOutboxMetrics(m)
	first.OnOutboxBacklog(ctx, backlog(3))
	second.OnOutboxBacklog(ctx, backlog(3))
	if got := decodeFailureCount(t, m)["default"]; got != 3 {
		t.Fatalf("two observers over one Metrics counted %v, want 3", got)
	}

	other := New()
	NewOutboxMetrics(other).OnOutboxBacklog(ctx, backlog(3))
	if got := decodeFailureCount(t, other)["default"]; got != 3 {
		t.Fatalf("a second Metrics counted %v, want 3 (it must not inherit the first's baseline)", got)
	}
}

// A nil *Metrics is the "metrics disabled" spelling every adapter accepts, and a
// negative reading (a store reporting nonsense) must not move the counter.
func TestOutboxDecodeFailuresNilAndNegativeAreInert(t *testing.T) {
	ctx := context.Background()
	NewOutboxMetrics(nil).OnOutboxBacklog(ctx, backlog(2))

	m := New()
	outbox := NewOutboxMetrics(m)
	outbox.OnOutboxBacklog(ctx, backlog(-3))
	if got := blobFamilies(t, m, metricOutboxDecodeFailures); len(got) != 0 {
		t.Fatalf("a negative reading created the counter: %v", got)
	}
}

// Backlog scans run on the dispatcher's goroutine, but several dispatchers can
// share one Metrics; the baseline must hold up under that. Run with -race.
func TestOutboxDecodeFailuresConcurrentScans(t *testing.T) {
	m := New()
	ctx := context.Background()
	outbox := NewOutboxMetrics(m)

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				outbox.OnOutboxBacklog(ctx, backlog(4))
			}
		}()
	}
	wg.Wait()

	if got := decodeFailureCount(t, m)["default"]; got != 4 {
		t.Fatalf("concurrent scans of the same 4 bad entries counted %v, want 4", got)
	}
}

// The help text is what tells an operator which results to alert on, so it has
// to name every one of them and say the series only appear after a first
// observation. Mutation: dropping a result or the registration note from the
// text turns this red.
func TestBlobHelpNamesEveryResultAndLaziness(t *testing.T) {
	for name, results := range map[string][]string{metricBlobPut: BlobPutResults(), metricBlobGet: BlobGetResults()} {
		help, ok := metricHelp[name]
		if !ok {
			t.Fatalf("no metricHelp entry for %s", name)
		}
		for _, result := range append(results, BlobResultUnknown) {
			if !strings.Contains(help, result) {
				t.Errorf("%s help does not mention result %q", name, result)
			}
		}
		if !strings.Contains(help, "exports no series") {
			t.Errorf("%s help does not say it only appears after a first observation", name)
		}
	}
	for _, name := range []string{metricBlobGCDeleted, metricBlobGCSkippedGeneration, metricOutboxDecodeFailures} {
		if _, ok := metricHelp[name]; !ok {
			t.Errorf("no metricHelp entry for %s", name)
		}
	}
}
