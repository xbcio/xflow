package metrics

import (
	"context"
	"testing"
)

// TestOnBatchAdmission_ReasonReachesPrometheus asserts the reason label
// survives the trip into the registry as a SEPARATE series, not merely as an
// argument the trigger package passes in.
//
// The two layers can disagree silently: the observer interface could carry
// reason while TriggerMetrics dropped it from the label map, and every test in
// the kafka package would still pass because they assert on the observer, not
// on what /metrics serves. This is the assertion that a dashboard can actually
// break the 599-failures number down.
func TestOnBatchAdmission_ReasonReachesPrometheus(t *testing.T) {
	m := New()
	tm := TriggerMetrics{Metrics: m}
	ctx := context.Background()

	// Two failures that share a state and differ only in reason — the exact
	// pair the flat counter could not separate.
	tm.OnBatchAdmission(ctx, "t", "error", "execute_group")
	tm.OnBatchAdmission(ctx, "t", "error", "seed_transport")
	tm.OnBatchAdmission(ctx, "t", "error", "seed_transport")
	tm.OnBatchAdmission(ctx, "t", "accepted", "none")

	family := gatherMetricFamily(t, m, "xflow_trigger_batch_admissions_total")

	byReason := map[string]float64{}
	for _, metric := range family.GetMetric() {
		if labelValue(metric, "state") != "error" {
			continue
		}
		reason := labelValue(metric, "reason")
		if reason == "" {
			t.Fatal("an error admission carries no reason label; the counter is back " +
				"to saying N batches failed and nothing about which failure it was")
		}
		byReason[reason] += metric.GetCounter().GetValue()
	}

	if len(byReason) != 2 {
		t.Fatalf("error admissions collapsed into %d series (%v); two distinct "+
			"failures must stay distinct in the registry", len(byReason), byReason)
	}
	if byReason["execute_group"] != 1 || byReason["seed_transport"] != 2 {
		t.Errorf("error admissions by reason = %v, want execute_group=1 "+
			"seed_transport=2", byReason)
	}
}

// A batch histogram whose smallest bucket sits above max_size is a counter
// wearing a histogram's clothes: every observation lands in the same bucket and
// the distribution the metric exists to show is gone. This asserts the batch
// size metric resolves WITHIN the configured range rather than merely existing.
func TestOnBatchFlushed_BatchSizeHistogramResolvesSmallCounts(t *testing.T) {
	m := New()
	tm := TriggerMetrics{Metrics: m}

	// Two batches on opposite ends of the default max_size=100 range.
	tm.OnBatchFlushed(context.Background(), "t", "size", 3)
	tm.OnBatchFlushed(context.Background(), "t", "timeout", 90)

	family := gatherMetricFamily(t, m, "xflow_trigger_batch_size")
	buckets := family.GetMetric()[0].GetHistogram().GetBucket()
	if len(buckets) == 0 {
		t.Fatal("no buckets found")
	}
	if got := buckets[0].GetUpperBound(); got > 1 {
		t.Fatalf("smallest bucket upper bound = %v; a single-record batch must be "+
			"distinguishable, so it must be <= 1", got)
	}

	// The two observations must not share a bucket — that is the whole point.
	var smallBucket, largeBucket uint64
	for _, b := range buckets {
		if b.GetUpperBound() >= 3 && smallBucket == 0 {
			smallBucket = b.GetCumulativeCount()
		}
		if b.GetUpperBound() >= 90 && largeBucket == 0 {
			largeBucket = b.GetCumulativeCount()
		}
	}
	if smallBucket != 1 {
		t.Errorf("cumulative count at the bucket covering size=3 is %d, want 1 "+
			"(only the size-3 batch); both observations collapsed into one bucket", smallBucket)
	}
	if largeBucket != 2 {
		t.Errorf("cumulative count at the bucket covering size=90 is %d, want 2", largeBucket)
	}
}

// TestOnBatchFlushed_BatchSizeUsesCountBuckets is the companion assertion to
// TestOnBatchFlushed_BatchSizeHistogramResolvesSmallCounts above: that test
// checks two sizes land in distinguishable buckets but never pins what the
// boundaries themselves are. This pins xflow_trigger_batch_size's bucket
// boundaries to the count family (ObserveCount), not the seconds family
// (Observe): the seconds buckets have fractional boundaries below 1 and top
// out at 10, so every plausible batch size — 1, 2, 3, ... a few hundred —
// would collapse into the same top bucket. That is a counter wearing a
// histogram's clothes: the distribution the metric exists to show is gone.
//
// The expected boundaries are written as literals, not by referencing
// countBuckets: a test that asserts a constant equals itself has no teeth,
// and if countBuckets is ever retuned this test must notice.
func TestOnBatchFlushed_BatchSizeUsesCountBuckets(t *testing.T) {
	m := New()
	tm := TriggerMetrics{Metrics: m}

	tm.OnBatchFlushed(context.Background(), "t", "size", 3)

	family := gatherMetricFamily(t, m, "xflow_trigger_batch_size")
	buckets := family.GetMetric()[0].GetHistogram().GetBucket()

	wantBounds := []float64{1, 2, 5, 10, 25, 50, 75, 100, 250, 500}
	if len(buckets) != len(wantBounds) {
		t.Fatalf("got %d bucket boundaries, want %d (count buckets); "+
			"buckets = %v", len(buckets), len(wantBounds), buckets)
	}
	for i, want := range wantBounds {
		if got := buckets[i].GetUpperBound(); got != want {
			t.Errorf("bucket[%d] upper bound = %v, want %v (count bucket, not "+
				"the seconds histogram family)", i, got, want)
		}
	}

	// The observed value itself (batch size 3) must still land at 3, not at
	// 3 seconds re-scaled into a count bucket — Observe already records
	// value.Seconds() correctly today; only the bucket family is wrong.
	var atThree uint64
	for _, b := range buckets {
		if b.GetUpperBound() >= 3 {
			atThree = b.GetCumulativeCount()
			break
		}
	}
	if atThree != 1 {
		t.Errorf("cumulative count at the bucket covering size=3 is %d, want 1", atThree)
	}
}
