package metrics

import "testing"

// A batch-size histogram whose buckets are the seconds family (fractional
// boundaries below 1, capped at 10) puts every plausible batch size — 1, 2,
// 3, ... a few hundred — in the same top bucket. That is a counter wearing a
// histogram's clothes: the distribution the metric exists to show is gone.
// This pins the bucket boundaries to the count family instead, the same way
// TestOnBatchFlushed_BatchSizeHistogramResolvesSmallCounts pins
// xflow_trigger_batch_size.
//
// The expected boundaries are written as literals, not by referencing
// countBuckets: a test that asserts a constant equals itself has no teeth,
// and if countBuckets is ever retuned this test must notice.
func TestOnGroupEmitBatchSizeUsesCountBuckets(t *testing.T) {
	m := New()
	gm := NewGroupMetrics(m)

	gm.OnGroupEmitBatchSize(3)

	family := gatherMetricFamily(t, m, "xflow_group_emit_batch_size")
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
