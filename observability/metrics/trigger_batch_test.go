package metrics

import (
	"context"
	"testing"
)

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
