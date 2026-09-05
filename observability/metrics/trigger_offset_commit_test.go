package metrics

import (
	"context"
	"testing"
	"time"

	kafkatrigger "github.com/xbcio/xflow/node/trigger/kafka"
)

// TestTriggerMetrics_OffsetCommitFansOutToBothSeries pins that one
// OnOffsetCommit call lands one duration sample and one size sample, and that
// the size series really is a COUNT histogram.
//
// The bucket assertion is the part that is not decoration. Both ObserveCount
// and Observe would produce a series named xflow_trigger_offset_commit_size
// carrying one sample with a sum of 86, so a test that checks only the name and
// the sum passes against either. They differ in where the sample lands:
// Observe's default buckets top out at 10 seconds' worth of float, so every
// realistic offset count would fall in +Inf and the distribution — the whole
// point of a histogram here — would be empty. Asserting that 86 is counted
// under an upper bound of 100 is what tells the two apart.
func TestTriggerMetrics_OffsetCommitFansOutToBothSeries(t *testing.T) {
	m := New()
	obs := NewTriggerMetrics(m)

	obs.OnOffsetCommit(context.Background(), "orders", "ok", 86, 1200*time.Millisecond)

	dur := gatherMetricFamily(t, m, "xflow_trigger_offset_commit_duration_seconds")
	if len(dur.GetMetric()) != 1 {
		t.Fatalf("duration series count = %d, want 1", len(dur.GetMetric()))
	}
	if got := labelValue(dur.GetMetric()[0], "topic"); got != "orders" {
		t.Errorf("duration topic label = %q, want %q", got, "orders")
	}
	if got := labelValue(dur.GetMetric()[0], "result"); got != "ok" {
		t.Errorf("duration result label = %q, want %q", got, "ok")
	}
	if got := dur.GetMetric()[0].GetHistogram().GetSampleSum(); got != 1.2 {
		t.Errorf("duration sum = %v, want 1.2 (seconds, not nanoseconds or millis)", got)
	}

	size := gatherMetricFamily(t, m, "xflow_trigger_offset_commit_size")
	if len(size.GetMetric()) != 1 {
		t.Fatalf("size series count = %d, want 1", len(size.GetMetric()))
	}
	if got := size.GetMetric()[0].GetHistogram().GetSampleSum(); got != 86 {
		t.Errorf("size sum = %v, want 86", got)
	}
	var counted bool
	for _, bucket := range size.GetMetric()[0].GetHistogram().GetBucket() {
		if bucket.GetUpperBound() == 100 {
			counted = bucket.GetCumulativeCount() == 1
		}
	}
	if !counted {
		t.Errorf("size sample of 86 is not counted under the le=100 bucket; buckets = %v, "+
			"which means this is not a count histogram and every realistic offset "+
			"count would land in +Inf", size.GetMetric()[0].GetHistogram().GetBucket())
	}
}

// TestTriggerMetrics_OffsetCommitResultSplitsDurationOnly pins the asymmetry
// between the two series: result partitions the duration and deliberately does
// not partition the size.
//
// It is asserted rather than left to the reader because the asymmetry looks
// like an oversight. An errored commit's duration is a different population —
// a timeout at aggregateCommitTimeout would otherwise drag the success
// distribution's tail with it — whereas an errored commit's size is just the
// size of the batch that gets retried, so splitting it would halve the sample
// count of the distribution for no gain. Without this test, "unify the labels"
// reads as a tidy-up.
func TestTriggerMetrics_OffsetCommitResultSplitsDurationOnly(t *testing.T) {
	m := New()
	obs := NewTriggerMetrics(m)

	obs.OnOffsetCommit(context.Background(), "orders", "ok", 10, 50*time.Millisecond)
	obs.OnOffsetCommit(context.Background(), "orders", "error", 10, 15*time.Second)

	dur := gatherMetricFamily(t, m, "xflow_trigger_offset_commit_duration_seconds")
	if len(dur.GetMetric()) != 2 {
		t.Fatalf("duration series count = %d, want 2 (one per result)", len(dur.GetMetric()))
	}

	size := gatherMetricFamily(t, m, "xflow_trigger_offset_commit_size")
	if len(size.GetMetric()) != 1 {
		t.Fatalf("size series count = %d, want 1 (result must not partition size)", len(size.GetMetric()))
	}
	if got := size.GetMetric()[0].GetHistogram().GetSampleCount(); got != 2 {
		t.Errorf("size sample count = %d, want 2; both commits belong to one distribution", got)
	}
}

// TestTriggerMetrics_OffsetCommitIsWiredToTheTriggerObserver pins that the
// adapter satisfies the interface the kafka trigger actually calls, not merely
// that a method of that name exists on TriggerMetrics.
//
// The var _ assertion in trigger.go covers the compile-time half. This covers
// the half a compiler cannot: that a call arriving through the interface
// reaches this implementation rather than noopObserver's, which is the shape
// every one of these observations takes in production.
func TestTriggerMetrics_OffsetCommitIsWiredToTheTriggerObserver(t *testing.T) {
	m := New()
	var obs kafkatrigger.Observer = NewTriggerMetrics(m)

	obs.OnOffsetCommit(context.Background(), "orders", "ok", 4, time.Second)

	dur := gatherMetricFamily(t, m, "xflow_trigger_offset_commit_duration_seconds")
	if got := dur.GetMetric()[0].GetHistogram().GetSampleCount(); got != 1 {
		t.Errorf("duration sample count through the interface = %d, want 1", got)
	}
}
