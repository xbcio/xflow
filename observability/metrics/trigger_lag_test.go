package metrics

import (
	"context"
	"testing"
	"time"
)

// A gauge Set without an identity label reports whichever caller wrote last.
// Lag is per-partition, so one partition catching up would erase a stalled
// partition's reading and the metric would report health during the outage it
// exists to catch. This asserts the two partitions reach the registry as two
// series with their own values, not that OnConsumerLag was merely called.
func TestOnConsumerLag_PartitionsDoNotCollapseIntoOneSeries(t *testing.T) {
	m := New()
	tm := TriggerMetrics{Metrics: m}
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0)

	tm.OnConsumerLag(ctx, "t", 0, 0, now)       // caught up
	tm.OnConsumerLag(ctx, "t", 1, 480_000, now) // stalled
	tm.OnConsumerLag(ctx, "t", 2, 12, now)

	family := gatherMetricFamily(t, m, "xflow_trigger_consumer_lag")

	byPartition := map[string]float64{}
	for _, metric := range family.GetMetric() {
		partition := labelValue(metric, "partition")
		if partition == "" {
			t.Fatal("consumer lag carries no partition label; every partition writes " +
				"the same series and the gauge reports whichever wrote last")
		}
		byPartition[partition] = metric.GetGauge().GetValue()
	}

	if len(byPartition) != 3 {
		t.Fatalf("three partitions produced %d series (%v)", len(byPartition), byPartition)
	}
	if byPartition["1"] != 480_000 {
		t.Errorf("stalled partition reports %v, want 480000 — a caught-up partition "+
			"overwrote it", byPartition["1"])
	}
	if byPartition["0"] != 0 {
		t.Errorf("caught-up partition reports %v, want 0", byPartition["0"])
	}
}

// Lag only advances when a message is fetched, so a consumer that has STOPPED
// fetching holds its last value forever and reads as healthy. The fetch
// timestamp is what makes that freeze detectable, and it is only usable if it
// carries the same label set as the lag it qualifies — otherwise the two cannot
// be joined in a query.
func TestOnConsumerLag_FetchTimestampSharesTheLagLabelSet(t *testing.T) {
	m := New()
	tm := TriggerMetrics{Metrics: m}
	fetchedAt := time.Unix(1_700_000_000, 250_000_000)

	tm.OnConsumerLag(context.Background(), "t", 3, 7, fetchedAt)

	lag := gatherMetricFamily(t, m, "xflow_trigger_consumer_lag")
	stamp := gatherMetricFamily(t, m, "xflow_trigger_last_fetch_timestamp_seconds")

	if len(lag.GetMetric()) != 1 || len(stamp.GetMetric()) != 1 {
		t.Fatalf("want one series each, got lag=%d stamp=%d",
			len(lag.GetMetric()), len(stamp.GetMetric()))
	}
	for _, label := range []string{"topic", "partition", "namespace"} {
		if got, want := labelValue(stamp.GetMetric()[0], label), labelValue(lag.GetMetric()[0], label); got != want {
			t.Errorf("%s label: lag has %q, fetch timestamp has %q; the two series "+
				"cannot be joined, so staleness cannot qualify the lag", label, want, got)
		}
	}

	// Sub-second resolution matters: the alert is on now-minus-this, and a
	// truncated-to-seconds stamp makes a sub-second staleness threshold
	// unrepresentable.
	if got := stamp.GetMetric()[0].GetGauge().GetValue(); got != 1_700_000_000.25 {
		t.Errorf("fetch timestamp = %v, want 1700000000.25", got)
	}
}

// The help text is what an operator reads next to the number. For this pair it
// carries the warning that the lag gauge freezes rather than falling to zero
// when a consumer stops, which is the whole reason the second series exists.
func TestConsumerLagHelpTextsRegistered(t *testing.T) {
	for _, name := range []string{
		"xflow_trigger_consumer_lag",
		"xflow_trigger_last_fetch_timestamp_seconds",
	} {
		if help := metricHelp[name]; help == "" {
			t.Errorf("metricHelp missing %q; it would fall back to the generic description", name)
		}
	}
}
