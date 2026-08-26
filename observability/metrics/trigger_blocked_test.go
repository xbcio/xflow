package metrics

import (
	"context"
	"testing"
)

// Four of TriggerMetrics' seven observer methods never reached this package's
// tests. The kafka trigger package tests them, but it tests them against the
// OBSERVER — a recording stub — so it proves the trigger calls the method and
// says nothing about what /metrics then serves. Between the two layers sits the
// label map built here, and a label map is exactly where a value gets dropped,
// mislabelled, or hard-coded without any caller noticing.
//
// OnConsumptionBlocked is the worst of the four to get wrong. It is the alarm
// for on_overflow=block, where a partition stops consuming instead of dropping
// records: nothing is lost, but nothing moves either, and consumer-group lag
// does NOT reveal it, because a consumer that stopped fetching stops advancing
// its lag reading too. This gauge is the only signal that distinguishes "blocked"
// from "idle", and inverting the one branch in it turns the alarm inside out —
// silent during the outage, firing during normal operation.

func TestOnConsumptionBlocked_ReportsOneWhileBlockedAndZeroAfter(t *testing.T) {
	m := New()
	tm := TriggerMetrics{Metrics: m}
	ctx := context.Background()

	tm.OnConsumptionBlocked(ctx, "t", 4, true)
	if got := blockedGauge(t, m, "4"); got != 1 {
		t.Fatalf("blocked gauge = %v while blocked, want 1: on_overflow=block "+
			"stops this partition consuming and consumer lag cannot show it, so a "+
			"wrong value here means the stall has no signal at all", got)
	}

	// The clearing edge is half the contract: a gauge Set only on transitions
	// holds its value between edges, so a resume that fails to write 0 leaves
	// the alarm latched forever and the next real block is indistinguishable
	// from the stale one.
	tm.OnConsumptionBlocked(ctx, "t", 4, false)
	if got := blockedGauge(t, m, "4"); got != 0 {
		t.Fatalf("blocked gauge = %v after resuming, want 0: the alarm stays "+
			"latched and every later block is invisible behind it", got)
	}
}

func TestOnConsumptionBlocked_PartitionsDoNotCollapseIntoOneSeries(t *testing.T) {
	// The state IS per-partition. Without the identity label, one healthy
	// partition's resume overwrites a blocked partition's alarm — the same
	// defect xflow_wasm_instance_total shipped with, where it reported 8
	// instances while 16 existed.
	m := New()
	tm := TriggerMetrics{Metrics: m}
	ctx := context.Background()

	tm.OnConsumptionBlocked(ctx, "t", 0, true)
	tm.OnConsumptionBlocked(ctx, "t", 1, false)
	tm.OnConsumptionBlocked(ctx, "t", 2, false)

	family := gatherMetricFamily(t, m, "xflow_trigger_consumption_blocked")
	byPartition := map[string]float64{}
	for _, metric := range family.GetMetric() {
		partition := labelValue(metric, "partition")
		if partition == "" {
			t.Fatal("consumption blocked carries no partition label; every partition " +
				"writes one series and a blocked partition is erased by the next " +
				"healthy one's report")
		}
		byPartition[partition] = metric.GetGauge().GetValue()
	}
	if len(byPartition) != 3 {
		t.Fatalf("three partitions produced %d series (%v)", len(byPartition), byPartition)
	}
	if byPartition["0"] != 1 {
		t.Errorf("the blocked partition reports %v, want 1 — a healthy partition "+
			"overwrote it", byPartition["0"])
	}
}

func blockedGauge(t *testing.T, m *Metrics, partition string) float64 {
	t.Helper()
	family := gatherMetricFamily(t, m, "xflow_trigger_consumption_blocked")
	for _, metric := range family.GetMetric() {
		if labelValue(metric, "partition") == partition {
			return metric.GetGauge().GetValue()
		}
	}
	t.Fatalf("no series for partition %q", partition)
	return 0
}

func TestOnMessageDiscarded_KeepsReasonsApartAndDoesNotSwapLabels(t *testing.T) {
	// The two reasons are not the same event and must not be summed on a
	// dashboard. schema_fail is a message that could not be used; buffer_overflow
	// is a usable message thrown away under load with the commit frontier moving
	// past its offset, so that series is a running total of permanently lost
	// records. Topic and reason are both plain strings, so nothing but this
	// assertion stops the two arguments from landing in each other's label.
	m := New()
	tm := TriggerMetrics{Metrics: m}
	ctx := context.Background()

	tm.OnMessageDiscarded(ctx, "orders", "schema_fail")
	tm.OnMessageDiscarded(ctx, "orders", "buffer_overflow")
	tm.OnMessageDiscarded(ctx, "orders", "buffer_overflow")

	family := gatherMetricFamily(t, m, "xflow_trigger_messages_discarded_total")
	byReason := map[string]float64{}
	for _, metric := range family.GetMetric() {
		if got := labelValue(metric, "topic"); got != "orders" {
			t.Fatalf("topic label = %q, want orders: the topic and reason arguments "+
				"are swapped, so every dashboard grouping by topic groups by failure "+
				"kind instead", got)
		}
		byReason[labelValue(metric, "reason")] += metric.GetCounter().GetValue()
	}
	if len(byReason) != 2 {
		t.Fatalf("two discard reasons produced %d series (%v); permanently lost "+
			"records are summed together with unusable ones", len(byReason), byReason)
	}
	if byReason["buffer_overflow"] != 2 || byReason["schema_fail"] != 1 {
		t.Errorf("discards by reason = %v, want buffer_overflow=2 schema_fail=1", byReason)
	}
}

func TestOnBatchFlushOutcome_CarriesTheResultItWasGiven(t *testing.T) {
	// This counter exists to be divided by the flush-attempt counter to get a
	// retry rate. A result label that is constant makes that ratio either 0 or 1
	// regardless of what happened, and a partition re-flushing the same buffer
	// forever reads as a pipeline that is merely slow.
	m := New()
	tm := TriggerMetrics{Metrics: m}
	ctx := context.Background()

	tm.OnBatchFlushOutcome(ctx, "orders", "ingest", "ok")
	tm.OnBatchFlushOutcome(ctx, "orders", "ingest", "error")
	tm.OnBatchFlushOutcome(ctx, "orders", "ingest", "error")

	byResult := map[string]float64{}
	for _, metric := range gatherMetricFamily(t, m, "xflow_trigger_batch_flush_outcomes_total").GetMetric() {
		if got := labelValue(metric, "trigger"); got != "ingest" {
			t.Fatalf("trigger label = %q, want ingest", got)
		}
		byResult[labelValue(metric, "result")] += metric.GetCounter().GetValue()
	}
	if byResult["error"] != 2 || byResult["ok"] != 1 {
		t.Fatalf("flush outcomes = %v, want error=2 ok=1: the retry rate this "+
			"counter exists to produce is not derivable from it", byResult)
	}
}

func TestOnMessageDeadLettered_SeparatesTheParkedFromTheStalled(t *testing.T) {
	// result="error" means the dead-letter publish itself failed, so the invalid
	// message is being redelivered rather than parked and its source partition is
	// wedged. Collapsing it into the "ok" series reports the more urgent of the
	// two conditions as the benign one.
	m := New()
	tm := TriggerMetrics{Metrics: m}
	ctx := context.Background()

	tm.OnMessageDeadLettered(ctx, "orders", "ok")
	tm.OnMessageDeadLettered(ctx, "orders", "error")

	byResult := map[string]float64{}
	for _, metric := range gatherMetricFamily(t, m, "xflow_trigger_messages_dead_lettered_total").GetMetric() {
		if got := labelValue(metric, "topic"); got != "orders" {
			t.Fatalf("topic label = %q, want orders", got)
		}
		byResult[labelValue(metric, "result")] += metric.GetCounter().GetValue()
	}
	if byResult["ok"] != 1 || byResult["error"] != 1 {
		t.Fatalf("dead-letter results = %v, want ok=1 error=1: a stalled partition "+
			"is being counted as a successfully parked message", byResult)
	}
}
