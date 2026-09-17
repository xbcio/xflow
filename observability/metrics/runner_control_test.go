package metrics

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func scrapeMetrics(t *testing.T, m *Metrics) string {
	t.Helper()
	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, req)
	return rec.Body.String()
}

func TestRunnerControlMetricsTransitionsUseBoundedLabels(t *testing.T) {
	m := New()
	observer := NewRunnerControlMetrics(m)
	ctx := context.Background()

	observer.OnRunnerControlTransition(ctx, RunnerControlActionDrain, RunnerControlResultTransitioned)
	observer.OnRunnerControlTransition(ctx, RunnerControlActionDrain, RunnerControlResultTransitioned)
	observer.OnRunnerControlTransition(ctx, RunnerControlActionResume, RunnerControlResultConflict)
	observer.OnRunnerControlTransition(ctx, "runner-secret-17", "reason must never become a label")

	counts := map[string]float64{}
	for _, metric := range gatherMetricFamily(t, m, metricRunnerControlTransitions).GetMetric() {
		if got := len(metric.GetLabel()); got != 2 {
			t.Fatalf("transition labels = %d, want action and result only: %v", got, metric.GetLabel())
		}
		key := labelValue(metric, "action") + "/" + labelValue(metric, "result")
		counts[key] = metric.GetCounter().GetValue()
	}

	for key, want := range map[string]float64{
		"drain/transitioned": 2,
		"resume/conflict":    1,
		"other/other":        1,
	} {
		if got := counts[key]; got != want {
			t.Errorf("transitions[%q] = %v, want %v (all: %v)", key, got, want, counts)
		}
	}
	if len(counts) != 3 {
		t.Fatalf("transition series = %v, want exactly the three bounded series", counts)
	}

	body := scrapeMetrics(t, m)
	for _, forbidden := range []string{"runner-secret-17", "reason must never become a label"} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("runner-control metric leaked %q:\n%s", forbidden, body)
		}
	}
}

func TestRunnerControlMetricsFleetSnapshotSetsExpectedGauges(t *testing.T) {
	m := New()
	observer := NewRunnerControlMetrics(m)

	observer.ObserveRunnerControlSnapshot(context.Background(), RunnerControlFleetSnapshot{
		DrainingCount:                      3,
		HandoffDebtCount:                   5,
		ReplayDebtCount:                    7,
		PendingActivationReceiptCount:      11,
		AwaitingRunnerAcknowledgementCount: 13,
		OverdueDrainCount:                  17,
	})

	draining := gatherMetricFamily(t, m, metricRunnerDrainingCount).GetMetric()
	if len(draining) != 1 {
		t.Fatalf("draining gauge series = %d, want 1", len(draining))
	}
	if got := len(draining[0].GetLabel()); got != 0 {
		t.Fatalf("draining gauge labels = %d, want none: %v", got, draining[0].GetLabel())
	}
	if got := draining[0].GetGauge().GetValue(); got != 3 {
		t.Fatalf("draining gauge = %v, want 3", got)
	}

	blockers := map[string]float64{}
	for _, metric := range gatherMetricFamily(t, m, metricRunnerDrainBlockers).GetMetric() {
		if got := len(metric.GetLabel()); got != 1 {
			t.Fatalf("blocker labels = %d, want kind only: %v", got, metric.GetLabel())
		}
		blockers[labelValue(metric, "kind")] = metric.GetGauge().GetValue()
	}
	for kind, want := range map[string]float64{
		runnerDrainBlockerHandoffDebt:       5,
		runnerDrainBlockerReplayDebt:        7,
		runnerDrainBlockerActivationReceipt: 11,
		runnerDrainBlockerRunnerAck:         13,
		runnerDrainBlockerDeadline:          17,
	} {
		if got := blockers[kind]; got != want {
			t.Errorf("blockers[%q] = %v, want %v (all: %v)", kind, got, want, blockers)
		}
	}
	if len(blockers) != 5 {
		t.Fatalf("blocker series = %v, want exactly five documented kinds", blockers)
	}

	// Gauges must publish the most recent full fleet projection rather than
	// accumulate values from earlier reconciliation passes.
	observer.ObserveRunnerControlSnapshot(context.Background(), RunnerControlFleetSnapshot{
		DrainingCount:                      -1,
		HandoffDebtCount:                   2,
		ReplayDebtCount:                    -1,
		PendingActivationReceiptCount:      0,
		AwaitingRunnerAcknowledgementCount: 1,
		OverdueDrainCount:                  -1,
	})
	if got := gatherMetricFamily(t, m, metricRunnerDrainingCount).GetMetric()[0].GetGauge().GetValue(); got != 0 {
		t.Fatalf("negative draining count = %v, want clamped zero", got)
	}
	latest := map[string]float64{}
	for _, metric := range gatherMetricFamily(t, m, metricRunnerDrainBlockers).GetMetric() {
		latest[labelValue(metric, "kind")] = metric.GetGauge().GetValue()
	}
	for kind, want := range map[string]float64{
		runnerDrainBlockerHandoffDebt:       2,
		runnerDrainBlockerReplayDebt:        0,
		runnerDrainBlockerActivationReceipt: 0,
		runnerDrainBlockerRunnerAck:         1,
		runnerDrainBlockerDeadline:          0,
	} {
		if got := latest[kind]; got != want {
			t.Errorf("latest blockers[%q] = %v, want %v (all: %v)", kind, got, want, latest)
		}
	}
}

func TestRunnerControlMetricsRecordsOnlyCompletedDrainDurations(t *testing.T) {
	m := New()
	observer := NewRunnerControlMetrics(m)

	observer.OnRunnerDrainCompleted(context.Background(), 2500*time.Millisecond)
	observer.OnRunnerDrainCompleted(context.Background(), -time.Second)

	observations := gatherMetricFamily(t, m, metricRunnerDrainDuration).GetMetric()
	if len(observations) != 1 {
		t.Fatalf("drain duration series = %d, want 1", len(observations))
	}
	if got := len(observations[0].GetLabel()); got != 0 {
		t.Fatalf("drain duration labels = %d, want none: %v", got, observations[0].GetLabel())
	}
	histogram := observations[0].GetHistogram()
	if got := histogram.GetSampleCount(); got != 1 {
		t.Fatalf("drain duration samples = %d, want 1; negative elapsed time must be dropped", got)
	}
	if got := histogram.GetSampleSum(); got != 2.5 {
		t.Fatalf("drain duration sum = %v seconds, want 2.5", got)
	}
}

func TestRunnerControlObserversAreNoopCompatible(t *testing.T) {
	var noop RunnerControlObserver = NoopRunnerControlObserver{}
	noop.OnRunnerControlTransition(context.Background(), "secret-action", "secret-result")
	noop.OnRunnerDrainCompleted(context.Background(), time.Second)
	noop.ObserveRunnerControlSnapshot(context.Background(), RunnerControlFleetSnapshot{DrainingCount: 1})

	// A concrete adapter without a registry is also safe for optional wiring.
	NewRunnerControlMetrics(nil).OnRunnerControlTransition(context.Background(), RunnerControlActionDrain, RunnerControlResultTransitioned)
	NewRunnerControlMetrics(nil).OnRunnerDrainCompleted(context.Background(), time.Second)
	NewRunnerControlMetrics(nil).ObserveRunnerControlSnapshot(context.Background(), RunnerControlFleetSnapshot{DrainingCount: 1})
}
