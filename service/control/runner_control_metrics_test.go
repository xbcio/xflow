package control

import (
	"context"
	"sync"
	"testing"
	"time"

	backendlocal "github.com/xbcio/xflow/backend/providers/local"
	observabilitymetrics "github.com/xbcio/xflow/observability/metrics"

	dto "github.com/prometheus/client_model/go"
)

type runnerControlMetricTransition struct {
	action string
	result string
}

type runnerControlMetricsObserverRecorder struct {
	mu          sync.Mutex
	transitions []runnerControlMetricTransition
	durations   []time.Duration
	fleets      []observabilitymetrics.RunnerControlFleetSnapshot
}

func (r *runnerControlMetricsObserverRecorder) OnRunnerControlTransition(_ context.Context, action, result string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.transitions = append(r.transitions, runnerControlMetricTransition{action: action, result: result})
}

func (r *runnerControlMetricsObserverRecorder) OnRunnerDrainCompleted(_ context.Context, elapsed time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.durations = append(r.durations, elapsed)
}

func (r *runnerControlMetricsObserverRecorder) ObserveRunnerControlSnapshot(_ context.Context, snapshot observabilitymetrics.RunnerControlFleetSnapshot) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fleets = append(r.fleets, snapshot)
}

func (r *runnerControlMetricsObserverRecorder) observations() ([]runnerControlMetricTransition, []time.Duration, []observabilitymetrics.RunnerControlFleetSnapshot) {
	r.mu.Lock()
	defer r.mu.Unlock()
	transitions := append([]runnerControlMetricTransition(nil), r.transitions...)
	durations := append([]time.Duration(nil), r.durations...)
	fleets := append([]observabilitymetrics.RunnerControlFleetSnapshot(nil), r.fleets...)
	return transitions, durations, fleets
}

type runnerControlMetricsLister struct {
	runners []RunnerSnapshot
}

func (l *runnerControlMetricsLister) ListLiveRunners(context.Context) []RunnerSnapshot {
	return l.runners
}

func TestRunnerControlMetricsCollectorAggregatesFleetAndDeduplicatesCompletion(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.September, 16, 12, 0, 0, 0, time.UTC)
	requestedComplete := now.Add(-8 * time.Second)
	lister := &runnerControlMetricsLister{runners: []RunnerSnapshot{
		{RunnerID: "active", Control: &RunnerControlSnapshot{DesiredState: RunnerDesiredStateActive}},
		{RunnerID: "complete", Control: &RunnerControlSnapshot{
			DesiredState: RunnerDesiredStateDraining,
			Generation:   3,
			RequestedAt:  &requestedComplete,
			Drain: &RunnerDrainSnapshot{
				Phase:                    RunnerDrainPhaseComplete,
				HandoffDebt:              2,
				ReplayableDebt:           3,
				PendingActivationCleanup: 4,
				RunnerQuiescent:          true,
			},
		}},
		{RunnerID: "timed-out", Control: &RunnerControlSnapshot{
			DesiredState: RunnerDesiredStateDraining,
			Generation:   7,
			Drain: &RunnerDrainSnapshot{
				Phase:                    RunnerDrainPhaseTimedOut,
				HandoffDebt:              5,
				ReplayableDebt:           7,
				PendingActivationCleanup: 11,
				RunnerQuiescent:          false,
			},
		}},
		{RunnerID: "awaiting-observation", Control: &RunnerControlSnapshot{
			DesiredState: RunnerDesiredStateDraining,
			Generation:   2,
		}},
	}}
	recorder := &runnerControlMetricsObserverRecorder{}
	collector := NewRunnerControlMetricsCollector(lister, recorder)
	collector.now = func() time.Time { return now }

	collector.Collect(ctx)
	collector.Collect(ctx)
	_, durations, fleets := recorder.observations()
	if len(fleets) != 2 {
		t.Fatalf("fleet observations = %d, want 2", len(fleets))
	}
	wantFleet := observabilitymetrics.RunnerControlFleetSnapshot{
		DrainingCount:                      3,
		HandoffDebtCount:                   7,
		ReplayDebtCount:                    10,
		PendingActivationReceiptCount:      15,
		AwaitingRunnerAcknowledgementCount: 2,
		OverdueDrainCount:                  1,
	}
	for i, got := range fleets {
		if got != wantFleet {
			t.Errorf("fleet[%d] = %+v, want %+v", i, got, wantFleet)
		}
	}
	if len(durations) != 1 || durations[0] != 8*time.Second {
		t.Fatalf("completed durations = %v, want [8s] after duplicate collection", durations)
	}

	// A new control generation has a new drain lifetime and therefore earns one
	// new duration observation. The old generation is also pruned from the
	// process-local dedup set because it is no longer in the fleet projection.
	requestedNext := now.Add(-3 * time.Second)
	lister.runners = []RunnerSnapshot{{RunnerID: "complete", Control: &RunnerControlSnapshot{
		DesiredState: RunnerDesiredStateDraining,
		Generation:   4,
		RequestedAt:  &requestedNext,
		Drain: &RunnerDrainSnapshot{
			Phase:           RunnerDrainPhaseComplete,
			RunnerQuiescent: true,
		},
	}}}
	collector.Collect(ctx)
	_, durations, fleets = recorder.observations()
	if len(durations) != 2 || durations[1] != 3*time.Second {
		t.Fatalf("completed durations after new generation = %v, want [8s 3s]", durations)
	}
	if got, want := fleets[len(fleets)-1].DrainingCount, 1; got != want {
		t.Fatalf("latest draining count = %d, want %d", got, want)
	}
}

func TestRunnerControlMetricsDirectoryRecordsBoundedOutcomes(t *testing.T) {
	ctx := context.Background()
	directory := NewMemoryRunnerDirectory()
	mustRegisterMemoryRunner(t, ctx, directory, "runner-a", 1)
	recorder := &runnerControlMetricsObserverRecorder{}
	collector := NewRunnerControlMetricsCollector(directory, recorder)
	adapter := newRunnerControlMetricsDirectory(directory, directory, recorder, collector)

	drain := controlRequest("runner-a", "drain", "request-drain", "hash-drain", RunnerDesiredStateDraining)
	if _, err := adapter.SetRunnerControl(ctx, drain); err != nil {
		t.Fatalf("transition drain: %v", err)
	}
	if _, err := adapter.SetRunnerControl(ctx, controlRequest("runner-a", "drain", "request-unchanged", "hash-unchanged", RunnerDesiredStateDraining)); err != nil {
		t.Fatalf("unchanged drain: %v", err)
	}
	resume := controlRequest("runner-a", "resume", "request-resume", "hash-resume", RunnerDesiredStateActive)
	if _, err := adapter.SetRunnerControl(ctx, resume); err != nil {
		t.Fatalf("transition resume: %v", err)
	}
	if _, err := adapter.SetRunnerControl(ctx, drain); err != nil {
		t.Fatalf("historical drain receipt replay: %v", err)
	}
	conflict := resume
	conflict.RequestHash = "different-body"
	if _, err := adapter.SetRunnerControl(ctx, conflict); err == nil {
		t.Fatal("conflicting receipt error = nil, want conflict")
	}
	if _, err := adapter.SetRunnerControl(ctx, controlRequest("missing", "drain", "request-missing", "hash-missing", RunnerDesiredStateDraining)); err == nil {
		t.Fatal("unknown runner error = nil, want not found")
	}
	if _, err := adapter.SetRunnerControl(ctx, RunnerControlRequest{
		RunnerID: "runner-a", DesiredState: RunnerDesiredState("user-controlled-state"), Action: "user-controlled-action", RequestID: "request-invalid", RequestHash: "hash-invalid",
	}); err == nil {
		t.Fatal("invalid state error = nil, want invalid")
	}

	transitions, _, fleets := recorder.observations()
	want := []runnerControlMetricTransition{
		{action: observabilitymetrics.RunnerControlActionDrain, result: observabilitymetrics.RunnerControlResultTransitioned},
		{action: observabilitymetrics.RunnerControlActionDrain, result: observabilitymetrics.RunnerControlResultUnchanged},
		{action: observabilitymetrics.RunnerControlActionResume, result: observabilitymetrics.RunnerControlResultTransitioned},
		{action: observabilitymetrics.RunnerControlActionDrain, result: observabilitymetrics.RunnerControlResultReplayed},
		{action: observabilitymetrics.RunnerControlActionResume, result: observabilitymetrics.RunnerControlResultConflict},
		{action: observabilitymetrics.RunnerControlActionDrain, result: observabilitymetrics.RunnerControlResultNotFound},
		{action: observabilitymetrics.RunnerControlActionOther, result: observabilitymetrics.RunnerControlResultInvalid},
	}
	if len(transitions) != len(want) {
		t.Fatalf("transitions = %#v, want %#v", transitions, want)
	}
	for i := range want {
		if transitions[i] != want[i] {
			t.Errorf("transition[%d] = %+v, want %+v", i, transitions[i], want[i])
		}
	}
	if got, want := len(fleets), 4; got != want {
		t.Fatalf("successful mutation fleet collections = %d, want %d", got, want)
	}
}

func TestControlPlaneWiresRunnerControlMetricsIntoManagementDirectory(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.September, 16, 13, 0, 0, 0, time.UTC)
	directory := NewMemoryRunnerDirectory()
	session := mustRegisterMemoryRunner(t, ctx, directory, "runner-metrics", 1)
	registry := observabilitymetrics.New()
	cp, err := NewControlPlane(Config{
		Backend:         backendlocal.New(),
		RunnerDirectory: directory,
		Metrics:         registry,
	})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	if cp.runnerControlMetrics == nil {
		t.Fatal("runner control collector = nil, want metrics wiring")
	}
	cp.runnerControlMetrics.now = func() time.Time { return now }

	managementDirectory := cp.RunnerDirectory()
	controlDirectory, ok := managementDirectory.(RunnerControlDirectory)
	if !ok {
		t.Fatalf("management directory %T does not expose RunnerControlDirectory", managementDirectory)
	}
	listDirectory, ok := managementDirectory.(interface {
		ListRunners(context.Context) ([]string, error)
	})
	if !ok {
		t.Fatalf("management directory %T lost ListRunners capability", managementDirectory)
	}
	if ids, err := listDirectory.ListRunners(ctx); err != nil || len(ids) != 1 || ids[0] != "runner-metrics" {
		t.Fatalf("ListRunners = %v, %v; want [runner-metrics], nil", ids, err)
	}

	drain, err := controlDirectory.SetRunnerControl(ctx, RunnerControlRequest{
		RunnerID:     "runner-metrics",
		DesiredState: RunnerDesiredStateDraining,
		Actor:        "operator-a",
		Action:       "drain",
		Reason:       "maintenance",
		RequestID:    "metrics-drain",
		RequestHash:  "metrics-drain-hash",
		Now:          now.Add(-5 * time.Second),
	})
	if err != nil {
		t.Fatalf("SetRunnerControl(drain): %v", err)
	}
	if err := directory.Heartbeat(ctx, quietDrainHeartbeat(session, drain.Generation, now)); err != nil {
		t.Fatalf("quiet drain heartbeat: %v", err)
	}
	cp.runnerControlMetrics.Collect(ctx)

	transitions := runnerControlMetricFamily(t, registry, "xflow_runner_control_transitions_total")
	if got := runnerControlMetricValue(transitions, map[string]string{"action": "drain", "result": "transitioned"}, "counter"); got != 1 {
		t.Fatalf("drain transitioned counter = %v, want 1", got)
	}
	for _, metric := range transitions.GetMetric() {
		if got := len(metric.GetLabel()); got != 2 {
			t.Fatalf("transition labels = %v, want action and result only", metric.GetLabel())
		}
	}

	draining := runnerControlMetricFamily(t, registry, "xflow_runner_draining_count")
	if got := runnerControlMetricValue(draining, nil, "gauge"); got != 1 {
		t.Fatalf("draining gauge = %v, want 1", got)
	}
	if got := len(draining.GetMetric()[0].GetLabel()); got != 0 {
		t.Fatalf("draining gauge labels = %v, want none", draining.GetMetric()[0].GetLabel())
	}

	blockers := runnerControlMetricFamily(t, registry, "xflow_runner_drain_blockers")
	for kind, want := range map[string]float64{
		"handoff_debt":       0,
		"replay_debt":        0,
		"activation_receipt": 0,
		"runner_ack":         0,
		"deadline":           0,
	} {
		if got := runnerControlMetricValue(blockers, map[string]string{"kind": kind}, "gauge"); got != want {
			t.Errorf("blocker[%q] = %v, want %v", kind, got, want)
		}
	}
	for _, metric := range blockers.GetMetric() {
		if got := len(metric.GetLabel()); got != 1 {
			t.Fatalf("blocker labels = %v, want kind only", metric.GetLabel())
		}
	}

	duration := runnerControlMetricFamily(t, registry, "xflow_runner_drain_duration_seconds")
	if got := runnerControlMetricValue(duration, nil, "histogram_count"); got != 1 {
		t.Fatalf("drain duration sample count = %v, want 1", got)
	}
	if got := runnerControlMetricValue(duration, nil, "histogram_sum"); got != 5 {
		t.Fatalf("drain duration sample sum = %v, want 5 seconds", got)
	}
}

func TestRunnerControlMetricsCollectorAndAdapterAreNilSafe(t *testing.T) {
	(&RunnerControlMetricsCollector{}).Collect(nil)

	ctx := context.Background()
	directory := NewMemoryRunnerDirectory()
	mustRegisterMemoryRunner(t, ctx, directory, "runner-nil-metrics", 1)
	adapter := newRunnerControlMetricsDirectory(directory, directory, nil, nil)
	if _, err := adapter.SetRunnerControl(ctx, controlRequest("runner-nil-metrics", "drain", "request-nil", "hash-nil", RunnerDesiredStateDraining)); err != nil {
		t.Fatalf("SetRunnerControl with nil observer: %v", err)
	}
}

func runnerControlMetricFamily(t *testing.T, registry *observabilitymetrics.Metrics, name string) *dto.MetricFamily {
	t.Helper()
	families, err := registry.Registry().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, family := range families {
		if family.GetName() == name {
			return family
		}
	}
	t.Fatalf("metric family %q not found", name)
	return nil
}

func runnerControlMetricValue(family *dto.MetricFamily, labels map[string]string, kind string) float64 {
	for _, metric := range family.GetMetric() {
		if !runnerControlMetricLabelsMatch(metric, labels) {
			continue
		}
		switch kind {
		case "counter":
			return metric.GetCounter().GetValue()
		case "gauge":
			return metric.GetGauge().GetValue()
		case "histogram_count":
			return float64(metric.GetHistogram().GetSampleCount())
		case "histogram_sum":
			return metric.GetHistogram().GetSampleSum()
		}
	}
	return -1
}

func runnerControlMetricLabelsMatch(metric *dto.Metric, want map[string]string) bool {
	if len(metric.GetLabel()) != len(want) {
		return false
	}
	for _, label := range metric.GetLabel() {
		if want[label.GetName()] != label.GetValue() {
			return false
		}
	}
	return true
}
