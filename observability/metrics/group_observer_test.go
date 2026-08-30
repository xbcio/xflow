package metrics

import (
	"context"
	"testing"
	"time"
)

// TestGroupObserverAdapter_RenewFansOutToBothSeries asserts that one
// OnGroupLeaseRenew call produces exactly one observation on each of the two
// series it is documented to fan out to: the result-partitioned counter and
// the duration histogram. Asserting through the registry's Gather() output
// (not by inspecting GroupObserverAdapter internals) is what proves the
// adapter is actually wired to real Prometheus collectors, not merely
// forwarding calls to methods that happen to compile.
func TestGroupObserverAdapter_RenewFansOutToBothSeries(t *testing.T) {
	m := New()
	obs := NewGroupObserver(m)

	obs.OnGroupLeaseRenew(context.Background(), "not_renewed", 42*time.Millisecond)

	counter := gatherMetricFamily(t, m, "xflow_group_lease_renew_total")
	if len(counter.GetMetric()) != 1 {
		t.Fatalf("xflow_group_lease_renew_total series count = %d, want 1", len(counter.GetMetric()))
	}
	if got := labelValue(counter.GetMetric()[0], "result"); got != "not_renewed" {
		t.Fatalf("xflow_group_lease_renew_total result label = %q, want %q", got, "not_renewed")
	}
	if got := counter.GetMetric()[0].GetCounter().GetValue(); got != 1 {
		t.Fatalf("xflow_group_lease_renew_total value = %v, want 1", got)
	}

	hist := gatherMetricFamily(t, m, "xflow_group_lease_renew_duration_seconds")
	if len(hist.GetMetric()) != 1 {
		t.Fatalf("xflow_group_lease_renew_duration_seconds series count = %d, want 1", len(hist.GetMetric()))
	}
	if got := hist.GetMetric()[0].GetHistogram().GetSampleCount(); got != 1 {
		t.Fatalf("xflow_group_lease_renew_duration_seconds sample count = %d, want 1", got)
	}
}

// TestGroupObserverAdapter_CommitFansOutToBothSeries is the OnGroupCommit
// analogue of the renew test above: one call must land one counter
// observation (partitioned by outcome) and one duration histogram
// observation.
func TestGroupObserverAdapter_CommitFansOutToBothSeries(t *testing.T) {
	m := New()
	obs := NewGroupObserver(m)

	obs.OnGroupCommit(context.Background(), "failed_fatal", 7*time.Second)

	counter := gatherMetricFamily(t, m, "xflow_group_commit_total")
	if len(counter.GetMetric()) != 1 {
		t.Fatalf("xflow_group_commit_total series count = %d, want 1", len(counter.GetMetric()))
	}
	if got := labelValue(counter.GetMetric()[0], "outcome"); got != "failed_fatal" {
		t.Fatalf("xflow_group_commit_total outcome label = %q, want %q", got, "failed_fatal")
	}
	if got := counter.GetMetric()[0].GetCounter().GetValue(); got != 1 {
		t.Fatalf("xflow_group_commit_total value = %v, want 1", got)
	}

	hist := gatherMetricFamily(t, m, "xflow_group_exec_duration_seconds")
	if len(hist.GetMetric()) != 1 {
		t.Fatalf("xflow_group_exec_duration_seconds series count = %d, want 1", len(hist.GetMetric()))
	}
	if got := hist.GetMetric()[0].GetHistogram().GetSampleCount(); got != 1 {
		t.Fatalf("xflow_group_exec_duration_seconds sample count = %d, want 1", got)
	}
}

// TestGroupObserverAdapter_AdmissionFansOutToBothSeries is the OnGroupAdmission
// analogue of the renew/commit tests above: one call must land one counter
// observation (partitioned by outcome) and one duration histogram
// observation.
func TestGroupObserverAdapter_AdmissionFansOutToBothSeries(t *testing.T) {
	m := New()
	obs := NewGroupObserver(m)

	obs.OnGroupAdmission(context.Background(), "conflict", 12*time.Millisecond)

	counter := gatherMetricFamily(t, m, "xflow_group_admission_total")
	if len(counter.GetMetric()) != 1 {
		t.Fatalf("xflow_group_admission_total series count = %d, want 1", len(counter.GetMetric()))
	}
	if got := labelValue(counter.GetMetric()[0], "outcome"); got != "conflict" {
		t.Fatalf("xflow_group_admission_total outcome label = %q, want %q", got, "conflict")
	}
	if got := counter.GetMetric()[0].GetCounter().GetValue(); got != 1 {
		t.Fatalf("xflow_group_admission_total value = %v, want 1", got)
	}

	hist := gatherMetricFamily(t, m, "xflow_group_admission_duration_seconds")
	if len(hist.GetMetric()) != 1 {
		t.Fatalf("xflow_group_admission_duration_seconds series count = %d, want 1", len(hist.GetMetric()))
	}
	if got := hist.GetMetric()[0].GetHistogram().GetSampleCount(); got != 1 {
		t.Fatalf("xflow_group_admission_duration_seconds sample count = %d, want 1", got)
	}
}

// TestGroupObserverAdapter_LeaseAcquiredAndExpiredCounters pins the two
// no-label counters end to end through the adapter.
func TestGroupObserverAdapter_LeaseAcquiredAndExpiredCounters(t *testing.T) {
	m := New()
	obs := NewGroupObserver(m)

	obs.OnGroupLeaseAcquired(context.Background())
	obs.OnGroupLeaseAcquired(context.Background())
	obs.OnGroupLeaseExpired(context.Background())

	acquired := gatherMetricFamily(t, m, "xflow_group_lease_acquired_total")
	if len(acquired.GetMetric()) != 1 {
		t.Fatalf("xflow_group_lease_acquired_total series count = %d, want 1", len(acquired.GetMetric()))
	}
	if got := acquired.GetMetric()[0].GetCounter().GetValue(); got != 2 {
		t.Fatalf("xflow_group_lease_acquired_total value = %v, want 2", got)
	}

	expired := gatherMetricFamily(t, m, "xflow_group_lease_expired_total")
	if len(expired.GetMetric()) != 1 {
		t.Fatalf("xflow_group_lease_expired_total series count = %d, want 1", len(expired.GetMetric()))
	}
	if got := expired.GetMetric()[0].GetCounter().GetValue(); got != 1 {
		t.Fatalf("xflow_group_lease_expired_total value = %v, want 1", got)
	}
}
