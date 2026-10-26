package control

import (
	"testing"
	"time"
)

// TestExecutionDeadlinePassLabelIsPinned fixes the metric label value of the
// deadline pass. observability/metrics forwards pass names as literals instead
// of importing these constants, so the literal is what dashboards and alerts
// select on: renaming the constant must be a deliberate, test-visible change.
func TestExecutionDeadlinePassLabelIsPinned(t *testing.T) {
	if SweepPassExecutionDeadline != "execution_deadline" {
		t.Fatalf("SweepPassExecutionDeadline = %q, want %q", SweepPassExecutionDeadline, "execution_deadline")
	}
}

// TestExecutionDeadlinePassReportsItsLiteralLabel drives the real pass and
// compares what reaches the observers with the string literal rather than the
// constant, for the outcomes a dashboard has to tell apart.
func TestExecutionDeadlinePassReportsItsLiteralLabel(t *testing.T) {
	t.Run("ran", func(t *testing.T) {
		h := newDeadlineHarness(LeaseSweeperConfig{})
		h.list("exec", 2, h.now.Add(-time.Hour))
		h.pass()
		assertDeadlinePassLabel(t, h, "ran", 2, 2)
	})

	t.Run("error", func(t *testing.T) {
		h := newDeadlineHarness(LeaseSweeperConfig{})
		h.state.err = errStoreDown
		h.pass()
		assertDeadlinePassLabel(t, h, "error", 0, 0)
	})

	t.Run("skipped_not_leader", func(t *testing.T) {
		h := newDeadlineHarness(LeaseSweeperConfig{})
		h.elector.leader.Store(false)
		h.pass()
		assertDeadlinePassLabel(t, h, "skipped_not_leader", 0, 0)
	})

	t.Run("skipped_cadence", func(t *testing.T) {
		h := newDeadlineHarness(LeaseSweeperConfig{})
		h.pass()
		h.pass()
		events := h.observer.snapshot()
		if len(events) != 2 || events[1].pass != "execution_deadline" || events[1].outcome != "skipped_cadence" {
			t.Fatalf("pass events = %+v, want the second to be execution_deadline/skipped_cadence", events)
		}
	})

	t.Run("unsupported", func(t *testing.T) {
		h := newDeadlineHarness(LeaseSweeperConfig{})
		h.engine = &fakeReclaimer{}
		h.pass()
		assertDeadlinePassLabel(t, h, "unsupported", 0, 0)
	})
}

// assertDeadlinePassLabel checks the single pass event and candidate event the
// harness's observer recorded against literal label and outcome strings.
func assertDeadlinePassLabel(t *testing.T, h *deadlineHarness, outcome string, released, inspected int) {
	t.Helper()
	events := h.observer.snapshot()
	if len(events) != 1 {
		t.Fatalf("pass events = %+v, want exactly one", events)
	}
	if events[0].pass != "execution_deadline" || events[0].outcome != outcome || events[0].released != released {
		t.Fatalf("pass event = %+v, want {execution_deadline %s %d}", events[0], outcome, released)
	}
	candidates := h.observer.candidateSnapshot()
	if len(candidates) != 1 {
		t.Fatalf("candidate events = %+v, want exactly one", candidates)
	}
	if candidates[0].pass != "execution_deadline" || candidates[0].outcome != outcome || candidates[0].inspected != inspected {
		t.Fatalf("candidate event = %+v, want {execution_deadline %s %d}", candidates[0], outcome, inspected)
	}
}
