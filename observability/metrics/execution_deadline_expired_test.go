package metrics

import (
	"context"
	"strings"
	"testing"

	"github.com/xbcio/xflow/namespace"
)

func TestExecutionDeadlineExpiredCountsPerSourceAndNamespace(t *testing.T) {
	m := New()
	s := NewSweepMetrics(m)
	a := namespace.WithNamespace(context.Background(), "ns-a")
	b := namespace.WithNamespace(context.Background(), "ns-b")

	s.OnExecutionDeadlineExpired(a, "sweeper")
	s.OnExecutionDeadlineExpired(a, "sweeper")
	s.OnExecutionDeadlineExpired(b, "sweeper")

	const name = "xflow_execution_deadline_expired_total"
	if got := mustCounterSeries(t, m, name, map[string]string{"source": "sweeper", "namespace": "ns-a"}); got != 2 {
		t.Errorf("ns-a sweeper = %v, want 2", got)
	}
	if got := mustCounterSeries(t, m, name, map[string]string{"source": "sweeper", "namespace": "ns-b"}); got != 1 {
		t.Errorf("ns-b sweeper = %v, want 1: one tenant's expiries must not count toward another's", got)
	}
	if _, ok := findCounterSeries(t, m, name, map[string]string{"source": "lease"}); ok {
		t.Error("a series exists for a source nothing reported")
	}
}

// TestExecutionDeadlineExpiredHasHelpText pins the registration: an unregistered
// name exports the generic "xflow metric <name>" fallback, which says nothing a
// reader could not read off the series name.
func TestExecutionDeadlineExpiredHasHelpText(t *testing.T) {
	const name = "xflow_execution_deadline_expired_total"
	help, ok := metricHelp[name]
	if !ok {
		t.Fatalf("no metricHelp entry for %s", name)
	}
	if !strings.Contains(help, "settings.timeout") || !strings.Contains(help, "source") {
		t.Errorf("help text for %s does not describe what it counts and its labels: %q", name, help)
	}
	// A deployment with no control plane reports through its embedded sweeper.
	// The help text is the only place a reader learns that source value exists.
	if !strings.Contains(help, "embedded") {
		t.Errorf("help text for %s does not name the embedded source, so the series "+
			"a NewLocal/NewCluster expiry produces cannot be interpreted: %q", name, help)
	}
}
