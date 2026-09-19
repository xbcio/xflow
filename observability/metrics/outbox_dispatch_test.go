package metrics

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
)

// TestOutboxDispatchObserverMetricsIncrement pins that the three dispatch-loop
// series an operator reads to diagnose a stalled outbox actually reach /metrics
// with values. Without them the only way to tell "delivery is slow" from
// "discovery cannot see the backlog" was to count Redis keys by hand, which is
// how a dispatch ceiling below the execution-creation rate stayed invisible
// across two releases.
func TestOutboxDispatchObserverMetricsIncrement(t *testing.T) {
	m := New()
	ctx := context.Background()
	outbox := NewOutboxMetrics(m)

	outbox.OnOutboxDrain(ctx, 42, 1500*time.Millisecond)
	outbox.OnOutboxBacklog(ctx, engine.OutboxMetricsSnapshot{Pending: 900, Ready: 120})

	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, req)
	body := rec.Body.String()

	for _, want := range []string{
		`xflow_outbox_drain_discovered{namespace="default"} 42`,
		`xflow_outbox_drain_duration_seconds_count{namespace="default"} 1`,
		`xflow_outbox_drain_duration_seconds_sum{namespace="default"} 1.5`,
		`xflow_outbox_ready{namespace="default"} 120`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics body missing %q:\n%s", want, body)
		}
	}
}

// TestOutboxDispatchObserverImplementsTheOptionalContract is the compile-time
// half of the wiring: the dispatcher reaches these observations through a type
// assert on the installed engine.OutboxObserver, so a metrics observer that
// stopped satisfying engine.OutboxDispatchObserver would silently stop
// exporting all three series — no error, no series, nothing to notice.
func TestOutboxDispatchObserverImplementsTheOptionalContract(t *testing.T) {
	var (
		_ engine.OutboxObserver         = OutboxMetrics{}
		_ engine.OutboxDispatchObserver = OutboxMetrics{}
	)
}

// TestOutboxReadyGaugeTracksTheLastBacklogScan: ready is a gauge, so a second
// scan that reports less must lower it rather than accumulate. A backlog
// counter that only ever grew would read as a permanently stalled outbox.
func TestOutboxReadyGaugeTracksTheLastBacklogScan(t *testing.T) {
	m := New()
	ctx := context.Background()
	outbox := NewOutboxMetrics(m)

	outbox.OnOutboxBacklog(ctx, engine.OutboxMetricsSnapshot{Pending: 900, Ready: 120})
	outbox.OnOutboxBacklog(ctx, engine.OutboxMetricsSnapshot{Pending: 10, Ready: 0})

	family := gatherMetricFamily(t, m, metricOutboxReady)
	if got := family.GetMetric()[0].GetGauge().GetValue(); got != 0 {
		t.Fatalf("xflow_outbox_ready = %v, want 0 -- a drained backlog must read as "+
			"drained, not as the largest value ever seen", got)
	}
}
