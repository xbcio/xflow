package control

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/xbcio/xflow/observability/metrics"
)

func TestDirectoryLivenessFollowsHeartbeatTTL(t *testing.T) {
	directory := NewMemoryRunnerDirectory()
	ctx := context.Background()
	// RegisterRunnerRequest.Now seeds LastHeartbeat, so the whole test runs on a
	// fixed clock instead of whatever time.Now happened to be.
	at := time.Unix(1754000000, 0).UTC()
	if _, err := directory.Register(ctx, RegisterRunnerRequest{
		RunnerID: "runner-a", Capacity: 1, Now: at,
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	live := NewDirectoryLiveness(directory, DefaultRunnerSelector())

	if !live.IsRunnerLive(ctx, "runner-a", at.Add(29*time.Second)) {
		t.Errorf("runner must still be live 29s after its heartbeat (TTL is 30s)")
	}
	if live.IsRunnerLive(ctx, "runner-a", at.Add(31*time.Second)) {
		t.Errorf("runner must be dead 31s after its heartbeat (TTL is 30s)")
	}
}

func TestDirectoryLivenessTreatsUnknownRunnerAsDead(t *testing.T) {
	live := NewDirectoryLiveness(NewMemoryRunnerDirectory(), DefaultRunnerSelector())
	if live.IsRunnerLive(context.Background(), "never-registered", time.Unix(1754000000, 0)) {
		t.Errorf("a runner the directory does not know must not be reported live")
	}
}

func TestDeadRunnerSeriesDisappear(t *testing.T) {
	directory := NewMemoryRunnerDirectory()
	ctx := context.Background()
	registeredAt := time.Unix(1754000000, 0).UTC()
	if _, err := directory.Register(ctx, RegisterRunnerRequest{
		RunnerID: "runner-a", Capacity: 1, Now: registeredAt,
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	clock := registeredAt
	reg := prometheus.NewRegistry()
	m := metrics.New()
	in := NewMetricsInbox(MetricsInboxConfig{
		Store:   NewMemoryMetricsStore(),
		Self:    reg,
		Live:    NewDirectoryLiveness(directory, DefaultRunnerSelector()),
		Metrics: m,
		Now:     func() time.Time { return clock },
	})

	body := encodeFamilies(t, counterFamily("alive_total", "h", 1))
	if err := in.Accept(ctx, "runner-a", body); err != nil {
		t.Fatalf("Accept: %v", err)
	}

	// Reverse assertions need a proven precondition: confirm the series is
	// there while the runner is alive, otherwise a bug that never writes to the
	// inbox at all would make the disappearance assertion pass.
	code, out := scrape(t, prometheus.Gatherers{reg, in})
	if code != 200 || !strings.Contains(out, `alive_total{runner_id="runner-a"} 1`) {
		t.Fatalf("precondition failed: status %d, body:\n%s", code, out)
	}
	// The xflow_runner_up gauge must read 1 while the runner is live.
	upOut := gatherText(t, m.Registry())
	if !strings.Contains(upOut, `xflow_runner_up{runner_id="runner-a"} 1`) {
		t.Fatalf("precondition: xflow_runner_up must be 1 while runner is live; got:\n%s", upOut)
	}

	// Advance the injected clock past the live TTL. No sleeping: the clock is
	// the injection point that makes this assertion possible at all.
	clock = clock.Add(31 * time.Second)

	code, out = scrape(t, prometheus.Gatherers{reg, in})
	if code != 200 {
		t.Fatalf("scrape status = %d, want 200; body:\n%s", code, out)
	}
	if strings.Contains(out, "alive_total") {
		t.Errorf("dead runner's series must disappear:\n%s", out)
	}
	// The xflow_runner_up gauge must read 0 after the runner is judged dead.
	upOut = gatherText(t, m.Registry())
	if !strings.Contains(upOut, `xflow_runner_up{runner_id="runner-a"} 0`) {
		t.Errorf("xflow_runner_up must be 0 after runner is dead; got:\n%s", upOut)
	}
}

// gatherText renders all families from a registry as exposition text.
func gatherText(t *testing.T, reg prometheus.Gatherer) string {
	t.Helper()
	code, out := scrape(t, prometheus.Gatherers{reg})
	if code != 200 {
		t.Fatalf("gatherText: unexpected status %d; body:\n%s", code, out)
	}
	return out
}
