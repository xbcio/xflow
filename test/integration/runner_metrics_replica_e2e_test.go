//go:build integration

package integration

import (
	"strings"
	"testing"
	"time"
)

// TestRunnerMetricsVisibleFromOtherReplica proves the inbox is shared, not
// process-local: the runner reports to replica A and the series is scraped off
// replica B.
//
// Both replicas share one Redis and one MySQL, which is the production shape.
// The runner points only at A — it never learns B exists. If the inbox were a
// process-local map (the obvious and wrong implementation), B's /metrics would
// have the server's own families and none of the runner's, and Prometheus would
// see the runner series flicker in and out with the scrape's replica affinity —
// worse than not seeing it at all.
func TestRunnerMetricsVisibleFromOtherReplica(t *testing.T) {
	redisAddr := requireRedis(t)
	dsn := requireMySQL(t)
	serverBin, runnerBin := buildR8Binaries(t)
	tokensFile := writeR8TokensFile(t)

	addrA, metricsA := freeAddr(t), freeAddr(t)
	addrB, metricsB := freeAddr(t), freeAddr(t)

	// Replica A: the runner reports here.
	urlA, _, stopA := mpStartMetricsProxyServer(t, serverBin, addrA, metricsA, redisAddr, dsn, tokensFile)
	t.Cleanup(func() { stopA() })

	// Replica B: shares the same Redis (and MySQL), but the runner never contacts it.
	_, _, stopB := mpStartMetricsProxyServer(t, serverBin, addrB, metricsB, redisAddr, dsn, tokensFile)
	t.Cleanup(func() { stopB() })

	// The runner knows only replica A — this is what guarantees the asymmetry.
	runner := mpStartReportingRunner(t, runnerBin, urlA, "runner-replica-a")
	t.Cleanup(func() { runner.stop(t) })

	// Positive half: A sees it. Asserting B first would leave a B-side failure
	// ambiguous between "not shared" and "never reported at all".
	//
	// Wait for xflow_runner_up first: this is emitted by the inbox's Gather only
	// when (1) the payload is in Redis AND (2) the runner is IsLive. Both must
	// hold before reports_total (a runner-originated metric in the payload) can
	// appear. Waiting on the bare runner_id label would match
	// xflow_runner_metrics_received_total (a server-side counter) immediately,
	// before the inbox has decoded and surfaced the runner's families.
	metricsURLa := "http://" + metricsA + "/metrics"
	mpWaitForServerMetric(t, metricsURLa, `xflow_runner_up{`, 60*time.Second)

	// reports_total is incremented AFTER a send completes, so it is absent from
	// the first payload and present from the second. Wait for it explicitly.
	bodyA := mpWaitForServerMetric(t, metricsURLa, "xflow_runner_metrics_reports_total", 60*time.Second)
	if !strings.Contains(bodyA, `runner_id="runner-replica-a"`) {
		t.Fatalf("replica A has reports_total but no runner_id label:\n%s", mpHeadOf(bodyA, 4000))
	}

	// The load-bearing half: B, which the runner never contacted.
	// Same wait sequence: first prove the inbox fires on B, then check the
	// runner-originated family.
	metricsURLb := "http://" + metricsB + "/metrics"
	mpWaitForServerMetric(t, metricsURLb, `xflow_runner_up{`, 60*time.Second)
	bodyB := mpWaitForServerMetric(t, metricsURLb, "xflow_runner_metrics_reports_total", 60*time.Second)
	if !strings.Contains(bodyB, `runner_id="runner-replica-a"`) {
		t.Fatalf("replica B sees reports_total but not the runner_id label "+
			"— the inbox is not labeling correctly:\n%s", mpHeadOf(bodyB, 4000))
	}

	// B must also still serve its OWN metrics: a merge that 500s or silently
	// drops the server's families would be a worse regression than a missing
	// runner series. The server uses a custom prometheus.Registry (not
	// DefaultGatherer), so go_* families are absent. Assert on a server-side
	// xflow counter that proves the server registry merged.
	if !strings.Contains(bodyB, "xflow_runner_metrics_received_total") &&
		!strings.Contains(bodyB, "xflow_outbox_pending") {
		t.Fatalf("replica B lost its own server-side metrics while merging the inbox:\n%s", mpHeadOf(bodyB, 4000))
	}
}

// TestDeadRunnerSeriesDisappearFromReplica is the liveness half of the same
// invariant, at the process level: kill the runner and both replicas must stop
// emitting its series. Task 5's unit test proves the Gather filter; this proves
// the wired-up production path agrees, using the real 30s DefaultRunnerLiveTTL.
func TestDeadRunnerSeriesDisappearFromReplica(t *testing.T) {
	redisAddr := requireRedis(t)
	dsn := requireMySQL(t)
	serverBin, runnerBin := buildR8Binaries(t)
	tokensFile := writeR8TokensFile(t)

	addrA, metricsA := freeAddr(t), freeAddr(t)
	urlA, _, stopA := mpStartMetricsProxyServer(t, serverBin, addrA, metricsA, redisAddr, dsn, tokensFile)
	t.Cleanup(func() { stopA() })

	runner := mpStartReportingRunner(t, runnerBin, urlA, "runner-dies")
	metricsURL := "http://" + metricsA + "/metrics"

	// Prove the positive first. A reverse assertion that never established the
	// positive passes trivially when nothing was ever written.
	// Wait for xflow_runner_up first to confirm the inbox is working, then for
	// the runner-originated family which is what we will assert disappears.
	mpWaitForServerMetric(t, metricsURL, `xflow_runner_up{`, 60*time.Second)
	mpWaitForServerMetric(t, metricsURL, "xflow_runner_metrics_reports_total", 60*time.Second)

	// SIGKILL, not stop: a graceful exit could deregister and prove the wrong
	// thing. The interesting case is a runner that vanishes.
	runner.kill(t)

	// DefaultRunnerLiveTTL is 30s and IsLive is what gates output, so allow
	// 30s + scrape slack. The Redis key's own 90s TTL is deliberately longer;
	// the point of this wait is that IsLive fires FIRST — if the series only
	// vanished at 90s, that would mean nothing is filtering on liveness and the
	// disappearance is just key expiry.
	//
	// What disappears: runner-originated families like
	// xflow_runner_metrics_reports_total. What STAYS: server-side counters
	// (xflow_runner_metrics_received_total) and xflow_runner_up (which flips to
	// 0 but remains registered). We check for the absence of the runner's own
	// family, not the bare runner_id label.
	killedAt := time.Now()
	const disappearBy = 60 * time.Second // 30s IsLive + generous scrape slack
	deadline := killedAt.Add(100 * time.Second)
	var goneAfter time.Duration
	gone := false
	for time.Now().Before(deadline) {
		body := mpFetchBody(t, metricsURL)
		if !strings.Contains(body, "xflow_runner_metrics_reports_total") {
			goneAfter = time.Since(killedAt)
			gone = true
			break
		}
		time.Sleep(time.Second)
	}
	if !gone {
		t.Fatalf("dead runner's series never disappeared within %v", time.Since(killedAt))
	}
	if goneAfter > disappearBy {
		t.Fatalf("series survived %v after the kill — it disappeared with the 90s key TTL, "+
			"not with the 30s IsLive gate, meaning liveness filtering is not wired", goneAfter)
	}
}
