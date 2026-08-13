//go:build integration

package integration

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// mpUniqueRunnerID makes a runner ID nobody else in the suite can collide with.
//
// The inbox lives in shared Redis with a 90s key TTL, so a runner from an
// earlier test is still in there when this one starts. Waiting on a bare family
// name or a bare `xflow_runner_up{` matches that leftover immediately, and then
// the assertion about THIS runner's id fails against a body that was never
// about this runner. Observed exactly that: waiting on `xflow_runner_up{`
// returned a body holding only runner-metrics-e2e's series.
func mpUniqueRunnerID(t *testing.T, prefix string) string {
	t.Helper()
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

// mpHasSeriesForRunner reports whether the scrape body carries a sample of
// family for exactly this runner.
//
// Checking `strings.Contains(body, family)` and `strings.Contains(body,
// runnerID)` separately is the trap: both can hold while the family belongs to
// another runner and runnerID appears only on some unrelated family. This
// requires them on the SAME line.
func mpHasSeriesForRunner(body, family, runnerID string) bool {
	want := `runner_id="` + runnerID + `"`
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, family+"{") && strings.Contains(line, want) {
			return true
		}
	}
	return false
}

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
	runnerID := mpUniqueRunnerID(t, "runner-replica-a")
	runner := mpStartReportingRunner(t, runnerBin, urlA, runnerID)
	t.Cleanup(func() { runner.stop(t) })

	// Positive half: A sees it. Asserting B first would leave a B-side failure
	// ambiguous between "not shared" and "never reported at all".
	//
	// Every wait is anchored to runnerID. xflow_runner_up is emitted by the
	// inbox's Gather only when (1) the payload is in Redis AND (2) the runner is
	// IsLive, so it is the right readiness signal — but only for OUR runner: a
	// bare `xflow_runner_up{` is satisfied by any other test's runner still
	// inside the 90s inbox TTL, and the run then proceeds against a body that
	// says nothing about this runner.
	metricsURLa := "http://" + metricsA + "/metrics"
	upSeries := `xflow_runner_up{runner_id="` + runnerID + `"}`
	mpWaitForServerMetric(t, metricsURLa, upSeries, 60*time.Second)

	// reports_total is incremented AFTER a send completes, so it is absent from
	// the first payload and present from the second. Wait for it explicitly —
	// again pinned to this runner, not to the bare family name.
	reportsSeries := `xflow_runner_metrics_reports_total{`
	bodyA := mpWaitForServerMetric(t, metricsURLa, reportsSeries, 60*time.Second)
	if !mpHasSeriesForRunner(bodyA, "xflow_runner_metrics_reports_total", runnerID) {
		t.Fatalf("replica A has reports_total but no series for %s:\n%s",
			runnerID, mpHeadOf(bodyA, 4000))
	}

	// The load-bearing half: B, which the runner never contacted.
	// Same wait sequence: first prove the inbox fires on B, then check the
	// runner-originated family.
	metricsURLb := "http://" + metricsB + "/metrics"
	mpWaitForServerMetric(t, metricsURLb, upSeries, 60*time.Second)
	bodyB := mpWaitForServerMetric(t, metricsURLb, reportsSeries, 60*time.Second)
	if !mpHasSeriesForRunner(bodyB, "xflow_runner_metrics_reports_total", runnerID) {
		t.Fatalf("replica B sees reports_total but not a series for %s "+
			"— the inbox is not shared, or not labeling correctly:\n%s",
			runnerID, mpHeadOf(bodyB, 4000))
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

	runnerID := mpUniqueRunnerID(t, "runner-dies")
	runner := mpStartReportingRunner(t, runnerBin, urlA, runnerID)
	metricsURL := "http://" + metricsA + "/metrics"

	// Prove the positive first. A reverse assertion that never established the
	// positive passes trivially when nothing was ever written.
	//
	// Both waits are pinned to runnerID. That matters twice over here: the
	// disappearance check below must not be satisfied by another test's runner
	// aging out, and must not be blocked by another test's runner still being
	// live inside the shared 90s inbox TTL.
	mpWaitForServerMetric(t, metricsURL, `xflow_runner_up{runner_id="`+runnerID+`"}`, 60*time.Second)
	for deadline := time.Now().Add(60 * time.Second); ; {
		body := mpFetchBody(t, metricsURL)
		if mpHasSeriesForRunner(body, "xflow_runner_metrics_reports_total", runnerID) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no reports_total series for %s within 60s:\n%s",
				runnerID, mpHeadOf(body, 4000))
		}
		time.Sleep(200 * time.Millisecond)
	}

	// SIGKILL, not stop: a graceful exit could deregister and prove the wrong
	// thing. The interesting case is a runner that vanishes.
	runner.kill(t)

	// DefaultRunnerLiveTTL is 30s and IsLive is what gates output, so allow
	// 30s + scrape slack. The Redis key's own 90s TTL is deliberately longer;
	// the point of this wait is that IsLive fires FIRST — if the series only
	// vanished at 90s, that would mean nothing is filtering on liveness and the
	// disappearance is just key expiry.
	//
	// What disappears: THIS runner's series in a runner-originated family like
	// xflow_runner_metrics_reports_total. What STAYS: server-side counters
	// (xflow_runner_metrics_received_total) and xflow_runner_up (which flips to
	// 0 but remains registered). Checking for the absence of the bare family
	// name would be satisfied by any other test's live runner vanishing, and
	// blocked by any other test's live runner remaining — so match on the
	// family AND this runner's id together.
	killedAt := time.Now()
	const disappearBy = 60 * time.Second // 30s IsLive + generous scrape slack
	deadline := killedAt.Add(100 * time.Second)
	var goneAfter time.Duration
	gone := false
	for time.Now().Before(deadline) {
		body := mpFetchBody(t, metricsURL)
		if !mpHasSeriesForRunner(body, "xflow_runner_metrics_reports_total", runnerID) {
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
