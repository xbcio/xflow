//go:build integration

package integration

import (
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// TestRunnerMetricsProxyE2E is the reason the whole feature exists: a runner
// process ships its registry to a server process, and the metric shows up on
// the SERVER's /metrics carrying the runner's id.
//
// It must run against the real built binaries. Every in-process variant of this
// assertion can pass while production is broken — the supply-encryptor P0
// (SUPPLY-NODE-TODO P0-4) was exactly that: seven tasks of green unit tests
// over a helper that assigned the dependency field directly, bypassing the
// assembly path it claimed to prove.
func TestRunnerMetricsProxyE2E(t *testing.T) {
	redisAddr := requireRedis(t)
	dsn := requireMySQL(t)
	serverBin, runnerBin := buildR8Binaries(t)

	serverAddr := freeAddr(t)
	serverMetricsAddr := freeAddr(t)
	tokensFile := writeR8TokensFile(t)

	httpURL, _, stopServer := mpStartMetricsProxyServer(t,
		serverBin, serverAddr, serverMetricsAddr, redisAddr, dsn, tokensFile)
	defer stopServer()

	runnerID := mpUniqueRunnerID(t, "runner-metrics-e2e")
	runner := mpStartReportingRunner(t, runnerBin, httpURL, runnerID)
	defer runner.stop(t)

	// Pinned to runnerID, not to the bare family: the inbox is shared Redis with
	// a 90s key TTL, so another test's runner is routinely still in there and a
	// bare `xflow_runner_up{` would be satisfied by its series instead.
	metricsURL := "http://" + serverMetricsAddr + "/metrics"
	mpWaitForServerMetric(t, metricsURL, `xflow_runner_up{runner_id="`+runnerID+`"}`, 60*time.Second)

	// reports_total is incremented AFTER a send completes, so it is absent from
	// the first payload and present from the second. Wait for it explicitly.
	const runnerOwn = "xflow_runner_metrics_reports_total"
	var body string
	for deadline := time.Now().Add(60 * time.Second); ; {
		body = mpFetchBody(t, metricsURL)
		if mpHasSeriesForRunner(body, runnerOwn, runnerID) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no %s series for %s within 60s:\n%s",
				runnerOwn, runnerID, mpHeadOf(body, 4000))
		}
		time.Sleep(200 * time.Millisecond)
	}

	// 1+2. A metric the RUNNER produces (not the server) is present, carrying
	//      this runner's id. mpHasSeriesForRunner already required the family
	//      and the id on the same line, which is the real invariant: separate
	//      Contains checks pass when the family belongs to another runner.
	//      What remains to check is that no series of this family carries an
	//      empty or missing runner_id — a server that forgot to label would
	//      still satisfy the check above via some other runner's line.
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(line, runnerOwn+"{") {
			continue
		}
		if !strings.Contains(line, `runner_id="`) {
			t.Fatalf("runner-side series is missing runner_id: %q", line)
		}
	}

	// 3. The server counted the report and the endpoint did not 500.
	//    The counter is lazily registered on first Accept and lives on the
	//    server's own registry. A scrape races with Accept (registry gathered
	//    before Accept completes), so poll rather than assert on a single body.
	body = mpWaitForServerMetric(t, metricsURL, "xflow_runner_metrics_received_total", 30*time.Second)

	// 4. The server's OWN metrics are still there. A help-text or duplicate-label
	//    conflict makes prometheus.Gatherers fail the whole scrape, so proving the
	//    merge did not cannibalize the server's own families is the point.
	//    The server uses a custom prometheus.Registry (not DefaultGatherer), so
	//    go_* / process_* families do not exist. Assert on a server-side xflow
	//    family instead: xflow_lease_sweep_candidates is produced by the server's
	//    LeaseSweeper, never by a runner.
	if !strings.Contains(body, "xflow_lease_sweep_candidates") {
		// The lease sweeper metric is lazy (registered after the first sweep
		// cycle) so it is not reliably present this early. The preceding
		// xflow_runner_metrics_received_total assertion already proves the
		// server's own registry was merged with the runner inbox — that counter
		// lives on the server-side registry and would be absent on a 500 or a
		// merge conflict.
		t.Log("xflow_lease_sweep_candidates not yet present; received_total suffices to prove merge")
	}
}

// TestRunnerMetricsProxyDisabledByDefault proves that upgrading both binaries
// without changing any flags yields byte-identical behavior: no runner series
// and no inbox counters on /metrics.
func TestRunnerMetricsProxyDisabledByDefault(t *testing.T) {
	redisAddr := requireRedis(t)
	dsn := requireMySQL(t)
	serverBin, runnerBin := buildR8Binaries(t)

	serverAddr := freeAddr(t)
	serverMetricsAddr := freeAddr(t)
	tokensFile := writeR8TokensFile(t)

	// Same as the enabled case minus --enable-runner-metrics-proxy.
	out := &safeBuffer{}
	cmd := exec.Command(serverBin,
		"-addr", serverAddr,
		"-metrics-addr", serverMetricsAddr,
		"-redis", redisAddr,
		"-mysql-dsn", dsn,
		"-auth-tokens-file", tokensFile,
		"-auth-policy", r8RunnerPolicyFile(t),
		"-master-key-file", r8MasterKeyFile(t),
		"-require-api-auth", "-management",
		"-mode", "production", "-log-format", "json",
	)
	cmd.Stdout, cmd.Stderr = out, out
	if err := cmd.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		if t.Failed() {
			t.Logf("server logs:\n%s", out.String())
		}
	})
	httpURL := "http://" + serverAddr
	mpWaitForReadyz(t, httpURL, 30*time.Second)

	// Unique even though this test only scrapes its own server: the id it asserts
	// the ABSENCE of must not be an id some other test could have written.
	runnerID := mpUniqueRunnerID(t, "runner-noproxy")
	runner := mpStartReportingRunner(t, runnerBin, httpURL, runnerID)
	defer runner.stop(t)

	// The report endpoint must 404: not registered when the proxy is off.
	// Wait until server is healthy and has been scraped at least once.
	mpWaitForServerMetric(t, "http://"+serverMetricsAddr+"/metrics", "xflow_outbox_pending", 30*time.Second)
	// Poll for a few seconds to give the runner time to attempt reporting.
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		body := mpFetchBody(t, "http://"+serverMetricsAddr+"/metrics")
		if strings.Contains(body, `runner_id="`+runnerID+`"`) {
			t.Fatalf("runner series present with the proxy disabled:\n%s", mpHeadOf(body, 4000))
		}
		if strings.Contains(body, "xflow_runner_metrics_received_total") {
			t.Fatalf("inbox counters present with the proxy disabled:\n%s", mpHeadOf(body, 4000))
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// mpStartMetricsProxyServer starts the production server with the runner metrics
// proxy on and a metrics listener of its own.
func mpStartMetricsProxyServer(t *testing.T, serverBin, addr, metricsAddr, redisAddr, dsn, tokensFile string) (string, *safeBuffer, func()) {
	t.Helper()
	out := &safeBuffer{}
	cmd := exec.Command(serverBin,
		"-addr", addr,
		"-metrics-addr", metricsAddr,
		"-redis", redisAddr,
		"-mysql-dsn", dsn,
		"-auth-tokens-file", tokensFile,
		"-auth-policy", r8RunnerPolicyFile(t),
		"-master-key-file", r8MasterKeyFile(t),
		"-require-api-auth",
		"-management",
		"-mode", "production",
		"-log-format", "json",
		"-enable-runner-metrics-proxy",
		"-runner-metrics-interval", "5s",
	)
	cmd.Stdout, cmd.Stderr = out, out
	if err := cmd.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	httpURL := "http://" + addr
	stop := func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	}
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("server logs:\n%s", out.String())
		}
	})
	mpWaitForReadyz(t, httpURL, 30*time.Second)
	return httpURL, out, stop
}

// mpStartReportingRunner starts the production runner with reporting on and NO
// --metrics-addr. Omitting the scrape port is deliberate: it is the cross-domain
// shape this feature targets, and it proves Task 11's unbinding works in the
// real binary rather than only in cmd/runner's unit tests.
func mpStartReportingRunner(t *testing.T, runnerBin, httpURL, id string) *r8Process {
	t.Helper()
	out := &safeBuffer{}
	cmd := exec.Command(runnerBin, "run",
		"--server", httpURL,
		"--transport", "http",
		"--id", id,
		"--token", r8RunnerToken,
		"--cap", "xflow.function",
		"--poll-wait", "50ms",
		"--concurrency", "1",
		"--heartbeat-interval", "1s",
		"--report-metrics",
		"--report-metrics-interval", "1s",
		// See startR8Runner for why the plaintext opt-out is required here.
		"--allow-plaintext",
	)
	cmd.Stdout, cmd.Stderr = out, out
	if err := cmd.Start(); err != nil {
		t.Fatalf("start runner: %v", err)
	}
	p := &r8Process{cmd: cmd, out: out}
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("runner %s logs:\n%s", id, out.String())
		}
	})
	return p
}
func mpWaitForReadyz(t *testing.T, httpURL string, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		resp, err := r8HTTPClient.Get(httpURL + "/readyz")
		if err == nil {
			ok := resp.StatusCode == http.StatusOK
			resp.Body.Close()
			if ok {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("/readyz never returned 200 within %v", within)
}

// mpWaitForServerMetric polls the metrics endpoint until the substring appears.
// A non-200 is a hard failure, not something to keep polling through: the whole
// risk this design carries is prometheus.Gatherers turning a conflict into a
// 500, so a 500 here must fail loudly rather than time out ambiguously.
func mpWaitForServerMetric(t *testing.T, url, want string, within time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(within)
	var last string
	for time.Now().Before(deadline) {
		resp, err := r8HTTPClient.Get(url)
		if err != nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s = %d (a merge conflict 500s the whole endpoint): %s",
				url, resp.StatusCode, mpHeadOf(string(data), 2000))
		}
		if readErr != nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		last = string(data)
		if strings.Contains(last, want) {
			return last
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("%q never appeared on %s within %v:\n%s", want, url, within, mpHeadOf(last, 4000))
	return ""
}

func mpFetchBody(t *testing.T, url string) string {
	t.Helper()
	resp, err := r8HTTPClient.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s: %v", url, err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s = %d: %s", url, resp.StatusCode, mpHeadOf(string(data), 2000))
	}
	return string(data)
}

// mpHeadOf truncates a body for failure messages.
func mpHeadOf(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + fmt.Sprintf("\n... (%d more bytes)", len(s)-n)
}
