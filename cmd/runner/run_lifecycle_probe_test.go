package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	xflowsdk "github.com/xbcio/xflow/sdk/xflow"
)

// stubBlockingRunnerServiceFactory installs a fake SDK runner whose Run blocks
// until ctx is cancelled — the shape runRunner actually reaches once assembly
// succeeds. Unlike stubRunnerServiceFactory (which returns immediately, so
// runRunner's deferred metrics-server shutdown races its own ListenAndServe
// goroutine), this keeps runRunner parked on "return runner.Run(ctx)" so the
// real --metrics-addr HTTP listener — registerLifecycleProbes included — comes
// up and stays up for the test to hit.
func stubBlockingRunnerServiceFactory() func() {
	previous := newRunnerService
	newRunnerService = func(_ xflowsdk.RunnerConfig, _ ...xflowsdk.RunnerOption) (runnerService, error) {
		return runnerServiceFunc(func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		}), nil
	}
	return func() { newRunnerService = previous }
}

// freeLoopbackAddr asks the OS for a currently unused loopback port by binding
// to :0 and immediately releasing it, rather than hardcoding one (which would
// flake against anything else already listening). There is an inherent TOCTOU
// race between this Close and the metrics server's own ListenAndServe binding
// the same port; the caller retries on a fresh address if that race is lost.
func freeLoopbackAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatalf("close probe listener: %v", err)
	}
	return addr
}

// waitForHTTPServer polls addr until something accepts a connection and
// answers, or deadline elapses.
func waitForHTTPServer(client *http.Client, addr string, deadline time.Duration) error {
	url := "http://" + addr + "/healthz"
	var lastErr error
	giveUp := time.Now().Add(deadline)
	for time.Now().Before(giveUp) {
		resp, err := client.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			return nil
		}
		lastErr = err
		time.Sleep(20 * time.Millisecond)
	}
	return lastErr
}

// TestRunRunnerServesLifecycleProbesOverMetricsAddr pins that runRunner itself
// mounts /healthz and /readyz on the --metrics-addr listener, by hitting them
// with real HTTP requests through the whole CLI/YAML assembly path.
//
// TestLifecycleProbeHandlers (lifecycle_test.go) already exercises
// registerLifecycleProbes directly against a bare mux, and would keep passing
// even if runRunner stopped calling it at all — that call is exactly what this
// test pins. Without registerLifecycleProbes(probeMux, lifecycle) in runRunner,
// both paths fall through to the mux's default handler and 404 instead of
// reaching the lifecycle state, so the assertion below checks the specific 503
// reason string, not merely "non-200" — a bare non-200 check would not
// distinguish the intended 503 from that 404.
func TestRunRunnerServesLifecycleProbesOverMetricsAddr(t *testing.T) {
	restore := stubBlockingRunnerServiceFactory()
	defer restore()

	client := &http.Client{Timeout: 2 * time.Second}
	const maxAttempts = 3
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := probeAttempt(t, client, freeLoopbackAddr(t)); err != nil {
			// Most likely lost the bind race for this port to something else in
			// the window between freeLoopbackAddr's Close and ListenAndServe.
			// Retry on a fresh address rather than flaking outright.
			lastErr = err
			continue
		}
		return
	}
	t.Fatalf("metrics server never accepted a connection after %d attempts: %v", maxAttempts, lastErr)
}

// probeAttempt runs one attempt against addr: it starts runRunner in the
// background, waits for its metrics listener to come up, and — if it does —
// asserts the lifecycle probes against it. It returns nil once the probe
// assertions have run, or the wait error when the listener never came up (the
// caller retries on a fresh address in that case).
//
// The cancel-and-drain of the background runRunner goroutine lives in a
// single deferred closure so it runs exactly once, on every exit from this
// function — including the runtime.Goexit that t.Fatalf performs inside
// assertLifecycleProbes. Previously that cleanup was two bare statements
// (cancel() then <-done) after the assertLifecycleProbes call: on exactly the
// regression this test exists to catch, assertLifecycleProbes's t.Fatalf
// unwinds past those statements without running them, so the stubbed Run
// stays parked on <-ctx.Done(), runRunner never returns, its deferred
// metricsServer.Shutdown never runs, and a goroutine plus the bound TCP port
// leak for the rest of the test binary's life. Using t.Errorf (not
// t.Fatalf) inside this deferred function avoids calling FailNow while a
// Goexit unwind from assertLifecycleProbes may already be in flight.
func probeAttempt(t *testing.T, client *http.Client, addr string) error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- executeRootWithOptions(commandOptions{
			runFunc: func(cfg runnerConfig) error {
				return runRunner(ctx, cfg)
			},
			out: &bytes.Buffer{},
			err: &bytes.Buffer{},
		}, "run", "--server", "http://server:8080", "--metrics-addr", addr, "--allow-plaintext")
	}()
	defer func() {
		cancel()
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("runRunner returned %v, want nil or context.Canceled after cancellation", err)
		}
	}()

	if err := waitForHTTPServer(client, addr, 2*time.Second); err != nil {
		return err
	}

	assertLifecycleProbes(t, client, addr)
	return nil
}

func assertLifecycleProbes(t *testing.T, client *http.Client, addr string) {
	t.Helper()

	readyResp, err := client.Get("http://" + addr + "/readyz")
	if err != nil {
		t.Fatalf("GET /readyz: %v", err)
	}
	defer readyResp.Body.Close()
	readyBody, err := io.ReadAll(readyResp.Body)
	if err != nil {
		t.Fatalf("read /readyz body: %v", err)
	}
	if readyResp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("/readyz status = %d (body %q), want 503 — a runner that never "+
			"registered must not read as ready, and must not 404 either (which is "+
			"what happens when registerLifecycleProbes is never wired into runRunner)",
			readyResp.StatusCode, string(readyBody))
	}
	const wantReason = "not registered with the control plane"
	if !strings.Contains(string(readyBody), wantReason) {
		t.Fatalf("/readyz body = %q, want it to contain %q", string(readyBody), wantReason)
	}

	healthResp, err := client.Get("http://" + addr + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	defer healthResp.Body.Close()
	if healthResp.StatusCode != http.StatusOK {
		t.Fatalf("/healthz status = %d, want 200 (liveness is unconditional)", healthResp.StatusCode)
	}
}
