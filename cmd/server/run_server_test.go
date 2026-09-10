package main

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

// The tests in this file drive runServer itself — the 190-line function that
// builds every dependency, calls buildServerOptions, constructs the SDK server
// and blocks in srv.Run. Everything else in this package's tests reconstructs
// pieces of that wiring by hand (main_test.go's "RunServer" tests call
// apiserver.New directly and never reach runServer), so a change that broke
// the assembly order, dropped a dependency, or stopped honouring the shutdown
// signal would leave all of them green.
//
// Two things make the whole function reachable from a test: runServer takes a
// caller-supplied ctx (main passes context.Background(), which never cancels,
// so the production lifecycle is unchanged), and the listen addresses come
// from flags, so the server can be pointed at an OS-assigned loopback port.

const (
	// runServerReadyTimeout bounds how long a start-up may take before the
	// attempt is written off. runServerShutdownTimeout bounds the drain;
	// apiserver's own shutdown budget is 15s, so this must exceed it to
	// distinguish "drain overran its budget" from "never returned at all".
	runServerReadyTimeout    = 20 * time.Second
	runServerShutdownTimeout = 45 * time.Second
)

// reserveLoopbackAddr returns a loopback address whose port the OS picked, by
// binding :0 and releasing it immediately. Hard-coding a port would collide
// with whatever else is listening on a shared CI machine.
//
// Handing cfg.addr a literal "127.0.0.1:0" is not an option even though the
// kernel would assign a port just the same: apiserver.Run passes the
// configured address straight to http.Server.ListenAndServe and never
// publishes the address the listener actually got, so a test that used :0
// would have no way to find the server it just started. Reserving the port
// here is what makes the readiness poll below possible.
//
// The cost is an inherent TOCTOU race: another process can claim the port
// between Close and the server's own bind. The caller retries on a fresh
// address rather than flaking.
func reserveLoopbackAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve loopback address: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("release loopback address: %v", err)
	}
	return addr
}

// TestRunServerServesReadyzThenShutsDownGracefully drives runServer end to end
// on the in-memory dev path: real argv through parseServerConfig, every
// dependency built (logger, authenticator, tracing, audit sink, backend
// target, enrollment stores), buildServerOptions, xflowsdk.NewServer, and
// srv.Run actually serving on a TCP port — then cancels the context and pins
// that the graceful drain completes with a nil error.
//
// Readiness is established by polling the server's own /readyz endpoint until
// it reports ready, never by sleeping a guessed interval: apiserver publishes
// readiness there and takes it down again at the top of its drain, so a 200
// with ready=true is proof the whole stack is up and serving. That is also why
// --management is set: /readyz lives in the management module, which is the
// only readiness signal apiserver exposes. Cancelling before the server is
// genuinely up would still return nil while testing almost nothing.
func TestRunServerServesReadyzThenShutsDownGracefully(t *testing.T) {
	const maxAttempts = 3
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		err := runServerLifecycleAttempt(t, reserveLoopbackAddr(t))
		if err == nil {
			return
		}
		// Most likely the reserved port was taken in the window between
		// reserveLoopbackAddr's Close and the server's bind. Retry on a fresh
		// address instead of flaking. A genuine assertion failure does not
		// come back through here — it fails the test inside the attempt.
		lastErr = err
	}
	t.Fatalf("runServer never reached a ready state after %d attempts: %v", maxAttempts, lastErr)
}

// runServerLifecycleAttempt runs one attempt against httpAddr. It returns a
// non-nil error only when the server never came up (the caller retries); every
// assertion about behaviour fails the test directly.
func runServerLifecycleAttempt(t *testing.T, httpAddr string) error {
	t.Helper()

	// From real argv, so the flag names and parseServerConfig's validation are
	// part of what is covered. grpc-addr is a literal :0 — nothing needs to
	// reach the gRPC listener, only to prove runServer opens it without error.
	cfg, err := parseServerConfig([]string{
		"-mode", "dev",
		"-memory",
		"-management",
		"-addr", httpAddr,
		"-grpc-addr", "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("parseServerConfig: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runServer(ctx, cfg) }()

	if err := waitForReadyz(t, httpAddr, done); err != nil {
		// Release the goroutine before giving up on this address, so a failed
		// attempt cannot leak a running server holding a port.
		cancel()
		select {
		case <-done:
		case <-time.After(runServerShutdownTimeout):
			t.Fatalf("runServer did not return after a failed start attempt (%v)", err)
		}
		return err
	}

	cancel()

	select {
	case err := <-done:
		// Pinned to exactly nil, not "nil or context.Canceled": apiserver.Run
		// treats a cancelled ctx as the ordinary shutdown trigger and reports
		// only real drain failures, so runServer must not surface the
		// cancellation itself as an error. main log.Fatals on a non-nil
		// return, which would turn every clean SIGTERM into a failed exit.
		if err != nil {
			t.Fatalf("runServer(cancelled ctx) = %v, want nil: cancelling the context "+
				"must drive the graceful drain to a clean exit", err)
		}
	case <-time.After(runServerShutdownTimeout):
		t.Fatalf("runServer did not return within %v of ctx cancellation; the signal "+
			"context is no longer rooted at the caller's ctx, or the drain is stuck",
			runServerShutdownTimeout)
	}
	return nil
}

// waitForReadyz polls httpAddr's /readyz until it reports ready. It gives up
// early if runServer exits first (a start-up failure — most often a lost race
// for the reserved port), so a broken start is not paid for with the full
// timeout.
func waitForReadyz(t *testing.T, httpAddr string, done <-chan error) error {
	t.Helper()
	client := &http.Client{Timeout: time.Second}
	url := "http://" + httpAddr + "/readyz"
	deadline := time.Now().Add(runServerReadyTimeout)
	var lastStatus string
	for time.Now().Before(deadline) {
		select {
		case err := <-done:
			return &startupError{msg: "runServer returned before serving: " + errString(err)}
		default:
		}
		resp, err := client.Get(url)
		if err != nil {
			lastStatus = "connect: " + err.Error()
		} else {
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			// 200 with ready=true is the server's own statement that the
			// control plane and its transports are up. Anything else (503
			// while warming up, or the 404 you get if the management module
			// was never wired) is not readiness.
			if resp.StatusCode == http.StatusOK && strings.Contains(string(body), `"ready":true`) {
				return nil
			}
			lastStatus = "status " + resp.Status + " body " + strings.TrimSpace(string(body))
		}
		time.Sleep(10 * time.Millisecond)
	}
	return &startupError{msg: "/readyz never reported ready within " +
		runServerReadyTimeout.String() + "; last: " + lastStatus}
}

type startupError struct{ msg string }

func (e *startupError) Error() string { return e.msg }

func errString(err error) string {
	if err == nil {
		return "<nil>"
	}
	return err.Error()
}

// TestRunServerProductionGateExplainsMissingFlags pins that runServer refuses
// to start in production posture when the required pieces are missing, and
// that the refusal is phrased in this binary's flags.
//
// This reaches the same dependency construction as the test above and then the
// other branch out of xflowsdk.NewServer: the error path through
// explainProductionGate. apiserver's own ProductionGateError says "apiserver:
// production posture not met" and names no flags at all, so asserting on the
// flag names is what proves runServer still routes the failure through
// explainProductionGate rather than returning the raw error — an operator
// given the raw error is told what is missing but not what to type.
//
// No listener is involved: NewServer fails, so srv.Run is never reached.
func TestRunServerProductionGateExplainsMissingFlags(t *testing.T) {
	// The master key is read from the process environment, so clear it to keep
	// the unmet-requirement list the same on a developer machine that happens
	// to export one. The two requirements asserted below are unmet either way.
	t.Setenv("XFLOW_MASTER_KEY", "")

	cfg, err := parseServerConfig([]string{
		"-mode", "production",
		"-memory",
		"-addr", "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("parseServerConfig: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	// Run in a goroutine purely as a hang guard: if the gate ever stopped
	// rejecting this config, runServer would fall through to srv.Run and block
	// forever instead of failing the test.
	go func() { done <- runServer(ctx, cfg) }()

	var runErr error
	select {
	case runErr = <-done:
	case <-time.After(runServerShutdownTimeout):
		t.Fatalf("runServer did not return within %v; --mode=production started a server "+
			"with no --auth-tokens-file and no --mysql-dsn", runServerShutdownTimeout)
	}

	if runErr == nil {
		t.Fatal("runServer(--mode=production) = nil, want an error: production must not " +
			"start without a principal-auth registry or a durable audit sink")
	}

	got := runErr.Error()
	// The header proves the ProductionGateError was recognised and rewritten;
	// the flag names prove each unmet requirement carries its remediation.
	for _, want := range []string{
		"--mode=production is not satisfied",
		"--auth-tokens-file",
		"--mysql-dsn",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("runServer(--mode=production) error does not mention %q; "+
				"an operator cannot act on it.\ngot: %s", want, got)
		}
	}
}
