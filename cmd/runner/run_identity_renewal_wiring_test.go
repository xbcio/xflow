package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	xflowsdk "github.com/xbcio/xflow/sdk/xflow"
)

// TODO §5: renew_test.go's TestDecideIdentityRenewal* cases only drive the
// pure decision function and assert its four return values. Nothing in the
// suite ran runRunner end to end and watched whether the renewal goroutine it
// spawns actually matches what decideIdentityRenewal decided -- a mutation to
// the if/else wiring in runRunner (run.go) could pass every one of those
// tests while silently starting or silently not starting the loop. The tests
// below close that gap through the startIdentityRenewal seam.

// identityRenewalCall records one invocation of the startIdentityRenewal
// seam. Sent over a channel rather than guarded by a bare bool: the seam runs
// on the goroutine runRunner spawns with `go`, and the test asserts from a
// different goroutine -- a bare bool write/read pair here is exactly the kind
// of race `go test -race` exists to catch.
type identityRenewalCall struct {
	runnerID string
	token    string
}

// stubIdentityRenewalSeam swaps startIdentityRenewal for a stub that reports
// each call on the returned channel instead of running the real renewal
// loop. The channel is buffered so the stub never blocks waiting for a
// receiver that may never come (the "not called" tests deliberately never
// drain it).
func stubIdentityRenewalSeam() (calls chan identityRenewalCall, restore func()) {
	previous := startIdentityRenewal
	calls = make(chan identityRenewalCall, 1)
	startIdentityRenewal = func(_ context.Context, _ renewClient, runnerID, token string, _ *slog.Logger) {
		calls <- identityRenewalCall{runnerID: runnerID, token: token}
	}
	return calls, func() { startIdentityRenewal = previous }
}

// stubBlockingRunnerServiceFactoryWithEntrySignal is stubBlockingRunnerServiceFactory
// (run_lifecycle_probe_test.go) plus one addition: it closes entered the
// instant Run is called. The identity-renewal decision in runRunner executes
// entirely before runner.Run(runCtx) is reached (run.go), in the same
// goroutine and strictly before that call in program order -- so once
// entered closes, the `if/else if` has already run to completion, including
// the `go startIdentityRenewal(...)` statement on the branch that takes it.
// That makes "wait for entered, then inspect calls" a deterministic way to
// observe the branch's outcome, with no sleep and no bare time.After standing
// in for a real synchronization point.
func stubBlockingRunnerServiceFactoryWithEntrySignal() (entered chan struct{}, restore func()) {
	previous := newRunnerService
	entered = make(chan struct{})
	newRunnerService = func(_ xflowsdk.RunnerConfig, _ ...xflowsdk.RunnerOption) (runnerService, error) {
		return runnerServiceFunc(func(ctx context.Context) error {
			close(entered)
			<-ctx.Done()
			return ctx.Err()
		}), nil
	}
	return entered, func() { newRunnerService = previous }
}

// writeIdentityFile seeds a file-backed identity store with a real issued
// identity at 0600, the permission fileIdentityStore.Load requires. The
// expires_at field is not part of the identity struct that Save/Load
// round-trips (runIdentityRenewal's doc comment: "the file deliberately does
// not store [ExpiresAt] -- identity file schema is unchanged by this task"),
// so it is included here only to mirror a realistic issued-identity payload;
// json.Unmarshal silently ignores it, which is what makes it safe to add
// without touching identity.go.
func writeIdentityFile(t *testing.T, path, runnerID, token string) {
	t.Helper()
	raw := `{"runner_id":"` + runnerID + `","token":"` + token + `","expires_at":"2030-01-01T00:00:00Z"}`
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatalf("seed identity file: %v", err)
	}
}

// waitForClosed blocks until ch closes or the safety-net deadline elapses. It
// is not used to pace or derive timing -- only as a failure mode for "this
// should have already happened" so a genuine regression fails the test
// instead of hanging the suite forever.
func waitForClosed(t *testing.T, ch <-chan struct{}, msg string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal(msg)
	}
}

// assertIdentityRenewalNeverCalled is the "not called" half of the wiring
// tests. Waiting for entered proves the if/else in runRunner has already run
// to completion (same goroutine, strictly before runner.Run in program
// order), so if the branch that calls startIdentityRenewal was correctly
// never taken, calls can never receive anything, ever -- the select below
// would then always take its time.After arm, deterministically, no matter how
// long it waits. The bounded wait exists only for the other side of that
// coin: a *regression* would launch the seam call from a separate goroutine
// (the `go` statement returns immediately, before its body runs), so a
// same-goroutine synchronization signal like entered cannot by itself prove
// the call will never land -- only that, in the correct implementation, nothing
// was ever scheduled to send it. The window gives a wrongly-launched goroutine
// a realistic chance to reach its channel send before this concludes, without
// it ever pacing or gating the passing case.
func assertIdentityRenewalNeverCalled(t *testing.T, entered chan struct{}, calls chan identityRenewalCall, msg string) {
	t.Helper()
	waitForClosed(t, entered, "runner.Run was never entered")
	select {
	case call := <-calls:
		t.Fatalf("%s: startIdentityRenewal called with %+v, want it never called", msg, call)
	case <-time.After(200 * time.Millisecond):
	}
}

// stopRunRunner cancels ctx and waits for runRunner's result. It is meant to
// be deferred right after the background runRunner goroutine is started, and
// deliberately uses t.Error/t.Errorf rather than t.Fatal: an earlier
// assertion in the same test may already be unwinding through runtime.Goexit
// (t.Fatal's mechanism) when this defer runs, and calling FailNow again
// during that unwind is unsafe -- the same reasoning probeAttempt's own
// deferred cleanup documents in run_lifecycle_probe_test.go. Callers must
// defer this immediately after starting the goroutine, before any assertion
// that might Fatal, so that on any exit path runRunner is cancelled and
// drained -- and the package-level seams are restored -- before the
// restoreRunner/restoreSeam defers (registered earlier, so they run later)
// swap the vars back while runRunner might still be reading them.
func stopRunRunner(t *testing.T, cancel context.CancelFunc, done chan error) {
	t.Helper()
	cancel()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("runRunner returned %v, want nil or context.Canceled after cancellation", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("runRunner did not return after cancellation")
	}
}

// TestRunRunnerStartsIdentityRenewalForAnIssuedIdentity is the brief's first
// case: a file-backed store already holding an issued identity, reached over
// https, must both decide start=true and actually launch the goroutine
// (through startIdentityRenewal) with the runnerID/token resolveRunnerIdentity
// settled on -- not merely the ones the test configured before the store
// overwrote them.
func TestRunRunnerStartsIdentityRenewalForAnIssuedIdentity(t *testing.T) {
	identityPath := filepath.Join(t.TempDir(), "identity.json")
	writeIdentityFile(t, identityPath, "issued-runner", "issued-token")

	cfg := defaultRunnerConfig()
	cfg.identityStoreKind = identityStoreFile
	cfg.identityFile = identityPath
	// https so decideIdentityRenewal's validateEnrollTransportSecurity gate
	// passes without needing --allow-plaintext. Nothing actually dials this
	// host: decideIdentityRenewal only builds a client, it never calls
	// RenewIdentity.
	cfg.serverURL = "https://example.invalid"

	entered, restoreRunner := stubBlockingRunnerServiceFactoryWithEntrySignal()
	defer restoreRunner()
	calls, restoreSeam := stubIdentityRenewalSeam()
	defer restoreSeam()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runRunner(ctx, cfg) }()
	// Registered after restoreRunner/restoreSeam above, so on any exit --
	// including a t.Fatal below -- defers unwind LIFO: this cancel-and-drain
	// runs first, then the two restores. That ordering is what keeps
	// runRunner from reading a package var mid-swap after this test returns.
	defer stopRunRunner(t, cancel, done)

	waitForClosed(t, entered, "runner.Run was never entered")

	select {
	case call := <-calls:
		if call.runnerID != "issued-runner" || call.token != "issued-token" {
			t.Fatalf("startIdentityRenewal called with (%q, %q), want (%q, %q) -- the identity "+
				"resolveRunnerIdentity settled on, not whatever cfg carried beforehand",
				call.runnerID, call.token, "issued-runner", "issued-token")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("startIdentityRenewal was never called for an issued identity over https")
	}
}

// TestRunRunnerDoesNotStartIdentityRenewalWithoutAnIssuedIdentity is the
// brief's second case: an empty (ephemeral) identity store is the static
// --token deployment shape. decideIdentityRenewal returns start=false,
// warnErr=nil for it, so runRunner must never reach the `go
// startIdentityRenewal(...)` statement at all.
func TestRunRunnerDoesNotStartIdentityRenewalWithoutAnIssuedIdentity(t *testing.T) {
	cfg := defaultRunnerConfig()
	// identityStoreKind stays "ephemeral" (defaultRunnerConfig's default) and
	// nothing is ever Saved to it -- runnerHasIssuedIdentity sees hasIssued=false.
	cfg.token = "static-token"

	entered, restoreRunner := stubBlockingRunnerServiceFactoryWithEntrySignal()
	defer restoreRunner()
	calls, restoreSeam := stubIdentityRenewalSeam()
	defer restoreSeam()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runRunner(ctx, cfg) }()
	defer stopRunRunner(t, cancel, done)

	assertIdentityRenewalNeverCalled(t, entered, calls, "static-token deployment (no issued identity)")
}

// TestRunRunnerDoesNotStartIdentityRenewalOverRefusedPlaintext is the brief's
// third case, resolved per the task's ambiguity note: it reuses the
// construction from renew_test.go's
// TestDecideIdentityRenewalRefusesPlaintextWithoutOptIn -- an issued identity
// plus a plaintext --server without --allow-plaintext -- because among that
// file's three TestDecideIdentityRenewal* cases, it is the only one that
// drives decideIdentityRenewal's warnErr!=nil return, and it is the cheapest
// of the three constructions available: no real server needed (decideIdentityRenewal
// never gets far enough to dial one), just an identity file and a URL string.
func TestRunRunnerDoesNotStartIdentityRenewalOverRefusedPlaintext(t *testing.T) {
	identityPath := filepath.Join(t.TempDir(), "identity.json")
	writeIdentityFile(t, identityPath, "runner-1", "issued-token")

	cfg := defaultRunnerConfig()
	cfg.identityStoreKind = identityStoreFile
	cfg.identityFile = identityPath
	cfg.serverURL = "http://example.invalid"
	cfg.allowPlaintext = false

	entered, restoreRunner := stubBlockingRunnerServiceFactoryWithEntrySignal()
	defer restoreRunner()
	calls, restoreSeam := stubIdentityRenewalSeam()
	defer restoreSeam()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runRunner(ctx, cfg) }()
	defer stopRunRunner(t, cancel, done)

	assertIdentityRenewalNeverCalled(t, entered, calls, "issued identity over a refused plaintext server")
}
