package runner

import (
	"context"
	"errors"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	xflowsdk "github.com/xbcio/xflow/sdk/xflow"
)

// reloadTestSignal bounds how long a test waits for installCredentialReloadSignal's
// goroutine to observe a signal or ctx cancellation. Mirrors
// cmd/server/reload_test.go's reloadTestSignal for the same reason: generous
// relative to the in-memory work involved, tight enough that a genuinely
// stuck reload fails the test instead of the suite timeout.
const reloadTestSignal = 2 * time.Second

// fakeReloadingRunner is the credentialReloadingRunner test double:
// installCredentialReloadSignal and newCredentialReloadFunc are both tested
// against this instead of a real *xflow.Runner, so these tests do not need a
// live control plane.
type fakeReloadingRunner struct {
	mu       sync.Mutex
	calls    []xflowsdk.CredentialReloaderSource
	nextErr  error
	callDone chan struct{}
}

func newFakeReloadingRunner() *fakeReloadingRunner {
	return &fakeReloadingRunner{callDone: make(chan struct{}, 16)}
}

func (f *fakeReloadingRunner) Reload(src xflowsdk.CredentialReloaderSource) error {
	f.mu.Lock()
	f.calls = append(f.calls, src)
	err := f.nextErr
	f.mu.Unlock()
	f.callDone <- struct{}{}
	return err
}

func (f *fakeReloadingRunner) setNextErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextErr = err
}

func (f *fakeReloadingRunner) lastCall() (xflowsdk.CredentialReloaderSource, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.calls) == 0 {
		return xflowsdk.CredentialReloaderSource{}, false
	}
	return f.calls[len(f.calls)-1], true
}

func (f *fakeReloadingRunner) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// --- newCredentialReloadFunc ---

// TestCredentialReloadFuncRereadsStaticTokenAndTLSFromEnv proves the
// re-resolution half of the contract: a reload picks up a token/TLS path
// change made to the process environment between startup and the reload
// call, exactly the shape a config-file edit followed by SIGHUP produces
// through resolveRunnerConfig's env-overlay step.
func TestCredentialReloadFuncRereadsStaticTokenAndTLSFromEnv(t *testing.T) {
	t.Setenv("XFLOW_RUNNER_TOKEN", "token-a")
	cfg := defaultRunnerConfig()
	cfg.serverURL = "https://example.invalid:8080"
	cfg.transport = transportHTTP
	cfg.changed = map[string]bool{"server": true, "transport": true}
	resolved, err := resolveRunnerConfig(cfg)
	if err != nil {
		t.Fatalf("resolveRunnerConfig: %v", err)
	}
	if resolved.token != "token-a" {
		t.Fatalf("resolved.token = %q, want token-a", resolved.token)
	}

	runner := newFakeReloadingRunner()
	fn := newCredentialReloadFunc(resolved, &ephemeralIdentityStore{}, runner)

	t.Setenv("XFLOW_RUNNER_TOKEN", "token-b")
	if err := fn(); err != nil {
		t.Fatalf("reload func: %v", err)
	}
	call, ok := runner.lastCall()
	if !ok {
		t.Fatal("Reload was not called")
	}
	if call.Token != "token-b" {
		t.Fatalf("Reload token = %q, want token-b", call.Token)
	}
}

// TestCredentialReloadFuncPrefersStoredIdentityTokenOverStaticConfig pins the
// runbook's axis-3b rule: an enrollment-issued identity's token always wins
// over whatever a static --token/XFLOW_RUNNER_TOKEN/config-file value says,
// so a stale static token left over in the config from before enrollment is
// never sent after a reload.
func TestCredentialReloadFuncPrefersStoredIdentityTokenOverStaticConfig(t *testing.T) {
	t.Setenv("XFLOW_RUNNER_TOKEN", "stale-static-token")
	cfg := defaultRunnerConfig()
	cfg.serverURL = "https://example.invalid:8080"
	cfg.transport = transportHTTP
	cfg.changed = map[string]bool{"server": true, "transport": true}

	store := &ephemeralIdentityStore{}
	if err := store.Save(identity{RunnerID: "issued-1", Token: "issued-token"}); err != nil {
		t.Fatalf("Save: %v", err)
	}

	runner := newFakeReloadingRunner()
	fn := newCredentialReloadFunc(cfg, store, runner)

	if err := fn(); err != nil {
		t.Fatalf("reload func: %v", err)
	}
	call, ok := runner.lastCall()
	if !ok {
		t.Fatal("Reload was not called")
	}
	if call.Token != "issued-token" {
		t.Fatalf("Reload token = %q, want issued-token (the stored identity, not the static config)", call.Token)
	}
}

// TestCredentialReloadFuncRereadsTLSMaterialPaths proves the TLS half: a
// changed --tls-client-cert/--tls-client-key/--tls-server-ca reaches Reload,
// re-read from the same config source every other field uses.
func TestCredentialReloadFuncRereadsTLSMaterialPaths(t *testing.T) {
	t.Setenv("XFLOW_RUNNER_TLS_SERVER_CA", "/path/to/ca-a.pem")
	t.Setenv("XFLOW_RUNNER_TLS_CLIENT_CERT", "/path/to/cert-a.pem")
	t.Setenv("XFLOW_RUNNER_TLS_CLIENT_KEY", "/path/to/key-a.pem")
	cfg := defaultRunnerConfig()
	cfg.serverURL = "https://example.invalid:8080"
	cfg.transport = transportHTTP
	cfg.changed = map[string]bool{"server": true, "transport": true}
	resolved, err := resolveRunnerConfig(cfg)
	if err != nil {
		t.Fatalf("resolveRunnerConfig: %v", err)
	}
	if resolved.tlsServerCA != "/path/to/ca-a.pem" {
		t.Fatalf("resolved.tlsServerCA = %q, want /path/to/ca-a.pem", resolved.tlsServerCA)
	}

	runner := newFakeReloadingRunner()
	fn := newCredentialReloadFunc(resolved, &ephemeralIdentityStore{}, runner)

	// Change the TLS material paths in the environment between fn's
	// construction and its call — the shape a config-file/env edit followed
	// by SIGHUP actually produces — and confirm fn re-reads them rather than
	// freezing the paths captured when it was built.
	t.Setenv("XFLOW_RUNNER_TLS_SERVER_CA", "/path/to/ca-b.pem")
	t.Setenv("XFLOW_RUNNER_TLS_CLIENT_CERT", "/path/to/cert-b.pem")
	t.Setenv("XFLOW_RUNNER_TLS_CLIENT_KEY", "/path/to/key-b.pem")
	if err := fn(); err != nil {
		t.Fatalf("reload func: %v", err)
	}
	call, ok := runner.lastCall()
	if !ok {
		t.Fatal("Reload was not called")
	}
	if call.TLSServerCA != "/path/to/ca-b.pem" || call.TLSClientCert != "/path/to/cert-b.pem" || call.TLSClientKey != "/path/to/key-b.pem" {
		t.Fatalf("Reload TLS material = %+v, want the -b paths", call)
	}
}

// TestCredentialReloadFuncPropagatesResolveError proves a config that fails
// to re-resolve (a malformed env override, mirroring the server-side
// malformed-YAML case) surfaces as an error from the reloadFunc rather than
// calling Reload with garbage.
func TestCredentialReloadFuncPropagatesResolveError(t *testing.T) {
	cfg := defaultRunnerConfig()
	cfg.serverURL = "https://example.invalid:8080"
	cfg.transport = transportHTTP
	cfg.changed = map[string]bool{"server": true, "transport": true}
	resolved, err := resolveRunnerConfig(cfg)
	if err != nil {
		t.Fatalf("resolveRunnerConfig: %v", err)
	}

	runner := newFakeReloadingRunner()
	fn := newCredentialReloadFunc(resolved, &ephemeralIdentityStore{}, runner)

	t.Setenv("XFLOW_RUNNER_CONCURRENCY", "not-a-number")
	if err := fn(); err == nil {
		t.Fatal("reload func with a malformed env override: want error, got nil")
	}
	if runner.callCount() != 0 {
		t.Fatalf("Reload was called %d times despite a resolve error", runner.callCount())
	}
}

// --- installCredentialReloadSignal ---

// TestInstallCredentialReloadSignalFiresOnSIGHUP drives the installed
// handler via a real SIGHUP delivered to this test process (the function
// under test installs its own signal.Notify, unlike cmd/server's reloader
// which takes an injected channel) and confirms the registered func runs and
// its outcome is observable through the fake runner — exercising the actual
// signal.Notify(syscall.SIGHUP) wiring end to end, which a channel-only test
// would not.
func TestInstallCredentialReloadSignalFiresOnSIGHUP(t *testing.T) {
	runner := newFakeReloadingRunner()
	fn := func() error {
		return runner.Reload(xflowsdk.CredentialReloaderSource{Token: "reloaded"})
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := installCredentialReloadSignal(ctx, fn)
	defer stop()

	proc, err := os.FindProcess(os.Getpid())
	if err != nil {
		t.Fatalf("FindProcess: %v", err)
	}
	if err := proc.Signal(syscall.SIGHUP); err != nil {
		t.Fatalf("signal self: %v", err)
	}

	select {
	case <-runner.callDone:
	case <-time.After(reloadTestSignal):
		t.Fatal("installCredentialReloadSignal did not invoke fn after SIGHUP")
	}
	call, ok := runner.lastCall()
	if !ok || call.Token != "reloaded" {
		t.Fatalf("lastCall = %+v, ok=%v; want Token=reloaded", call, ok)
	}
}

// TestInstallCredentialReloadSignalStopsOnContextCancel pins that the
// returned stop func (backed by ctx cancellation) lets the goroutine exit
// promptly, mirroring cmd/server's reloader.run contract.
func TestInstallCredentialReloadSignalStopsOnContextCancel(t *testing.T) {
	runner := newFakeReloadingRunner()
	fn := func() error { return runner.Reload(xflowsdk.CredentialReloaderSource{}) }

	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	stop := installCredentialReloadSignal(ctx, fn)
	go func() {
		cancel()
		stop()
		close(stopped)
	}()

	select {
	case <-stopped:
	case <-time.After(reloadTestSignal):
		t.Fatal("installCredentialReloadSignal's stop func did not return after ctx cancellation")
	}
}

// TestInstallCredentialReloadSignalContinuesAfterFailure pins that a failed
// reload does not stop the handler from reacting to a later SIGHUP —
// matching cmd/server's reloader, which keeps running the remaining
// registered funcs (and, by extension here, remains armed for the next
// signal) after one failure.
func TestInstallCredentialReloadSignalContinuesAfterFailure(t *testing.T) {
	runner := newFakeReloadingRunner()
	runner.setNextErr(errors.New("boom"))
	fn := func() error {
		return runner.Reload(xflowsdk.CredentialReloaderSource{Token: "attempt"})
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := installCredentialReloadSignal(ctx, fn)
	defer stop()

	proc, err := os.FindProcess(os.Getpid())
	if err != nil {
		t.Fatalf("FindProcess: %v", err)
	}
	sighup := syscall.SIGHUP
	if err := proc.Signal(sighup); err != nil {
		t.Fatalf("signal self (1st): %v", err)
	}
	select {
	case <-runner.callDone:
	case <-time.After(reloadTestSignal):
		t.Fatal("first SIGHUP was not observed")
	}

	runner.setNextErr(nil)
	if err := proc.Signal(sighup); err != nil {
		t.Fatalf("signal self (2nd): %v", err)
	}
	select {
	case <-runner.callDone:
	case <-time.After(reloadTestSignal):
		t.Fatal("second SIGHUP was not observed after the first failed")
	}
	if runner.callCount() != 2 {
		t.Fatalf("callCount = %d, want 2", runner.callCount())
	}
}
