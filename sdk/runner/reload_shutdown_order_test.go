package runner

import (
	"context"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	xflowsdk "github.com/xbcio/xflow/sdk/xflow"
)

// orderRecordingReloadingRunner is a runnerService that also implements
// credentialReloadingRunner, recording the relative order of Close() and any
// Reload() call it observes. It stands in for a real *xflow.Runner the way
// stubRunnerServiceFactory's runnerServiceFunc already does for the
// non-reloading tests in this package.
//
// Reload blocks until releaseReload is closed, which is what lets this test
// put cancel() (and therefore runRunner's whole defer-unwind, including
// runner.Close()) strictly WHILE a SIGHUP-triggered Reload call is already
// in flight inside installCredentialReloadSignal's goroutine — the actual
// race the defer-order fix protects against. installCredentialReloadSignal's
// stop() func (signal.Stop + close(sig) + <-done) cannot return until that
// in-flight Reload call returns, so if stopReload truly runs before
// runner.Close (the fixed ordering), Close is provably blocked behind the
// still-running Reload; if it does not (the regression this pins), Close is
// free to run concurrently with, or before, Reload finishes.
type orderRecordingReloadingRunner struct {
	entered       chan struct{}
	releaseReload chan struct{}

	mu     sync.Mutex
	events []string
}

func newOrderRecordingReloadingRunner() *orderRecordingReloadingRunner {
	return &orderRecordingReloadingRunner{
		entered:       make(chan struct{}),
		releaseReload: make(chan struct{}),
	}
}

func (r *orderRecordingReloadingRunner) Run(ctx context.Context) error {
	close(r.entered)
	<-ctx.Done()
	return ctx.Err()
}

func (r *orderRecordingReloadingRunner) Close() error {
	r.record("close")
	return nil
}

func (r *orderRecordingReloadingRunner) Reload(xflowsdk.CredentialReloaderSource) error {
	r.record("reload-start")
	<-r.releaseReload
	r.record("reload-end")
	return nil
}

func (r *orderRecordingReloadingRunner) record(ev string) {
	r.mu.Lock()
	r.events = append(r.events, ev)
	r.mu.Unlock()
}

func (r *orderRecordingReloadingRunner) recordedEvents() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.events...)
}

// stubRunnerServiceFactoryReloadable installs a newRunnerService stub whose
// returned runnerService also satisfies credentialReloadingRunner, so
// runRunner's `if reloadable, ok := runner.(credentialReloadingRunner)`
// branch is taken.
func stubRunnerServiceFactoryReloadable(runner *orderRecordingReloadingRunner) func() {
	previous := newRunnerService
	newRunnerService = func(xflowsdk.RunnerConfig, ...xflowsdk.RunnerOption) (runnerService, error) {
		return runner, nil
	}
	return func() { newRunnerService = previous }
}

// TestRunRunnerWaitsForInFlightReloadBeforeClosingTheRunner is a regression
// pin for the defer ordering in runRunner: on shutdown, runner.Close() must
// not run while a SIGHUP-triggered Reload call is still in flight —
// stopReload (installCredentialReloadSignal's returned stop func) blocks on
// <-done until its goroutine's current fn() call returns, so Close must wait
// behind it.
//
// This drives a REAL SIGHUP (as installCredentialReloadSignal's own doc
// requires any caller exercising it to), lets the fake runner's Reload
// block indefinitely until released, and only then cancels runRunner's
// context. If Close is observed before Reload finishes, stopReload did not
// actually block shutdown behind the in-flight call — which is exactly the
// defect the registration-order fix in run.go closes.
func TestRunRunnerWaitsForInFlightReloadBeforeClosingTheRunner(t *testing.T) {
	runner := newOrderRecordingReloadingRunner()
	restore := stubRunnerServiceFactoryReloadable(runner)
	defer restore()

	cfg := defaultRunnerConfig()
	cfg.serverURL = "https://example.invalid:8080"
	cfg.transport = transportHTTP
	cfg.changed = map[string]bool{"server": true, "transport": true}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runRunner(ctx, cfg) }()

	select {
	case <-runner.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("runner.Run was never entered")
	}

	proc, err := os.FindProcess(os.Getpid())
	if err != nil {
		t.Fatalf("FindProcess: %v", err)
	}
	if err := proc.Signal(syscall.SIGHUP); err != nil {
		t.Fatalf("signal self: %v", err)
	}

	// Wait for Reload to actually have started (and be blocked inside it)
	// before cancelling, so cancel() is guaranteed to land while Reload is
	// in flight rather than racing its start.
	deadline := time.After(2 * time.Second)
	for {
		events := runner.recordedEvents()
		if len(events) > 0 && events[0] == "reload-start" {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("Reload never started; recorded events so far: %v", events)
		case <-time.After(5 * time.Millisecond):
		}
	}

	// Cancel while Reload is blocked inside r.releaseReload. If the fix is
	// correct, runRunner's unwind calls stopReload() first, which blocks on
	// <-done until the in-flight Reload call returns -- so runner.Close()
	// must not run until this goroutine releases it below.
	cancel()

	// Give runRunner's shutdown path a moment to reach (and, if the defect
	// were present, run past) runner.Close -- then confirm Close has NOT
	// run yet, because Reload is still blocked.
	time.Sleep(150 * time.Millisecond)
	if events := runner.recordedEvents(); len(events) != 1 || events[0] != "reload-start" {
		t.Fatalf("recorded events after cancel while Reload still blocked = %v, want exactly [reload-start] "+
			"(runner.Close must not run while a SIGHUP-triggered Reload is in flight)", events)
	}

	close(runner.releaseReload)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runRunner did not return after Reload was released")
	}

	events := runner.recordedEvents()
	t.Logf("final recorded events: %v", events)
	if len(events) != 3 || events[0] != "reload-start" || events[1] != "reload-end" || events[2] != "close" {
		t.Fatalf("recorded events = %v, want exactly [reload-start reload-end close]", events)
	}
}
