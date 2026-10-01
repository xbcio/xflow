package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/observability/metrics"
	"github.com/xbcio/xflow/service/apiserver"
	"github.com/xbcio/xflow/service/control"
)

// reloadTestSignal bounds how long a test waits for a reloader.run goroutine
// to observe a signal or ctx cancellation. Generous relative to the in-memory
// work involved (a file read + YAML parse) so it never flakes under CI load,
// tight enough that a genuinely stuck reloader fails the test instead of the
// suite timeout.
const reloadTestSignal = 2 * time.Second

// runReloaderSync sends one signal to r via a channel it owns (never a real
// OS signal — this test process must not have SIGHUP delivered to it) and
// blocks until reloadAll has had a chance to run, using a func wrapper that
// signals completion. reloader has no public "done" hook, so this drives run
// in its own goroutine and synchronizes via a second channel fed by a
// wrapped reloadFunc's own completion instead of sleeping a guessed interval.
func TestReloaderRunsRegisteredFuncsOnSignal(t *testing.T) {
	r := newReloader()
	done := make(chan struct{}, 1)
	var calls int
	var mu sync.Mutex
	r.add("test", func() error {
		mu.Lock()
		calls++
		mu.Unlock()
		done <- struct{}{}
		return nil
	})

	sig := make(chan os.Signal, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.run(ctx, sig)

	sig <- os.Interrupt // any os.Signal value; reloader does not inspect which one
	select {
	case <-done:
	case <-time.After(reloadTestSignal):
		t.Fatal("reloader did not invoke the registered func in time")
	}

	mu.Lock()
	got := calls
	mu.Unlock()
	if got != 1 {
		t.Fatalf("calls = %d, want 1", got)
	}
}

// TestReloaderStopsOnContextCancel pins that run returns promptly once ctx is
// cancelled, without requiring the signal channel to be closed — cmd/server's
// wiring relies on this to let its reloader goroutine exit during shutdown.
func TestReloaderStopsOnContextCancel(t *testing.T) {
	r := newReloader()
	sig := make(chan os.Signal, 1)
	ctx, cancel := context.WithCancel(context.Background())

	returned := make(chan struct{})
	go func() {
		r.run(ctx, sig)
		close(returned)
	}()

	cancel()
	select {
	case <-returned:
	case <-time.After(reloadTestSignal):
		t.Fatal("reloader.run did not return after ctx cancellation")
	}
}

// TestReloaderContinuesAfterOneFuncFails pins that a failing reload does not
// stop the reloader from running the remaining registered funcs on the same
// signal — the policy axis and the TLS axis must not be coupled that way.
func TestReloaderContinuesAfterOneFuncFails(t *testing.T) {
	r := newReloader()
	var mu sync.Mutex
	var secondRan bool
	done := make(chan struct{}, 1)

	r.add("failing", func() error { return errors.New("boom") })
	r.add("second", func() error {
		mu.Lock()
		secondRan = true
		mu.Unlock()
		done <- struct{}{}
		return nil
	})

	sig := make(chan os.Signal, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.run(ctx, sig)

	sig <- os.Interrupt
	select {
	case <-done:
	case <-time.After(reloadTestSignal):
		t.Fatal("second reload func was not invoked after the first failed")
	}

	mu.Lock()
	ran := secondRan
	mu.Unlock()
	if !ran {
		t.Fatal("second reload func did not run")
	}
}

// TestPolicyReloadFuncNewTokenAuthenticatesAfterReload exercises the actual
// reloadFunc cmd/server registers for --auth-policy, end to end against a
// FilePolicyStore backed by a real file: adding a second entry with a new
// token and calling the reloadFunc makes the new token authenticate.
func TestPolicyReloadFuncNewTokenAuthenticatesAfterReload(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "runners.yaml")
	writeReloadTestPolicy(t, path, "original-token")

	store, err := control.NewFilePolicyStore(path, false)
	if err != nil {
		t.Fatalf("NewFilePolicyStore: %v", err)
	}
	fn := newPolicyReloadFunc(store, path, nil)

	writeReloadTestPolicy(t, path, "rotated-token")
	if err := fn(); err != nil {
		t.Fatalf("reload func: %v", err)
	}

	if _, err := store.AuthenticateRegister("order-1", "rotated-token", control.TransportInfo{}); err != nil {
		t.Fatalf("AuthenticateRegister(rotated-token) err = %v, want nil", err)
	}
	if _, err := store.AuthenticateRegister("order-1", "original-token", control.TransportInfo{}); !errors.Is(err, control.ErrAuthUnknownToken) {
		t.Fatalf("AuthenticateRegister(original-token) err = %v, want ErrAuthUnknownToken", err)
	}
}

// TestPolicyReloadFuncRemovedTokenIsDeniedAfterReload mirrors the removal
// side of the same contract.
func TestPolicyReloadFuncRemovedTokenIsDeniedAfterReload(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "runners.yaml")
	writeReloadTestPolicy(t, path, "original-token")

	store, err := control.NewFilePolicyStore(path, false)
	if err != nil {
		t.Fatalf("NewFilePolicyStore: %v", err)
	}
	fn := newPolicyReloadFunc(store, path, nil)

	writeReloadTestPolicy(t, path, "different-token")
	if err := fn(); err != nil {
		t.Fatalf("reload func: %v", err)
	}

	if _, err := store.AuthenticateRegister("order-1", "original-token", control.TransportInfo{}); !errors.Is(err, control.ErrAuthUnknownToken) {
		t.Fatalf("AuthenticateRegister(original-token) err = %v, want ErrAuthUnknownToken", err)
	}
}

// TestPolicyReloadFuncMalformedYAMLReturnsErrorAndKeepsOldTokens pins the
// error-path contract at the reloadFunc boundary: a malformed policy file
// must surface as an error AND the previous tokens must still authenticate,
// and (when a metrics sink is supplied) the failure counter must increment.
func TestPolicyReloadFuncMalformedYAMLReturnsErrorAndKeepsOldTokens(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "runners.yaml")
	writeReloadTestPolicy(t, path, "original-token")

	store, err := control.NewFilePolicyStore(path, false)
	if err != nil {
		t.Fatalf("NewFilePolicyStore: %v", err)
	}
	m := metrics.New()
	fn := newPolicyReloadFunc(store, path, m)

	if err := os.WriteFile(path, []byte("not: [valid: yaml"), 0o600); err != nil {
		t.Fatalf("write malformed policy: %v", err)
	}
	if err := fn(); err == nil {
		t.Fatal("reload func with malformed YAML: want error, got nil")
	}

	if _, err := store.AuthenticateRegister("order-1", "original-token", control.TransportInfo{}); err != nil {
		t.Fatalf("AuthenticateRegister(original-token) after failed reload err = %v, want nil", err)
	}
	assertCounterAtLeast(t, m, metricPolicyReloadFailures, 1)
}

// TestPolicyReloadFuncWorldReadableTokenFileReturnsErrorAndKeepsOldTokens is
// the token_file-permission analogue of the malformed-YAML case above.
func TestPolicyReloadFuncWorldReadableTokenFileReturnsErrorAndKeepsOldTokens(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "runners.yaml")
	writeReloadTestPolicy(t, path, "original-token")

	store, err := control.NewFilePolicyStore(path, false)
	if err != nil {
		t.Fatalf("NewFilePolicyStore: %v", err)
	}
	fn := newPolicyReloadFunc(store, path, nil)

	tokenFile := filepath.Join(dir, "token.txt")
	if err := os.WriteFile(tokenFile, []byte("insecure-token\n"), 0o644); err != nil {
		t.Fatalf("write token_file: %v", err)
	}
	policy := "version: 1\nrunners:\n  - name: orders\n    id_prefix: order-\n    token_file: \"" + tokenFile + "\"\n    allowed_node_types: [\"*\"]\n"
	if err := os.WriteFile(path, []byte(policy), 0o600); err != nil {
		t.Fatalf("write policy: %v", err)
	}

	if err := fn(); err == nil {
		t.Fatal("reload func with 0644 token_file: want error, got nil")
	}

	if _, err := store.AuthenticateRegister("order-1", "original-token", control.TransportInfo{}); err != nil {
		t.Fatalf("AuthenticateRegister(original-token) after failed reload err = %v, want nil", err)
	}
}

// TestPolicyReloadFuncMissingFileReturnsErrorAndKeepsOldTokens is the
// missing-file analogue.
func TestPolicyReloadFuncMissingFileReturnsErrorAndKeepsOldTokens(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "runners.yaml")
	writeReloadTestPolicy(t, path, "original-token")

	store, err := control.NewFilePolicyStore(path, false)
	if err != nil {
		t.Fatalf("NewFilePolicyStore: %v", err)
	}
	fn := newPolicyReloadFunc(store, path, nil)

	if err := os.Remove(path); err != nil {
		t.Fatalf("remove policy file: %v", err)
	}
	if err := fn(); err == nil {
		t.Fatal("reload func with missing file: want error, got nil")
	}

	if _, err := store.AuthenticateRegister("order-1", "original-token", control.TransportInfo{}); err != nil {
		t.Fatalf("AuthenticateRegister(original-token) after failed reload err = %v, want nil", err)
	}
}

// TestPolicyReloadFuncConcurrentAuthenticateIsRaceFree exercises the
// reloadFunc racing against concurrent AuthenticateRegister calls. Run with
// -race; asserts no crash / no data race rather than a specific outcome for a
// request that straddles the swap.
func TestPolicyReloadFuncConcurrentAuthenticateIsRaceFree(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "runners.yaml")
	writeReloadTestPolicy(t, path, "original-token")

	store, err := control.NewFilePolicyStore(path, false)
	if err != nil {
		t.Fatalf("NewFilePolicyStore: %v", err)
	}
	fn := newPolicyReloadFunc(store, path, nil)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_, _ = store.AuthenticateRegister("order-1", "original-token", control.TransportInfo{})
				}
			}
		}()
	}

	for i := 0; i < 50; i++ {
		if err := fn(); err != nil {
			t.Errorf("reload func iteration %d: %v", i, err)
		}
	}
	close(stop)
	wg.Wait()
}

// TestTLSReloadFuncNoopsWhenReloaderNotYetAvailable pins that the getter
// returning nil (TLS not configured, or Run has not started transports yet)
// is treated as "nothing to reload" rather than an error — a plaintext
// deployment sending SIGHUP for its policy file must not see a spurious TLS
// failure logged against it.
func TestTLSReloadFuncNoopsWhenReloaderNotYetAvailable(t *testing.T) {
	fn := newTLSReloadFunc(func() *apiserver.TLSReloader { return nil })
	if err := fn(); err != nil {
		t.Fatalf("tls reload func with nil reloader: err = %v, want nil", err)
	}
}

// writeReloadTestPolicy writes a one-entry runners.yaml binding token to the
// order- prefix, at the 0600 mode Reload requires.
func writeReloadTestPolicy(t *testing.T, path, token string) {
	t.Helper()
	contents := "version: 1\nrunners:\n  - name: orders\n    id_prefix: order-\n    token: \"" + token + "\"\n    allowed_node_types: [\"*\"]\n"
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// assertCounterAtLeast fails the test unless the named counter's value across
// every label combination sums to at least want. Used instead of an exact
// scrape-and-parse because the point here is "the failure was counted at
// all", not a specific Prometheus text-format assertion.
func assertCounterAtLeast(t *testing.T, m *metrics.Metrics, name string, want float64) {
	t.Helper()
	mfs, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	var total float64
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, metric := range mf.GetMetric() {
			if c := metric.GetCounter(); c != nil {
				total += c.GetValue()
			}
		}
	}
	if total < want {
		t.Fatalf("counter %s = %v, want >= %v", name, total, want)
	}
}
