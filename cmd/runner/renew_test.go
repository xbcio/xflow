package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/service/protocol"
)

// testLogger discards output; these tests assert on call counts and
// timing, not log lines.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// errTransient is a stand-in for a network/auth failure a real renewClient
// would return.
var errTransient = errors.New("transient renewal failure")

// fakeRenewClient is the renewClient test double every test in this file
// drives runIdentityRenewal/renewOnce through, so none of it dials a real
// server.
type fakeRenewClient struct {
	mu    sync.Mutex
	calls int
	// respond is invoked with the 1-based call number and the request that
	// was sent, and returns what the fake server hands back.
	respond func(call int, req protocol.RenewIdentityRequest) (protocol.RenewIdentityResponse, error)
}

func (f *fakeRenewClient) RenewIdentity(_ context.Context, req protocol.RenewIdentityRequest) (protocol.RenewIdentityResponse, error) {
	f.mu.Lock()
	f.calls++
	call := f.calls
	f.mu.Unlock()
	return f.respond(call, req)
}

func (f *fakeRenewClient) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// TestRunIdentityRenewalNoExpiryRetiresTheLoop covers brief Step 1 case 1: a
// server that reports no expiry (TTL=0) must retire the loop, and never call
// again. The "never again" half of that needs real wall-clock time to prove --
// a reverse assertion needs time to fail to happen.
func TestRunIdentityRenewalNoExpiryRetiresTheLoop(t *testing.T) {
	fake := &fakeRenewClient{
		respond: func(int, protocol.RenewIdentityRequest) (protocol.RenewIdentityResponse, error) {
			return protocol.RenewIdentityResponse{}, nil
		},
	}

	done := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		runIdentityRenewal(ctx, fake, "runner-1", "tok", testLogger())
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runIdentityRenewal did not return after a no-expiry response")
	}

	if got := fake.callCount(); got != 1 {
		t.Fatalf("calls = %d, want 1", got)
	}

	time.Sleep(300 * time.Millisecond)
	if got := fake.callCount(); got != 1 {
		t.Fatalf("calls after retirement = %d, want still 1 (loop must not call again)", got)
	}
}

// TestRunIdentityRenewalRenewsBeforeExpiry covers brief Step 1 case 2: a
// future expiry schedules a second call once remaining validity drops below
// a third of the window.
func TestRunIdentityRenewalRenewsBeforeExpiry(t *testing.T) {
	restoreRetry := renewRetryWait
	restoreMin := renewMinWait
	renewRetryWait = 50 * time.Millisecond
	renewMinWait = 50 * time.Millisecond

	// A deadline 300ms out gives a TTL/3 cadence of 100ms, well clear of the
	// 50ms floor set above, so the second call is driven by the computed
	// cadence rather than the floor.
	expiry := time.Now().Add(300 * time.Millisecond).UTC().Format(time.RFC3339)
	fake := &fakeRenewClient{
		respond: func(int, protocol.RenewIdentityRequest) (protocol.RenewIdentityResponse, error) {
			return protocol.RenewIdentityResponse{ExpiresAt: expiry}, nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		runIdentityRenewal(ctx, fake, "runner-1", "tok", testLogger())
		close(done)
	}()

	deadlineWait := time.Now().Add(2 * time.Second)
	ok := false
	for time.Now().Before(deadlineWait) {
		if fake.callCount() >= 2 {
			ok = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Join before restoring the shared vars: a still-running goroutine
	// reading renewRetryWait/renewMinWait while this test writes them back
	// is exactly the race the earlier t.Cleanup-based version hit under
	// -race.
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runIdentityRenewal did not return after cancellation")
	}
	renewRetryWait = restoreRetry
	renewMinWait = restoreMin

	if !ok {
		t.Fatalf("calls = %d, want >= 2 within 2s", fake.callCount())
	}
}

// TestRunIdentityRenewalSurvivesAnInitialError covers brief Step 1 case 3 and
// is the reason renewNoExpiry and renewFailed must stay separate outcomes: a
// transient error on the first call must not retire the loop, only a
// genuine no-expiry response may.
func TestRunIdentityRenewalSurvivesAnInitialError(t *testing.T) {
	restoreRetry := renewRetryWait
	renewRetryWait = 20 * time.Millisecond

	fake := &fakeRenewClient{
		respond: func(call int, _ protocol.RenewIdentityRequest) (protocol.RenewIdentityResponse, error) {
			if call == 1 {
				return protocol.RenewIdentityResponse{}, errTransient
			}
			return protocol.RenewIdentityResponse{ExpiresAt: time.Now().Add(time.Hour).UTC().Format(time.RFC3339)}, nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		runIdentityRenewal(ctx, fake, "runner-1", "tok", testLogger())
		close(done)
	}()

	deadlineWait := time.Now().Add(2 * time.Second)
	ok := false
	for time.Now().Before(deadlineWait) {
		if fake.callCount() >= 2 {
			ok = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runIdentityRenewal did not return after cancellation")
	}
	renewRetryWait = restoreRetry

	if !ok {
		t.Fatalf("calls = %d, want >= 2 (loop must survive the first error)", fake.callCount())
	}
}

// TestRenewOnceUnparsableExpiryIsFailedNotNoExpiry covers brief Step 1 case
// 4: a malformed expires_at is a server bug (renewFailed, keep retrying), not
// a legitimate "no expiry" signal (renewNoExpiry, retire the loop).
func TestRenewOnceUnparsableExpiryIsFailedNotNoExpiry(t *testing.T) {
	fake := &fakeRenewClient{
		respond: func(int, protocol.RenewIdentityRequest) (protocol.RenewIdentityResponse, error) {
			return protocol.RenewIdentityResponse{ExpiresAt: "not-a-timestamp"}, nil
		},
	}
	_, outcome := renewOnce(context.Background(), fake, "runner-1", "tok", testLogger())
	if outcome != renewFailed {
		t.Fatalf("outcome = %v, want renewFailed", outcome)
	}
}

// TestRunIdentityRenewalExitsOnContextCancel covers brief Step 1 case 5: a
// cancelled context unwinds the goroutine cleanly. Run with -race.
func TestRunIdentityRenewalExitsOnContextCancel(t *testing.T) {
	restoreRetry := renewRetryWait
	renewRetryWait = time.Hour
	t.Cleanup(func() { renewRetryWait = restoreRetry })

	fake := &fakeRenewClient{
		respond: func(int, protocol.RenewIdentityRequest) (protocol.RenewIdentityResponse, error) {
			return protocol.RenewIdentityResponse{}, errTransient
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		runIdentityRenewal(ctx, fake, "runner-1", "tok", testLogger())
		close(done)
	}()

	// Let the first call land, then cancel while the loop is parked in its
	// retry wait (renewRetryWait is an hour, so it is certainly still
	// waiting).
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runIdentityRenewal did not return after context cancellation")
	}
}

// TestRunIdentityRenewalPastDeadlineFallsBackToRetryWait is the only defense
// against the hot-loop hole in 修正11(c): once a learned deadline has
// lapsed, renewFailed keeps it in the past forever (Renew refuses an
// already-expired identity by design), so the cadence must fall back to
// renewRetryWait rather than clamp to renewMinWait on every iteration.
func TestRunIdentityRenewalPastDeadlineFallsBackToRetryWait(t *testing.T) {
	restoreRetry := renewRetryWait
	restoreMin := renewMinWait
	// A wide gap between the two so the two behaviors are distinguishable by
	// call count: if the guard is missing, the loop free-runs at renewMinWait
	// and racks up many calls; with the guard, it is paced by renewRetryWait
	// and racks up very few.
	renewRetryWait = 200 * time.Millisecond
	renewMinWait = 2 * time.Millisecond

	// First call returns a deadline that has already lapsed by the time the
	// loop reads it back (a handful of milliseconds in the past); every call
	// after that fails, exactly as Renew refusing an expired identity would
	// produce in production.
	pastDeadline := time.Now().Add(-5 * time.Millisecond).UTC().Format(time.RFC3339)
	fake := &fakeRenewClient{
		respond: func(call int, _ protocol.RenewIdentityRequest) (protocol.RenewIdentityResponse, error) {
			if call == 1 {
				return protocol.RenewIdentityResponse{ExpiresAt: pastDeadline}, nil
			}
			return protocol.RenewIdentityResponse{}, errTransient
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		runIdentityRenewal(ctx, fake, "runner-1", "tok", testLogger())
		close(done)
	}()

	// Run for a window that would rack up dozens of calls at renewMinWait
	// (2ms) but only 2-3 at renewRetryWait (200ms).
	time.Sleep(500 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runIdentityRenewal did not return after cancellation")
	}

	got := fake.callCount()
	renewRetryWait = restoreRetry
	renewMinWait = restoreMin

	if got > 5 {
		t.Fatalf("calls = %d in 500ms; want <= 5 (renewRetryWait pacing), "+
			"got hot-loop pacing instead (a past deadline is still driving the cadence)", got)
	}
}

// TestRenewClientForCarriesAuthorizationHeader is the only defense against
// the hole in 修正1: a renewClientFor that forgets .WithToken(cfg.token)
// builds a client that never sends Authorization, so every renewal
// authenticates against an empty token and fails silently forever.
func TestRenewClientForCarriesAuthorizationHeader(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(protocol.RenewIdentityResponse{})
	}))
	defer srv.Close()

	cfg := defaultRunnerConfig()
	cfg.transport = transportHTTP
	cfg.serverURL = srv.URL
	cfg.allowPlaintext = true
	cfg.runnerID = "runner-1"
	cfg.token = "t-1"

	rc, err := renewClientFor(cfg)
	if err != nil {
		t.Fatalf("renewClientFor: %v", err)
	}
	if _, err := rc.RenewIdentity(context.Background(), protocol.RenewIdentityRequest{RunnerID: cfg.runnerID}); err != nil {
		t.Fatalf("RenewIdentity: %v", err)
	}
	if gotAuth != "Bearer t-1" {
		t.Fatalf("Authorization header = %q, want %q", gotAuth, "Bearer t-1")
	}
}

// TestRunnerHasIssuedIdentity covers 修正3's gate: an ephemeral store with
// nothing saved reports no issued identity (the static --token deployment),
// and one with a saved identity (the enrolled deployment) reports true.
func TestRunnerHasIssuedIdentity(t *testing.T) {
	store := &ephemeralIdentityStore{}
	hasIssued, err := runnerHasIssuedIdentity(store)
	if err != nil {
		t.Fatalf("runnerHasIssuedIdentity: %v", err)
	}
	if hasIssued {
		t.Fatal("hasIssued = true on an empty store, want false (static --token deployment)")
	}

	if err := store.Save(identity{RunnerID: "runner-1", Token: "tok"}); err != nil {
		t.Fatalf("seed store: %v", err)
	}
	hasIssued, err = runnerHasIssuedIdentity(store)
	if err != nil {
		t.Fatalf("runnerHasIssuedIdentity: %v", err)
	}
	if !hasIssued {
		t.Fatal("hasIssued = false after Save, want true (enrolled deployment)")
	}
}

// TestDecideIdentityRenewalStaticTokenDoesNotStart is the direct defense
// against the hole in 修正9's third mandated mutation: a static --token
// deployment (nothing ever Saved to the identity store) must come back
// start=false, warnErr=nil from the gate itself -- not merely from
// runnerHasIssuedIdentity in isolation, which a wiring-level regression could
// bypass without this test noticing. The server here would answer any
// RenewIdentity call with 200, so if the gate mistakenly proceeds anyway this
// test's start=true/warnErr!=nil assertions are what catches it, not a
// crashed or hanging test.
func TestDecideIdentityRenewalStaticTokenDoesNotStart(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(protocol.RenewIdentityResponse{})
	}))
	defer srv.Close()

	cfg := defaultRunnerConfig()
	cfg.transport = transportHTTP
	cfg.serverURL = srv.URL
	cfg.allowPlaintext = true
	cfg.runnerID = "runner-1"
	cfg.token = "static-token"

	store := &ephemeralIdentityStore{}
	rc, start, warnMsg, warnErr := decideIdentityRenewal(cfg, store)
	if start {
		t.Fatal("start = true for a static-token deployment (empty identity store), want false")
	}
	if rc != nil {
		t.Fatal("rc != nil for a static-token deployment, want nil")
	}
	if warnErr != nil {
		t.Fatalf("warnErr = %v, want nil (static --token is a silent no-op, not a warning)", warnErr)
	}
	if warnMsg != "" {
		t.Fatalf("warnMsg = %q, want empty", warnMsg)
	}
}

// TestDecideIdentityRenewalIssuedIdentityStarts is
// TestDecideIdentityRenewalStaticTokenDoesNotStart's positive counterpart: an
// enrolled deployment (something Saved to the identity store) over a valid
// transport must come back start=true with a usable renewClient.
func TestDecideIdentityRenewalIssuedIdentityStarts(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(protocol.RenewIdentityResponse{})
	}))
	defer srv.Close()

	cfg := defaultRunnerConfig()
	cfg.transport = transportHTTP
	cfg.serverURL = srv.URL
	cfg.allowPlaintext = true
	cfg.runnerID = "runner-1"
	cfg.token = "issued-token"

	store := &ephemeralIdentityStore{}
	if err := store.Save(identity{RunnerID: "runner-1", Token: "issued-token"}); err != nil {
		t.Fatalf("seed store: %v", err)
	}

	rc, start, warnMsg, warnErr := decideIdentityRenewal(cfg, store)
	if warnErr != nil {
		t.Fatalf("warnErr = %v, want nil", warnErr)
	}
	if warnMsg != "" {
		t.Fatalf("warnMsg = %q, want empty", warnMsg)
	}
	if !start {
		t.Fatal("start = false for an enrolled deployment, want true")
	}
	if rc == nil {
		t.Fatal("rc = nil for an enrolled deployment, want a usable renewClient")
	}
}

// TestDecideIdentityRenewalRefusesPlaintextWithoutOptIn is the direct
// defense for 修正4/修正12: an enrolled deployment pointed at a plaintext
// http:// server without --allow-plaintext must refuse to start the loop
// (the runner token must not cross the network unencrypted), and the Warn
// message must not be validateEnrollTransportSecurity's own wording verbatim
// -- that message says "refusing to enroll" and mentions a registration
// code, neither of which applies on the renewal path.
func TestDecideIdentityRenewalRefusesPlaintextWithoutOptIn(t *testing.T) {
	cfg := defaultRunnerConfig()
	cfg.transport = transportHTTP
	cfg.serverURL = "http://example.invalid"
	cfg.allowPlaintext = false
	cfg.runnerID = "runner-1"
	cfg.token = "issued-token"

	store := &ephemeralIdentityStore{}
	if err := store.Save(identity{RunnerID: "runner-1", Token: "issued-token"}); err != nil {
		t.Fatalf("seed store: %v", err)
	}

	rc, start, warnMsg, warnErr := decideIdentityRenewal(cfg, store)
	if start {
		t.Fatal("start = true for a plaintext server without --allow-plaintext, want false")
	}
	if rc != nil {
		t.Fatal("rc != nil for a refused plaintext deployment, want nil")
	}
	if warnErr == nil {
		t.Fatal("warnErr = nil for a plaintext server without --allow-plaintext, want a non-nil error")
	}
	if warnMsg == "" {
		t.Fatal("warnMsg is empty, want an actionable message naming --allow-plaintext")
	}
	if strings.Contains(warnMsg, "refusing to enroll") || strings.Contains(warnMsg, "registration code") {
		t.Fatalf("warnMsg = %q surfaces validateEnrollTransportSecurity's own enroll-specific wording verbatim", warnMsg)
	}
	if !strings.Contains(warnMsg, "allow-plaintext") {
		t.Fatalf("warnMsg = %q, want it to name the --allow-plaintext escape hatch", warnMsg)
	}
}
