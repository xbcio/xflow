package xflow

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/service/protocol"
)

// flakyControlPlane is a control plane that serves a configurable number of
// successful poll requests and then fails every one after that.
//
// It models the failure this package's reconnect exists for: a transient
// control-plane fault (a restart, a 500 from one replica, a slow commit) that
// runnersvc.Run reports by returning. Register always succeeds, so a runner that
// comes back finds a working server — which is what makes "did it come back?" a
// meaningful question rather than a test of the fault's duration.
type flakyControlPlane struct {
	srv *httptest.Server

	// pollsBeforeFailure is how many poll requests succeed before the server
	// starts returning 500.
	pollsBeforeFailure int32

	polls      atomic.Int32
	registers  atomic.Int32
	failedOnce atomic.Bool
}

func newFlakyControlPlane(t *testing.T, pollsBeforeFailure int32) *flakyControlPlane {
	t.Helper()
	cp := &flakyControlPlane{pollsBeforeFailure: pollsBeforeFailure}
	mux := http.NewServeMux()
	mux.HandleFunc(protocol.RegisterRunnerPath, func(w http.ResponseWriter, r *http.Request) {
		cp.registers.Add(1)
		writeJSONBody(w, protocol.RegisterRunnerResponse{
			RunnerID:  "reconnect-probe",
			SessionID: "session-1",
		})
	})
	mux.HandleFunc(protocol.HeartbeatPath, func(w http.ResponseWriter, r *http.Request) {
		// Deliberately slower than the heartbeat interval, so that when the poll
		// failure makes runnersvc.Run cancel its session-scoped heartbeat context
		// there is essentially always a heartbeat in flight to be cancelled.
		//
		// That in-flight heartbeat fails with a wrapped context.Canceled and wins
		// the race to Run's error channel, which Run prefers over the poll error.
		// The result is a Run that reports `context canceled` while the caller's
		// context is perfectly alive — a shutdown-shaped error for a fault that is
		// not a shutdown. Without this delay the race lands about a third of the
		// time and the test reads as flaky rather than as the defect it is.
		select {
		case <-time.After(80 * time.Millisecond):
		case <-r.Context().Done():
			return
		}
		writeJSONBody(w, protocol.HeartbeatResponse{})
	})
	mux.HandleFunc(protocol.PollTaskPath, func(w http.ResponseWriter, r *http.Request) {
		if cp.polls.Add(1) > cp.pollsBeforeFailure {
			cp.failedOnce.Store(true)
			// The exact status does not matter, only that runnersvc.Run treats
			// it as an error and returns. 500 is what a control plane rolling
			// its own replicas actually emits.
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		writeJSONBody(w, protocol.PollTaskResponse{})
	})
	cp.srv = httptest.NewServer(mux)
	t.Cleanup(cp.srv.Close)
	return cp
}

func writeJSONBody(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// TestRunnerReconnectsAfterATransportError is the contract that keeps a runner
// consuming across a transient control-plane fault.
//
// runnersvc.Run returns on the FIRST error from any of its loops — a
// ReportResult timeout, a heartbeat failure, a non-2xx poll. Every one of those
// is transient, but Run does not distinguish them, so a caller that invokes it
// once turns a momentary fault into a permanently dead runner. Nothing looks
// broken when that happens: the process keeps serving and any hosted trigger
// stays activated in the control plane's view; messages simply stop being
// consumed. Both cmd/runner and the SAS host hit this in production and each
// wrote its own reconnect loop around Run.
//
// The proof is a SECOND Register after the fault: Run opens with client.Register
// and derives a fresh session, so a re-registration is the only externally
// visible evidence that Run was actually re-entered rather than the first call
// merely surviving.
func TestRunnerReconnectsAfterATransportError(t *testing.T) {
	cp := newFlakyControlPlane(t, 1)

	r, err := NewRunner(RunnerConfig{
		ServerURL:         cp.srv.URL,
		RunnerID:          "reconnect-probe",
		Concurrency:       1,
		Capabilities:      []string{"xflow.function"},
		PollWait:          20 * time.Millisecond,
		HeartbeatInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	defer func() { _ = r.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	// Wait for the fault to land and the runner to come back. The backoff floor
	// dominates this wait, so it is generous relative to the poll interval.
	deadline := time.After(30 * time.Second)
	for {
		if cp.failedOnce.Load() && cp.registers.Load() >= 2 {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("Run returned after a transient poll failure (err=%v); "+
				"registers=%d. One 500 from the control plane silently stopped "+
				"this runner: the process keeps serving and any hosted trigger "+
				"stays activated, but nothing is consumed again.",
				err, cp.registers.Load())
		case <-deadline:
			t.Fatalf("runner did not re-register within 30s: failedOnce=%v registers=%d",
				cp.failedOnce.Load(), cp.registers.Load())
		case <-time.After(20 * time.Millisecond):
		}
	}

	cancel()
	select {
	case err := <-done:
		if err != nil && !isContextCanceled(err) {
			t.Fatalf("Run returned %v after cancel, want nil or context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return within 10s of cancel; the reconnect loop ignores ctx")
	}
}

// TestRunnerRunStopsOnContextCancel guards the other direction: the reconnect
// loop must not turn a cancelled context into an endless re-registration storm
// against a control plane that is failing every request.
func TestRunnerRunStopsOnContextCancel(t *testing.T) {
	// Zero successful polls: every request fails, so the loop is always in its
	// backoff when the cancel arrives.
	cp := newFlakyControlPlane(t, 0)

	r, err := NewRunner(RunnerConfig{
		ServerURL:         cp.srv.URL,
		RunnerID:          "cancel-probe",
		Concurrency:       1,
		Capabilities:      []string{"xflow.function"},
		PollWait:          20 * time.Millisecond,
		HeartbeatInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	defer func() { _ = r.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	// Let it fail and enter backoff at least once.
	time.Sleep(200 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil && !isContextCanceled(err) {
			t.Fatalf("Run returned %v after cancel, want nil or context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return within 10s of cancel; a cancelled runner " +
			"that keeps re-registering holds its leases and blocks shutdown")
	}
}

// isContextCanceled must unwrap: the runner's loops wrap the cause, so a
// cancelled Run surfaces as `Post ".../heartbeat": context canceled` rather
// than the sentinel itself.
func isContextCanceled(err error) bool {
	return errors.Is(err, context.Canceled)
}
