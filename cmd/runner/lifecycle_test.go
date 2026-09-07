package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestLifecycleStateReadyRequiresAllThreeConditions(t *testing.T) {
	s := newLifecycleState()
	if ready, why := s.Ready(); ready {
		t.Fatalf("a fresh state is ready (%q); nothing has connected yet", why)
	}

	s.OnRegistered(context.Background(), "runner-1", false)
	if ready, _ := s.Ready(); ready {
		t.Fatal("ready after registration alone; no heartbeat has landed")
	}

	s.OnHeartbeat(context.Background(), true)
	if ready, _ := s.Ready(); ready {
		t.Fatal("ready before the supply gate reported in; supply readiness is unknown")
	}

	s.OnSupplyGateWired(true)
	if ready, _ := s.Ready(); ready {
		t.Fatal("ready with a wired gate that has never fetched anything")
	}

	s.OnSupplyFetch(context.Background(), "cfg", "ok")
	if ready, why := s.Ready(); !ready {
		t.Fatalf("not ready after all three conditions held: %s", why)
	}
}

func TestLifecycleStateWithoutASupplyGateNeedsNoFetch(t *testing.T) {
	s := newLifecycleState()
	s.OnSupplyGateWired(false)
	s.OnRegistered(context.Background(), "runner-1", false)
	s.OnHeartbeat(context.Background(), true)
	if ready, why := s.Ready(); !ready {
		t.Fatalf("a runner with no supply gate never becomes ready: %s", why)
	}
}

func TestLifecycleStateFailedHeartbeatRevokesReadiness(t *testing.T) {
	s := newLifecycleState()
	s.OnSupplyGateWired(false)
	s.OnRegistered(context.Background(), "runner-1", false)
	s.OnHeartbeat(context.Background(), true)
	if ready, _ := s.Ready(); !ready {
		t.Fatal("precondition: expected ready")
	}
	s.OnHeartbeat(context.Background(), false)
	if ready, _ := s.Ready(); ready {
		t.Fatal("still ready after a failed heartbeat; the connection is gone and the pod should stop taking work")
	}
}

func TestLifecycleStateFailsFastWhenSupplyEncryptionIsRequiredButAbsent(t *testing.T) {
	s := newLifecycleState()
	s.requireSupplyEncryption = true

	var got error
	s.SetOnFatal(func(err error) { got = err })

	s.OnRegistered(context.Background(), "runner-1", false)
	if got == nil {
		t.Fatal("registration without a supply key did not trip the fatal callback under --require-supply-encryption")
	}
	if s.Fatal() == nil {
		t.Fatal("Fatal() is nil after the callback fired")
	}
	if ready, _ := s.Ready(); ready {
		t.Fatal("a runner in a fatal state reported ready")
	}
}

func TestLifecycleStateAcceptsAnIssuedSupplyKey(t *testing.T) {
	s := newLifecycleState()
	s.requireSupplyEncryption = true
	s.SetOnFatal(func(error) { t.Error("fatal callback fired despite an issued supply key") })
	s.OnRegistered(context.Background(), "runner-1", true)
	if s.Fatal() != nil {
		t.Fatalf("Fatal() = %v, want nil", s.Fatal())
	}
}

func TestLifecycleStateIgnoresAMissingKeyWhenNotRequired(t *testing.T) {
	s := newLifecycleState()
	s.SetOnFatal(func(error) { t.Error("fatal callback fired without --require-supply-encryption") })
	s.OnRegistered(context.Background(), "runner-1", false)
	if s.Fatal() != nil {
		t.Fatalf("Fatal() = %v, want nil", s.Fatal())
	}
}

func TestLifecycleProbeHandlers(t *testing.T) {
	s := newLifecycleState()
	mux := http.NewServeMux()
	registerLifecycleProbes(mux, s)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/healthz = %d, want 200: the process is alive regardless of connection state", rec.Code)
	}

	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz on a disconnected runner = %d, want 503", rec.Code)
	}

	s.OnSupplyGateWired(false)
	s.OnRegistered(context.Background(), "runner-1", false)
	s.OnHeartbeat(context.Background(), true)

	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/readyz on a connected runner = %d, want 200 (body %q)", rec.Code, rec.Body.String())
	}
}
