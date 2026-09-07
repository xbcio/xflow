package main

import (
	"context"
	"net/http"
	"sync"
)

// lifecycleState is the runner's probe-visible connection state. It implements
// runnersvc.LifecycleObserver; every method is called from the registration or
// heartbeat path, so all of them are O(1) under a plain mutex and none of them
// do I/O.
type lifecycleState struct {
	mu sync.Mutex
	// registered latches on the first successful registration. It does not
	// clear on a failed heartbeat: the process reconnects, and the identity it
	// registered under is still the one it will use.
	registered bool
	// heartbeatOK tracks the LAST heartbeat, not "ever". A runner whose
	// connection has just died must stop reporting ready even though it
	// registered fine minutes ago.
	heartbeatOK bool
	// supplyGateKnown/supplyGatePresent/supplyFetched are the third readiness
	// condition. Before assembly reports in, readiness is withheld: "we have
	// not been told whether a gate exists" must not read as "no gate exists".
	supplyGateKnown   bool
	supplyGatePresent bool
	supplyFetched     bool
}

func newLifecycleState() *lifecycleState { return &lifecycleState{} }

func (s *lifecycleState) OnRegistered(_ context.Context, _ string, _ bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.registered = true
}

func (s *lifecycleState) OnHeartbeat(_ context.Context, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.heartbeatOK = ok
}

func (s *lifecycleState) OnSupplyGateWired(present bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.supplyGateKnown = true
	s.supplyGatePresent = present
}

func (s *lifecycleState) OnSupplyFetch(_ context.Context, _, result string) {
	if result != "ok" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.supplyFetched = true
}

// Live reports process liveness. It is deliberately unconditional: a runner
// that cannot reach its control plane is not broken, it is disconnected, and
// killing it does not help. Readiness is what takes it out of rotation.
func (s *lifecycleState) Live() bool { return true }

// Ready reports readiness plus, when not ready, the reason — which is what
// makes a 503 actionable instead of an unexplained pod that never comes up.
func (s *lifecycleState) Ready() (bool, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case !s.registered:
		return false, "not registered with the control plane"
	case !s.heartbeatOK:
		return false, "no successful heartbeat"
	case !s.supplyGateKnown:
		return false, "supply gate state unknown"
	case s.supplyGatePresent && !s.supplyFetched:
		return false, "no supply has been fetched yet"
	}
	return true, "ok"
}

// registerLifecycleProbes mounts the two probes on the mux that already serves
// /metrics. No new port: an operator who exposed one has exposed all three.
func registerLifecycleProbes(mux *http.ServeMux, s *lifecycleState) {
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		if !s.Live() {
			http.Error(w, "dead", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		ready, why := s.Ready()
		if !ready {
			http.Error(w, why, http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok"))
	})
}
