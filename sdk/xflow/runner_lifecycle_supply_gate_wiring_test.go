package xflow

import (
	"context"
	"sync"
	"testing"

	"github.com/xbcio/xflow/engine"
)

// lifecycleObserverSpy is a minimal runnersvc.LifecycleObserver that records
// what wireSupplyGateObserver reports directly, and what the supply gate's
// fanout forwards to it after a real Admit call. It intentionally implements
// only the recording behavior the tests below assert on; OnRegistered and
// OnHeartbeat are no-ops because this package's runner assembly never drives
// them (that happens inside service/runner.Runner.Run, which needs a live
// control plane connection this test does not have).
type lifecycleObserverSpy struct {
	mu           sync.Mutex
	wiredCalled  bool
	wiredPresent bool
	fetches      []lifecycleFetchCall
}

type lifecycleFetchCall struct {
	name   string
	result string
}

func (s *lifecycleObserverSpy) OnRegistered(context.Context, string, bool) {}
func (s *lifecycleObserverSpy) OnHeartbeat(context.Context, bool)          {}

func (s *lifecycleObserverSpy) OnSupplyGateWired(present bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.wiredCalled = true
	s.wiredPresent = present
}

func (s *lifecycleObserverSpy) OnSupplyFetch(_ context.Context, name, result string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fetches = append(s.fetches, lifecycleFetchCall{name: name, result: result})
}

func (s *lifecycleObserverSpy) snapshot() (wiredCalled, wiredPresent bool, fetches []lifecycleFetchCall) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.wiredCalled, s.wiredPresent, append([]lifecycleFetchCall(nil), s.fetches...)
}

// TestWireSupplyGateObserverReachesTheLifecycleObserverOnFetch pins that the
// lifecycle observer passed to WithRunnerLifecycleObserver is actually wired
// into the supply gate's fanout observer — not merely fed the one-shot
// OnSupplyGateWired(true) call below and then dropped.
//
// This is the test that kills the mutation which empties supplyGateFanout's
// lifecycle field (fanout := supplyGateFanout{} instead of
// supplyGateFanout{lifecycle: o.lifecycleObserver}): with that mutation
// OnSupplyGateWired(true) still fires exactly as asserted here, but the spy
// would never observe the OnSupplyFetch this test drives through a real
// Admit call — which is exactly how a runner's /readyz stays stuck on "no
// supply has been fetched yet" forever.
func TestWireSupplyGateObserverReachesTheLifecycleObserverOnFetch(t *testing.T) {
	spy := &lifecycleObserverSpy{}
	cfg, err := buildRunnerServiceConfig(RunnerConfig{
		// 127.0.0.1:1 removes DNS resolution from this test's assertion: the
		// bare hostname "server" used elsewhere in this package could
		// resolve and have something answer on :8080 in some environments,
		// making the "error" assertion below flaky. Port 1 on loopback is
		// never listening, so the connection is refused immediately and
		// deterministically, with no name lookup involved.
		ServerURL:    "http://127.0.0.1:1",
		Capabilities: []string{"xflow.trigger.kafka"},
	}, WithRunnerLifecycleObserver(spy))
	if err != nil {
		t.Fatalf("buildRunnerServiceConfig: %v", err)
	}
	if cfg.SupplyGate == nil {
		t.Fatal("cfg.SupplyGate is nil — a trigger capability should have wired it")
	}

	wiredCalled, wiredPresent, _ := spy.snapshot()
	if !wiredCalled || !wiredPresent {
		t.Fatalf("OnSupplyGateWired: called=%v present=%v, want called=true present=true", wiredCalled, wiredPresent)
	}

	// Admit declines — the fetcher targets an unreachable origin — but that
	// decline is exactly the codepath that reports OnSupplyFetch(name,
	// "error"). The failed fetch is the trigger this test needs, not a defect
	// in it.
	_ = cfg.SupplyGate.Admit(context.Background(), "wf-1", []engine.SupplyRequirement{
		{Node: "rules", Resource: "rules", RequireReady: true},
	})

	_, _, fetches := spy.snapshot()
	if len(fetches) != 1 {
		t.Fatalf("lifecycle observer saw %d OnSupplyFetch call(s), want 1 — the "+
			"fanout the assembly builds must forward every fetch to the lifecycle "+
			"observer, not only to metrics", len(fetches))
	}
	if fetches[0].name != "rules" {
		t.Errorf("OnSupplyFetch name = %q, want %q", fetches[0].name, "rules")
	}
	if fetches[0].result != "error" {
		t.Errorf("OnSupplyFetch result = %q, want %q (the fetch targets an unreachable origin)",
			fetches[0].result, "error")
	}
}

// TestWireSupplyGateObserverReportsGateAbsent pins that a runner with no
// supply gate (no hosted trigger capability) still reports
// OnSupplyGateWired(false) — the call happens unconditionally, before the
// nil-gate early return, not gated behind it.
//
// This is the test that kills the mutation deleting the whole
// "o.lifecycleObserver.OnSupplyGateWired(...)" call: without it, a runner
// that hosts no triggers never reports its supply-gate state at all, and
// cmd/runner's lifecycleState.Ready() reads "supply gate state unknown"
// forever — a passive runner that never hosted a trigger would sit un-ready
// alongside one still waiting on a real fetch.
func TestWireSupplyGateObserverReportsGateAbsent(t *testing.T) {
	spy := &lifecycleObserverSpy{}
	cfg, err := buildRunnerServiceConfig(RunnerConfig{
		ServerURL:    "http://127.0.0.1:1",
		Capabilities: []string{"xflow.function"},
	}, WithRunnerLifecycleObserver(spy))
	if err != nil {
		t.Fatalf("buildRunnerServiceConfig: %v", err)
	}
	if cfg.SupplyGate != nil {
		t.Fatal("cfg.SupplyGate is non-nil — xflow.function is not a trigger capability")
	}

	wiredCalled, wiredPresent, _ := spy.snapshot()
	if !wiredCalled {
		t.Fatal("OnSupplyGateWired was never called for a runner with no supply gate; " +
			"it would stay un-ready forever waiting for a fetch that cannot happen")
	}
	if wiredPresent {
		t.Error("OnSupplyGateWired reported present=true with a nil SupplyGate")
	}
}
