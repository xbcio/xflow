package runner

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/node/supply"
)

// recordingGateObserver captures every call so tests can assert both the
// content and, critically, that a gauge-style call is made on EVERY Admit —
// not just the first time a condition becomes true.
type recordingGateObserver struct {
	fetches     []fetchCall
	notReady    []notReadyCall
	unavailable []unavailableCall
}

type fetchCall struct{ name, result string }
type notReadyCall struct {
	workflow, supply string
	notReady         bool
}
type unavailableCall struct {
	name    string
	serving bool
}

func (r *recordingGateObserver) OnSupplyFetch(_ context.Context, name, result string) {
	r.fetches = append(r.fetches, fetchCall{name, result})
}
func (r *recordingGateObserver) OnSupplyNotReady(_ context.Context, workflow, supplyName string, notReady bool) {
	r.notReady = append(r.notReady, notReadyCall{workflow, supplyName, notReady})
}
func (r *recordingGateObserver) OnSupplyServingUnavailable(_ context.Context, name string, serving bool) {
	r.unavailable = append(r.unavailable, unavailableCall{name, serving})
}

// A successful fetch must report OnSupplyFetch("ok") and OnSupplyNotReady with
// notReady=false — not merely the absence of a notReady=true call, since a
// gauge that is never touched at all reads identically to one that is
// correctly at zero, which is not what this test wants to guard.
func TestAdmitNotifiesObserverOnSuccessfulFetch(t *testing.T) {
	rec := &recordingGateObserver{}
	f := &stubFetcher{content: map[string][]byte{"shared-rules": []byte(`{"rules":[]}`)}}
	reg := supply.NewRegistry()
	g := NewSupplyGate(f, reg, quietLogger())
	g.SetObserver(rec)

	if err := g.Admit(context.Background(), "wf1", []engine.SupplyRequirement{
		{Node: "rules", Resource: "shared-rules", RequireReady: true},
	}); err != nil {
		t.Fatalf("Admit: %v", err)
	}

	if len(rec.fetches) != 1 || rec.fetches[0] != (fetchCall{"shared-rules", "ok"}) {
		t.Fatalf("fetches = %#v, want one ok fetch for shared-rules", rec.fetches)
	}
	if len(rec.notReady) != 1 || rec.notReady[0] != (notReadyCall{"wf1", "rules", false}) {
		t.Fatalf("notReady = %#v, want one false entry for wf1/rules", rec.notReady)
	}
}

// A failed fetch of a required supply must report OnSupplyFetch("error") and
// OnSupplyNotReady(...,true).
func TestAdmitNotifiesObserverOnFetchError(t *testing.T) {
	rec := &recordingGateObserver{}
	f := &stubFetcher{err: errFetchBoom}
	reg := supply.NewRegistry()
	g := NewSupplyGate(f, reg, quietLogger())
	g.SetObserver(rec)

	err := g.Admit(context.Background(), "wf1", []engine.SupplyRequirement{
		{Node: "rules", Resource: "shared-rules", RequireReady: true},
	})
	if err == nil {
		t.Fatal("expected NotReadyError")
	}

	if len(rec.fetches) != 1 || rec.fetches[0] != (fetchCall{"shared-rules", "error"}) {
		t.Fatalf("fetches = %#v, want one error fetch for shared-rules", rec.fetches)
	}
	if len(rec.notReady) != 1 || rec.notReady[0] != (notReadyCall{"wf1", "rules", true}) {
		t.Fatalf("notReady = %#v, want one true entry for wf1/rules", rec.notReady)
	}
}

// require_ready:false with no content must report OnSupplyServingUnavailable
// with serving=true — this is the incident signal rule_count==0 cannot express.
func TestAdmitNotifiesObserverServingUnavailableWhenNotRequiredAndAbsent(t *testing.T) {
	rec := &recordingGateObserver{}
	f := &stubFetcher{content: map[string][]byte{}}
	reg := supply.NewRegistry()
	g := NewSupplyGate(f, reg, quietLogger())
	g.SetObserver(rec)

	if err := g.Admit(context.Background(), "wf1", []engine.SupplyRequirement{
		{Node: "hints", Resource: "hints", RequireReady: false},
	}); err != nil {
		t.Fatalf("Admit with require_ready=false must pass: %v", err)
	}

	if len(rec.unavailable) != 1 || rec.unavailable[0] != (unavailableCall{"hints", true}) {
		t.Fatalf("unavailable = %#v, want one serving=true entry for hints", rec.unavailable)
	}
}

// Once content arrives for a previously-unavailable-but-serving supply, the
// gauge must be told serving=false — it must clear, not just stop being set.
func TestAdmitNotifiesObserverServingResolvedWhenContentArrives(t *testing.T) {
	rec := &recordingGateObserver{}
	f := &stubFetcher{content: map[string][]byte{}}
	reg := supply.NewRegistry()
	g := NewSupplyGate(f, reg, quietLogger())
	g.SetObserver(rec)

	reqs := []engine.SupplyRequirement{{Node: "hints", Resource: "hints", RequireReady: false}}
	if err := g.Admit(context.Background(), "wf1", reqs); err != nil {
		t.Fatalf("first Admit: %v", err)
	}

	// Content now arrives.
	f.content["hints"] = []byte(`{"rules":[]}`)
	if err := g.Admit(context.Background(), "wf1", reqs); err != nil {
		t.Fatalf("second Admit: %v", err)
	}

	if len(rec.unavailable) != 2 {
		t.Fatalf("unavailable calls = %#v, want 2 (true then false)", rec.unavailable)
	}
	if rec.unavailable[0].serving != true || rec.unavailable[1].serving != false {
		t.Fatalf("unavailable = %#v, want [true, false]", rec.unavailable)
	}
}

// A nil observer must not panic — most Admit callers in this codebase run
// without one (e.g. every existing supply_gate_test.go case).
func TestAdmitWithNoObserverDoesNotPanic(t *testing.T) {
	f := &stubFetcher{content: map[string][]byte{"shared-rules": []byte(`ok`)}}
	reg := supply.NewRegistry()
	g := NewSupplyGate(f, reg, quietLogger())
	// Deliberately no SetObserver call.
	if err := g.Admit(context.Background(), "wf1", []engine.SupplyRequirement{
		{Node: "rules", Resource: "shared-rules", RequireReady: true},
	}); err != nil {
		t.Fatalf("Admit: %v", err)
	}
}

var errFetchBoom = fetchError("boom")

type fetchError string

func (e fetchError) Error() string { return string(e) }
