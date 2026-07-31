package wasm

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/supply"
)

// Registering a consumer for a module that has NOT been compiled yet must still
// mark it source-driven: at activation time the module is usually uncompiled, and
// losing this fact sends production traffic down the globals path where there are
// no rules at all.
func TestRegisterSupplyConsumerBeforeCompileMarksSourceDriven(t *testing.T) {
	code := testReactorCode(t)
	reg := supply.NewRegistry()
	if err := RegisterSupplyConsumer(code, "rules", reg); err != nil {
		t.Fatalf("RegisterSupplyConsumer: %v", err)
	}
	if !sharedReactorHost.isSourceDriven(code) {
		t.Fatal("code must be recorded source-driven before its engine exists")
	}
	e, err := sharedReactorHost.engineForCode(context.Background(), code)
	if err != nil {
		t.Fatalf("engineForCode: %v", err)
	}
	if !e.configFromSource.Load() {
		t.Fatal("engine created after registration must be source-driven")
	}
}

// Applying content must build a pool whose revision is the SERVER revision, so
// config_generation is comparable across runners.
func TestOnSupplyChangedCarriesServerRevision(t *testing.T) {
	code := testReactorCode(t)
	reg := supply.NewRegistry()
	if err := RegisterSupplyConsumer(code, "rules", reg); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := reg.Apply(context.Background(), supply.Snapshot{
		Name: "rules", Content: []byte(`{"rules":[]}`), Hash: "h1", Revision: 42,
		FetchedAt: time.Now(),
	}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	e, err := sharedReactorHost.engineForCode(context.Background(), code)
	if err != nil {
		t.Fatalf("engineForCode: %v", err)
	}
	if got := e.Generation(); got != 42 {
		t.Fatalf("Generation() = %d, want the server revision 42", got)
	}
}

// The double-check in engineForCode: a registration racing engine creation must
// not leave the module on the globals path.
func TestConcurrentRegisterAndEngineCreate(t *testing.T) {
	code := testReactorCode(t)
	reg := supply.NewRegistry()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = RegisterSupplyConsumer(code, "rules", reg)
	}()
	go func() {
		defer wg.Done()
		_, _ = sharedReactorHost.engineForCode(context.Background(), code)
	}()
	wg.Wait()

	e, err := sharedReactorHost.engineForCode(context.Background(), code)
	if err != nil {
		t.Fatalf("engineForCode: %v", err)
	}
	if !e.configFromSource.Load() {
		t.Fatal("a registration racing engine creation must still mark the engine source-driven")
	}
}

// A rejected supply content change must not tear down the module's last-good
// pool: OnSupplyChanged returns the error, but Execute keeps serving.
func TestOnSupplyChangedRejectsBadContentKeepsLastGood(t *testing.T) {
	code := testReactorCode(t)
	reg := supply.NewRegistry()
	if err := RegisterSupplyConsumer(code, "rules", reg); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := reg.Apply(context.Background(), supply.Snapshot{
		Name: "rules", Content: []byte(`{"rules":[{"name":"ok","expr":"x > 0"}]}`),
		Hash: "good", Revision: 1, FetchedAt: time.Now(),
	}); err != nil {
		t.Fatalf("Apply(good): %v", err)
	}
	e, err := sharedReactorHost.engineForCode(context.Background(), code)
	if err != nil {
		t.Fatalf("engineForCode: %v", err)
	}
	if got := e.availability(); got != AvailFresh {
		t.Fatalf("availability after good content = %v, want Fresh", got)
	}

	if err := reg.Apply(context.Background(), supply.Snapshot{
		Name: "rules", Content: []byte(`{"rules":[{"bad":true}]}`),
		Hash: "bad", Revision: 2, FetchedAt: time.Now(),
	}); err == nil {
		t.Fatal("bad content must be rejected by the consumer")
	}
	if got := e.Generation(); got != 1 {
		t.Fatalf("Generation() = %d after rejected content, want the last-good revision 1", got)
	}
}
