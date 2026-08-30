package wasm

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/internal/code/script/engine"
	"github.com/xbcio/xflow/node/supply"
)

// newSourceDrivenModuleWithoutContent builds a fresh reactor module, marks it
// source-driven via RegisterSupplyConsumer, and forces its engine into
// existence (mirroring TestRegisterSupplyConsumerBeforeCompileMarksSourceDriven,
// supply_consumer_test.go:19) WITHOUT ever applying supply content. It returns
// the module's artifact digest ("sha256:<64 hex>") so callers can drive
// SupplyConfiguredByDigest the same way production does.
func newSourceDrivenModuleWithoutContent(t *testing.T) string {
	t.Helper()
	code := testReactorCode(t)
	reg := supply.NewRegistry()
	if err := RegisterSupplyConsumer(code, "rules", reg); err != nil {
		t.Fatalf("RegisterSupplyConsumer: %v", err)
	}
	if _, err := sharedReactorHost.engineForCode(context.Background(), code); err != nil {
		t.Fatalf("engineForCode: %v", err)
	}
	return "sha256:" + mustModuleKey(t, code)
}

// newSourceDrivenModuleWithContent is newSourceDrivenModuleWithoutContent plus
// one supply.Registry.Apply carrying content and revision (following
// TestRegistrationAndEngineCreationResolveInEitherOrder's and
// TestOnSupplyChangedCarriesServerRevision's engine-construction and
// supply-apply conventions). It returns the module's artifact digest.
func newSourceDrivenModuleWithContent(t *testing.T, content []byte, revision uint64) string {
	t.Helper()
	code := testReactorCode(t)
	reg := supply.NewRegistry()
	if err := RegisterSupplyConsumer(code, "rules", reg); err != nil {
		t.Fatalf("RegisterSupplyConsumer: %v", err)
	}
	if _, err := sharedReactorHost.engineForCode(context.Background(), code); err != nil {
		t.Fatalf("engineForCode: %v", err)
	}
	if err := reg.Apply(context.Background(), supply.Snapshot{
		Name: "rules", Content: content, Hash: "h1", Revision: revision,
		FetchedAt: time.Now(),
	}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	return "sha256:" + mustModuleKey(t, code)
}

// TestSupplyConfiguredIsFalseBeforeContent is the predicate §4.3.1 hangs on. A
// module that is marked source-driven but has never been handed a supply-borne
// config refuses every message; running it anyway is the silent pass-through
// this whole change exists to close. "Registration returned nil" does not
// distinguish the two states -- OnSupplyChanged returns nil when the engine does
// not exist yet (supply_consumer.go:145-155).
func TestSupplyConfiguredIsFalseBeforeContent(t *testing.T) {
	// Build a module and mark it source-driven WITHOUT applying any content.
	// (Copy the engine-construction lines from
	// TestRegisterSupplyConsumerBeforeCompileMarksSourceDriven at
	// supply_consumer_test.go:19 -- same helper, same guest.)
	digest := newSourceDrivenModuleWithoutContent(t)

	if SupplyConfiguredByDigest(digest) {
		t.Fatal("a source-driven module with no supply content reported configured; " +
			"§4.3.1's guard would let it run against an empty rule set")
	}
}

// TestSupplyConfiguredIsTrueAfterApply: the positive control. Without it the
// test above would pass for a function that always returns false.
func TestSupplyConfiguredIsTrueAfterApply(t *testing.T) {
	digest := newSourceDrivenModuleWithContent(t, []byte(`{"rules":[{"name":"r","expr":"true"}]}`), 42)
	if !SupplyConfiguredByDigest(digest) {
		t.Fatal("a module configured from a supply reported NOT configured; the guard " +
			"is too tight and would fail every execution")
	}
}

// TestSupplyConfiguredAcceptsEmptyRuleset: an operator may legitimately publish
// an empty rule set (supply_consumer_test.go:254 pins that it is accepted). The
// predicate must key on the config's PROVENANCE, not its byte length, or that
// deployment fails closed forever.
func TestSupplyConfiguredAcceptsEmptyRuleset(t *testing.T) {
	digest := newSourceDrivenModuleWithContent(t, []byte(`{"rules":[]}`), 7)
	if !SupplyConfiguredByDigest(digest) {
		t.Fatal("an empty-but-supply-borne rule set reported NOT configured; a legitimate " +
			"empty ruleset would fail-close every message forever")
	}
}

// TestSupplyConfiguredRejectsMalformedDigest: the string is caller-supplied and
// must never be turned into a map key without shape validation.
func TestSupplyConfiguredRejectsMalformedDigest(t *testing.T) {
	for _, d := range []string{"", "sha256:", "deadbeef", "sha256:zz"} {
		if SupplyConfiguredByDigest(d) {
			t.Fatalf("malformed digest %q reported configured", d)
		}
	}
}

// TestSupplyConfiguredFalseWhenLegacyPoolPredatesSupplyRegistration pins the
// state review round 1 found missing coverage for: configFromSource==true with
// an active pool whose revision==0, reached through the SAME production
// functions that produce it outside tests -- not by poking struct fields.
//
// This state is reachable, and it does not need a race or a misconfiguration
// flag: reactorFacade.Execute's legacy branch (reactor.go, gated on
// !configFromSource.Load()) calls ensurePool, which always swaps in a pool via
// swapConfig(..., revision=0) (host.go's ensurePool, "Legacy globals path: the
// content has no SupplyResource revision"). reactorHost.engines is keyed
// process-wide by module content sha256 (host.go:28), with no per-workflow
// scoping, so any module whose bytes were EVER run through the legacy $config
// path already carries a revision==0 active pool under that key. Registering
// that same content hash as a supply consumer afterward
// (RegisterSupplyConsumerByDigest -> seedSourceDrivenByKey, host.go:385-390)
// flips configFromSource on the very same *reactorEngine immediately --
// Registry.RegisterConsumer only synchronously overwrites the pool if the
// registry already has cached content for that supply name (registry.go's
// `if !has { return }`); against a freshly constructed registry with nothing
// applied yet, it does not, and the stale legacy pool is left in place with
// configFromSource now true.
//
// This is why the check cannot be "engine exists and is marked source-driven":
// SupplyConfiguredByDigest must also confirm the pool THIS module is serving
// actually came from a supply (revision != 0), not from an unrelated legacy
// caller that happened to run the same bytes first.
func TestSupplyConfiguredFalseWhenLegacyPoolPredatesSupplyRegistration(t *testing.T) {
	ctx := context.Background()
	code := testReactorCode(t)

	// Legacy path: some caller (a plain xflow.Script node with no supply
	// binding) runs this module through globals["$config"], which builds an
	// active pool with revision 0 -- exactly what reactorFacade.Execute's
	// legacy branch does via ensurePool.
	e, err := sharedReactorHost.engineForCode(ctx, code)
	if err != nil {
		t.Fatalf("engineForCode: %v", err)
	}
	if err := e.ensurePool(ctx, []byte(`{"rules":[]}`), defaultPoolSize()); err != nil {
		t.Fatalf("ensurePool: %v", err)
	}

	// Activation time, later: this same module's content hash is registered as
	// a supply consumer against a registry that has not yet fetched/cached
	// "rules". RegisterConsumer's synchronous notify is skipped (nothing
	// cached), so nothing swaps the pool -- but the module is now marked
	// source-driven.
	reg := supply.NewRegistry()
	digest := "sha256:" + mustModuleKey(t, code)
	if err := RegisterSupplyConsumerByDigest(digest, "rules", "test-owner", reg); err != nil {
		t.Fatalf("RegisterSupplyConsumerByDigest: %v", err)
	}

	if SupplyConfiguredByDigest(digest) {
		t.Fatal("a module reported configured while its active pool is the stale " +
			"legacy (revision==0) one built before supply registration; a p != nil " +
			"check alone cannot tell this apart from a real supply-borne pool")
	}
}

// TestSourceDrivenEngineRefusesLegacyPool pins Z.1: once a module is marked
// source-driven, a pool that came from the legacy globals path
// (activePool.revision == 0) must NOT be served. It is not the registering
// node that this protects -- SupplyConfiguredByDigest already fail-closes that
// one (see TestSupplyConfiguredFalseWhenLegacyPoolPredatesSupplyRegistration,
// which builds the identical state) -- it is a SECOND node sharing the same
// byte-identical module and declaring no supplies of its own. That node is
// gated by nothing: script.go:281-282 admits only declared nodes to the
// execution-time guard, so it reaches reactorFacade.Execute directly, takes the
// source-driven branch on the shared engine, and availability() is the only
// thing standing between it and the stale legacy rules.
func TestSourceDrivenEngineRefusesLegacyPool(t *testing.T) {
	ctx := context.Background()
	code := testReactorCode(t)

	e, err := sharedReactorHost.engineForCode(ctx, code)
	if err != nil {
		t.Fatalf("engineForCode: %v", err)
	}
	if err := e.ensurePool(ctx, []byte(`{"rules":[]}`), defaultPoolSize()); err != nil {
		t.Fatalf("ensurePool: %v", err)
	}
	// Before registration this is a legitimate legacy module and must serve.
	if got := e.availability(); got != AvailFresh {
		t.Fatalf("legacy module before any supply registration: availability = %v, want AvailFresh", got)
	}

	reg := supply.NewRegistry()
	digest := "sha256:" + mustModuleKey(t, code)
	if err := RegisterSupplyConsumerByDigest(digest, "rules", "test-owner", reg); err != nil {
		t.Fatalf("RegisterSupplyConsumerByDigest: %v", err)
	}

	if got := e.availability(); got != AvailUnavailable {
		t.Fatalf("source-driven module whose active pool is the legacy revision-0 one: "+
			"availability = %v, want AvailUnavailable (serving it hands an undeclared "+
			"sibling node stale globals rules under a Fresh verdict)", got)
	}
}

// TestUndeclaredSiblingIsRefusedAfterDigestRegistration is the data-plane half
// of TestSourceDrivenEngineRefusesLegacyPool: it drives reactorFacade.Execute
// the way an undeclared node does (globals carrying $config, no declaration
// anywhere) and requires a transient refusal rather than a served result.
func TestUndeclaredSiblingIsRefusedAfterDigestRegistration(t *testing.T) {
	ctx := context.Background()
	code := testReactorCode(t)

	f := &reactorFacade{host: sharedReactorHost}
	src := engine.Source{Code: code}
	globals := map[string]any{reactorConfigGlobal: map[string]any{"rules": []any{}}}

	// The undeclared node's first message: legacy path, builds a revision-0 pool.
	if _, err := f.Execute(ctx, src, globals, engine.DefaultHelpers()); err != nil {
		t.Fatalf("legacy execute before registration: %v", err)
	}

	// A DIFFERENT node, sharing the identical module, registers as a supply
	// consumer. Nothing about the undeclared node changed.
	reg := supply.NewRegistry()
	if err := RegisterSupplyConsumerByDigest("sha256:"+mustModuleKey(t, code), "rules", "test-owner", reg); err != nil {
		t.Fatalf("RegisterSupplyConsumerByDigest: %v", err)
	}

	_, err := f.Execute(ctx, src, globals, engine.DefaultHelpers())
	if err == nil {
		t.Fatal("undeclared sibling was served after the module became source-driven; " +
			"it must fail closed rather than run against the stale legacy rules")
	}
	if !strings.Contains(err.Error(), "wasm.unconfigured") {
		t.Fatalf("refusal reason = %q, want it to carry the wasm.unconfigured cause", err)
	}
}
