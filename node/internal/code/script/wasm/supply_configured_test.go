package wasm

import (
	"context"
	"testing"
	"time"

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
