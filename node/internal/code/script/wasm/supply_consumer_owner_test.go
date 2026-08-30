package wasm

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/supply"
)

// TestUnregisterByDigestKeepsOtherOwnerRegistered pins the owner-SET contract at
// the heart of this fix: RegisterSupplyConsumerByDigest / UnregisterSupplyConsumerByDigest
// key on (registry pointer, moduleKey, supplyNode) but track WHO asked for that
// registration as a set of owner identities, because three independent call
// sites (activation-time legacy binding, warm-up declaration consumer,
// execution-time guard) can all register the SAME (digest, supplyNode) pair
// while only one of them (Deactivate) ever calls the release side. A release
// that drops the underlying registry registration as soon as ANY owner leaves
// -- rather than only when the LAST owner leaves -- silently stops content
// delivery for every other owner still relying on it, with no error anywhere.
//
// Guards mutation M1: UnregisterSupplyConsumerByDigest calling
// reg.UnregisterConsumer unconditionally, ignoring the owner set. The
// observable here is deliberately NOT SupplyConfiguredByDigest -- that
// predicate only inspects configFromSource and the active pool's revision,
// both of which stay true/nonzero forever once set, so it cannot distinguish
// "still registered" from "registration silently dropped". The observable
// instead is whether a SUBSEQUENT supply.Registry.Apply still reaches the
// module at all: reg.RegisterConsumer/UnregisterConsumer, its own comment
// documents, deliver a notify to a registered consumer on every Apply whose
// content changed, so an engine that stops advancing its Generation() after
// owner A's release, even though owner B never left, proves the registration
// itself was torn down out from under B.
func TestUnregisterByDigestKeepsOtherOwnerRegistered(t *testing.T) {
	ctx := context.Background()
	code := testReactorCode(t)
	reg := supply.NewRegistry()
	digest := "sha256:" + mustModuleKey(t, code)

	// Two independent owners register against the identical (digest,
	// supplyNode) pair -- e.g. the activation-time legacy binding and the
	// execution-time guard, both keyed off the same module and the same
	// supply node, but registering under distinct owner identities.
	const (
		ownerA = "activation:ns/wf-1/v1/entry/0"
		ownerB = "node:wf-1/tagger"
	)
	if err := RegisterSupplyConsumerByDigest(digest, "rules", ownerA, reg); err != nil {
		t.Fatalf("RegisterSupplyConsumerByDigest(ownerA): %v", err)
	}
	if err := RegisterSupplyConsumerByDigest(digest, "rules", ownerB, reg); err != nil {
		t.Fatalf("RegisterSupplyConsumerByDigest(ownerB): %v", err)
	}
	t.Cleanup(func() {
		UnregisterSupplyConsumerByDigest(digest, "rules", ownerA, reg)
		UnregisterSupplyConsumerByDigest(digest, "rules", ownerB, reg)
	})

	e, err := sharedReactorHost.engineForCode(ctx, code)
	if err != nil {
		t.Fatalf("engineForCode: %v", err)
	}

	if err := reg.Apply(ctx, supply.Snapshot{
		Name: "rules", Content: contentWithRule("r1", "true"), Hash: "h1", Revision: 1,
		FetchedAt: time.Now(),
	}); err != nil {
		t.Fatalf("Apply(rev 1): %v", err)
	}
	if got := e.Generation(); got != 1 {
		t.Fatalf("Generation() = %d after the first Apply, want 1 -- precondition: the "+
			"registration must be live before either owner releases", got)
	}

	// Owner A releases. Owner B never does. The underlying registry
	// registration must survive, because it is still owned.
	UnregisterSupplyConsumerByDigest(digest, "rules", ownerA, reg)

	if err := reg.Apply(ctx, supply.Snapshot{
		Name: "rules", Content: contentWithRule("r2", "true"), Hash: "h2", Revision: 2,
		FetchedAt: time.Now(),
	}); err != nil {
		t.Fatalf("Apply(rev 2): %v", err)
	}
	if got := e.Generation(); got != 2 {
		t.Fatalf("Generation() = %d after owner A released and content changed again, want "+
			"2 -- owner A's release tore down the registration owner B still holds, so the "+
			"module stopped receiving supply content", got)
	}
}

// TestUnregisterByDigestIsIdempotentPerOwnerNotARefcount pins the other half of
// the contract: the SAME owner registering the same (digest, supplyNode) pair
// twice -- which happens in production whenever a generation upgrade re-sends
// an identical binding (service/runner/trigger_activation_handler.go) -- must
// NOT require two releases to fully unregister. The owner tracking is a SET
// (membership), not a map[string]int refcount: a refcount would increment to 2
// on the second register and only reach 0 -- and therefore only call
// reg.UnregisterConsumer -- after a SECOND release that this owner is never
// going to issue, since nothing in this codebase re-registers and then
// releases the same owner twice for one logical membership.
//
// Guards mutation M2: consumerOwners degraded from map[consumerOwnerKey]map[string]struct{}
// to map[consumerOwnerKey]map[string]int (or equivalent increment/decrement
// bookkeeping). As with M1, the observable is a subsequent Apply reaching the
// engine, not SupplyConfiguredByDigest.
func TestUnregisterByDigestIsIdempotentPerOwnerNotARefcount(t *testing.T) {
	ctx := context.Background()
	code := testReactorCode(t)
	reg := supply.NewRegistry()
	digest := "sha256:" + mustModuleKey(t, code)
	const owner = "node:wf-1/tagger"

	// The SAME owner registers twice -- e.g. two generations of the same
	// activation re-sending an identical binding.
	if err := RegisterSupplyConsumerByDigest(digest, "rules", owner, reg); err != nil {
		t.Fatalf("RegisterSupplyConsumerByDigest (1st): %v", err)
	}
	if err := RegisterSupplyConsumerByDigest(digest, "rules", owner, reg); err != nil {
		t.Fatalf("RegisterSupplyConsumerByDigest (2nd, same owner): %v", err)
	}
	t.Cleanup(func() { UnregisterSupplyConsumerByDigest(digest, "rules", owner, reg) })

	e, err := sharedReactorHost.engineForCode(ctx, code)
	if err != nil {
		t.Fatalf("engineForCode: %v", err)
	}

	if err := reg.Apply(ctx, supply.Snapshot{
		Name: "rules", Content: contentWithRule("r1", "true"), Hash: "h1", Revision: 1,
		FetchedAt: time.Now(),
	}); err != nil {
		t.Fatalf("Apply(rev 1): %v", err)
	}
	if got := e.Generation(); got != 1 {
		t.Fatalf("Generation() = %d after the first Apply, want 1 -- precondition: the "+
			"registration must be live before the single release below", got)
	}

	// ONE release for an owner that registered TWICE must fully unregister:
	// membership in a set is binary, not a count.
	UnregisterSupplyConsumerByDigest(digest, "rules", owner, reg)

	if err := reg.Apply(ctx, supply.Snapshot{
		Name: "rules", Content: contentWithRule("r2", "true"), Hash: "h2", Revision: 2,
		FetchedAt: time.Now(),
	}); err != nil {
		t.Fatalf("Apply(rev 2): %v", err)
	}
	if got := e.Generation(); got != 1 {
		t.Fatalf("Generation() = %d after Apply following the single release, want it to "+
			"stay at 1 -- a single release fully removed the only owner, so the "+
			"registration must be gone and this Apply must not reach the module; "+
			"Generation() advancing to 2 means the owner tracking degraded into a "+
			"refcount that needed a second release", got)
	}
}
