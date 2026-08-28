package runner

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/supply"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// TestWarmupCompilesOnPointerChange is §4.5's whole point: the module named by
// the NEW pointer must be compiled and serving supply-borne config before the
// first message arrives, not during it.
//
// The criterion is node.WasmSupplyConfigured, not "Apply returned nil". Apply
// returns nil for a consumer that did nothing.
//
// DigestExpr below uses $supplies["name"] bracket indexing rather than
// $supplies.name dot access (the plan's literal draft used dot access). The
// pointer name comes from testSupplyName, which prefixes every test's name
// with "rules-" and appends "-ptr" -- and expr-lang parses a bare
// $supplies.rules-Foo-ptr as subtraction of undefined identifiers, not a
// single member-access chain, so the expression fails to render and every
// assertion below reads as "warm-up never ran" regardless of whether the
// consumer is implemented. Bracket indexing is this codebase's own existing
// pattern for a supply key that is not a plain identifier (see
// exprx/supplies_test.go's $supplies.rules["$revision"]); it is not a scope
// change to the consumer itself, which still renders whatever DigestExpr a
// real control plane sends.
func TestWarmupCompilesOnPointerChange(t *testing.T) {
	f := newWasmActivationFixture(t)
	const workflow, nodeName = "TestWarmupCompilesOnPointerChange-collect", "decode"
	ptr := testSupplyName(t) + "-ptr"

	// The pointer supply carries the digest the expression reads.
	if err := supply.Default.Apply(context.Background(), supply.Snapshot{
		Name:      ptr,
		Content:   []byte(`{"decode":{"digest":"` + f.digest + `","version":"v1"}}`),
		Hash:      "p1",
		Revision:  1,
		FetchedAt: time.Now(),
	}); err != nil {
		t.Fatalf("apply pointer: %v", err)
	}

	fh := &fakeTriggerHandler{}
	h := NewTriggerActivationHandler("https://control.internal", "",
		fakeLookup{handlers: map[string]types.TriggerHandler{"fake": fh}},
		WithArtifactCodeResolver(f.resolver))

	decl := engine.SupplyConsumerBinding{
		SupplyNode:   ptr,
		WorkflowName: workflow,
		NodeName:     nodeName,
		DigestExpr:   "${{ $supplies[\"" + ptr + "\"].decode.digest }}",
	}
	t.Cleanup(func() {
		_ = h.Deactivate(protocol.DeactivateDirective{WorkflowID: "wf-1", EntryUnitID: "trig"})
	})

	if err := h.Activate(context.Background(), protocol.ActivateDirective{
		WorkflowID: "wf-1", EntryUnitID: "trig", NodeType: "fake", Generation: 1,
		Supplies:        []engine.SupplyRequirement{{Node: ptr}},
		SupplyConsumers: []engine.SupplyConsumerBinding{decl},
	}); err != nil {
		t.Fatalf("activate: %v", err)
	}
	if fh.gotInput == nil {
		t.Fatal("the trigger handler was never activated")
	}
	if !node.WasmSupplyConfigured(f.digest) {
		t.Fatalf("module %s is not configured after activation; the warm-up consumer "+
			"did not compile it and the first message will pay the compile", f.digest)
	}
}

// TestPointerSupplyIsNotReadyUntilWarmupSucceeds is §9.3 probe (2)'s criterion.
// Without a consumer on the pointer supply, isReadyLocked's loop body is empty
// and the supply reads ready the instant the snapshot lands (registry.go:331-332
// says so outright) -- a probe hung on that would go green with this feature
// entirely unimplemented.
func TestPointerSupplyIsNotReadyUntilWarmupSucceeds(t *testing.T) {
	const workflow, nodeName = "TestPointerNotReady-collect", "decode"
	ptr := testSupplyName(t) + "-ptr"

	// A pointer naming a digest no resolver can serve: warm-up must fail.
	const missing = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	failing := func(_ context.Context, _ string) ([]byte, error) {
		return nil, context.DeadlineExceeded
	}
	fh := &fakeTriggerHandler{}
	h := NewTriggerActivationHandler("https://control.internal", "",
		fakeLookup{handlers: map[string]types.TriggerHandler{"fake": fh}},
		WithArtifactCodeResolver(failing))

	if err := supply.Default.Apply(context.Background(), supply.Snapshot{
		Name:      ptr,
		Content:   []byte(`{"decode":{"digest":"` + missing + `"}}`),
		Hash:      "p1",
		Revision:  1,
		FetchedAt: time.Now(),
	}); err != nil {
		t.Fatalf("apply pointer: %v", err)
	}
	decl := engine.SupplyConsumerBinding{
		SupplyNode:   ptr,
		WorkflowName: workflow,
		NodeName:     nodeName,
		DigestExpr:   "${{ $supplies[\"" + ptr + "\"].decode.digest }}",
	}
	t.Cleanup(func() {
		_ = h.Deactivate(protocol.DeactivateDirective{WorkflowID: "wf-2", EntryUnitID: "trig"})
	})
	_ = h.Activate(context.Background(), protocol.ActivateDirective{
		WorkflowID: "wf-2", EntryUnitID: "trig", NodeType: "fake", Generation: 1,
		Supplies:        []engine.SupplyRequirement{{Node: ptr}},
		SupplyConsumers: []engine.SupplyConsumerBinding{decl},
	})

	if supply.Default.IsReady(ptr) {
		t.Fatal("the pointer supply reads ready although its module could not be " +
			"fetched; the run-state report would claim the new version is live")
	}
}

// TestWarmupRetriesAfterFailure: §4.5 leans on level-triggered hints for
// recovery -- a failed compile keeps the hash out of Observed, the next
// heartbeat hint still mismatches, and the runner re-applies. That claim must be
// measured, not assumed. Re-applying the same content must re-notify, because
// the consumer's verdict for it is still an error.
func TestWarmupRetriesAfterFailure(t *testing.T) {
	f := newWasmActivationFixture(t)
	ptr := testSupplyName(t) + "-ptr"

	var calls int
	flaky := func(ctx context.Context, digest string) ([]byte, error) {
		calls++
		if calls == 1 {
			return nil, context.DeadlineExceeded
		}
		return f.raw, nil
	}
	fh := &fakeTriggerHandler{}
	h := NewTriggerActivationHandler("https://control.internal", "",
		fakeLookup{handlers: map[string]types.TriggerHandler{"fake": fh}},
		WithArtifactCodeResolver(flaky))

	snap := supply.Snapshot{
		Name:      ptr,
		Content:   []byte(`{"decode":{"digest":"` + f.digest + `"}}`),
		Hash:      "p1",
		Revision:  1,
		FetchedAt: time.Now(),
	}
	if err := supply.Default.Apply(context.Background(), snap); err != nil {
		t.Fatalf("apply pointer: %v", err)
	}
	decl := engine.SupplyConsumerBinding{
		SupplyNode: ptr, WorkflowName: "TestWarmupRetries-collect", NodeName: "decode",
		DigestExpr: "${{ $supplies[\"" + ptr + "\"].decode.digest }}",
	}
	t.Cleanup(func() {
		_ = h.Deactivate(protocol.DeactivateDirective{WorkflowID: "wf-3", EntryUnitID: "trig"})
	})
	_ = h.Activate(context.Background(), protocol.ActivateDirective{
		WorkflowID: "wf-3", EntryUnitID: "trig", NodeType: "fake", Generation: 1,
		Supplies:        []engine.SupplyRequirement{{Node: ptr}},
		SupplyConsumers: []engine.SupplyConsumerBinding{decl},
	})
	if supply.Default.IsReady(ptr) {
		t.Fatal("ready after the first (failing) warm-up")
	}

	// Same content, same hash: the level-triggered re-send.
	if err := supply.Default.Apply(context.Background(), snap); err != nil {
		t.Fatalf("re-apply: %v", err)
	}
	if calls < 2 {
		t.Fatalf("the resolver was called %d time(s); re-applying identical content did "+
			"NOT re-notify, so a failed warm-up would never recover", calls)
	}
	if !supply.Default.IsReady(ptr) {
		t.Fatal("still not ready after a successful retry")
	}
}

// TestWarmupConsumerSurvivesIdenticalGenerationUpgrade pins RULING 4's hazard:
// a generation upgrade that re-sends an IDENTICAL declaration binding must NOT
// drop the warm-up consumer it (re)registered. The declaration shape carries
// two independent registrations -- the refcounted declaration table (which
// storeSubscription must undeclare unconditionally for every old entry, to
// avoid a refcount leak) and the warm-up consumer's SET registration on the
// pointer supply (which must be reconciled by DIFFERENCE, exactly like the
// legacy shape, or a retained pair's warm-up consumer is deregistered the
// instant it is re-registered).
//
// If the warm-up consumer's unregistration is incorrectly routed through the
// refcount's "undeclare every old declaration" pass (e.g. by calling
// unregisterSupplyConsumers(oldDeclarations) from storeSubscription, as a
// naive reading of the brief's Step 3 diff would do), this test fails: after
// the generation upgrade the pointer supply has no consumer at all, so the
// SECOND pointer flip (to digest B) is never warmed up and
// node.WasmSupplyConfigured(B) stays false.
func TestWarmupConsumerSurvivesIdenticalGenerationUpgrade(t *testing.T) {
	fa := newWasmActivationFixture(t) // digest A
	fb := newWasmActivationFixture(t) // digest B, distinct content (random nonce)
	const workflow, nodeName = "TestWarmupSurvivesUpgrade-collect", "decode"
	ptr := testSupplyName(t) + "-ptr"

	resolver := func(_ context.Context, digest string) ([]byte, error) {
		switch digest {
		case fa.digest:
			return fa.raw, nil
		case fb.digest:
			return fb.raw, nil
		default:
			return nil, context.DeadlineExceeded
		}
	}

	// Pointer initially names digest A.
	if err := supply.Default.Apply(context.Background(), supply.Snapshot{
		Name:      ptr,
		Content:   []byte(`{"decode":{"digest":"` + fa.digest + `"}}`),
		Hash:      "p1",
		Revision:  1,
		FetchedAt: time.Now(),
	}); err != nil {
		t.Fatalf("apply pointer (A): %v", err)
	}

	fh := &fakeTriggerHandler{}
	h := NewTriggerActivationHandler("https://control.internal", "",
		fakeLookup{handlers: map[string]types.TriggerHandler{"fake": fh}},
		WithArtifactCodeResolver(resolver))

	decl := engine.SupplyConsumerBinding{
		SupplyNode:   ptr,
		WorkflowName: workflow,
		NodeName:     nodeName,
		DigestExpr:   "${{ $supplies[\"" + ptr + "\"].decode.digest }}",
	}
	t.Cleanup(func() {
		_ = h.Deactivate(protocol.DeactivateDirective{WorkflowID: "wf-hazard", EntryUnitID: "trig"})
	})

	d := protocol.ActivateDirective{
		WorkflowID: "wf-hazard", EntryUnitID: "trig", NodeType: "fake", Generation: 1,
		Supplies:        []engine.SupplyRequirement{{Node: ptr}},
		SupplyConsumers: []engine.SupplyConsumerBinding{decl},
	}
	if err := h.Activate(context.Background(), d); err != nil {
		t.Fatalf("activate gen1: %v", err)
	}
	if !node.WasmSupplyConfigured(fa.digest) {
		t.Fatalf("digest A (%s) was not warmed up on gen1 activation", fa.digest)
	}

	// Gen2 re-sends the IDENTICAL declaration binding -- the ordinary
	// generation-upgrade case.
	d.Generation = 2
	if err := h.Activate(context.Background(), d); err != nil {
		t.Fatalf("activate gen2: %v", err)
	}

	// The level-triggered flip this warm-up consumer exists to catch: the
	// pointer now names a SECOND, previously-unseen digest.
	if err := supply.Default.Apply(context.Background(), supply.Snapshot{
		Name:      ptr,
		Content:   []byte(`{"decode":{"digest":"` + fb.digest + `"}}`),
		Hash:      "p2",
		Revision:  2,
		FetchedAt: time.Now(),
	}); err != nil {
		t.Fatalf("apply pointer (B): %v", err)
	}

	if !node.WasmSupplyConfigured(fb.digest) {
		t.Fatalf("digest B (%s) was not warmed up after the pointer flip following a "+
			"generation upgrade with an identical declaration -- the warm-up consumer "+
			"was dropped by the upgrade, so this runner never compiles the new module "+
			"and silently reverts to bare L1 readiness", fb.digest)
	}
}

// TestWarmupConsumerSurvivesReplicaSiblingDeactivate is fix-round-1 finding
// 1's regression. Replicas of the SAME activation compute the IDENTICAL
// warmupConsumerKey (it is keyed on workflow+node+supply, with no activation
// or replica identity -- see warmupConsumerKey's doc comment), because one
// activation may be hosted at several replicas on one runner, each declaring
// the same pair (activation_tracker.go's documented model; activationID
// differs only by ReplicaIndex). A naive register/unregister pair on that
// shared key would let one replica's Deactivate strip a still-live sibling's
// warm-up consumer. warmupConsumerRegistry (wasm_supply_declaration.go)
// exists to survive exactly this by refcounting the registration the same
// way the execution-time declaration table already does.
func TestWarmupConsumerSurvivesReplicaSiblingDeactivate(t *testing.T) {
	fa := newWasmActivationFixture(t) // digest A
	fb := newWasmActivationFixture(t) // digest B, distinct content
	const workflow, nodeName = "TestWarmupSurvivesReplicaSibling-collect", "decode"
	ptr := testSupplyName(t) + "-ptr"

	resolver := func(_ context.Context, digest string) ([]byte, error) {
		switch digest {
		case fa.digest:
			return fa.raw, nil
		case fb.digest:
			return fb.raw, nil
		default:
			return nil, context.DeadlineExceeded
		}
	}

	if err := supply.Default.Apply(context.Background(), supply.Snapshot{
		Name:      ptr,
		Content:   []byte(`{"decode":{"digest":"` + fa.digest + `"}}`),
		Hash:      "p1",
		Revision:  1,
		FetchedAt: time.Now(),
	}); err != nil {
		t.Fatalf("apply pointer (A): %v", err)
	}

	fh := &fakeTriggerHandler{}
	h := NewTriggerActivationHandler("https://control.internal", "",
		fakeLookup{handlers: map[string]types.TriggerHandler{"fake": fh}},
		WithArtifactCodeResolver(resolver))

	decl := engine.SupplyConsumerBinding{
		SupplyNode:   ptr,
		WorkflowName: workflow,
		NodeName:     nodeName,
		DigestExpr:   "${{ $supplies[\"" + ptr + "\"].decode.digest }}",
	}

	// Two replicas of the SAME activation (identical WorkflowID/EntryUnitID),
	// differing only by ReplicaIndex -- activationIDFromActivate therefore
	// assigns them two distinct activation identities, matching
	// activation_tracker.go's documented multi-replica model exactly.
	base := protocol.ActivateDirective{
		WorkflowID: "wf-replica", EntryUnitID: "trig", NodeType: "fake", Generation: 1,
		Supplies:        []engine.SupplyRequirement{{Node: ptr}},
		SupplyConsumers: []engine.SupplyConsumerBinding{decl},
	}
	replica0, replica1 := base, base
	replica0.ReplicaIndex, replica1.ReplicaIndex = 0, 1

	t.Cleanup(func() {
		_ = h.Deactivate(protocol.DeactivateDirective{
			WorkflowID: "wf-replica", EntryUnitID: "trig", ReplicaIndex: 1,
		})
	})
	if err := h.Activate(context.Background(), replica0); err != nil {
		t.Fatalf("activate replica 0: %v", err)
	}
	if err := h.Activate(context.Background(), replica1); err != nil {
		t.Fatalf("activate replica 1: %v", err)
	}
	if !node.WasmSupplyConfigured(fa.digest) {
		t.Fatalf("digest A (%s) was not warmed up after both replicas activated", fa.digest)
	}

	// Deactivate ONLY replica 0 (protocol.DeactivateDirective carries
	// ReplicaIndex, so this targets a single replica -- see
	// activationIDFromDeactivate). Replica 1 is still live and still depends
	// on the warm-up consumer registered under the (workflow, node, supply)
	// key both replicas share.
	if err := h.Deactivate(protocol.DeactivateDirective{
		WorkflowID: "wf-replica", EntryUnitID: "trig", ReplicaIndex: 0,
	}); err != nil {
		t.Fatalf("deactivate replica 0: %v", err)
	}

	// The level-triggered flip this warm-up consumer exists to catch: the
	// pointer now names a second, previously-unseen digest. If replica 0's
	// Deactivate stripped the SHARED warm-up consumer, nothing compiles B.
	if err := supply.Default.Apply(context.Background(), supply.Snapshot{
		Name:      ptr,
		Content:   []byte(`{"decode":{"digest":"` + fb.digest + `"}}`),
		Hash:      "p2",
		Revision:  2,
		FetchedAt: time.Now(),
	}); err != nil {
		t.Fatalf("apply pointer (B): %v", err)
	}

	if !node.WasmSupplyConfigured(fb.digest) {
		t.Fatalf("digest B (%s) was not warmed up after deactivating a SIBLING replica -- "+
			"the warm-up consumer replica 1 still depends on was stripped by replica 0's "+
			"Deactivate, so replica 1 silently reverts to bare L1 readiness", fb.digest)
	}
	if !supply.Default.IsReady(ptr) {
		t.Fatal("pointer supply not ready although the surviving replica's warm-up " +
			"consumer successfully compiled digest B")
	}
}

// TestWarmupSkipLogDoesNotLeakDigestExpr is fix-round-1 finding 2's
// regression. The scope-limit branch in OnSupplyChanged must not put the
// operator-authored DigestExpr text into the process log: exprx's underlying
// compiler error echoes the expression source verbatim (e.g. "unknown name
// $input ... | $input.<whatever the expression names> ..."), and DigestExpr
// is node configuration the standing constraint and the org's log-content
// policy both forbid echoing.
func TestWarmupSkipLogDoesNotLeakDigestExpr(t *testing.T) {
	const sentinel = "super_secret_token_value_xyz"
	buf := &strings.Builder{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	c := &wasmWarmupConsumer{
		// $input, not $supplies: the documented scope-limit path. The sentinel
		// stands in for whatever real node configuration (potentially a
		// credential reference) a real DigestExpr might carry.
		digestExpr: "${{ $input." + sentinel + " }}",
		supplyNode: "some-supply",
		workflow:   "some-workflow",
		node:       "decode",
	}
	if err := c.OnSupplyChanged(context.Background(), supply.Snapshot{Name: "some-supply"}); err != nil {
		t.Fatalf("OnSupplyChanged: %v (the scope-limit path must return nil, not an error)", err)
	}
	if strings.Contains(buf.String(), sentinel) {
		t.Fatalf("warm-up skip log leaked the DigestExpr text: %s", buf.String())
	}
	if buf.Len() == 0 {
		t.Fatal("expected a warning to be logged for the scope-limit path; the log " +
			"assertion above would otherwise pass vacuously")
	}
}
