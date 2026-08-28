package statestoretest

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// sampleEntryActivation builds a desired activation with a per-subtest unique
// EntryUnitID. The unit parameter keeps subtests isolated even when the backend
// (e.g. miniredis) shares one datastore across subtests — mirroring how the
// EntryAdmission contract uses distinct admission keys (k1, k2, ...).
func sampleEntryActivation(unit string) engine.EntryActivation {
	return engine.EntryActivation{
		Namespace:       namespace.Default,
		WorkflowID:      "wf-1",
		WorkflowVersion: "v1",
		EntryUnitID:     unit,
		NodeType:        "http.request",
		Params:          map[string]any{"url": "https://example.test", "method": "GET"},
		PackageHash:     "pkg-abc",
		Selector:        &types.RunnerSelector{Mode: types.RunnerSelectorModeRequired, MatchLabels: map[string]string{"zone": "a"}},
		Requirements:    []engine.CapabilityRequirement{{NodeType: "http.request", NodeVersion: 2, Feature: "trigger.v1"}},
		Supplies:        []engine.SupplyRequirement{{Node: "rules", Resource: "shared-rules", RequireReady: true}},
		SupplyConsumers: []engine.SupplyConsumerBinding{{
			ModuleDigest: "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			SupplyNode:   "rules",
		}},
		Desired: true,
	}
}

func entryActivationKey(a engine.EntryActivation) engine.EntryActivationKey {
	return engine.EntryActivationKey{
		Namespace:       a.Namespace,
		WorkflowID:      a.WorkflowID,
		WorkflowVersion: a.WorkflowVersion,
		EntryUnitID:     a.EntryUnitID,
		ReplicaIndex:    a.ReplicaIndex,
	}
}

// RunEntryActivationContract exercises the EntryActivationStore contract against
// a concrete backend. Both the memory and Redis implementations must pass
// identically. newStore returns a fresh, empty store per subtest.
func RunEntryActivationContract(t *testing.T, newStore func(*testing.T) engine.EntryActivationStore) {
	ctx := context.Background()

	t.Run("UpsertGetList", func(t *testing.T) {
		s := newStore(t)
		act := sampleEntryActivation("u-getlist")
		key := entryActivationKey(act)

		if err := s.Upsert(ctx, act); err != nil {
			t.Fatalf("Upsert: %v", err)
		}
		got, ok, err := s.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if !ok {
			t.Fatal("Get: activation must exist after Upsert")
		}
		if !got.Desired || got.PackageHash != "pkg-abc" || got.EntryUnitID != "u-getlist" {
			t.Fatalf("Get returned unexpected record: %+v", got)
		}
		// Node-generic desired-state fields must round-trip.
		if got.NodeType != "http.request" {
			t.Fatalf("NodeType not round-tripped: got %q want http.request", got.NodeType)
		}
		if !reflect.DeepEqual(got.Params, act.Params) {
			t.Fatalf("Params not round-tripped: got %+v want %+v", got.Params, act.Params)
		}
		// Requirements must round-trip (set on Upsert, read back on Get).
		if !reflect.DeepEqual(got.Requirements, act.Requirements) {
			t.Fatalf("Requirements not round-tripped: got %+v want %+v", got.Requirements, act.Requirements)
		}
		// Supplies must round-trip (set on Upsert, read back on Get).
		if !reflect.DeepEqual(got.Supplies, act.Supplies) {
			t.Fatalf("Supplies not round-tripped: got %+v want %+v", got.Supplies, act.Supplies)
		}
		// SupplyConsumers must round-trip too. Supplies alone only says which
		// content this unit needs; without the consumer bindings the hosting
		// runner fetches the content and hands it to nobody, and the wasm module
		// silently evaluates against no rules.
		if !reflect.DeepEqual(got.SupplyConsumers, act.SupplyConsumers) {
			t.Fatalf("SupplyConsumers not round-tripped: got %+v want %+v", got.SupplyConsumers, act.SupplyConsumers)
		}
		list, err := s.List(ctx, namespace.Default)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		// A shared backend (e.g. miniredis) may carry records from sibling
		// subtests; assert our record is present rather than an exact count.
		found := false
		for _, a := range list {
			if a.EntryUnitID == "u-getlist" {
				found = true
			}
		}
		if !found {
			t.Fatalf("List %+v does not contain u-getlist", list)
		}
	})

	t.Run("ReplicaSiblingsAreIsolated", func(t *testing.T) {
		s := newStore(t)
		first := sampleEntryActivation("u-siblings")
		second := first
		second.ReplicaIndex = 1
		second.PackageHash = "pkg-sibling-1"

		if err := s.Upsert(ctx, first); err != nil {
			t.Fatalf("Upsert replica 0: %v", err)
		}
		if err := s.Upsert(ctx, second); err != nil {
			t.Fatalf("Upsert replica 1: %v", err)
		}

		deadline := time.Now().Add(time.Minute).Truncate(time.Second)
		if ok, err := s.Assign(ctx, entryActivationKey(first), "runner-0", "session-0", 3, deadline); err != nil || !ok {
			t.Fatalf("Assign replica 0: ok=%v err=%v", ok, err)
		}
		if ok, err := s.Assign(ctx, entryActivationKey(second), "runner-1", "session-1", 7, deadline); err != nil || !ok {
			t.Fatalf("Assign replica 1: ok=%v err=%v", ok, err)
		}

		got0, ok, err := s.Get(ctx, entryActivationKey(first))
		if err != nil || !ok {
			t.Fatalf("Get replica 0: ok=%v err=%v", ok, err)
		}
		got1, ok, err := s.Get(ctx, entryActivationKey(second))
		if err != nil || !ok {
			t.Fatalf("Get replica 1: ok=%v err=%v", ok, err)
		}
		if got0.ReplicaIndex != 0 || got0.RunnerID != "runner-0" || got0.Generation != 3 || got0.PackageHash != "pkg-abc" {
			t.Fatalf("replica 0 was aliased or corrupted: %+v", got0)
		}
		if got1.ReplicaIndex != 1 || got1.RunnerID != "runner-1" || got1.Generation != 7 || got1.PackageHash != "pkg-sibling-1" {
			t.Fatalf("replica 1 was aliased or corrupted: %+v", got1)
		}

		if err := s.Fence(ctx, entryActivationKey(first), 3); err != nil {
			t.Fatalf("Fence replica 0: %v", err)
		}
		got1, ok, err = s.Get(ctx, entryActivationKey(second))
		if err != nil || !ok {
			t.Fatalf("Get replica 1 after sibling fence: ok=%v err=%v", ok, err)
		}
		if got1.RunnerID != "runner-1" || got1.Generation != 7 {
			t.Fatalf("fencing replica 0 changed replica 1: %+v", got1)
		}

		list, err := s.List(ctx, namespace.Default)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		seen := map[uint32]engine.EntryActivation{}
		for _, act := range list {
			if act.WorkflowID == first.WorkflowID && act.WorkflowVersion == first.WorkflowVersion && act.EntryUnitID == first.EntryUnitID {
				seen[act.ReplicaIndex] = act
			}
		}
		if len(seen) != 2 {
			t.Fatalf("List must return both sibling identities, got %+v", seen)
		}
	})

	// Re-upserting an EXISTING record must refresh the consumer bindings. This
	// is a separate case from the first Upsert on purpose: a store that copies
	// the whole record on create but refreshes desired-state field by field on
	// update (the memory store does exactly that) passes the create path while
	// silently dropping the field on every subsequent workflow update.
	t.Run("UpsertRefreshesSupplyConsumers", func(t *testing.T) {
		s := newStore(t)
		act := sampleEntryActivation("u-rebind")
		act.SupplyConsumers = nil
		if err := s.Upsert(ctx, act); err != nil {
			t.Fatalf("Upsert (first): %v", err)
		}
		want := []engine.SupplyConsumerBinding{{
			ModuleDigest: "sha256:fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210",
			SupplyNode:   "rules",
		}}
		act.SupplyConsumers = want
		if err := s.Upsert(ctx, act); err != nil {
			t.Fatalf("Upsert (second): %v", err)
		}
		got, ok, err := s.Get(ctx, entryActivationKey(act))
		if err != nil || !ok {
			t.Fatalf("Get: ok=%v err=%v", ok, err)
		}
		if !reflect.DeepEqual(got.SupplyConsumers, want) {
			t.Fatalf("SupplyConsumers after re-upsert = %+v, want %+v", got.SupplyConsumers, want)
		}
	})

	// A record with no consumer bindings must read back nil, not an empty
	// slice: the directive's wire format relies on omitempty, and a backend
	// that materializes an empty slice would change every existing directive's
	// JSON.
	t.Run("UpsertWithoutSupplyConsumers", func(t *testing.T) {
		s := newStore(t)
		act := sampleEntryActivation("u-nobindings")
		act.SupplyConsumers = nil
		if err := s.Upsert(ctx, act); err != nil {
			t.Fatalf("Upsert: %v", err)
		}
		got, ok, err := s.Get(ctx, entryActivationKey(act))
		if err != nil || !ok {
			t.Fatalf("Get: ok=%v err=%v", ok, err)
		}
		if got.SupplyConsumers != nil {
			t.Fatalf("SupplyConsumers = %#v, want nil", got.SupplyConsumers)
		}
	})

	// A declaration-shaped binding must survive the store round-trip with all
	// four identity fields intact. Losing WorkflowName/NodeName in the state
	// store would put the runner back on the legacy branch, where it would try
	// to compile the empty digest -- the same silent pass-through this whole
	// change exists to close.
	t.Run("SupplyConsumerDeclarationRoundTrip", func(t *testing.T) {
		st := newStore(t)
		act := engine.EntryActivation{
			Namespace:   namespace.Default,
			WorkflowID:  "wf-decl",
			EntryUnitID: "trig",
			NodeType:    "xflow.trigger.kafka",
			Desired:     true,
			SupplyConsumers: []engine.SupplyConsumerBinding{{
				SupplyNode:   "wasm_versions",
				WorkflowName: "collect",
				NodeName:     "decode",
				DigestExpr:   "${{ $supplies.wasm_versions.decode.digest }}",
			}},
		}
		if err := st.Upsert(context.Background(), act); err != nil {
			t.Fatalf("upsert: %v", err)
		}
		got, err := st.List(context.Background(), namespace.Default)
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		var found *engine.EntryActivation
		for i := range got {
			if got[i].WorkflowID == "wf-decl" {
				found = &got[i]
			}
		}
		if found == nil {
			t.Fatal("activation not found after upsert")
		}
		if !reflect.DeepEqual(found.SupplyConsumers, act.SupplyConsumers) {
			t.Fatalf("SupplyConsumers = %+v, want %+v", found.SupplyConsumers, act.SupplyConsumers)
		}
	})

	t.Run("GetAbsent", func(t *testing.T) {
		s := newStore(t)
		_, ok, err := s.Get(ctx, entryActivationKey(sampleEntryActivation("u-absent")))
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if ok {
			t.Fatal("Get on empty store must report absent")
		}
	})

	t.Run("AssignFirstWriterWins", func(t *testing.T) {
		s := newStore(t)
		act := sampleEntryActivation("u-fww")
		key := entryActivationKey(act)
		deadline := time.Now().Add(time.Minute)
		if err := s.Upsert(ctx, act); err != nil {
			t.Fatalf("Upsert: %v", err)
		}

		ok, err := s.Assign(ctx, key, "runner-1", "sess-1", 1, deadline)
		if err != nil {
			t.Fatalf("Assign gen1: %v", err)
		}
		if !ok {
			t.Fatal("first Assign must win")
		}

		// Stale generation must not overwrite the owner.
		ok, err = s.Assign(ctx, key, "runner-2", "sess-2", 1, deadline)
		if err != nil {
			t.Fatalf("Assign stale gen1: %v", err)
		}
		if ok {
			t.Fatal("Assign with equal generation must fail")
		}
		got, _, err := s.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.RunnerID != "runner-1" || got.SessionID != "sess-1" || got.Generation != 1 {
			t.Fatalf("stale assign clobbered owner: %+v", got)
		}
	})

	t.Run("AssignSnapshotsPackageHashFenceClears", func(t *testing.T) {
		s := newStore(t)
		act := sampleEntryActivation("u-pkgsnap")
		key := entryActivationKey(act)
		deadline := time.Now().Add(time.Minute)
		if err := s.Upsert(ctx, act); err != nil {
			t.Fatalf("Upsert: %v", err)
		}
		if ok, err := s.Assign(ctx, key, "runner-1", "sess-1", 1, deadline); err != nil || !ok {
			t.Fatalf("Assign gen1: ok=%v err=%v", ok, err)
		}
		// Assign must snapshot the desired PackageHash in effect at claim time.
		got, _, err := s.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.AssignedPackageHash != act.PackageHash {
			t.Fatalf("Assign must snapshot PackageHash: got %q want %q", got.AssignedPackageHash, act.PackageHash)
		}
		// A desired PackageHash change (Upsert) must NOT alter the assigned
		// snapshot — that drift is exactly what lets the reconciler detect a
		// material change.
		changed := act
		changed.PackageHash = "pkg-CHANGED"
		if err := s.Upsert(ctx, changed); err != nil {
			t.Fatalf("Upsert (change pkg): %v", err)
		}
		got, _, _ = s.Get(ctx, key)
		if got.PackageHash != "pkg-CHANGED" {
			t.Fatalf("desired PackageHash not updated by Upsert: %+v", got)
		}
		if got.AssignedPackageHash != act.PackageHash {
			t.Fatalf("Upsert must not touch AssignedPackageHash: got %q want %q", got.AssignedPackageHash, act.PackageHash)
		}
		// Fence clears the assignment snapshot along with the owner.
		if err := s.Fence(ctx, key, got.Generation); err != nil {
			t.Fatalf("Fence: %v", err)
		}
		got, _, _ = s.Get(ctx, key)
		if got.RunnerID != "" || got.AssignedPackageHash != "" {
			t.Fatalf("Fence must clear owner + AssignedPackageHash: %+v", got)
		}
	})

	t.Run("RenewKeepsGenerationExtendsDeadline", func(t *testing.T) {
		s := newStore(t)
		act := sampleEntryActivation("u-renew")
		key := entryActivationKey(act)
		if err := s.Upsert(ctx, act); err != nil {
			t.Fatalf("Upsert: %v", err)
		}
		d1 := time.Now().Add(30 * time.Second).Truncate(time.Second)
		if ok, err := s.Assign(ctx, key, "runner-1", "sess-1", 1, d1); err != nil || !ok {
			t.Fatalf("Assign gen1: ok=%v err=%v", ok, err)
		}

		// Renew at the matching generation extends the deadline, keeps generation
		// and owner stable.
		d2 := time.Now().Add(90 * time.Second).Truncate(time.Second)
		ok, err := s.Renew(ctx, key, 1, d2)
		if err != nil {
			t.Fatalf("Renew gen1: %v", err)
		}
		if !ok {
			t.Fatal("Renew at matching generation must succeed")
		}
		got, _, err := s.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.Generation != 1 || got.RunnerID != "runner-1" {
			t.Fatalf("Renew changed owner/generation: %+v", got)
		}
		if !got.LeaseDeadline.Equal(d2) {
			t.Fatalf("Renew did not extend deadline: got %v want %v", got.LeaseDeadline, d2)
		}

		// Renew at a non-matching generation is a no-op (false).
		ok, err = s.Renew(ctx, key, 2, time.Now().Add(5*time.Minute))
		if err != nil {
			t.Fatalf("Renew gen2: %v", err)
		}
		if ok {
			t.Fatal("Renew at non-matching generation must fail")
		}
	})

	t.Run("RenewAbsentReturnsFalse", func(t *testing.T) {
		s := newStore(t)
		key := entryActivationKey(sampleEntryActivation("u-renew-absent"))
		ok, err := s.Renew(ctx, key, 1, time.Now().Add(time.Minute))
		if err != nil {
			t.Fatalf("Renew on absent: %v", err)
		}
		if ok {
			t.Fatal("Renew on a non-existent activation must return false")
		}
	})

	t.Run("FenceThenReassign", func(t *testing.T) {
		s := newStore(t)
		act := sampleEntryActivation("u-fence")
		key := entryActivationKey(act)
		deadline := time.Now().Add(time.Minute)
		if err := s.Upsert(ctx, act); err != nil {
			t.Fatalf("Upsert: %v", err)
		}
		if ok, err := s.Assign(ctx, key, "runner-1", "sess-1", 1, deadline); err != nil || !ok {
			t.Fatalf("Assign gen1: ok=%v err=%v", ok, err)
		}

		if err := s.Fence(ctx, key, 1); err != nil {
			t.Fatalf("Fence gen1: %v", err)
		}
		// After fence, a strictly higher generation wins.
		ok, err := s.Assign(ctx, key, "runner-3", "sess-3", 2, deadline)
		if err != nil {
			t.Fatalf("Assign gen2: %v", err)
		}
		if !ok {
			t.Fatal("Assign gen2 after fence must win")
		}
		got, _, err := s.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.RunnerID != "runner-3" || got.Generation != 2 {
			t.Fatalf("gen2 assign not applied: %+v", got)
		}
	})

	t.Run("GenerationMonotonicity", func(t *testing.T) {
		s := newStore(t)
		act := sampleEntryActivation("u-mono")
		key := entryActivationKey(act)
		deadline := time.Now().Add(time.Minute)
		if err := s.Upsert(ctx, act); err != nil {
			t.Fatalf("Upsert: %v", err)
		}
		if ok, err := s.Assign(ctx, key, "runner-5", "sess-5", 5, deadline); err != nil || !ok {
			t.Fatalf("Assign gen5: ok=%v err=%v", ok, err)
		}
		// A lower generation must never win over a higher one.
		ok, err := s.Assign(ctx, key, "runner-4", "sess-4", 4, deadline)
		if err != nil {
			t.Fatalf("Assign gen4: %v", err)
		}
		if ok {
			t.Fatal("non-monotonic Assign (gen4 < gen5) must fail")
		}
		got, _, err := s.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.RunnerID != "runner-5" || got.Generation != 5 {
			t.Fatalf("monotonicity violated: %+v", got)
		}
	})

	t.Run("AssignAbsentReturnsFalse", func(t *testing.T) {
		s := newStore(t)
		key := entryActivationKey(sampleEntryActivation("u-assign-absent"))
		ok, err := s.Assign(ctx, key, "runner-1", "sess-1", 1, time.Now().Add(time.Minute))
		if err != nil {
			t.Fatalf("Assign on absent: %v", err)
		}
		if ok {
			t.Fatal("Assign on a non-existent activation must return false")
		}
	})

	t.Run("FenceAbsentIsNoop", func(t *testing.T) {
		s := newStore(t)
		if err := s.Fence(ctx, entryActivationKey(sampleEntryActivation("u-fence-absent")), 1); err != nil {
			t.Fatalf("Fence on absent must be a no-op: %v", err)
		}
	})
}
