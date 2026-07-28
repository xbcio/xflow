package statestoretest

import (
	"context"
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
		PackageHash:     "pkg-abc",
		Selector:        &types.RunnerSelector{Mode: types.RunnerSelectorModeRequired, MatchLabels: map[string]string{"zone": "a"}},
		Desired:         true,
	}
}

func entryActivationKey(a engine.EntryActivation) engine.EntryActivationKey {
	return engine.EntryActivationKey{
		Namespace:       a.Namespace,
		WorkflowID:      a.WorkflowID,
		WorkflowVersion: a.WorkflowVersion,
		EntryUnitID:     a.EntryUnitID,
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
