package control

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
)

// TestCoreEntrySeedGenerationFence verifies spec §11.6: a seed carrying a stale
// activation generation is rejected for a NEW admission key (so a forged/stale
// runner can never seed a fresh execution), but is still duplicate-accepted for
// an ALREADY-ACCEPTED admission key (so the stale runner can commit its Kafka
// offset and stop redelivering).
func TestCoreEntrySeedGenerationFence(t *testing.T) {
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)

	backend := local.New()
	eng := engine.New(backend.State(), backend.Queue())
	activations := NewMemoryEntryActivationStore()
	core := &Core{engine: eng, entryActivations: activations}

	g := entrySeedTestGraph(t)
	gm := g.Groups()[0]

	act := engine.EntryActivation{
		Namespace:       namespace.Default,
		WorkflowID:      "wf-test",
		WorkflowVersion: "v1",
		EntryUnitID:     gm.Name,
		PackageHash:     "pkg-1",
		Desired:         true,
	}
	key := engine.EntryActivationKey{
		Namespace:       act.Namespace,
		WorkflowID:      act.WorkflowID,
		WorkflowVersion: act.WorkflowVersion,
		EntryUnitID:     act.EntryUnitID,
	}
	if err := activations.Upsert(ctx, act); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	// Current assigned generation is 2.
	if ok, err := activations.Assign(ctx, key, "runner-1", "sess-1", 2, time.Now().Add(time.Minute)); err != nil || !ok {
		t.Fatalf("Assign gen2: ok=%v err=%v", ok, err)
	}

	newReq := func(admissionKey string, gen uint64) engine.SeedExecutionFromEntryRequest {
		outcome := engine.GroupOutcomeSuccess
		exits := []engine.BoundaryExit{{NodeName: "body", Port: "main", Data: map[string]any{"x": 1}}}
		return engine.SeedExecutionFromEntryRequest{
			AdmissionKey:    engine.AdmissionKey(admissionKey),
			WorkflowID:      "wf-test",
			WorkflowVersion: "v1",
			EntryUnitID:     gm.Name,
			EntryUnitIdx:    gm.UnitIdx,
			Graph:           g,
			Outcome:         outcome,
			Exits:           exits,
			ResultHash:      engine.ComputeResultHash(outcome, exits),
			Generation:      gen,
		}
	}

	// A stale-generation seed (gen1) for a NEW admission key must be rejected and
	// must NOT create an execution.
	staleNew := newReq("k-new", 1)
	if _, err := core.SeedExecutionFromEntry(ctx, staleNew); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("stale-generation new key: err = %v, want ErrStaleGeneration", err)
	}
	if _, err := eng.Inspect(ctx, engine.DeterministicExecutionID("k-new")); !errors.Is(err, engine.ErrExecutionNotFound) {
		t.Fatalf("stale-generation new key must not create an execution, inspect err = %v", err)
	}

	// A current-generation seed (gen2) for an admission key is accepted.
	accepted := newReq("k-accepted", 2)
	resp, err := core.SeedExecutionFromEntry(ctx, accepted)
	if err != nil {
		t.Fatalf("current-generation seed: %v", err)
	}
	if resp.State != engine.AdmissionStateAccepted || resp.Duplicate {
		t.Fatalf("current-generation seed: state=%q duplicate=%v", resp.State, resp.Duplicate)
	}

	// A stale-generation seed (gen1) for the ALREADY-ACCEPTED key returns
	// duplicate accepted so the stale runner can commit its offset.
	staleDup := newReq("k-accepted", 1)
	resp2, err := core.SeedExecutionFromEntry(ctx, staleDup)
	if err != nil {
		t.Fatalf("stale-generation already-accepted key: %v", err)
	}
	if resp2.State != engine.AdmissionStateAccepted || !resp2.Duplicate {
		t.Fatalf("stale-generation already-accepted key: state=%q duplicate=%v, want accepted+duplicate", resp2.State, resp2.Duplicate)
	}
	if resp2.ExecutionID != resp.ExecutionID {
		t.Fatalf("duplicate execution ID = %q, want %q", resp2.ExecutionID, resp.ExecutionID)
	}
}

// TestCoreEntrySeedGenerationFence_ForgedAheadRejected verifies spec §11.6 fails
// closed against a forged-ahead generation: a seed carrying a generation ABOVE
// the currently assigned generation (impossible in honest operation, since every
// legitimate runner receives its generation from Assign) must be treated as stale
// and rejected for a NEW admission key, never seeding a fresh execution.
func TestCoreEntrySeedGenerationFence_ForgedAheadRejected(t *testing.T) {
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)

	backend := local.New()
	eng := engine.New(backend.State(), backend.Queue())
	activations := NewMemoryEntryActivationStore()
	core := &Core{engine: eng, entryActivations: activations}

	g := entrySeedTestGraph(t)
	gm := g.Groups()[0]

	act := engine.EntryActivation{
		Namespace:       namespace.Default,
		WorkflowID:      "wf-test",
		WorkflowVersion: "v1",
		EntryUnitID:     gm.Name,
		PackageHash:     "pkg-1",
		Desired:         true,
	}
	key := engine.EntryActivationKey{
		Namespace:       act.Namespace,
		WorkflowID:      act.WorkflowID,
		WorkflowVersion: act.WorkflowVersion,
		EntryUnitID:     act.EntryUnitID,
	}
	if err := activations.Upsert(ctx, act); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	// Current assigned generation is 2.
	if ok, err := activations.Assign(ctx, key, "runner-1", "sess-1", 2, time.Now().Add(time.Minute)); err != nil || !ok {
		t.Fatalf("Assign gen2: ok=%v err=%v", ok, err)
	}

	newReq := func(admissionKey string, gen uint64) engine.SeedExecutionFromEntryRequest {
		outcome := engine.GroupOutcomeSuccess
		exits := []engine.BoundaryExit{{NodeName: "body", Port: "main", Data: map[string]any{"x": 1}}}
		return engine.SeedExecutionFromEntryRequest{
			AdmissionKey:    engine.AdmissionKey(admissionKey),
			WorkflowID:      "wf-test",
			WorkflowVersion: "v1",
			EntryUnitID:     gm.Name,
			EntryUnitIdx:    gm.UnitIdx,
			Graph:           g,
			Outcome:         outcome,
			Exits:           exits,
			ResultHash:      engine.ComputeResultHash(outcome, exits),
			Generation:      gen,
		}
	}

	// A forged-ahead seed (gen MaxUint64, above the assigned gen2) for a NEW
	// admission key must be rejected and must NOT create an execution.
	forged := newReq("k-forged", math.MaxUint64)
	if _, err := core.SeedExecutionFromEntry(ctx, forged); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("forged-ahead new key: err = %v, want ErrStaleGeneration", err)
	}
	if _, err := eng.Inspect(ctx, engine.DeterministicExecutionID("k-forged")); !errors.Is(err, engine.ErrExecutionNotFound) {
		t.Fatalf("forged-ahead new key must not create an execution, inspect err = %v", err)
	}
}

func TestCoreEntrySeedGenerationFence_UnknownReplicaRejected(t *testing.T) {
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)
	backend := local.New()
	eng := engine.New(backend.State(), backend.Queue())
	activations := NewMemoryEntryActivationStore()
	core := &Core{engine: eng, entryActivations: activations}

	g := entrySeedTestGraph(t)
	gm := g.Groups()[0]
	act := engine.EntryActivation{
		Namespace:       namespace.Default,
		WorkflowID:      "wf-test",
		WorkflowVersion: "v1",
		EntryUnitID:     gm.Name,
		ReplicaIndex:    0,
		PackageHash:     "pkg-1",
		Desired:         true,
	}
	if err := activations.Upsert(ctx, act); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	key := engine.EntryActivationKey{
		Namespace:       act.Namespace,
		WorkflowID:      act.WorkflowID,
		WorkflowVersion: act.WorkflowVersion,
		EntryUnitID:     act.EntryUnitID,
		ReplicaIndex:    act.ReplicaIndex,
	}
	if ok, err := activations.Assign(ctx, key, "runner-1", "sess-1", 2, time.Now().Add(time.Minute)); err != nil || !ok {
		t.Fatalf("Assign gen2: ok=%v err=%v", ok, err)
	}

	newReq := func(admissionKey string, replica uint32, gen uint64) engine.SeedExecutionFromEntryRequest {
		outcome := engine.GroupOutcomeSuccess
		exits := []engine.BoundaryExit{{NodeName: "body", Port: "main", Data: map[string]any{"x": 1}}}
		return engine.SeedExecutionFromEntryRequest{
			AdmissionKey:    engine.AdmissionKey(admissionKey),
			WorkflowID:      "wf-test",
			WorkflowVersion: "v1",
			EntryUnitID:     gm.Name,
			EntryUnitIdx:    gm.UnitIdx,
			Graph:           g,
			Outcome:         outcome,
			Exits:           exits,
			ResultHash:      engine.ComputeResultHash(outcome, exits),
			Generation:      gen,
			ReplicaIndex:    replica,
		}
	}

	forged := newReq("k-forged-replica", 99, 2)
	if _, err := core.SeedExecutionFromEntry(ctx, forged); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("unknown replica new key: err = %v, want ErrStaleGeneration", err)
	}
	if _, err := eng.Inspect(ctx, engine.DeterministicExecutionID(forged.AdmissionKey)); !errors.Is(err, engine.ErrExecutionNotFound) {
		t.Fatalf("unknown replica must not create an execution, inspect err = %v", err)
	}

	accepted := newReq("k-cross-replica-duplicate", 0, 2)
	resp, err := core.SeedExecutionFromEntry(ctx, accepted)
	if err != nil {
		t.Fatalf("known replica seed: %v", err)
	}
	forgedDuplicate := newReq("k-cross-replica-duplicate", 99, math.MaxUint64)
	dup, err := core.SeedExecutionFromEntry(ctx, forgedDuplicate)
	if err != nil {
		t.Fatalf("unknown replica duplicate: %v", err)
	}
	if !dup.Duplicate || dup.ExecutionID != resp.ExecutionID {
		t.Fatalf("unknown replica duplicate = %+v, want duplicate of %q", dup, resp.ExecutionID)
	}
}

func TestCoreEntrySeedGenerationFence_SiblingsUseOwnGeneration(t *testing.T) {
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)
	backend := local.New()
	eng := engine.New(backend.State(), backend.Queue())
	activations := NewMemoryEntryActivationStore()
	core := &Core{engine: eng, entryActivations: activations}

	g := entrySeedTestGraph(t)
	gm := g.Groups()[0]
	for replica, generation := range map[uint32]uint64{0: 2, 1: 7} {
		act := engine.EntryActivation{
			Namespace:       namespace.Default,
			WorkflowID:      "wf-test",
			WorkflowVersion: "v1",
			EntryUnitID:     gm.Name,
			ReplicaIndex:    replica,
			PackageHash:     "pkg-1",
			Desired:         true,
		}
		if err := activations.Upsert(ctx, act); err != nil {
			t.Fatalf("Upsert replica %d: %v", replica, err)
		}
		key := engine.EntryActivationKey{
			Namespace:       act.Namespace,
			WorkflowID:      act.WorkflowID,
			WorkflowVersion: act.WorkflowVersion,
			EntryUnitID:     act.EntryUnitID,
			ReplicaIndex:    replica,
		}
		if ok, err := activations.Assign(ctx, key, "runner", "session", generation, time.Now().Add(time.Minute)); err != nil || !ok {
			t.Fatalf("Assign replica %d gen%d: ok=%v err=%v", replica, generation, ok, err)
		}
	}

	newReq := func(admissionKey string, replica uint32, gen uint64) engine.SeedExecutionFromEntryRequest {
		outcome := engine.GroupOutcomeSuccess
		exits := []engine.BoundaryExit{{NodeName: "body", Port: "main", Data: map[string]any{"x": 1}}}
		return engine.SeedExecutionFromEntryRequest{
			AdmissionKey:    engine.AdmissionKey(admissionKey),
			WorkflowID:      "wf-test",
			WorkflowVersion: "v1",
			EntryUnitID:     gm.Name,
			EntryUnitIdx:    gm.UnitIdx,
			Graph:           g,
			Outcome:         outcome,
			Exits:           exits,
			ResultHash:      engine.ComputeResultHash(outcome, exits),
			Generation:      gen,
			ReplicaIndex:    replica,
		}
	}

	for _, tc := range []struct {
		name       string
		replica    uint32
		generation uint64
		wantStale  bool
	}{
		{name: "replica zero own generation", replica: 0, generation: 2},
		{name: "replica one own generation", replica: 1, generation: 7},
		{name: "replica zero sibling generation", replica: 0, generation: 7, wantStale: true},
		{name: "replica one sibling generation", replica: 1, generation: 2, wantStale: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := newReq("k-"+tc.name, tc.replica, tc.generation)
			_, err := core.SeedExecutionFromEntry(ctx, req)
			if tc.wantStale {
				if !errors.Is(err, ErrStaleGeneration) {
					t.Fatalf("err = %v, want ErrStaleGeneration", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("seed: %v", err)
			}
		})
	}
}
