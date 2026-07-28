package control

import (
	"context"
	"errors"
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
