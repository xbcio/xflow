package control

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
)

// seedCaptureStore records the entry-admission request the engine passes to the
// store. The control plane's contribution is what this test observes: the
// request the engine receives must already carry the registry record's hash.
type seedCaptureStore struct {
	engine.StateStore

	mu    sync.Mutex
	seeds []engine.SeedExecutionFromEntryRequest
}

func (s *seedCaptureStore) SeedExecutionFromEntry(ctx context.Context, req engine.SeedExecutionFromEntryRequest) (engine.SeedExecutionFromEntryResponse, error) {
	s.mu.Lock()
	s.seeds = append(s.seeds, req)
	s.mu.Unlock()
	admitter, ok := s.StateStore.(engine.EntryAdmissionStore)
	if !ok {
		return engine.SeedExecutionFromEntryResponse{}, errors.New("captured state does not admit entry seeds")
	}
	return admitter.SeedExecutionFromEntry(ctx, req)
}

func (s *seedCaptureStore) lastSeed(t *testing.T) engine.SeedExecutionFromEntryRequest {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.seeds) == 0 {
		t.Fatal("no entry-admission request reached the store")
	}
	return s.seeds[len(s.seeds)-1]
}

// TestCoreEntrySeedCarriesTheRegistryDefinitionHash pins that the authoritative
// resolution path stamps the hash of the very record it took the graph from
// onto the request, so the backend can record it with the execution. The remote
// runner never sends a hash — it cannot know one — and a supplied value would
// not be trusted over the registry's.
func TestCoreEntrySeedCarriesTheRegistryDefinitionHash(t *testing.T) {
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)

	be := local.New()
	capture := &seedCaptureStore{StateStore: be.State()}
	eng := engine.New(capture, be.Queue())

	def := entrySeedResolveDef()
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	reg := be.WorkflowRegistry()
	rec, err := reg.AddWorkflow(ctx, backend.WorkflowRecord{
		ID:             "wf-defhash-seed",
		Key:            "default/trig-down@v1",
		Namespace:      string(namespace.Default),
		Name:           "trig-down",
		Version:        "v1",
		DefinitionHash: "sha256:registry-seed-hash",
		Definition:     def,
		Graph:          g,
	})
	if err != nil {
		t.Fatalf("AddWorkflow: %v", err)
	}

	core := &Core{engine: eng, workflowRegistry: reg}
	outcome := engine.GroupOutcomeSuccess
	exits := []engine.BoundaryExit{{NodeName: "trig", Port: "main", Data: map[string]any{"x": 1}}}
	req := engine.SeedExecutionFromEntryRequest{
		AdmissionKey:    "seed-defhash-1",
		WorkflowID:      rec.ID,
		WorkflowVersion: "v1",
		EntryUnitID:     "trig",
		Outcome:         outcome,
		Exits:           exits,
		ResultHash:      engine.ComputeResultHash(outcome, exits),
	}
	if _, err := core.SeedExecutionFromEntry(ctx, req); err != nil {
		t.Fatalf("SeedExecutionFromEntry: %v", err)
	}

	seed := capture.lastSeed(t)
	if seed.DefinitionHash != rec.DefinitionHash {
		t.Fatalf("seed definition hash = %q, want the resolved record's %q", seed.DefinitionHash, rec.DefinitionHash)
	}
	if seed.Graph != g {
		t.Fatal("captured seed does not carry the graph of the record whose hash it carries")
	}
}
