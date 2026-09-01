package engine

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// artifactUseWiringDef builds the smallest single-node workflow the two
// stamping tests below need: one root task to build (or recover) a lease for.
func artifactUseWiringDef(t *testing.T) (*graph.Graph, *fakeState, *fakeQueue) {
	t.Helper()
	def := &types.WorkflowDef{
		Name:  "artifact-use-wiring",
		Nodes: []types.NodeDef{{Name: "n", Type: "test.echo"}},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return g, newFakeState(), &fakeQueue{}
}

// TestBuildTaskLeaseStampsTheEnginesArtifactCollector pins the first hop of
// the cross-boundary wiring: BuildTaskLease must stamp the lease with the
// EXACT SAME collector pointer the engine was constructed with, not a copy or
// a fresh one. Pointer identity is the whole point -- Record calls made by the
// leaf handler must land in the one collector the caller reads back out, so
// content equality (e.g. two empty collectors) would not catch a wiring bug
// that stamps a look-alike instead of the real one.
func TestBuildTaskLeaseStampsTheEnginesArtifactCollector(t *testing.T) {
	g, state, queue := artifactUseWiringDef(t)
	_, collector := types.WithArtifactUseCollector(context.Background())
	eng := New(state, queue, WithArtifactUseCollector(collector))
	ctx := context.Background()

	if _, err := eng.Submit(ctx, g, nil); err != nil {
		t.Fatalf("submit: %v", err)
	}
	task := queue.Drain()[0]
	lease, err := eng.BuildTaskLease(ctx, task)
	if err != nil {
		t.Fatalf("BuildTaskLease: %v", err)
	}
	if lease.ArtifactUses != collector {
		t.Fatalf("lease.ArtifactUses = %p, want the exact engine collector %p", lease.ArtifactUses, collector)
	}
}

// TestRecoverTaskLeaseStampsTheEnginesArtifactCollector is
// TestBuildTaskLeaseStampsTheEnginesArtifactCollector's RecoverTaskLease
// counterpart: a control-plane crash between BuildTaskLease committing state
// and the lease reaching a runner must not silently drop the collector on
// replay. Same pointer-identity assertion, same reason.
func TestRecoverTaskLeaseStampsTheEnginesArtifactCollector(t *testing.T) {
	g, state, queue := artifactUseWiringDef(t)
	_, collector := types.WithArtifactUseCollector(context.Background())
	eng := New(state, queue, WithArtifactUseCollector(collector))
	ctx := context.Background()

	if _, err := eng.Submit(ctx, g, nil); err != nil {
		t.Fatalf("submit: %v", err)
	}
	task := queue.Drain()[0]
	if _, err := eng.BuildTaskLease(ctx, task); err != nil {
		t.Fatalf("BuildTaskLease: %v", err)
	}

	recovered, err := eng.RecoverTaskLease(ctx, task)
	if err != nil {
		t.Fatalf("RecoverTaskLease: %v", err)
	}
	if recovered.ArtifactUses != collector {
		t.Fatalf("recovered.ArtifactUses = %p, want the exact engine collector %p", recovered.ArtifactUses, collector)
	}
}

// TestArtifactCollectorIsNotSerializedOntoTheWire pins the trap the brief
// documents: TaskLease travels the durable path as JSON, and ArtifactUses is a
// live pointer to a mutex-guarded struct. If its json tag ever stops being
// "-", encoding does not error and decoding does not panic -- it silently
// produces a fresh, forever-empty collector on the far side. err == nil alone
// cannot catch that (a `{}` field encodes and decodes without error), so this
// asserts the field's name is entirely ABSENT from the wire bytes, and that a
// round trip yields a nil collector rather than a resurrected empty one.
func TestArtifactCollectorIsNotSerializedOntoTheWire(t *testing.T) {
	_, collector := types.WithArtifactUseCollector(context.Background())
	collector.Record("n", "sha256:deadbeef")

	lease := &TaskLease{
		Task:         Task{ExecutionID: types.ExecutionID("exec-1"), NodeName: "n"},
		ArtifactUses: collector,
	}

	data, err := json.Marshal(lease)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	for _, spelling := range []string{"artifact_uses", "ArtifactUses"} {
		if strings.Contains(string(data), spelling) {
			t.Fatalf("wire bytes contain %q; the collector's mutex would encode as \"{}\" and "+
				"decode into a fresh, forever-empty collector on the far side -- ArtifactUses "+
				"must stay json:\"-\": %s", spelling, data)
		}
	}

	var decoded TaskLease
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if decoded.ArtifactUses != nil {
		t.Fatalf("decoded.ArtifactUses = %#v, want nil (the collector must never survive a wire round trip)", decoded.ArtifactUses)
	}
}
