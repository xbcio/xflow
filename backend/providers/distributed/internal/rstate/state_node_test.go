package rstate

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// TestLoadGraphFailsClosedOnBrokenPersistedSnapshot covers T1's third leg: a
// running execution's persisted graph (execKey ...:graph) must never be
// silently recompiled or degraded when it turns out to be a grouped-but-
// missing-unit-IR snapshot (or any other snapshot Graph.UnmarshalJSON fails
// closed on). The durable remaining/in-degree counters for a running
// execution were already seeded from the ORIGINAL correct unit topology; a
// running execution's LoadGraph must surface a stable error and refuse to
// continue rather than transparently recompiling a possibly-different graph
// (unlike the workflow registry's registration-time fallback, which is safe
// because no durable per-execution counters exist yet at that point).
func TestLoadGraphFailsClosedOnBrokenPersistedSnapshot(t *testing.T) {
	rdb := newRedisStateTestClient(t)
	s := New(rdb, nil, time.Hour)
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)
	id := types.ExecutionID("exec-broken-graph")

	brokenGraphJSON := `{
		"graph_hash": "sha256:broken",
		"name": "broken",
		"nodes": [
			{"Name":"ingest","Type":"test.ingest","group_idx":0},
			{"Name":"analyze","Type":"test.analyze","group_idx":0}
		],
		"index": {"ingest":0,"analyze":1},
		"entry_indexes": {"ingest":0},
		"out_edges": [[],[]],
		"in_edges": [[],[]],
		"in_degree": [0,0]
	}`
	t_ns := namespace.FromContext(ctx)
	if err := rdb.Set(ctx, execKey(t_ns, id, "graph"), brokenGraphJSON, time.Hour).Err(); err != nil {
		t.Fatalf("seed broken graph: %v", err)
	}

	g, err := s.LoadGraph(ctx, id)
	if err == nil {
		t.Fatalf("LoadGraph() error = nil, want fail-closed error for broken persisted snapshot; got graph = %+v", g)
	}
	if g != nil {
		t.Fatalf("LoadGraph() graph = %+v, want nil on error", g)
	}
}

// TestLoadGraphSucceedsOnNormalPersistedSnapshot is the non-broken companion:
// a normally-compiled-and-persisted graph must still load successfully,
// proving the fail-closed test above exercises a real failure mode and not a
// universally-broken LoadGraph.
func TestLoadGraphSucceedsOnNormalPersistedSnapshot(t *testing.T) {
	rdb := newRedisStateTestClient(t)
	s := New(rdb, nil, time.Hour)
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)
	id := types.ExecutionID("exec-normal-graph")

	g := testGraphOneNode()
	data, err := g.MarshalJSON()
	if err != nil {
		t.Fatalf("marshal graph: %v", err)
	}
	tns := namespace.FromContext(ctx)
	if err := rdb.Set(ctx, execKey(tns, id, "graph"), string(data), time.Hour).Err(); err != nil {
		t.Fatalf("seed graph: %v", err)
	}

	got, err := s.LoadGraph(ctx, id)
	if err != nil {
		t.Fatalf("LoadGraph() error = %v, want success", err)
	}
	if got == nil || got.Hash() != g.Hash() {
		t.Fatalf("LoadGraph() = %+v, want hash %q", got, g.Hash())
	}
}
