package rstate

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// seedMetricsGraph builds the shape the Kafka trigger admits through: a
// co-location group whose boundary exit fans out to a downstream node, so
// applying the fan-in leaves a real pending outbox entry behind.
func seedMetricsGraph(t *testing.T, name string) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name: name,
		Nodes: []types.NodeDef{
			{Name: "entry", Type: "test.echo"},
			{Name: "body", Type: "test.echo"},
			{Name: "sink", Type: "test.echo"},
		},
		Connections: types.Connections{
			"entry": {"main": types.PortConnections{Targets: []types.Connection{{Node: "body", Input: "main"}}}},
			"body":  {"main": types.PortConnections{Targets: []types.Connection{{Node: "sink", Input: "main"}}}},
		},
		Groups: []types.GroupDef{{Name: "tg", Members: []string{"entry", "body"}}},
	})
	if err != nil {
		t.Fatalf("graph.Compile: %v", err)
	}
	return g
}

// TestOutboxMetricsAgeOnEntrySeedPath pins that the pending age reported for an
// entry-seeded outbox entry is the entry's real age.
//
// The entry-seed path is the one a Kafka trigger group is admitted through, and
// it is the only path that enqueues with a literal 0 score
// (entry_admission.go's ZADD). The metrics scan derives OldestPendingAt from
// that score, so an entry seeded a moment ago reports as having been pending
// since 1970 — measured live against SAS traffic as a mean of ~1.79e9 seconds
// with p50/p90/p99 all +Inf.
//
// The bound is deliberately loose (one hour). The defect overshoots by more
// than fifty years, so anything in that neighbourhood catches it, while a tight
// bound would make this test a clock-skew flake. What it must NOT do is assert
// only IsZero(): time.UnixMilli(0) is 1970, and Go's zero Time is year 1, so
// IsZero() returns false for the corrupt value and such an assertion passes
// against the very bug it looks like it covers.
func TestOutboxMetricsAgeOnEntrySeedPath(t *testing.T) {
	state, _, _ := newTestRedisState(t)
	ctx := context.Background()

	g := seedMetricsGraph(t, "outbox-age-seed")
	gm := g.Groups()[0]
	exits := []engine.BoundaryExit{{NodeName: "body", Port: "main", Data: map[string]any{"x": 1}}}

	// Downstream arrivals are supplied by the caller, not derived from the
	// graph, so a seed with none leaves no outbox entry and this probe would
	// measure nothing. The sink is the group's single downstream unit.
	sinkNodeIdx, ok := g.NodeIndex("sink")
	if !ok {
		t.Fatal("sink node not found in the compiled graph")
	}
	downstream := []engine.DownstreamArrival{{
		NodeName:     "sink",
		NodeIdx:      sinkNodeIdx,
		UnitIdx:      g.UnitIndexForNode(sinkNodeIdx),
		ArrivalCount: 1,
		ActiveCount:  1,
	}}

	before := time.Now().UTC()
	resp, err := state.SeedExecutionFromEntry(ctx, engine.SeedExecutionFromEntryRequest{
		AdmissionKey:    engine.AdmissionKey("outbox-age-seed-1"),
		WorkflowID:      "wf-outbox-age",
		WorkflowVersion: "v1",
		EntryUnitID:     gm.Name,
		EntryUnitIdx:    gm.UnitIdx,
		Graph:           g,
		Outcome:         engine.GroupOutcomeSuccess,
		Exits:           exits,
		Downstream:      downstream,
		ResultHash:      engine.ComputeResultHash(engine.GroupOutcomeSuccess, exits),
		TraceID:         "trace-outbox-age",
	})
	if err != nil {
		t.Fatalf("SeedExecutionFromEntry: %v", err)
	}
	if resp.State != engine.AdmissionStateAccepted {
		t.Fatalf("admission state = %q, want accepted", resp.State)
	}

	snapshot, err := state.OutboxMetrics(ctx)
	if err != nil {
		t.Fatalf("OutboxMetrics: %v", err)
	}
	if snapshot.Pending == 0 {
		t.Fatal("the seed left no pending outbox entry, so this probe never " +
			"reaches the age it exists to check — the graph shape stopped " +
			"producing a downstream task")
	}
	if snapshot.OldestPendingAt.IsZero() {
		t.Fatal("OldestPendingAt is the zero Time with entries pending")
	}

	age := time.Since(snapshot.OldestPendingAt)
	if age > time.Hour {
		t.Errorf("oldest pending age = %v (OldestPendingAt = %s), want a duration "+
			"since the seed at %s. An age in the decades means the ready-ZSET "+
			"score was read as a creation time; the entry-seed path scores every "+
			"entry 0, which reads back as 1970.",
			age, snapshot.OldestPendingAt.Format(time.RFC3339), before.Format(time.RFC3339))
	}
	if snapshot.OldestPendingAt.After(time.Now().Add(time.Minute)) {
		t.Errorf("OldestPendingAt = %s is in the future: the score carries "+
			"available-at, not created-at, and a leased or delayed entry pushes "+
			"it forward — which clamps the reported age to 0 rather than "+
			"reporting the real backlog",
			snapshot.OldestPendingAt.Format(time.RFC3339))
	}
}
