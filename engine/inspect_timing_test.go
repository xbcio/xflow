package engine

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

func inspectTimingGraph(t *testing.T) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:  "audit-timing",
		Nodes: []types.NodeDef{{Name: "work", Type: "test.audit"}},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	return g
}

// TestInspectProjectsExecutionAndNodeTiming pins that the backend timing facts
// reach the inspect payload — as fields and as RFC3339 JSON — in both the full
// and the status-only read. The editor's run bar derives its duration from
// exactly these values, so dropping any of them makes a finished run render
// without timing while every other inspect assertion stays green.
func TestInspectProjectsExecutionAndNodeTiming(t *testing.T) {
	ctx := context.Background()
	state := newFakeState()
	eng := New(state, &fakeQueue{})
	g := inspectTimingGraph(t)

	created := time.Date(2026, 10, 19, 9, 30, 0, 123000000, time.UTC)
	updated := created.Add(90 * time.Second)
	started := created.Add(10 * time.Second)
	finished := created.Add(80 * time.Second)

	id := types.ExecutionID("exec-timing")
	if err := state.CreateExecution(ctx, &ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusSuccess,
		CreatedAt: created, UpdatedAt: updated,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	if err := state.UpsertNode(ctx, &NodeSnapshot{
		ExecutionID: id, Name: "work", Status: types.NodeStatusSuccess,
		StartedAt: started, FinishedAt: finished,
	}); err != nil {
		t.Fatalf("UpsertNode() error = %v", err)
	}

	detail, err := eng.Inspect(ctx, id)
	if err != nil {
		t.Fatalf("Inspect() error = %v", err)
	}
	if detail.CreatedAt == nil || !detail.CreatedAt.Equal(created) {
		t.Fatalf("CreatedAt = %v, want %v", detail.CreatedAt, created)
	}
	if detail.UpdatedAt == nil || !detail.UpdatedAt.Equal(updated) {
		t.Fatalf("UpdatedAt = %v, want %v", detail.UpdatedAt, updated)
	}
	if len(detail.Nodes) != 1 {
		t.Fatalf("Nodes len = %d, want 1", len(detail.Nodes))
	}
	node := detail.Nodes[0]
	if node.StartedAt == nil || !node.StartedAt.Equal(started) {
		t.Fatalf("StartedAt = %v, want %v", node.StartedAt, started)
	}
	if node.FinishedAt == nil || !node.FinishedAt.Equal(finished) {
		t.Fatalf("FinishedAt = %v, want %v", node.FinishedAt, finished)
	}

	// The HTTP surface serializes these structs directly, so the wire format is
	// part of the contract: RFC3339 (Nano) strings, not epoch numbers or the
	// zero time.
	payload, err := json.Marshal(detail)
	if err != nil {
		t.Fatalf("json.Marshal(ExecutionDetail) error = %v", err)
	}
	for key, want := range map[string]time.Time{
		"created_at":  created,
		"updated_at":  updated,
		"started_at":  started,
		"finished_at": finished,
	} {
		encoded := `"` + key + `":"` + want.Format(time.RFC3339Nano) + `"`
		if !strings.Contains(string(payload), encoded) {
			t.Fatalf("inspect JSON missing %s, want %q in %s", encoded, encoded, payload)
		}
	}

	statusOnly, err := eng.InspectStatus(ctx, id)
	if err != nil {
		t.Fatalf("InspectStatus() error = %v", err)
	}
	if statusOnly.CreatedAt == nil || !statusOnly.CreatedAt.Equal(created) {
		t.Fatalf("InspectStatus CreatedAt = %v, want %v", statusOnly.CreatedAt, created)
	}
	if statusOnly.UpdatedAt == nil || !statusOnly.UpdatedAt.Equal(updated) {
		t.Fatalf("InspectStatus UpdatedAt = %v, want %v", statusOnly.UpdatedAt, updated)
	}
}

// TestInspectOmitsUnknownTiming pins the backward-compatible half: an execution
// or node recorded before the backends stamped timing reports absence, never a
// zero time. The JSON keys must be omitted rather than sent as
// "0001-01-01T00:00:00Z", which a client would treat as a real timestamp.
func TestInspectOmitsUnknownTiming(t *testing.T) {
	ctx := context.Background()
	state := newFakeState()
	eng := New(state, &fakeQueue{})
	g := inspectTimingGraph(t)

	id := types.ExecutionID("exec-timing-unknown")
	if err := state.CreateExecution(ctx, &ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusSuccess,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	if err := state.UpsertNode(ctx, &NodeSnapshot{
		ExecutionID: id, Name: "work", Status: types.NodeStatusPending,
	}); err != nil {
		t.Fatalf("UpsertNode() error = %v", err)
	}

	detail, err := eng.Inspect(ctx, id)
	if err != nil {
		t.Fatalf("Inspect() error = %v", err)
	}
	if detail.CreatedAt != nil || detail.UpdatedAt != nil {
		t.Fatalf("execution timing = (%v, %v), want nil for an unstamped execution",
			detail.CreatedAt, detail.UpdatedAt)
	}
	if len(detail.Nodes) != 1 {
		t.Fatalf("Nodes len = %d, want 1", len(detail.Nodes))
	}
	node := detail.Nodes[0]
	if node.StartedAt != nil || node.FinishedAt != nil {
		t.Fatalf("node timing = (%v, %v), want nil for a node that never ran",
			node.StartedAt, node.FinishedAt)
	}

	payload, err := json.Marshal(detail)
	if err != nil {
		t.Fatalf("json.Marshal(ExecutionDetail) error = %v", err)
	}
	for _, key := range []string{"created_at", "updated_at", "started_at", "finished_at"} {
		if strings.Contains(string(payload), `"`+key+`"`) {
			t.Fatalf("inspect JSON for an unstamped execution contains %q: %s", key, payload)
		}
	}
}
