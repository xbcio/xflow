package rstate

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// TestBuildExecutionRecordStampsStatisticsColumns pins the three execution-table
// columns the statistics work added to the creation projection: trigger_kind,
// node_total, and the write-at-creation finished_at for an already-terminal
// creation. Each has a "UNKNOWN is not zero" rule that a plausible refactor
// (always stamping, or stamping only in one path) would break.
func TestBuildExecutionRecordStampsStatisticsColumns(t *testing.T) {
	def := &types.WorkflowDef{
		ID:    "wf-stats",
		Name:  "stats",
		Nodes: []types.NodeDef{{Name: "Start", Type: "xflow.start"}, {Name: "Work", Type: "xflow.function"}},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	ctx := context.Background()
	now := time.Unix(500, 0).UTC()

	running, err := buildExecutionRecord(ctx, &engine.ExecutionSnapshot{
		ID: "exec-running", Graph: g, Status: types.ExecutionStatusRunning,
	}, now)
	if err != nil {
		t.Fatalf("buildExecutionRecord(running) error = %v", err)
	}
	if running.TriggerKind != types.TriggerKindAPI {
		t.Fatalf("TriggerKind = %q, want %q for a snapshot with no explicit kind", running.TriggerKind, types.TriggerKindAPI)
	}
	if running.NodeTotal != g.NodeCount() {
		t.Fatalf("NodeTotal = %d, want the compiled graph's %d nodes", running.NodeTotal, g.NodeCount())
	}
	if running.FinishedAt != nil {
		t.Fatalf("FinishedAt = %v on a running creation, want nil", running.FinishedAt)
	}

	terminal, err := buildExecutionRecord(ctx, &engine.ExecutionSnapshot{
		ID: "exec-done", Graph: g, Status: types.ExecutionStatusSuccess, TriggerKind: types.TriggerKindTimer,
	}, now)
	if err != nil {
		t.Fatalf("buildExecutionRecord(terminal) error = %v", err)
	}
	if terminal.TriggerKind != types.TriggerKindTimer {
		t.Fatalf("TriggerKind = %q, want the snapshot's explicit %q", terminal.TriggerKind, types.TriggerKindTimer)
	}
	if terminal.FinishedAt == nil || !terminal.FinishedAt.Equal(now) {
		t.Fatalf("FinishedAt = %v, want the creation instant %v for an already-terminal creation", terminal.FinishedAt, now)
	}

	// A seed with no compiled body has no denominator: 0 means UNKNOWN, not
	// "a graph with zero nodes".
	seed, err := buildExecutionRecord(ctx, &engine.ExecutionSnapshot{
		ID: "exec-seed", Status: types.ExecutionStatusRunning,
	}, now)
	if err != nil {
		t.Fatalf("buildExecutionRecord(seed) error = %v", err)
	}
	if seed.NodeTotal != 0 {
		t.Fatalf("NodeTotal = %d for a nil graph, want 0 (UNKNOWN)", seed.NodeTotal)
	}
	if store.IsTerminalExecutionStatus(seed.Status) {
		t.Fatalf("fixture mistake: %q is terminal", seed.Status)
	}
}
