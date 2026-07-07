//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/sqlstore/mysqlstore"
	"github.com/xbcio/xflow/types"
)

func TestSQLStoreRealCRUD(t *testing.T) {
	dsn := requireMySQL(t)

	p, err := mysqlstore.New(dsn)
	if err != nil {
		t.Fatalf("mysqlstore.New(%q): %v", dsn, err)
	}
	// p implements store.Store (Executions + Nodes + Signals)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	execID := types.ExecutionID("e2e-mysql-" + time.Now().Format("150405.000"))

	// workflow_def, params, runtime are NOT NULL JSON columns in MySQL —
	// supply minimal valid JSON to satisfy the constraint.
	emptyJSON := []byte(`{}`)

	// --- CreateExecution ---
	snap := &store.ExecutionRecord{
		ExecutionID:  execID,
		WorkflowName: "wf-sqlstore-real",
		WorkflowDef:  emptyJSON,
		Params:       emptyJSON,
		Runtime:      emptyJSON,
		Status:       types.ExecutionStatusRunning,
	}
	if err := p.CreateExecution(ctx, snap); err != nil {
		t.Fatalf("CreateExecution(%v): %v", snap, err)
	}

	// --- GetExecution ---
	got, err := p.GetExecution(ctx, execID)
	if err != nil {
		t.Fatalf("GetExecution(%q): %v", execID, err)
	}
	if got.ExecutionID != execID {
		t.Fatalf("got.ExecutionID = %q, want %q", got.ExecutionID, execID)
	}
	if got.Status != types.ExecutionStatusRunning {
		t.Fatalf("got.Status = %q, want %q", got.Status, types.ExecutionStatusRunning)
	}

	// --- UpsertNode ---
	node := &store.NodeRecord{
		ExecutionID: execID,
		NodeName:    "n1",
		NodeType:    "task",
		Status:      types.NodeStatusRunning,
	}
	if err := p.UpsertNode(ctx, node); err != nil {
		t.Fatalf("UpsertNode(%v): %v", node, err)
	}

	// --- GetNode ---
	gn, err := p.GetNode(ctx, execID, "n1")
	if err != nil {
		t.Fatalf("GetNode(%q, %q): %v", execID, "n1", err)
	}
	if gn.NodeName != "n1" {
		t.Fatalf("gn.NodeName = %q, want %q", gn.NodeName, "n1")
	}
	if gn.ExecutionID != execID {
		t.Fatalf("gn.ExecutionID = %q, want %q", gn.ExecutionID, execID)
	}

	// --- UpdateExecutionStatus ---
	if err := p.UpdateExecutionStatus(ctx, execID, types.ExecutionStatusSuccess, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus(%q, success): %v", execID, err)
	}

	// --- Verify terminal state ---
	got2, err := p.GetExecution(ctx, execID)
	if err != nil {
		t.Fatalf("GetExecution (after update) %q: %v", execID, err)
	}
	if got2.Status != types.ExecutionStatusSuccess {
		t.Fatalf("status = %q, want %q", got2.Status, types.ExecutionStatusSuccess)
	}
}
