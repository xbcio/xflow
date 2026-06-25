package asynq

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

func TestBuildExecutionRecordPersistsWorkflowAuditContext(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "vulnerability-approval",
		Nodes: []types.NodeDef{{
			Name:       "SecurityApproval",
			Type:       "xflow.approval",
			Parameters: map[string]any{"mode": "all", "approvers": []any{"alice", "bob"}},
		}},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	params := map[string]any{"vuln_id": "VULN-2026-001", "severity": "critical"}
	ctx := engine.WithWorkflowDef(context.Background(), def)

	rec, err := buildExecutionRecord(ctx, &engine.ExecutionSnapshot{
		ID:     "exec-1",
		Graph:  g,
		Status: types.ExecutionStatusRunning,
		Params: params,
	}, time.Unix(100, 0))
	if err != nil {
		t.Fatalf("buildExecutionRecord() error = %v", err)
	}
	if rec.WorkflowName != "vulnerability-approval" {
		t.Fatalf("WorkflowName = %q, want vulnerability-approval", rec.WorkflowName)
	}

	var gotDef types.WorkflowDef
	if err := json.Unmarshal(rec.WorkflowDef, &gotDef); err != nil {
		t.Fatalf("WorkflowDef unmarshal error = %v", err)
	}
	if gotDef.Name != "vulnerability-approval" || len(gotDef.Nodes) != 1 {
		t.Fatalf("WorkflowDef = %#v, want submitted definition", gotDef)
	}

	var gotParams map[string]any
	if err := json.Unmarshal(rec.Params, &gotParams); err != nil {
		t.Fatalf("Params unmarshal error = %v", err)
	}
	if gotParams["vuln_id"] != "VULN-2026-001" || gotParams["severity"] != "critical" {
		t.Fatalf("Params = %#v, want submitted params", gotParams)
	}
}
