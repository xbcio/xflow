package engine

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// traceInspectGraph is one node, enough for an inspect that reads the graph and
// a node snapshot; the trace projection is execution-level.
func traceInspectGraph(t *testing.T) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:  "audit-trace",
		Nodes: []types.NodeDef{{Name: "work", Type: "test.audit"}},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	return g
}

// TestInspectProjectsTheSubmissionTraceID pins that the trace id captured at
// submission reaches the inspect payload (both the full and the status-only
// read) and its JSON as `trace_id`. The editor's 运行日志 tab offers this value
// for copying and for an external trace link; dropping the projection would
// leave every execution's trace unknown even though the submission recorded it.
func TestInspectProjectsTheSubmissionTraceID(t *testing.T) {
	ctx := context.Background()
	state := newFakeState()
	eng := New(state, &fakeQueue{})
	g := traceInspectGraph(t)

	id := types.ExecutionID("exec-trace")
	if err := state.CreateExecution(ctx, &ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusSuccess,
		TraceID: "0123456789abcdef0123456789abcdef",
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}

	detail, err := eng.Inspect(ctx, id)
	if err != nil {
		t.Fatalf("Inspect() error = %v", err)
	}
	if detail.TraceID != "0123456789abcdef0123456789abcdef" {
		t.Fatalf("TraceID = %q, want the submission trace id", detail.TraceID)
	}

	// The HTTP surface serializes this struct directly, so the wire name is part
	// of the contract.
	payload, err := json.Marshal(detail)
	if err != nil {
		t.Fatalf("json.Marshal(ExecutionDetail) error = %v", err)
	}
	if !strings.Contains(string(payload), `"trace_id":"0123456789abcdef0123456789abcdef"`) {
		t.Fatalf("inspect JSON missing trace_id: %s", payload)
	}

	statusOnly, err := eng.InspectStatus(ctx, id)
	if err != nil {
		t.Fatalf("InspectStatus() error = %v", err)
	}
	if statusOnly.TraceID != detail.TraceID {
		t.Fatalf("InspectStatus TraceID = %q, want %q", statusOnly.TraceID, detail.TraceID)
	}
}

// TestInspectOmitsUnknownTraceID pins the other half: an execution submitted
// without trace context reports absence, never a placeholder id or a
// reconstructed one. `omitempty` must drop the key, because an empty string
// would read as a trace id that can be linked to.
func TestInspectOmitsUnknownTraceID(t *testing.T) {
	ctx := context.Background()
	state := newFakeState()
	eng := New(state, &fakeQueue{})
	g := traceInspectGraph(t)

	id := types.ExecutionID("exec-no-trace")
	if err := state.CreateExecution(ctx, &ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusSuccess,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}

	detail, err := eng.Inspect(ctx, id)
	if err != nil {
		t.Fatalf("Inspect() error = %v", err)
	}
	if detail.TraceID != "" {
		t.Fatalf("TraceID = %q, want empty for a submission without trace context", detail.TraceID)
	}

	payload, err := json.Marshal(detail)
	if err != nil {
		t.Fatalf("json.Marshal(ExecutionDetail) error = %v", err)
	}
	if strings.Contains(string(payload), `"trace_id"`) {
		t.Fatalf("inspect JSON for an untraced execution contains trace_id: %s", payload)
	}
}
