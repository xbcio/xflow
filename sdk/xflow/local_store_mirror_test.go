package xflow

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/memstore"
	"github.com/xbcio/xflow/types"
)

// TestNewLocalWithStoreMirrorsExecution is the end-to-end form of the local
// store contract: an ordinary embedded workflow runs to completion and its
// execution/node history is readable from the durable store afterwards. The
// run itself must be unaffected by the mirror.
func TestNewLocalWithStoreMirrorsExecution(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	st := memstore.New()
	eng, err := NewLocal(WithStore(st))
	if err != nil {
		t.Fatalf("NewLocal(WithStore) error = %v", err)
	}
	defer eng.Stop()

	wf := Workflow("local-store-mirror")
	start := wf.Node("start", node.Start())
	set := wf.Node("set", node.Set(map[string]any{"done": true}))
	wf.Connect(start, set)

	workflowID, err := eng.AddWorkflow(ctx, wf)
	if err != nil {
		t.Fatalf("AddWorkflow() error = %v", err)
	}
	id, err := eng.Invoke(ctx, workflowID, Start(), map[string]any{"ticket": "T-42"})
	if err != nil {
		t.Fatalf("Invoke() error = %v", err)
	}
	if _, err := eng.Wait(ctx, id); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}

	rec := waitForMirroredExecution(t, st, id)
	if rec.WorkflowID != workflowID {
		t.Fatalf("mirrored workflow id = %q, want %q", rec.WorkflowID, workflowID)
	}
	if rec.WorkflowName != "local-store-mirror" {
		t.Fatalf("mirrored workflow name = %q, want local-store-mirror", rec.WorkflowName)
	}
	if len(rec.WorkflowDef) == 0 {
		t.Fatal("mirrored workflow definition is empty")
	}
	var params map[string]any
	if err := json.Unmarshal(rec.Params, &params); err != nil {
		t.Fatalf("mirrored params are not JSON: %v", err)
	}
	if params["ticket"] != "T-42" {
		t.Fatalf("mirrored params = %v, want ticket T-42", params)
	}

	for _, name := range []string{"start", "set"} {
		waitForMirroredNode(t, st, id, name)
	}
}

// waitForMirroredExecution polls for the terminal row. Wait returns when the
// in-memory execution completes, which happens inside the commit transition;
// the post-commit mirror write is best-effort and lands just after it, so a
// reader that starts at Wait's return can briefly observe "running". The
// documented contract is convergence, not a synchronous write.
func waitForMirroredExecution(t *testing.T, st store.Store, id types.ExecutionID) *store.ExecutionRecord {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	var last *store.ExecutionRecord
	var lastErr error
	for time.Now().Before(deadline) {
		last, lastErr = st.GetExecution(context.Background(), id)
		if lastErr == nil && last.Status == types.ExecutionStatusSuccess {
			return last
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("mirrored execution never reached success: %+v, err = %v", last, lastErr)
	return nil
}

func waitForMirroredNode(t *testing.T, st store.Store, id types.ExecutionID, name string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	var last *store.NodeRecord
	var lastErr error
	for time.Now().Before(deadline) {
		last, lastErr = st.GetNode(context.Background(), id, name)
		if lastErr == nil && last.Status == types.NodeStatusSuccess {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("mirrored node %q never reached success: %+v, err = %v", name, last, lastErr)
}

// TestNewLocalWithStoreOptionIsOptional pins the compatibility half of the
// option: without WithStore, NewLocal installs no handle and the same workflow
// still completes.
func TestNewLocalWithStoreOptionIsOptional(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	eng, err := NewLocal()
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	defer eng.Stop()

	wf := Workflow("local-no-store")
	wf.Node("start", node.Start())
	workflowID, err := eng.AddWorkflow(ctx, wf)
	if err != nil {
		t.Fatalf("AddWorkflow() error = %v", err)
	}
	id, err := eng.Invoke(ctx, workflowID, Start(), nil)
	if err != nil {
		t.Fatalf("Invoke() error = %v", err)
	}
	if _, err := eng.Wait(ctx, id); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	detail, err := eng.Inspect(ctx, id)
	if err != nil || detail.Status != types.ExecutionStatusSuccess {
		t.Fatalf("Inspect() = %v, err = %v, want success", detail.Status, err)
	}
}
