//go:build integration

package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// TestSQLStoreTransactionCommit verifies that writes to Execution/Node/Signal
// inside a Transaction all commit when fn returns nil.
func TestSQLStoreTransactionCommit(t *testing.T) {
	p := newSQLStoreProvider(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	execID := newExecutionID(t, "tx-commit")

	err := p.Transaction(ctx, func(s store.Set) error {
		if err := s.Execution.CreateExecution(ctx, &store.ExecutionRecord{
			ExecutionID:  execID,
			WorkflowName: "wf-tx-commit",
			WorkflowDef:  emptyJSON,
			Params:       emptyJSON,
			Runtime:      emptyJSON,
			Status:       types.ExecutionStatusRunning,
		}); err != nil {
			return err
		}
		if err := s.Node.UpsertNode(ctx, &store.NodeRecord{
			ExecutionID: execID,
			NodeName:    "n1",
			NodeType:    "task",
			Status:      types.NodeStatusRunning,
		}); err != nil {
			return err
		}
		return s.Signal.SaveSignal(ctx, &store.SignalRecord{
			ExecutionID: execID,
			SignalName:  "sig1",
			Payload:     []byte(`{"v":1}`),
		})
	})
	if err != nil {
		t.Fatalf("Transaction commit: %v", err)
	}

	// All three tables should reflect the committed writes.
	if got, err := p.GetExecution(ctx, execID); err != nil || got.ExecutionID != execID {
		t.Fatalf("GetExecution after commit: got=%v err=%v", got, err)
	}
	if got, err := p.GetNode(ctx, execID, "n1"); err != nil || got.NodeName != "n1" {
		t.Fatalf("GetNode after commit: got=%v err=%v", got, err)
	}
	sigs, err := p.ListSignalsByNames(ctx, execID, []string{"sig1"}, store.DefaultListOptions())
	if err != nil || len(sigs) != 1 || sigs[0].Status != types.SignalStatusActive {
		t.Fatalf("ListSignalsByNames after commit: sigs=%v err=%v", sigs, err)
	}
}

// TestSQLStoreTransactionRollback verifies that writes inside a Transaction are
// rolled back when fn returns an error, and writes outside the tx survive.
func TestSQLStoreTransactionRollback(t *testing.T) {
	p := newSQLStoreProvider(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	execID := newExecutionID(t, "tx-rollback")
	newBaseExecution(ctx, t, p, execID) // baseline execution, outside tx

	sentinel := errors.New("force rollback")
	err := p.Transaction(ctx, func(s store.Set) error {
		if err := s.Node.UpsertNode(ctx, &store.NodeRecord{
			ExecutionID: execID,
			NodeName:    "rb-node",
			NodeType:    "task",
			Status:      types.NodeStatusRunning,
		}); err != nil {
			return err
		}
		_ = s.Signal.SaveSignal(ctx, &store.SignalRecord{
			ExecutionID: execID,
			SignalName:  "rb-sig",
			Payload:     emptyJSON,
		})
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("Transaction returned %v, want sentinel %v", err, sentinel)
	}

	// Rolled-back writes must not be visible.
	if _, err := p.GetNode(ctx, execID, "rb-node"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("GetNode(rb-node) after rollback: err=%v, want ErrNotFound", err)
	}
	sigs, err := p.ListSignalsByNames(ctx, execID, []string{"rb-sig"}, store.DefaultListOptions())
	if err != nil || len(sigs) != 0 {
		t.Fatalf("ListSignalsByNames(rb-sig) after rollback: sigs=%v err=%v", sigs, err)
	}

	// Baseline execution must survive the rollback.
	got, err := p.GetExecution(ctx, execID)
	if err != nil {
		t.Fatalf("GetExecution baseline after rollback: %v", err)
	}
	if got.Status != types.ExecutionStatusRunning {
		t.Fatalf("baseline status = %q, want %q", got.Status, types.ExecutionStatusRunning)
	}
}

// TestSQLStoreUpsertNodeUpdate verifies the ON CONFLICT update path: a second
// UpsertNode with the same execID+name updates status/attempt/output/port.
func TestSQLStoreUpsertNodeUpdate(t *testing.T) {
	p := newSQLStoreProvider(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	execID := newExecutionID(t, "upsert-update")
	newBaseExecution(ctx, t, p, execID)

	// First upsert: running, attempt 1, empty output.
	mustUpsertNode(ctx, t, p, &store.NodeRecord{
		ExecutionID: execID,
		NodeName:    "n1",
		NodeType:    "task",
		Status:      types.NodeStatusRunning,
		Attempt:     1,
		Output:      emptyJSON,
		Port:        "main",
	})
	// Second upsert: success, attempt 2, real output.
	mustUpsertNode(ctx, t, p, &store.NodeRecord{
		ExecutionID: execID,
		NodeName:    "n1",
		NodeType:    "task",
		Status:      types.NodeStatusSuccess,
		Attempt:     2,
		Output:      []byte(`{"result":"ok"}`),
		Port:        "main",
	})

	got, err := p.GetNode(ctx, execID, "n1")
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got.Status != types.NodeStatusSuccess {
		t.Fatalf("status = %q, want %q", got.Status, types.NodeStatusSuccess)
	}
	if got.Attempt != 2 {
		t.Fatalf("attempt = %d, want 2", got.Attempt)
	}
	if !jsonEqual(got.Output, []byte(`{"result":"ok"}`)) {
		t.Fatalf("output=%s, want {\"result\":\"ok\"}", string(got.Output))
	}
}

// TestSQLStoreUpdateExecutionStatusNotFound verifies the ErrNotFound contract
// when the execution id does not exist (RowsAffected == 0 path).
func TestSQLStoreUpdateExecutionStatusNotFound(t *testing.T) {
	p := newSQLStoreProvider(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	execID := newExecutionID(t, "not-found")
	err := p.UpdateExecutionStatus(ctx, execID, types.ExecutionStatusSuccess, "")
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("UpdateExecutionStatus(missing) err=%v, want ErrNotFound (wrapped)", err)
	}
}
