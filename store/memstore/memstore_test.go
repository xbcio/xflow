package memstore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

func newExec(t *testing.T, id types.ExecutionID) *store.ExecutionRecord {
	t.Helper()
	return &store.ExecutionRecord{
		ExecutionID:  id,
		WorkflowName: "wf",
		Status:       types.ExecutionStatusRunning,
	}
}

// TestTransaction_Rollback verifies that writes performed inside a failed
// transaction are discarded and the store is restored to its pre-tx snapshot.
func TestTransaction_Rollback(t *testing.T) {
	ctx := context.Background()
	s := New()

	execID := types.ExecutionID("exec-rollback")
	if err := s.CreateExecution(ctx, newExec(t, execID)); err != nil {
		t.Fatalf("seed execution: %v", err)
	}

	// Mutate inside a failing transaction; everything should be undone.
	txErr := s.Transaction(ctx, func(set store.Set) error {
		_ = set.Execution.CreateExecution(ctx, newExec(t, types.ExecutionID("exec-rollback-2")))
		_ = set.Node.UpsertNode(ctx, &store.NodeRecord{
			ExecutionID: execID,
			NodeName:    "n1",
			NodeType:    "task",
			Status:      types.NodeStatusRunning,
		})
		_ = set.Signal.SaveSignal(ctx, &store.SignalRecord{
			ExecutionID: execID,
			SignalName:  "sig",
			Payload:     []byte(`{}`),
		})
		return errors.New("boom")
	})
	if txErr == nil || txErr.Error() != "boom" {
		t.Fatalf("tx must propagate error, got %v", txErr)
	}

	if _, err := s.GetExecution(ctx, types.ExecutionID("exec-rollback-2")); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("rolled-back execution must not exist, got %v", err)
	}
	if _, err := s.GetNode(ctx, execID, "n1"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("rolled-back node must not exist, got %v", err)
	}
	if _, err := s.ConsumeSignal(ctx, execID, "sig"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("rolled-back signal must not exist, got %v", err)
	}
	// Original execution still present.
	if got, err := s.GetExecution(ctx, execID); err != nil || got.ExecutionID != execID {
		t.Fatalf("original execution must survive rollback, got %v err=%v", got, err)
	}
}

// TestTransaction_Commit verifies that successful transactions persist.
func TestTransaction_Commit(t *testing.T) {
	ctx := context.Background()
	s := New()
	execID := types.ExecutionID("exec-commit")
	if err := s.CreateExecution(ctx, newExec(t, execID)); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := s.Transaction(ctx, func(set store.Set) error {
		return set.Node.UpsertNode(ctx, &store.NodeRecord{
			ExecutionID: execID,
			NodeName:    "n1",
			NodeType:    "task",
			Status:      types.NodeStatusRunning,
		})
	}); err != nil {
		t.Fatalf("commit tx: %v", err)
	}
	if got, err := s.GetNode(ctx, execID, "n1"); err != nil || got.NodeType != "task" {
		t.Fatalf("committed node must exist, got %v err=%v", got, err)
	}
}

// TestUpsertNode_FullFieldUpdate is the memstore half of the UpsertNode field
// contract (see store.UpsertNodeUpdateFields): after an upsert on an existing
// key, every field in the contract must reflect the new record. The sqlstore
// twin uses the same field list (see node.go UpsertNode OnConflict DoUpdates);
// keep them in sync via store.UpsertNodeUpdateFields.
func TestUpsertNode_FullFieldUpdate(t *testing.T) {
	ctx := context.Background()
	s := New()
	execID := types.ExecutionID("exec-upsert")

	original := &store.NodeRecord{
		ExecutionID:  execID,
		NodeName:     "n1",
		NodeType:     "task",
		Status:       types.NodeStatusRunning,
		LeaseID:      "lease-1",
		LeaseToken:   "token-1",
		Attempt:      1,
		Output:       []byte(`{"v":1}`),
		Port:         "8080",
		SignalName:   "sig-1",
		SignalConfig: []byte(`{"k":1}`),
		Timeout:      ptrTime(time.Now().Add(time.Minute)),
	}
	if err := s.UpsertNode(ctx, original); err != nil {
		t.Fatalf("initial upsert: %v", err)
	}

	updated := &store.NodeRecord{
		ExecutionID:  execID,
		NodeName:     "n1",
		NodeType:     "gateway",
		Status:       types.NodeStatusSuspended,
		LeaseID:      "lease-2",
		LeaseToken:   "token-2",
		Attempt:      2,
		Output:       []byte(`{"v":2}`),
		Port:         "9090",
		SignalName:   "sig-2",
		SignalConfig: []byte(`{"k":2}`),
		Timeout:      ptrTime(time.Now().Add(2 * time.Minute)),
	}
	if err := s.UpsertNode(ctx, updated); err != nil {
		t.Fatalf("second upsert: %v", err)
	}

	got, err := s.GetNode(ctx, execID, "n1")
	if err != nil {
		t.Fatalf("get node: %v", err)
	}
	if got.NodeType != "gateway" {
		t.Errorf("NodeType = %q, want gateway", got.NodeType)
	}
	if got.Status != types.NodeStatusSuspended {
		t.Errorf("Status = %q, want suspended", got.Status)
	}
	if got.LeaseID != "lease-2" {
		t.Errorf("LeaseID = %q, want lease-2", got.LeaseID)
	}
	if got.LeaseToken != "token-2" {
		t.Errorf("LeaseToken = %q, want token-2", got.LeaseToken)
	}
	if got.Attempt != 2 {
		t.Errorf("Attempt = %d, want 2", got.Attempt)
	}
	if string(got.Output) != `{"v":2}` {
		t.Errorf("Output = %q, want {\"v\":2}", string(got.Output))
	}
	if got.Port != "9090" {
		t.Errorf("Port = %q, want 9090", got.Port)
	}
	if got.SignalName != "sig-2" {
		t.Errorf("SignalName = %q, want sig-2", got.SignalName)
	}
	if string(got.SignalConfig) != `{"k":2}` {
		t.Errorf("SignalConfig = %q, want {\"k\":2}", string(got.SignalConfig))
	}
	if got.Timeout == nil || !got.Timeout.After(time.Now()) {
		t.Errorf("Timeout not refreshed, got %v", got.Timeout)
	}
}

// TestSignalLifecycle covers the active -> consumed -> revoked state machine
// and that ConsumeSignal/RevokeSignal reject non-active signals.
func TestSignalLifecycle(t *testing.T) {
	ctx := context.Background()
	s := New()
	execID := types.ExecutionID("exec-sig")

	// Revoke on missing signal returns (false, nil), not an error.
	ok, err := s.RevokeSignal(ctx, execID, "missing")
	if err != nil || ok {
		t.Fatalf("revoke missing: ok=%v err=%v", ok, err)
	}
	// Consume on missing signal returns ErrNotFound.
	if _, err := s.ConsumeSignal(ctx, execID, "missing"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("consume missing: %v", err)
	}

	if err := s.SaveSignal(ctx, &store.SignalRecord{
		ExecutionID: execID,
		SignalName:  "sig",
		Payload:     []byte(`{"p":1}`),
	}); err != nil {
		t.Fatalf("save: %v", err)
	}

	// active -> consumed.
	rec, err := s.ConsumeSignal(ctx, execID, "sig")
	if err != nil {
		t.Fatalf("consume active: %v", err)
	}
	if rec.SignalName != "sig" {
		t.Fatalf("consumed record name = %q", rec.SignalName)
	}

	// Consume again on consumed signal -> ErrNotFound.
	if _, err := s.ConsumeSignal(ctx, execID, "sig"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("consume consumed: %v", err)
	}

	// Revoke on consumed signal -> (false, nil), since it is no longer active.
	ok, err = s.RevokeSignal(ctx, execID, "sig")
	if err != nil || ok {
		t.Fatalf("revoke consumed: ok=%v err=%v", ok, err)
	}

	// Count and List only count active signals.
	if n, err := s.CountSignalsByNames(ctx, execID, []string{"sig"}); err != nil || n != 0 {
		t.Fatalf("count after consume: n=%d err=%v", n, err)
	}
	if recs, err := s.ListSignalsByNames(ctx, execID, []string{"sig"}, store.DefaultListOptions()); err != nil || len(recs) != 0 {
		t.Fatalf("list after consume: len=%d err=%v", len(recs), err)
	}
}

// TestSignal_Revoke_FromActive verifies revoke transitions active -> revoked
// and a revoked signal cannot be consumed.
func TestSignal_Revoke_FromActive(t *testing.T) {
	ctx := context.Background()
	s := New()
	execID := types.ExecutionID("exec-revoke")

	if err := s.SaveSignal(ctx, &store.SignalRecord{ExecutionID: execID, SignalName: "sig"}); err != nil {
		t.Fatalf("save: %v", err)
	}
	ok, err := s.RevokeSignal(ctx, execID, "sig")
	if err != nil || !ok {
		t.Fatalf("revoke active: ok=%v err=%v", ok, err)
	}
	if _, err := s.ConsumeSignal(ctx, execID, "sig"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("consume revoked: %v", err)
	}
}

// TestSaveSignal_Reactivate verifies that re-saving a consumed/revoked signal
// flips it back to active.
func TestSaveSignal_Reactivate(t *testing.T) {
	ctx := context.Background()
	s := New()
	execID := types.ExecutionID("exec-react")
	if err := s.SaveSignal(ctx, &store.SignalRecord{ExecutionID: execID, SignalName: "sig", Payload: []byte(`1`)}); err != nil {
		t.Fatalf("save: %v", err)
	}
	if _, err := s.ConsumeSignal(ctx, execID, "sig"); err != nil {
		t.Fatalf("consume: %v", err)
	}
	if err := s.SaveSignal(ctx, &store.SignalRecord{ExecutionID: execID, SignalName: "sig", Payload: []byte(`2`)}); err != nil {
		t.Fatalf("re-save: %v", err)
	}
	if n, _ := s.CountSignalsByNames(ctx, execID, []string{"sig"}); n != 1 {
		t.Fatalf("reactivated count = %d, want 1", n)
	}
}

// TestPagination covers memstore pagination, including the negative-offset /
// limit panic guard provided by ListOptions.Normalized via paginate[T].
func TestPagination(t *testing.T) {
	ctx := context.Background()
	s := New()
	execID := types.ExecutionID("exec-page")
	_ = s.CreateExecution(ctx, newExec(t, execID))

	for i := 0; i < 5; i++ {
		_ = s.UpsertNode(ctx, &store.NodeRecord{
			ExecutionID: execID,
			NodeName:    string(rune('a' + i)),
			NodeType:    "task",
			Status:      types.NodeStatusSuspended,
			SignalName:  "sig",
			Timeout:     ptrTime(time.Now().Add(-time.Hour)), // expired
		})
	}

	cases := []struct {
		name string
		opts store.ListOptions
		want int
	}{
		{"default", store.DefaultListOptions(), 5},
		{"limit 2", store.ListOptions{Limit: 2}, 2},
		{"offset 3", store.ListOptions{Offset: 3}, 2},
		{"offset 3 limit 1", store.ListOptions{Offset: 3, Limit: 1}, 1},
		{"offset beyond end", store.ListOptions{Offset: 100}, 0},
		{"negative offset", store.ListOptions{Offset: -1, Limit: 10}, 5},
		{"negative limit", store.ListOptions{Limit: -1}, 5},
		{"limit over cap", store.ListOptions{Limit: 10_000}, 5},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := s.ListNodes(ctx, execID, c.opts)
			if err != nil {
				t.Fatalf("ListNodes: %v", err)
			}
			if len(got) != c.want {
				t.Errorf("ListNodes len = %d, want %d", len(got), c.want)
			}
			// ListExpiredSuspensions uses the same paginate path.
			exp, err := s.ListExpiredSuspensions(ctx, time.Now(), c.opts)
			if err != nil {
				t.Fatalf("ListExpiredSuspensions: %v", err)
			}
			if len(exp) != c.want {
				t.Errorf("ListExpiredSuspensions len = %d, want %d", len(exp), c.want)
			}
		})
	}
}

// TestListOptions_Normalized is the unit test for the clamp rules.
func TestListOptions_Normalized(t *testing.T) {
	cases := []struct {
		in    store.ListOptions
		wantL int
		wantO int
	}{
		{store.ListOptions{Limit: -5, Offset: -5}, 0, 0},
		{store.ListOptions{Limit: 0, Offset: 0}, 0, 0},
		{store.ListOptions{Limit: 50, Offset: 10}, 50, 10},
		{store.ListOptions{Limit: 10_000, Offset: 0}, 1000, 0},
	}
	for _, c := range cases {
		got := c.in.Normalized()
		if got.Limit != c.wantL || got.Offset != c.wantO {
			t.Errorf("Normalized(%+v) = %+v, want Limit=%d Offset=%d", c.in, got, c.wantL, c.wantO)
		}
	}
}

func ptrTime(t time.Time) *time.Time { return &t }
