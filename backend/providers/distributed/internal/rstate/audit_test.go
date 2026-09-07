package rstate

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

type failingStore struct {
	upsertErr error
	calls     map[string]int
}

type countingStore struct {
	createExecutionCalls int
	updateStatusCalls    int
	upsertNodeCalls      int
	saveSignalCalls      int
	revokeSignalCalls    int
}

func newFailingStore(upsertErr error) *failingStore {
	return &failingStore{upsertErr: upsertErr, calls: map[string]int{}}
}

func (f *failingStore) CreateExecution(context.Context, *store.ExecutionRecord) error {
	f.calls["create_execution"]++
	return nil
}

func (f *failingStore) UpdateExecutionStatus(context.Context, types.ExecutionID, types.ExecutionStatus, string) error {
	f.calls["update_execution_status"]++
	return f.upsertErr
}

func (f *failingStore) GetExecution(context.Context, types.ExecutionID) (*store.ExecutionRecord, error) {
	return nil, nil
}

func (f *failingStore) UpsertNode(context.Context, *store.NodeRecord) error {
	f.calls["upsert_node"]++
	return f.upsertErr
}

func (f *failingStore) GetNode(context.Context, types.ExecutionID, string) (*store.NodeRecord, error) {
	return nil, nil
}

func (f *failingStore) ListNodes(context.Context, types.ExecutionID, store.ListOptions) ([]*store.NodeRecord, error) {
	return nil, nil
}

func (f *failingStore) ListSuspendedBySignal(context.Context, types.ExecutionID, string) ([]*store.NodeRecord, error) {
	return nil, nil
}

func (f *failingStore) ListExpiredSuspensions(context.Context, time.Time, store.ListOptions) ([]*store.NodeRecord, error) {
	return nil, nil
}

func (f *failingStore) SaveSignal(context.Context, *store.SignalRecord) error {
	f.calls["save_signal"]++
	return f.upsertErr
}

func (f *failingStore) ConsumeSignal(context.Context, types.ExecutionID, string) (*store.SignalRecord, error) {
	return nil, nil
}

func (f *failingStore) RevokeSignal(context.Context, types.ExecutionID, string) (bool, error) {
	f.calls["revoke_signal"]++
	return true, f.upsertErr
}

func (f *failingStore) CountSignalsByNames(context.Context, types.ExecutionID, []string) (int, error) {
	return 0, nil
}

func (f *failingStore) ListSignalsByNames(context.Context, types.ExecutionID, []string, store.ListOptions) ([]*store.SignalRecord, error) {
	return nil, nil
}

func (f *failingStore) AppendAudit(context.Context, *store.AuditRecord) error {
	return nil
}

func (f *failingStore) GetSupply(context.Context, string, string) (*store.SupplyResource, error) {
	return nil, nil
}

func (f *failingStore) PutSupply(_ context.Context, _ *store.SupplyResource, _ *uint64) (*store.SupplyResource, error) {
	f.calls["put_supply"]++
	return nil, f.upsertErr
}

func (c *countingStore) CreateExecution(context.Context, *store.ExecutionRecord) error {
	c.createExecutionCalls++
	return nil
}

func (c *countingStore) UpdateExecutionStatus(context.Context, types.ExecutionID, types.ExecutionStatus, string) error {
	c.updateStatusCalls++
	return nil
}

func (c *countingStore) GetExecution(context.Context, types.ExecutionID) (*store.ExecutionRecord, error) {
	return nil, nil
}

func (c *countingStore) UpsertNode(context.Context, *store.NodeRecord) error {
	c.upsertNodeCalls++
	return nil
}

func (c *countingStore) GetNode(context.Context, types.ExecutionID, string) (*store.NodeRecord, error) {
	return nil, nil
}

func (c *countingStore) ListNodes(context.Context, types.ExecutionID, store.ListOptions) ([]*store.NodeRecord, error) {
	return nil, nil
}

func (c *countingStore) ListSuspendedBySignal(context.Context, types.ExecutionID, string) ([]*store.NodeRecord, error) {
	return nil, nil
}

func (c *countingStore) ListExpiredSuspensions(context.Context, time.Time, store.ListOptions) ([]*store.NodeRecord, error) {
	return nil, nil
}

func (c *countingStore) SaveSignal(context.Context, *store.SignalRecord) error {
	c.saveSignalCalls++
	return nil
}

func (c *countingStore) ConsumeSignal(context.Context, types.ExecutionID, string) (*store.SignalRecord, error) {
	return nil, nil
}

func (c *countingStore) RevokeSignal(context.Context, types.ExecutionID, string) (bool, error) {
	c.revokeSignalCalls++
	return true, nil
}

func (c *countingStore) CountSignalsByNames(context.Context, types.ExecutionID, []string) (int, error) {
	return 0, nil
}

func (c *countingStore) ListSignalsByNames(context.Context, types.ExecutionID, []string, store.ListOptions) ([]*store.SignalRecord, error) {
	return nil, nil
}

func (c *countingStore) AppendAudit(context.Context, *store.AuditRecord) error {
	return nil
}

func (c *countingStore) GetSupply(context.Context, string, string) (*store.SupplyResource, error) {
	return nil, nil
}

func (c *countingStore) PutSupply(context.Context, *store.SupplyResource, *uint64) (*store.SupplyResource, error) {
	return nil, nil
}

type recordingObserver struct {
	ok     map[string]int
	failed map[string]int
}

func newRecordingObserver() *recordingObserver {
	return &recordingObserver{ok: map[string]int{}, failed: map[string]int{}}
}

func (r *recordingObserver) OnAuditOK(ctx context.Context, op string)              { r.ok[op]++ }
func (r *recordingObserver) OnAuditFailed(ctx context.Context, op string, _ error) { r.failed[op]++ }

func TestAuditWriteRecordsFailureButDoesNotPropagate(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	dbErr := errors.New("connection refused")
	db := newFailingStore(dbErr)
	obs := newRecordingObserver()

	state := New(rdb, db, time.Minute)
	state.audit = obs

	snap := &engine.NodeSnapshot{
		ExecutionID: "exec-1",
		Name:        "n",
		Status:      types.NodeStatusSuccess,
	}
	if err := state.UpsertNode(context.Background(), snap); err != nil {
		t.Fatalf("UpsertNode() error = %v, want nil (audit failure must not propagate)", err)
	}
	if db.calls["upsert_node"] != 1 {
		t.Fatalf("audit store upsert called %d times, want 1", db.calls["upsert_node"])
	}
	if obs.failed["upsert_node"] != 1 {
		t.Fatalf("observer recorded %d failures, want 1", obs.failed["upsert_node"])
	}
	stats := state.auditCounters.snapshot()
	if stats.Failed["upsert_node"] != 1 {
		t.Fatalf("audit counter failed=%d, want 1", stats.Failed["upsert_node"])
	}
}

func TestAuditWriteSuccessIncrementsOK(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	db := newFailingStore(nil) // no error → success
	state := New(rdb, db, time.Minute)
	state.audit = newRecordingObserver()

	if err := state.UpsertNode(context.Background(), &engine.NodeSnapshot{
		ExecutionID: "exec-1", Name: "n", Status: types.NodeStatusSuccess,
	}); err != nil {
		t.Fatal(err)
	}
	stats := state.auditCounters.snapshot()
	if stats.OK["upsert_node"] != 1 || stats.Failed["upsert_node"] != 0 {
		t.Fatalf("audit counters ok=%d failed=%d, want 1/0", stats.OK["upsert_node"], stats.Failed["upsert_node"])
	}
}

func TestAuditWriteNoopWhenStoreNil(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	state := New(rdb, nil, time.Minute)
	if err := state.UpsertNode(context.Background(), &engine.NodeSnapshot{
		ExecutionID: "exec-1", Name: "n", Status: types.NodeStatusSuccess,
	}); err != nil {
		t.Fatal(err)
	}
	stats := state.auditCounters.snapshot()
	if len(stats.OK) != 0 || len(stats.Failed) != 0 {
		t.Fatalf("counters non-empty without audit store: %+v", stats)
	}
}

func TestTransientModeSkipsAuditWrites(t *testing.T) {
	rdb := newRedisStateTestClient(t)
	store := &countingStore{}
	state := New(rdb, store, time.Hour)
	state.transient = true
	state.transientTTL = time.Minute
	state.transientCompletionTTL = 30 * time.Second

	err := state.CreateExecution(context.Background(), &engine.ExecutionSnapshot{
		ID:     "exec-transient-audit",
		Status: types.ExecutionStatusRunning,
		Graph:  testGraphOneNode(),
	})
	if err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	if store.createExecutionCalls != 0 {
		t.Fatalf("audit create calls = %d, want 0", store.createExecutionCalls)
	}
	if err := state.UpsertNode(context.Background(), &engine.NodeSnapshot{
		ExecutionID: "exec-transient-audit",
		Name:        "start",
		Status:      types.NodeStatusSuccess,
	}); err != nil {
		t.Fatalf("UpsertNode() error = %v", err)
	}
	if store.upsertNodeCalls != 0 {
		t.Fatalf("audit node upsert calls = %d, want 0", store.upsertNodeCalls)
	}
}

// TestAuditWriteSkipsTransientEvenWithStore locks in the central auditWrite
// guard: in transient mode the audit fn must never run, even when a Store is
// configured. This covers call sites (e.g. acquire_task_lease) that previously
// gated only on s.db != nil and would have audited in transient mode.
func TestAuditWriteSkipsTransientEvenWithStore(t *testing.T) {
	rdb := newRedisStateTestClient(t)
	store := &countingStore{}
	state := New(rdb, store, time.Hour)
	state.transient = true
	state.transientTTL = time.Minute
	state.transientCompletionTTL = 30 * time.Second

	called := false
	state.auditWrite(context.Background(), "acquire_task_lease", func(context.Context) error {
		called = true
		return nil
	})
	if called {
		t.Fatal("audit fn ran in transient mode, want skipped")
	}
	if store.upsertNodeCalls != 0 {
		t.Fatalf("store upsert calls = %d, want 0", store.upsertNodeCalls)
	}
	stats := state.auditCounters.snapshot()
	if len(stats.OK) != 0 || len(stats.Failed) != 0 {
		t.Fatalf("audit counters non-empty in transient mode: %+v", stats)
	}
}
