//go:build integration

package integration

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// --- SaveSignal ---

func TestSQLStoreSaveSignalInsert(t *testing.T) {
	p := newSQLStoreProvider(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	execID := newExecutionID(t, "sig-insert")
	newBaseExecution(ctx, t, p, execID)
	mustSaveSignal(ctx, t, p, execID, "sig1", []byte(`{"a":1}`))

	sigs, err := p.ListSignalsByNames(ctx, execID, []string{"sig1"}, store.DefaultListOptions())
	if err != nil {
		t.Fatalf("ListSignalsByNames: %v", err)
	}
	if len(sigs) != 1 {
		t.Fatalf("len(sigs)=%d, want 1", len(sigs))
	}
	if sigs[0].Status != types.SignalStatusActive {
		t.Fatalf("status=%q, want active", sigs[0].Status)
	}
	if !jsonEqual(sigs[0].Payload, []byte(`{"a":1}`)) {
		t.Fatalf("payload=%s, want {\"a\":1}", string(sigs[0].Payload))
	}
	if sigs[0].CreatedAt.IsZero() {
		t.Fatalf("CreatedAt is zero")
	}
}

func TestSQLStoreSaveSignalUpsert(t *testing.T) {
	p := newSQLStoreProvider(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	execID := newExecutionID(t, "sig-upsert")
	newBaseExecution(ctx, t, p, execID)
	mustSaveSignal(ctx, t, p, execID, "sig1", []byte(`{"v":1}`))
	mustSaveSignal(ctx, t, p, execID, "sig1", []byte(`{"v":2}`)) // same name → upsert

	sigs, err := p.ListSignalsByNames(ctx, execID, []string{"sig1"}, store.DefaultListOptions())
	if err != nil {
		t.Fatalf("ListSignalsByNames: %v", err)
	}
	if len(sigs) != 1 {
		t.Fatalf("len(sigs)=%d, want 1 (upsert, not insert)", len(sigs))
	}
	if !jsonEqual(sigs[0].Payload, []byte(`{"v":2}`)) {
		t.Fatalf("payload=%s, want {\"v\":2} (upserted)", string(sigs[0].Payload))
	}
	if sigs[0].Status != types.SignalStatusActive {
		t.Fatalf("status=%q, want active", sigs[0].Status)
	}
}

// --- ConsumeSignal ---

func TestSQLStoreConsumeSignalHit(t *testing.T) {
	p := newSQLStoreProvider(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	execID := newExecutionID(t, "sig-consume-hit")
	newBaseExecution(ctx, t, p, execID)
	mustSaveSignal(ctx, t, p, execID, "sig1", []byte(`{"x":1}`))

	rec, err := p.ConsumeSignal(ctx, execID, "sig1")
	if err != nil {
		t.Fatalf("ConsumeSignal: %v", err)
	}
	if rec == nil || rec.SignalName != "sig1" {
		t.Fatalf("rec=%v, want SignalName=sig1", rec)
	}

	// After consume, the signal is no longer active.
	sigs, _ := p.ListSignalsByNames(ctx, execID, []string{"sig1"}, store.DefaultListOptions())
	if len(sigs) != 0 {
		t.Fatalf("active signals after consume=%d, want 0", len(sigs))
	}
	// Re-consuming the same name must report ErrNotFound.
	if _, err := p.ConsumeSignal(ctx, execID, "sig1"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("re-ConsumeSignal err=%v, want ErrNotFound", err)
	}
}

func TestSQLStoreConsumeSignalMiss(t *testing.T) {
	p := newSQLStoreProvider(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	execID := newExecutionID(t, "sig-consume-miss")
	newBaseExecution(ctx, t, p, execID)

	if _, err := p.ConsumeSignal(ctx, execID, "never-saved"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("ConsumeSignal(missing) err=%v, want ErrNotFound", err)
	}
}

// TestSQLStoreConsumeSignalConcurrent is the core FOR UPDATE row-lock
// verification: N goroutines race to consume one active signal; exactly one
// wins, the rest get ErrNotFound.
func TestSQLStoreConsumeSignalConcurrent(t *testing.T) {
	p := newSQLStoreProvider(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	execID := newExecutionID(t, "sig-consume-race")
	newBaseExecution(ctx, t, p, execID)
	mustSaveSignal(ctx, t, p, execID, "sig-race", []byte(`{"n":1}`))

	const N = 8
	type result struct {
		rec *store.SignalRecord
		err error
	}
	results := make([]result, N)
	ready := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(i int) {
			defer wg.Done()
			<-ready
			rec, err := p.ConsumeSignal(ctx, execID, "sig-race")
			results[i] = result{rec: rec, err: err}
		}(i)
	}
	close(ready)
	wg.Wait()

	wins := 0
	notFound := 0
	otherErr := 0
	for _, r := range results {
		switch {
		case r.err == nil && r.rec != nil:
			wins++
		case errors.Is(r.err, store.ErrNotFound):
			notFound++
		default:
			otherErr++
		}
	}
	if wins != 1 {
		t.Fatalf("wins=%d, want exactly 1 (FOR UPDATE must serialize consumers)", wins)
	}
	if notFound != N-1 {
		t.Fatalf("notFound=%d, want %d", notFound, N-1)
	}
	if otherErr != 0 {
		t.Fatalf("otherErr=%d, want 0", otherErr)
	}
	// Final DB state: consumed (no active left).
	sigs, _ := p.ListSignalsByNames(ctx, execID, []string{"sig-race"}, store.DefaultListOptions())
	if len(sigs) != 0 {
		t.Fatalf("active signals after race=%d, want 0", len(sigs))
	}
}

// --- RevokeSignal ---

func TestSQLStoreRevokeSignalHit(t *testing.T) {
	p := newSQLStoreProvider(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	execID := newExecutionID(t, "sig-revoke-hit")
	newBaseExecution(ctx, t, p, execID)
	mustSaveSignal(ctx, t, p, execID, "sig1", emptyJSON)

	ok, err := p.RevokeSignal(ctx, execID, "sig1")
	if err != nil {
		t.Fatalf("RevokeSignal: %v", err)
	}
	if !ok {
		t.Fatalf("ok=false, want true")
	}
	sigs, _ := p.ListSignalsByNames(ctx, execID, []string{"sig1"}, store.DefaultListOptions())
	if len(sigs) != 0 {
		t.Fatalf("active signals after revoke=%d, want 0", len(sigs))
	}
}

func TestSQLStoreRevokeSignalMiss(t *testing.T) {
	p := newSQLStoreProvider(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	execID := newExecutionID(t, "sig-revoke-miss")
	newBaseExecution(ctx, t, p, execID)

	// Never saved → false, no error.
	ok, err := p.RevokeSignal(ctx, execID, "never")
	if err != nil {
		t.Fatalf("RevokeSignal(missing) err=%v, want nil", err)
	}
	if ok {
		t.Fatalf("ok=true for missing signal, want false")
	}

	// Already consumed → false.
	mustSaveSignal(ctx, t, p, execID, "consumed", emptyJSON)
	if _, err := p.ConsumeSignal(ctx, execID, "consumed"); err != nil {
		t.Fatalf("ConsumeSignal setup: %v", err)
	}
	ok2, err := p.RevokeSignal(ctx, execID, "consumed")
	if err != nil {
		t.Fatalf("RevokeSignal(consumed) err=%v, want nil", err)
	}
	if ok2 {
		t.Fatalf("ok=true for consumed signal, want false")
	}
}

// --- Count / List ---

func TestSQLStoreCountSignalsByNames(t *testing.T) {
	p := newSQLStoreProvider(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	execID := newExecutionID(t, "sig-count")
	newBaseExecution(ctx, t, p, execID)
	mustSaveSignal(ctx, t, p, execID, "a", emptyJSON)
	mustSaveSignal(ctx, t, p, execID, "b", emptyJSON)
	mustSaveSignal(ctx, t, p, execID, "c", emptyJSON)
	if _, err := p.ConsumeSignal(ctx, execID, "b"); err != nil {
		t.Fatalf("ConsumeSignal b: %v", err)
	}

	got, err := p.CountSignalsByNames(ctx, execID, []string{"a", "b", "c"})
	if err != nil {
		t.Fatalf("CountSignalsByNames: %v", err)
	}
	if got != 2 { // a + c active, b consumed
		t.Fatalf("count=%d, want 2", got)
	}
}

func TestSQLStoreListSignalsByNames(t *testing.T) {
	p := newSQLStoreProvider(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	execID := newExecutionID(t, "sig-list")
	newBaseExecution(ctx, t, p, execID)
	mustSaveSignal(ctx, t, p, execID, "s1", emptyJSON)
	mustSaveSignal(ctx, t, p, execID, "s2", emptyJSON)
	mustSaveSignal(ctx, t, p, execID, "s3", emptyJSON)
	names := []string{"s1", "s2", "s3"}

	// Page 1: limit 2, offset 0 → s1, s2 (ordered by id ASC).
	page1, err := p.ListSignalsByNames(ctx, execID, names, store.ListOptions{Limit: 2, Offset: 0})
	if err != nil {
		t.Fatalf("ListSignalsByNames page1: %v", err)
	}
	if len(page1) != 2 {
		t.Fatalf("page1 len=%d, want 2", len(page1))
	}
	if page1[0].SignalName != "s1" || page1[1].SignalName != "s2" {
		t.Fatalf("page1 names=%q,%q, want s1,s2", page1[0].SignalName, page1[1].SignalName)
	}
	// Page 2: limit 2, offset 2 → s3.
	page2, err := p.ListSignalsByNames(ctx, execID, names, store.ListOptions{Limit: 2, Offset: 2})
	if err != nil {
		t.Fatalf("ListSignalsByNames page2: %v", err)
	}
	if len(page2) != 1 || page2[0].SignalName != "s3" {
		t.Fatalf("page2=%v, want single s3", page2)
	}
}

// --- Node list queries (sweeper inputs) ---

func TestSQLStoreListSuspendedBySignal(t *testing.T) {
	p := newSQLStoreProvider(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	execID := newExecutionID(t, "list-suspended")
	newBaseExecution(ctx, t, p, execID)

	mustUpsertNode(ctx, t, p, &store.NodeRecord{
		ExecutionID: execID, NodeName: "n1", NodeType: "wait", Status: types.NodeStatusSuspended, SignalName: "approval",
	})
	mustUpsertNode(ctx, t, p, &store.NodeRecord{
		ExecutionID: execID, NodeName: "n2", NodeType: "wait", Status: types.NodeStatusSuspended, SignalName: "approval",
	})
	mustUpsertNode(ctx, t, p, &store.NodeRecord{
		ExecutionID: execID, NodeName: "n3", NodeType: "task", Status: types.NodeStatusRunning, SignalName: "approval", // running, excluded
	})
	mustUpsertNode(ctx, t, p, &store.NodeRecord{
		ExecutionID: execID, NodeName: "n4", NodeType: "wait", Status: types.NodeStatusSuspended, SignalName: "other", // different signal
	})

	got, err := p.ListSuspendedBySignal(ctx, execID, "approval")
	if err != nil {
		t.Fatalf("ListSuspendedBySignal: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len=%d, want 2 (only suspended+approval)", len(got))
	}
	names := map[string]bool{}
	for _, n := range got {
		names[n.NodeName] = true
	}
	if !names["n1"] || !names["n2"] {
		t.Fatalf("got names=%v, want n1+n2", names)
	}
}

func TestSQLStoreListExpiredSuspensions(t *testing.T) {
	p := newSQLStoreProvider(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	execID := newExecutionID(t, "list-expired")
	newBaseExecution(ctx, t, p, execID)

	past := time.Now().Add(-1 * time.Minute)
	future := time.Now().Add(1 * time.Minute)

	mustUpsertNode(ctx, t, p, &store.NodeRecord{
		ExecutionID: execID, NodeName: "expired", NodeType: "wait", Status: types.NodeStatusSuspended, Timeout: &past,
	})
	mustUpsertNode(ctx, t, p, &store.NodeRecord{
		ExecutionID: execID, NodeName: "future", NodeType: "wait", Status: types.NodeStatusSuspended, Timeout: &future,
	})
	mustUpsertNode(ctx, t, p, &store.NodeRecord{
		ExecutionID: execID, NodeName: "running", NodeType: "task", Status: types.NodeStatusRunning, Timeout: &past, // not suspended
	})

	got, err := p.ListExpiredSuspensions(ctx, time.Now(), store.DefaultListOptions())
	if err != nil {
		t.Fatalf("ListExpiredSuspensions: %v", err)
	}
	// Filter to this execution (other tests may leave rows).
	var mine []*store.NodeRecord
	for _, n := range got {
		if n.ExecutionID == execID {
			mine = append(mine, n)
		}
	}
	if len(mine) != 1 {
		t.Fatalf("expired for this exec=%d, want 1", len(mine))
	}
	if mine[0].NodeName != "expired" {
		t.Fatalf("got %q, want expired", mine[0].NodeName)
	}
}
