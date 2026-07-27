//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend/providers/distributed"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/store"
	"github.com/redis/go-redis/v9"
)

// TestAuditReconcileWorkerLeaderGatedRealRedis proves the T9 worker's leader
// gating works with a REAL RedisLeaderElector (not a fake): only the replica
// that holds the lease scans and appends outcomes; the non-leader replica is
// a no-op. Uses the real Redis at XFLOW_TEST_REDIS_ADDR (host port 6380);
// skips honestly when Redis is unreachable.
func TestAuditReconcileWorkerLeaderGatedRealRedis(t *testing.T) {
	addr := requireRedis(t)
	key := fmt.Sprintf("xflow:ns:est:audit-reconcile-leader:%s:%d", t.Name(), time.Now().UnixNano())
	ttl := 2 * time.Second

	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = rdb.Close() })
	_ = rdb.Del(context.Background(), key).Err()

	a := distributed.NewRedisLeaderElector(rdb, key, ttl)
	b := distributed.NewRedisLeaderElector(rdb, key, ttl)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.Campaign(ctx); err != nil {
		t.Fatalf("a campaign: %v", err)
	}
	// b must NOT win while a holds the lease.
	bctx, bcancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer bcancel()
	if err := b.Campaign(bctx); err == nil {
		t.Fatal("b unexpectedly became leader while a holds lease")
	}
	if !a.IsLeader() {
		t.Fatalf("a.IsLeader() = %v, want true", a.IsLeader())
	}
	if b.IsLeader() {
		t.Fatalf("b.IsLeader() = %v, want false", b.IsLeader())
	}

	// Two workers sharing one in-memory audit reconciler + authority. Only
	// the leader (a) may settle; b must be a no-op even though it can see the
	// pending admission through the shared store.
	audit := &sharedFakeAuditReconciler{}
	audit.addRow(&store.AuditRecord{
		RequestID:   "req-leader-gate",
		Principal:   "alice",
		Namespace:   "namespace-leader",
		Operation:   "workflow.create",
		ExecutionID: "exec-leader",
		Decision:    "allow",
		Outcome:     store.AuditOutcomeAdmitted,
		Phase:       store.AuditPhaseAdmission,
		Timestamp:   time.Now().Add(-2 * time.Minute),
	})
	authority := &scriptedAuthority{effect: control.EffectConfirmed}

	wA := control.NewAuditReconcileWorker(audit, authority, control.AuditReconcileConfig{
		Period: 10 * time.Millisecond, BacklogAge: time.Millisecond, Batch: 64, Elector: a,
	})
	wB := control.NewAuditReconcileWorker(audit, authority, control.AuditReconcileConfig{
		Period: 10 * time.Millisecond, BacklogAge: time.Millisecond, Batch: 64, Elector: b,
	})

	if n := wA.ReconcileOnce(context.Background()); n != 1 {
		t.Fatalf("leader worker settled = %d, want 1", n)
	}
	// The non-leader worker must not append a duplicate outcome.
	if n := wB.ReconcileOnce(context.Background()); n != 0 {
		t.Fatalf("non-leader worker settled = %d, want 0 (leader-gated no-op)", n)
	}
	if got := audit.outcomeCount(); got != 1 {
		t.Fatalf("outcome rows = %d, want 1 (leader-gated, no duplicate)", got)
	}
}

// sharedFakeAuditReconciler is a tiny in-memory store.AuditReconciler for the
// leader-gating integration test (test/integration cannot import the control
// package's unexported fakeAuditReconciler).
type sharedFakeAuditReconciler struct {
	rows    []*store.AuditRecord
	nextSeq uint64
}

func (s *sharedFakeAuditReconciler) addRow(rec *store.AuditRecord) {
	s.nextSeq++
	rec.SeqID = s.nextSeq
	rec.ID = s.nextSeq
	cp := *rec
	s.rows = append(s.rows, &cp)
}

func (s *sharedFakeAuditReconciler) ListUnreconciledAdmissions(_ context.Context, _ time.Time, afterSeqID uint64, _ int) ([]*store.AuditRecord, error) {
	out := make([]*store.AuditRecord, 0, len(s.rows))
	hasOutcome := make(map[string]bool)
	for _, r := range s.rows {
		if r.Phase == store.AuditPhaseOutcome && r.RequestID != "" {
			hasOutcome[r.Namespace+"|"+r.RequestID] = true
		}
	}
	for _, r := range s.rows {
		if afterSeqID > 0 && r.SeqID <= afterSeqID {
			continue
		}
		if r.Phase == store.AuditPhaseAdmission && r.Outcome == store.AuditOutcomeAdmitted &&
			!(r.RequestID != "" && hasOutcome[r.Namespace+"|"+r.RequestID]) {
			cp := *r
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (s *sharedFakeAuditReconciler) AppendOutcomeIfAbsent(_ context.Context, rec *store.AuditRecord) (bool, error) {
	rec.Phase = store.AuditPhaseOutcome
	for _, r := range s.rows {
		if r.Phase == store.AuditPhaseOutcome && r.RequestID == rec.RequestID && r.Namespace == rec.Namespace {
			return false, nil
		}
	}
	cp := *rec
	s.rows = append(s.rows, &cp)
	return true, nil
}

func (s *sharedFakeAuditReconciler) CountUnreconciledAdmissions(_ context.Context, _ time.Time) (int, time.Time, error) {
	hasOutcome := make(map[string]bool)
	for _, r := range s.rows {
		if r.Phase == store.AuditPhaseOutcome && r.RequestID != "" {
			hasOutcome[r.Namespace+"|"+r.RequestID] = true
		}
	}
	var count int
	var oldest time.Time
	for _, r := range s.rows {
		if r.Phase == store.AuditPhaseAdmission && r.Outcome == store.AuditOutcomeAdmitted &&
			!(r.RequestID != "" && hasOutcome[r.Namespace+"|"+r.RequestID]) {
			count++
			if oldest.IsZero() || (!r.Timestamp.IsZero() && r.Timestamp.Before(oldest)) {
				oldest = r.Timestamp
			}
		}
	}
	return count, oldest, nil
}

func (s *sharedFakeAuditReconciler) outcomeCount() int {
	n := 0
	for _, r := range s.rows {
		if r.Phase == store.AuditPhaseOutcome {
			n++
		}
	}
	return n
}

// scriptedAuthority returns a fixed effect for every probe.
type scriptedAuthority struct{ effect control.MutationEffect }

func (a *scriptedAuthority) Probe(context.Context, *store.AuditRecord) (control.MutationEffect, error) {
	return a.effect, nil
}
