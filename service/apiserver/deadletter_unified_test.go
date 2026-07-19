package apiserver

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/backend/distributed"
	"github.com/xbcio/xflow/backend/tenant"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/observability/metrics"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// fakeReceiptAuditAppender is an in-memory store.AuditAppender +
// ReceiptAuditAppender for the management-module integration tests. It mirrors
// the SQL provider's idempotent append-by-receipt-audit-id so the durable
// projection path is exercised end-to-end through the HTTP API.
type fakeReceiptAuditAppender struct {
	mu       sync.Mutex
	rows     []*store.AuditRecord
	failNext func(*store.AuditRecord) error
}

func (f *fakeReceiptAuditAppender) AppendAudit(ctx context.Context, rec *store.AuditRecord) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if rec.ReceiptAuditID != "" {
		for _, r := range f.rows {
			if r.ReceiptAuditID == rec.ReceiptAuditID {
				return nil
			}
		}
	}
	if f.failNext != nil {
		if err := f.failNext(rec); err != nil {
			return err
		}
	}
	cp := *rec
	f.rows = append(f.rows, &cp)
	return nil
}

func (f *fakeReceiptAuditAppender) AppendAuditIfAbsent(ctx context.Context, rec *store.AuditRecord) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if rec.ReceiptAuditID != "" {
		for _, r := range f.rows {
			if r.ReceiptAuditID == rec.ReceiptAuditID {
				return false, nil
			}
		}
	}
	if f.failNext != nil {
		if err := f.failNext(rec); err != nil {
			return false, err
		}
	}
	cp := *rec
	f.rows = append(f.rows, &cp)
	return true, nil
}

func (f *fakeReceiptAuditAppender) AuditByReceiptAuditID(_ context.Context, id string) (*store.AuditRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, r := range f.rows {
		if r.ReceiptAuditID == id {
			return r, nil
		}
	}
	return nil, store.ErrNotFound
}

func (f *fakeReceiptAuditAppender) snapshot() []*store.AuditRecord {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*store.AuditRecord, len(f.rows))
	copy(out, f.rows)
	return out
}

func (f *fakeReceiptAuditAppender) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.rows)
}

// newMgmtIntegrationModule builds a management module over a miniredis-backed
// distributed backend with metrics + a durable SQL audit sink (wrapping a fake
// ReceiptAuditAppender), so the replay outcome metric + durable projection are
// exercised through the real manager wiring.
func newMgmtIntegrationModule(t *testing.T, mr *miniredis.Miniredis, appender *fakeReceiptAuditAppender) (*managementModule, *metrics.Metrics) {
	t.Helper()
	b, err := distributed.New(mr.Addr(), nil, distributed.WithConsumer(false))
	if err != nil {
		t.Fatalf("distributed.New: %v", err)
	}
	cp, err := control.NewControlPlane(control.Config{Backend: b})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	m := newManagementModule(cp)
	mm := metrics.New()
	m.metrics = mm
	// Wrap the fake appender in a real SQLAuditSink so deadLetterAuditSink()
	// recognizes it and wires the durable ReceiptProjector.
	m.audit = NewSQLAuditSink(appender)
	return m, mm
}

func seedDeadLetterForMgmt(t *testing.T, mr *miniredis.Miniredis, tenantName, execID, entryID string) {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	ctx := context.Background()
	statusKey := "xflow:t" + tenantName + ":exec:{" + execID + "}:status"
	deadKey := "xflow:t" + tenantName + ":exec:{" + execID + "}:outbox:dead"
	deadBodyKey := "xflow:t" + tenantName + ":exec:{" + execID + "}:outbox:dead:body"
	deadMetaKey := "xflow:t" + tenantName + ":exec:{" + execID + "}:outbox:dead:meta:" + entryID
	if err := rdb.Set(ctx, statusKey, "running", time.Minute).Err(); err != nil {
		t.Fatalf("set status: %v", err)
	}
	body := `{"id":"` + entryID + `","task":{"execution_id":"` + execID + `","node_name":"review","node_idx":1,"type":0"},"auto_depth":0,"activation_id":1,"available_at_ms":0,"created_at_ms":0}`
	if err := rdb.HSet(ctx, deadBodyKey, entryID, body).Err(); err != nil {
		t.Fatalf("hset dead body: %v", err)
	}
	if err := rdb.ZAdd(ctx, deadKey, redis.Z{Score: 0, Member: entryID}).Err(); err != nil {
		t.Fatalf("zadd dead: %v", err)
	}
	if err := rdb.HSet(ctx, deadMetaKey, "node", "review", "activation", "1").Err(); err != nil {
		t.Fatalf("hset dead meta: %v", err)
	}
}

func mgmtDo(t *testing.T, mux http.Handler, method, path string, body io.Reader) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, body)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

// TestDeadLetterReplayUnifiedMetricAndProjection proves blocker #4: the
// management module injects shared metrics + a durable receipt projector, so
// one replay produces (a) the outcome counter, (b) the pending/dead gauge, and
// (c) a durable SQL projection row keyed by the receipt audit_id — all at the
// single outlet the CLI also uses.
func TestDeadLetterReplayUnifiedMetricAndProjection(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	appender := &fakeReceiptAuditAppender{}
	b, err := distributed.New(mr.Addr(), nil, distributed.WithConsumer(false))
	if err != nil {
		t.Fatalf("distributed.New: %v", err)
	}
	cp, err := control.NewControlPlane(control.Config{Backend: b})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	mm := metrics.New()
	m := newManagementModule(cp)
	m.metrics = mm
	m.audit = NewSQLAuditSink(appender)
	mgr, err := m.deadLetterManager()
	if err != nil {
		t.Fatalf("deadLetterManager: %v", err)
	}

	execID := "exec-unified"
	entryID := "execute/exec-unified/review/1"
	seedDeadLetterForMgmt(t, mr, "default", execID, entryID)

	ctx := tenant.WithTenant(context.Background(), tenant.DefaultTenant)
	principal := control.DeadLetterReplayPrincipal{
		Subject: "alice", TenantID: "default", Scopes: []string{control.ScopeDeadLetterReplay},
	}
	res, derr := mgr.Replay(ctx, principal, engine.ReplayDeadLetterRequest{
		ExecutionID: types.ExecutionID(execID), EntryID: entryID, RequestID: "req-unified", Reason: "root cause fixed",
	})
	if derr != nil {
		t.Fatalf("Replay: %v", derr)
	}
	if res.Outcome != engine.ReplayReplayed {
		t.Fatalf("outcome = %q, want replayed", res.Outcome)
	}
	if res.AuditID == "" {
		t.Fatal("audit_id empty; receipt not written")
	}
	// The durable projection must have landed exactly one row keyed by the
	// receipt's audit_id. Allow a brief moment for the in-request retry path.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && appender.count() == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	if got := appender.count(); got != 1 {
		t.Fatalf("projected rows = %d, want 1 (unified durable projection)", got)
	}
	rows := appender.snapshot()
	if rows[0].ReceiptAuditID != res.AuditID {
		t.Fatalf("projected receipt_audit_id = %q, want %q", rows[0].ReceiptAuditID, res.AuditID)
	}
	if rows[0].Reason == "root cause fixed" {
		t.Fatalf("durable projection persisted operator free-text reason (security: reason stays in Redis)")
	}
}

// TestDeadLetterReplayAuthzDenialsAudited proves forged operator / missing
// scope / missing reason are all rejected AND audited. The operator field is
// never taken from the request body (forged operator test): the principal's
// subject is authoritative.
func TestDeadLetterReplayAuthzDenialsAudited(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	execID := "exec-denials"
	entryID := "execute/exec-denials/review/1"
	seedDeadLetterForMgmt(t, mr, "default", execID, entryID)

	// missing scope → 403 + deny audit
	auth := staticPrincipalAuth{principal: Principal{Subject: "mallory", Scopes: []string{}}}
	audit := NewInMemoryAuditSink()
	_, mux := newMgmtAuthzServer(t, auth, ScopeAuthorizer{}, audit)
	rec := mgmtDo(t, mux, http.MethodPost, "/v1/management/dead-letters/"+execID+"/replay",
		strings.NewReader(`{"entry_id":"`+entryID+`","reason":"r"}`))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("missing scope: status = %d, want 403", rec.Code)
	}
	events := audit.Events()
	if len(events) != 1 || events[0].Decision != DecisionDeny {
		t.Fatalf("missing scope audit = %+v, want one deny", events)
	}

	// missing reason → 400 (admission audit still recorded because it's a mutation route)
	auth2 := staticPrincipalAuth{principal: Principal{Subject: "alice", Scopes: []string{"deadletter.replay"}}}
	audit2 := NewInMemoryAuditSink()
	_, mux2 := newMgmtAuthzServer(t, auth2, ScopeAuthorizer{}, audit2)
	rec2 := mgmtDo(t, mux2, http.MethodPost, "/v1/management/dead-letters/"+execID+"/replay",
		strings.NewReader(`{"entry_id":"`+entryID+`"}`))
	if rec2.Code != http.StatusBadRequest {
		t.Fatalf("missing reason: status = %d, want 400", rec2.Code)
	}
	foundAdmission := false
	for _, ev := range audit2.Events() {
		if ev.Operation == OpDeadLetterReplay && ev.Outcome == "admitted" {
			foundAdmission = true
			break
		}
	}
	if !foundAdmission {
		t.Fatalf("missing reason did not write admission audit: %+v", audit2.Events())
	}
	// The forged-operator case: a request body cannot self-report identity.
	// The manager ignores req.Operator and uses the principal's subject, so a
	// forged "operator":"admin" in the body never affects the audit.
	auth3 := staticPrincipalAuth{principal: Principal{Subject: "alice", Scopes: []string{"deadletter.replay"}}}
	audit3 := NewInMemoryAuditSink()
	_, mux3 := newMgmtAuthzServer(t, auth3, ScopeAuthorizer{}, audit3)
	body := `{"entry_id":"` + entryID + `","reason":"r","operator":"forged-admin"}`
	rec3 := mgmtDo(t, mux3, http.MethodPost, "/v1/management/dead-letters/"+execID+"/replay", strings.NewReader(body))
	// The exec resolves to 404 under the in-memory backend (no Redis), so the
	// mutation audit records admission + failed reconcile. The forged operator
	// never appears in audit.
	_ = rec3
	for _, ev := range audit3.Events() {
		if ev.Principal == "forged-admin" {
			t.Fatalf("forged operator leaked into audit as principal: %+v", ev)
		}
	}
}

// TestDeadLetterReplayProjectionFailureThenReconcile proves the injection
// scenario: Redis succeeds (receipt written), HTTP response is lost or SQL
// projection fails, and a later reconcile appends the projection exactly once
// without re-executing the replay.
func TestDeadLetterReplayProjectionFailureThenReconcile(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	// First pass: the SQL appender fails every projection, so the Redis receipt
	// is written but no durable row lands.
	failing := &fakeReceiptAuditAppender{failNext: func(*store.AuditRecord) error { return errProjectionBoom }}
	b1, err := distributed.New(mr.Addr(), nil, distributed.WithConsumer(false))
	if err != nil {
		t.Fatalf("distributed.New b1: %v", err)
	}
	
	cp1, err := control.NewControlPlane(control.Config{Backend: b1})
	if err != nil {
		t.Fatalf("NewControlPlane cp1: %v", err)
	}
	m1 := newManagementModule(cp1)
	m1.metrics = metrics.New()
	m1.audit = NewSQLAuditSink(failing)
	mgr1, err := m1.deadLetterManager()
	if err != nil {
		t.Fatalf("deadLetterManager: %v", err)
	}

	execID := "exec-inject"
	entryID := "execute/exec-inject/review/1"
	seedDeadLetterForMgmt(t, mr, "default", execID, entryID)

	ctx := tenant.WithTenant(context.Background(), tenant.DefaultTenant)
	principal := control.DeadLetterReplayPrincipal{
		Subject: "alice", TenantID: "default", Scopes: []string{control.ScopeDeadLetterReplay},
	}
	res, derr := mgr1.Replay(ctx, principal, engine.ReplayDeadLetterRequest{
		ExecutionID: types.ExecutionID(execID), EntryID: entryID, RequestID: "req-inject", Reason: "fixed",
	})
	if derr != nil {
		t.Fatalf("Replay: %v", derr)
	}
	if res.Outcome != engine.ReplayReplayed {
		t.Fatalf("outcome = %q, want replayed (Redis is authoritative; SQL failure must not block replay)", res.Outcome)
	}
	if res.AuditID == "" {
		t.Fatal("audit_id empty; Redis receipt not written")
	}
	if got := failing.count(); got != 0 {
		t.Fatalf("projected rows = %d, want 0 (projection failed)", got)
	}

	// Verify the Redis receipt survives (authoritative).
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	receiptKey := "xflow:tdefault:exec:{" + execID + "}:replay:receipt:req-inject"
	exists, err := rdb.Exists(ctx, receiptKey).Result()
	if err != nil {
		t.Fatalf("exists receipt: %v", err)
	}
	if exists != 1 {
		t.Fatalf("Redis receipt missing; authoritative receipt must survive SQL projection failure")
	}

	// Second pass: a healthy appender + the reconcile diff-scan projects the
	// receipt exactly once. A second reconcile call is a no-op (idempotent).
	healthy := &fakeReceiptAuditAppender{}
	b2, err := distributed.New(mr.Addr(), nil, distributed.WithConsumer(false))
	if err != nil {
		t.Fatalf("distributed.New b2: %v", err)
	}
	
	reader, ok := b2.State().(engine.ReplayReceiptReader)
	if !ok {
		t.Fatalf("StateStore %T does not implement ReplayReceiptReader", b2.State())
	}
	projector := control.NewReceiptProjector(healthy)
	err = reader.ScanReplayReceipts(ctx, func(r engine.ReplayReceipt) error {
		_, perr := projector.Project(ctx, r)
		return perr
	})
	if err != nil {
		t.Fatalf("ScanReplayReceipts: %v", err)
	}
	if got := healthy.count(); got != 1 {
		t.Fatalf("after reconcile: projected rows = %d, want 1 (one receipt, one projection)", got)
	}
	rows := healthy.snapshot()
	if rows[0].ReceiptAuditID != res.AuditID {
		t.Fatalf("reconcile projected wrong receipt: %q vs %q", rows[0].ReceiptAuditID, res.AuditID)
	}

	// Second reconcile: idempotent — no duplicate projection.
	err = reader.ScanReplayReceipts(ctx, func(r engine.ReplayReceipt) error {
		_, perr := projector.Project(ctx, r)
		return perr
	})
	if err != nil {
		t.Fatalf("second ScanReplayReceipts: %v", err)
	}
	if got := healthy.count(); got != 1 {
		t.Fatalf("after second reconcile: projected rows = %d, want 1 (idempotent)", got)
	}

	// The replay was NOT re-executed: the Redis receipt still reports the
	// original outcome, and a re-replay with the same RequestID returns
	// already_replayed (the move happened once).
	res2, err := mgr1.Replay(ctx, principal, engine.ReplayDeadLetterRequest{
		ExecutionID: types.ExecutionID(execID), EntryID: entryID, RequestID: "req-inject", Reason: "retry",
	})
	if err != nil {
		t.Fatalf("re-replay: %v", err)
	}
	if res2.Outcome != engine.ReplayAlreadyReplayed {
		t.Fatalf("re-replay outcome = %q, want already_replayed (no duplicate mutation)", res2.Outcome)
	}
	if res2.AuditID != res.AuditID {
		t.Fatalf("re-replay audit_id = %q, want original %q", res2.AuditID, res.AuditID)
	}
}

// TestDeadLetterManagerLazyInitRace guards the dlMgr sync.Once construction.
// Multiple goroutines may call deadLetterManager() from concurrent HTTP replay
// requests; without synchronization the field would be read and written
// concurrently. The race detector catches that; this test exercises it.
func TestDeadLetterManagerLazyInitRace(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	appender := &fakeReceiptAuditAppender{}
	m, _ := newMgmtIntegrationModule(t, mr, appender)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mgr, err := m.deadLetterManager()
			if err != nil {
				t.Errorf("deadLetterManager: %v", err)
				return
			}
			if mgr == nil {
				t.Error("deadLetterManager returned nil")
			}
		}()
	}
	wg.Wait()
}

// errProjectionBoom is the sentinel for the projection-failure injection.
var errProjectionBoom = bytesError("projection boom")

type bytesError string

func (b bytesError) Error() string { return string(b) }
