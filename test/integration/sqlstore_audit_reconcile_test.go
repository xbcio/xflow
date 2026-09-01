//go:build integration

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/sqlstore"
	"github.com/xbcio/xflow/store/sqlstore/mysqlstore"
	"gorm.io/gorm"
)

type auditSchemaColumn struct {
	ColumnName             string         `gorm:"column:column_name"`
	DataType               string         `gorm:"column:data_type"`
	CharacterMaximumLength sql.NullInt64  `gorm:"column:character_maximum_length"`
	IsNullable             string         `gorm:"column:is_nullable"`
	ColumnDefault          sql.NullString `gorm:"column:column_default"`
	Extra                  string         `gorm:"column:extra"`
	GenerationExpression   string         `gorm:"column:generation_expression"`
	CharacterSetName       sql.NullString `gorm:"column:character_set_name"`
	CollationName          sql.NullString `gorm:"column:collation_name"`
}

type auditSchemaIndexColumn struct {
	IndexName  string        `gorm:"column:index_name"`
	NonUnique  int           `gorm:"column:non_unique"`
	SeqInIndex int           `gorm:"column:seq_in_index"`
	ColumnName string        `gorm:"column:column_name"`
	SubPart    sql.NullInt64 `gorm:"column:sub_part"`
	IndexType  string        `gorm:"column:index_type"`
	IsVisible  string        `gorm:"column:is_visible"`
}

// TestSQLStoreAuditSchemaAndConcurrentOutcomeIdempotency verifies both the
// physical MySQL contract and the race that contract closes. The create hook
// holds every writer immediately before INSERT, proving that all writers have
// missed the historical-row lookup before they contend on uk_phase_key.
func TestSQLStoreAuditSchemaAndConcurrentOutcomeIdempotency(t *testing.T) {
	p := newAuditTestProvider(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	assertAuditReconcileSchema(t, ctx, p.DB())
	// Artifact and embedded-server integration tests use the documented
	// development AutoMigrate path against this shared database. Prove that it
	// cannot silently restore the database default collation and collapse
	// byte-distinct audit identities before exercising concurrent inserts.
	if err := sqlstore.AutoMigrate(p.DB()); err != nil {
		t.Fatalf("AutoMigrate audit schema: %v", err)
	}
	assertAuditReconcileSchema(t, ctx, p.DB())
	namespace := auditTestNamespace(t, p, "concurrent")
	requestID := namespace + "-request"

	const writers = 16
	callbackName := "xflow:audit-create-barrier:" + namespace
	arrived := make(chan struct{}, writers)
	release := make(chan struct{})
	if err := p.DB().Callback().Create().Before("gorm:create").Register(callbackName, func(tx *gorm.DB) {
		if tx.Statement == nil || tx.Statement.Table != "xflow_audit_events" {
			return
		}
		arrived <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
			tx.AddError(ctx.Err())
		}
	}); err != nil {
		t.Fatalf("register create barrier: %v", err)
	}
	defer func() {
		if err := p.DB().Callback().Create().Remove(callbackName); err != nil {
			t.Errorf("remove create barrier: %v", err)
		}
	}()

	type appendResult struct {
		writer   int
		appended bool
		err      error
	}
	start := make(chan struct{})
	results := make(chan appendResult, writers)
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(writer int) {
			defer wg.Done()
			<-start
			appended, err := p.AppendOutcomeIfAbsent(ctx, outcomeRecord(namespace, requestID))
			results <- appendResult{writer: writer, appended: appended, err: err}
		}(i)
	}
	close(start)

	for i := 0; i < writers; i++ {
		select {
		case <-arrived:
		case <-ctx.Done():
			close(release)
			wg.Wait()
			t.Fatalf("only %d/%d writers reached the pre-insert barrier: %v", i, writers, ctx.Err())
		}
	}
	close(release)
	wg.Wait()
	close(results)

	appendedCount := 0
	var appendErrors []string
	for result := range results {
		if result.err != nil {
			appendErrors = append(appendErrors, fmt.Sprintf("writer %d: %v", result.writer, result.err))
		} else if result.appended {
			appendedCount++
		}
	}
	if len(appendErrors) != 0 {
		t.Fatalf("concurrent AppendOutcomeIfAbsent returned errors: %s", strings.Join(appendErrors, "; "))
	}
	if appendedCount != 1 {
		t.Fatalf("concurrent AppendOutcomeIfAbsent appended %d rows, want exactly 1", appendedCount)
	}
	assertAuditOutcomeCount(t, ctx, p.DB(), namespace, requestID, 1)
}

func assertAuditReconcileSchema(t *testing.T, ctx context.Context, db *gorm.DB) {
	t.Helper()

	var columns []auditSchemaColumn
	query := db.WithContext(ctx).Raw(`
SELECT COLUMN_NAME AS column_name,
       DATA_TYPE AS data_type,
       CHARACTER_MAXIMUM_LENGTH AS character_maximum_length,
       IS_NULLABLE AS is_nullable,
       COLUMN_DEFAULT AS column_default,
       EXTRA AS extra,
       COALESCE(GENERATION_EXPRESSION, '') AS generation_expression,
       CHARACTER_SET_NAME AS character_set_name,
       COLLATION_NAME AS collation_name
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'xflow_audit_events'
  AND COLUMN_NAME IN ('namespace', 'request_id', 'phase', 'phase_key')`).Scan(&columns)
	if query.Error != nil {
		t.Fatalf("audit schema migration required: inspect audit columns: %v", query.Error)
	}
	byColumn := make(map[string]auditSchemaColumn, len(columns))
	for _, column := range columns {
		byColumn[column.ColumnName] = column
	}

	phaseKey, ok := byColumn["phase_key"]
	if !ok {
		t.Fatal("audit schema migration required: phase_key is missing (run `make env-migrate`)")
	}
	if !strings.EqualFold(phaseKey.DataType, "varchar") ||
		!phaseKey.CharacterMaximumLength.Valid || phaseKey.CharacterMaximumLength.Int64 != 320 ||
		!strings.EqualFold(phaseKey.IsNullable, "YES") || phaseKey.ColumnDefault.Valid ||
		!phaseKey.CharacterSetName.Valid || phaseKey.CharacterSetName.String != "utf8mb4" ||
		!phaseKey.CollationName.Valid || phaseKey.CollationName.String != "utf8mb4_0900_bin" {
		t.Fatalf("audit schema migration required: phase_key metadata=%+v, want VARCHAR(320) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NULL DEFAULT NULL", phaseKey)
	}
	if strings.Contains(strings.ToUpper(phaseKey.Extra), "GENERATED") || strings.TrimSpace(phaseKey.GenerationExpression) != "" {
		t.Fatalf("audit schema migration required: phase_key must be non-generated (EXTRA=%q, GENERATION_EXPRESSION=%q)",
			phaseKey.Extra, phaseKey.GenerationExpression)
	}

	for _, name := range []string{"namespace", "request_id", "phase"} {
		column, ok := byColumn[name]
		if !ok {
			t.Fatalf("audit schema migration required: identity column %s is missing", name)
		}
		if !column.CharacterSetName.Valid || column.CharacterSetName.String != "utf8mb4" ||
			!column.CollationName.Valid || column.CollationName.String != "utf8mb4_0900_bin" {
			t.Fatalf("audit schema migration required: %s charset/collation=(%q,%q), want (utf8mb4,utf8mb4_0900_bin)",
				name, column.CharacterSetName.String, column.CollationName.String)
		}
	}
	var padAttribute string
	if err := db.WithContext(ctx).Raw(`
SELECT PAD_ATTRIBUTE
FROM INFORMATION_SCHEMA.COLLATIONS
WHERE COLLATION_NAME = 'utf8mb4_0900_bin'`).Scan(&padAttribute).Error; err != nil {
		t.Fatalf("inspect audit identity collation padding: %v", err)
	}
	if padAttribute != "NO PAD" {
		t.Fatalf("utf8mb4_0900_bin PAD_ATTRIBUTE=%q, want NO PAD", padAttribute)
	}

	expected := []struct {
		name    string
		unique  bool
		columns []string
	}{
		{name: "uk_phase_key", unique: true, columns: []string{"phase_key"}},
		{name: "idx_namespace_request_phase", columns: []string{"namespace", "request_id", "phase"}},
	}
	indexNames := make([]string, len(expected))
	for i, index := range expected {
		indexNames[i] = index.name
	}
	var rows []auditSchemaIndexColumn
	query = db.WithContext(ctx).Raw(`
SELECT INDEX_NAME AS index_name,
       NON_UNIQUE AS non_unique,
       SEQ_IN_INDEX AS seq_in_index,
       COLUMN_NAME AS column_name,
       SUB_PART AS sub_part,
       INDEX_TYPE AS index_type,
       IS_VISIBLE AS is_visible
FROM INFORMATION_SCHEMA.STATISTICS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'xflow_audit_events'
  AND INDEX_NAME IN ?
ORDER BY INDEX_NAME, SEQ_IN_INDEX`, indexNames).Scan(&rows)
	if query.Error != nil {
		t.Fatalf("audit schema migration required: inspect audit indexes: %v", query.Error)
	}
	byName := make(map[string][]auditSchemaIndexColumn, len(expected))
	for _, row := range rows {
		byName[row.IndexName] = append(byName[row.IndexName], row)
	}
	for _, want := range expected {
		got := byName[want.name]
		if len(got) != len(want.columns) {
			t.Fatalf("audit schema migration required: index %s has %d columns, want %d in order %v",
				want.name, len(got), len(want.columns), want.columns)
		}
		for i, column := range got {
			if column.SeqInIndex != i+1 || column.ColumnName != want.columns[i] {
				t.Fatalf("audit schema migration required: index %s column %d=(%q, sequence %d), want (%q, sequence %d)",
					want.name, i, column.ColumnName, column.SeqInIndex, want.columns[i], i+1)
			}
			if (column.NonUnique == 0) != want.unique || column.SubPart.Valid ||
				column.IndexType != "BTREE" || column.IsVisible != "YES" {
				t.Fatalf("audit schema migration required: index %s metadata=%+v, want unique=%t, no prefix, BTREE, visible",
					want.name, column, want.unique)
			}
		}
	}
}

// Historical outcome rows can have a NULL phase_key. Both public outcome
// append paths must recognize them via the byte-exact lookup rather than add a
// second row that the nullable unique index cannot reject.
func TestSQLStoreAuditHistoricalNullOutcomeIsIdempotent(t *testing.T) {
	p := newAuditTestProvider(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	namespace := auditTestNamespace(t, p, "historical-null")

	workerRequest := namespace + "-worker"
	insertHistoricalNullOutcome(t, ctx, p.DB(), namespace, workerRequest)
	appended, err := p.AppendOutcomeIfAbsent(ctx, outcomeRecord(namespace, workerRequest))
	if err != nil {
		t.Fatalf("AppendOutcomeIfAbsent over historical NULL outcome: %v", err)
	}
	if appended {
		t.Fatal("AppendOutcomeIfAbsent over historical NULL outcome appended a duplicate")
	}
	assertAuditOutcomeCount(t, ctx, p.DB(), namespace, workerRequest, 1)

	inlineRequest := namespace + "-inline"
	insertHistoricalNullOutcome(t, ctx, p.DB(), namespace, inlineRequest)
	inline := outcomeRecord(namespace, inlineRequest)
	if err := p.AppendAudit(ctx, inline); err != nil {
		t.Fatalf("AppendAudit over historical NULL outcome: %v", err)
	}
	if inline.ID != 0 {
		t.Fatalf("idempotent AppendAudit assigned duplicate ID %d, want 0", inline.ID)
	}
	assertAuditOutcomeCount(t, ctx, p.DB(), namespace, inlineRequest, 1)

	freshRequest := namespace + "-fresh-inline"
	fresh := outcomeRecord(namespace, freshRequest)
	if err := p.AppendAudit(ctx, fresh); err != nil {
		t.Fatalf("AppendAudit fresh outcome: %v", err)
	}
	if fresh.ID == 0 {
		t.Fatal("AppendAudit fresh outcome did not receive an ID")
	}
	if err := p.AppendAudit(ctx, outcomeRecord(namespace, freshRequest)); err != nil {
		t.Fatalf("AppendAudit duplicate fresh outcome: %v", err)
	}
	assertAuditOutcomeCount(t, ctx, p.DB(), namespace, freshRequest, 1)
}

func TestSQLStoreAuditNonOutcomeRowsShareNullPhaseKey(t *testing.T) {
	p := newAuditTestProvider(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	namespace := auditTestNamespace(t, p, "nullable-non-outcome")
	requestID := namespace + "-request"

	for i, phase := range []string{
		store.AuditPhaseAdmission,
		store.AuditPhaseAdmission,
		store.AuditPhaseReceipt,
		store.AuditPhaseReceipt,
	} {
		rec := &store.AuditRecord{
			RequestID: requestID,
			Principal: "audit-null-key-test",
			Namespace: namespace,
			Operation: "workflow.create",
			Decision:  "allow",
			Outcome:   store.AuditOutcomeAdmitted,
			Phase:     phase,
			Timestamp: time.Now().UTC().Add(time.Duration(i) * time.Millisecond),
		}
		if err := p.AppendAudit(ctx, rec); err != nil {
			t.Fatalf("AppendAudit phase %q row %d: %v", phase, i, err)
		}
	}

	var total, keyed int64
	if err := p.DB().WithContext(ctx).Table("xflow_audit_events").
		Where("namespace = ? AND request_id = ?", namespace, requestID).
		Count(&total).Error; err != nil {
		t.Fatalf("count non-outcome rows: %v", err)
	}
	if err := p.DB().WithContext(ctx).Table("xflow_audit_events").
		Where("namespace = ? AND request_id = ? AND phase_key IS NOT NULL", namespace, requestID).
		Count(&keyed).Error; err != nil {
		t.Fatalf("count keyed non-outcome rows: %v", err)
	}
	if total != 4 || keyed != 0 {
		t.Fatalf("non-outcome rows total/keyed = %d/%d, want 4/0", total, keyed)
	}
}

// MySQL lookup, NOT EXISTS joins, and the Go hash must agree on identity.
// These pairs differ only by case, accent, or a trailing space; each pair must
// remain two identities under the production collation.
func TestSQLStoreAuditIdentityIsByteExact(t *testing.T) {
	p := newAuditTestProvider(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	namespace := auditTestNamespace(t, p, "byte-exact")
	prefix := namespace + "-"
	pairs := [][2]string{
		{prefix + "Case", prefix + "case"},
		{prefix + "café", prefix + "cafe"},
		{prefix + "trailing", prefix + "trailing "},
	}

	var firstID, lastID uint64
	for _, pair := range pairs {
		for _, requestID := range pair {
			rec := admissionRecord(namespace, requestID)
			if err := p.AppendAudit(ctx, rec); err != nil {
				t.Fatalf("AppendAudit admission %q: %v", requestID, err)
			}
			if firstID == 0 {
				firstID = rec.ID
			}
			lastID = rec.ID
		}
	}

	for _, pair := range pairs {
		appended, err := p.AppendOutcomeIfAbsent(ctx, outcomeRecord(namespace, pair[0]))
		if err != nil || !appended {
			t.Fatalf("append first exact outcome %q: appended=%t err=%v", pair[0], appended, err)
		}
	}

	pending := listAuditAdmissionsThrough(t, ctx, p, time.Now().UTC().Add(time.Second), firstID-1, lastID, 2, namespace)
	pendingIDs := make(map[string]bool, len(pending))
	for _, rec := range pending {
		pendingIDs[rec.RequestID] = true
	}
	for _, pair := range pairs {
		if pendingIDs[pair[0]] {
			t.Fatalf("settled byte-exact identity %q remained pending", pair[0])
		}
		if !pendingIDs[pair[1]] {
			t.Fatalf("distinct byte-exact identity %q was incorrectly settled by %q", pair[1], pair[0])
		}
	}
	if len(pendingIDs) != len(pairs) {
		t.Fatalf("byte-exact pending identities=%v, want exactly second member of each pair", pendingIDs)
	}

	for _, pair := range pairs {
		appended, err := p.AppendOutcomeIfAbsent(ctx, outcomeRecord(namespace, pair[1]))
		if err != nil || !appended {
			t.Fatalf("append second exact outcome %q: appended=%t err=%v", pair[1], appended, err)
		}
		for _, requestID := range pair {
			again, err := p.AppendOutcomeIfAbsent(ctx, outcomeRecord(namespace, requestID))
			if err != nil || again {
				t.Fatalf("repeat exact outcome %q: appended=%t err=%v, want false/nil", requestID, again, err)
			}
			var exactRows int64
			if err := p.DB().WithContext(ctx).Table("xflow_audit_events").
				Where("namespace = ? AND request_id = ?", namespace, requestID).
				Count(&exactRows).Error; err != nil {
				t.Fatalf("count exact identity %q: %v", requestID, err)
			}
			if exactRows != 2 {
				t.Fatalf("exact identity %q matched %d rows, want admission+outcome only", requestID, exactRows)
			}
		}
	}

	var outcomes, distinctKeys int64
	base := p.DB().WithContext(ctx).Table("xflow_audit_events").
		Where("namespace = ? AND phase = ?", namespace, store.AuditPhaseOutcome)
	if err := base.Count(&outcomes).Error; err != nil {
		t.Fatalf("count byte-exact outcomes: %v", err)
	}
	if err := base.Distinct("phase_key").Count(&distinctKeys).Error; err != nil {
		t.Fatalf("count byte-exact phase keys: %v", err)
	}
	if outcomes != 6 || distinctKeys != 6 {
		t.Fatalf("byte-exact outcomes/distinct keys = %d/%d, want 6/6", outcomes, distinctKeys)
	}
}

// TestSQLStoreAuditReconcilePendingScanAndIdempotentOutcome proves the pending
// scan and settle contract against real MySQL without relying on shared-table
// LIMIT behavior.
func TestSQLStoreAuditReconcilePendingScanAndIdempotentOutcome(t *testing.T) {
	p := newAuditTestProvider(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	namespace := auditTestNamespace(t, p, "reconcile")

	pending := admissionRecord(namespace, namespace+"-pending")
	if err := p.AppendAudit(ctx, pending); err != nil {
		t.Fatalf("AppendAudit pending: %v", err)
	}
	settled := admissionRecord(namespace, namespace+"-settled")
	if err := p.AppendAudit(ctx, settled); err != nil {
		t.Fatalf("AppendAudit settled admission: %v", err)
	}
	if err := p.AppendAudit(ctx, outcomeRecord(namespace, settled.RequestID)); err != nil {
		t.Fatalf("AppendAudit settled outcome: %v", err)
	}

	cutoff := time.Now().UTC().Add(time.Second)
	candidates := listAuditAdmissionsThrough(t, ctx, p, cutoff, pending.ID-1, settled.ID, 1, namespace)
	if len(candidates) != 1 || candidates[0].RequestID != pending.RequestID {
		t.Fatalf("pending scan = %v, want only %q", auditRequestIDs(candidates), pending.RequestID)
	}

	appended, err := p.AppendOutcomeIfAbsent(ctx, outcomeRecord(namespace, pending.RequestID))
	if err != nil || !appended {
		t.Fatalf("AppendOutcomeIfAbsent first: appended=%t err=%v", appended, err)
	}
	appended, err = p.AppendOutcomeIfAbsent(ctx, outcomeRecord(namespace, pending.RequestID))
	if err != nil || appended {
		t.Fatalf("AppendOutcomeIfAbsent second: appended=%t err=%v, want false/nil", appended, err)
	}
	if got := countPendingForNamespace(t, ctx, p.DB(), namespace, cutoff); got != 0 {
		t.Fatalf("pending rows after settle = %d, want 0", got)
	}
}

// TestSQLStoreAuditCursorPagination proves cursor pagination and validates the
// global count as an exact delta, so historical shared backlog cannot make the
// assertion pass accidentally.
func TestSQLStoreAuditCursorPagination(t *testing.T) {
	p := newAuditTestProvider(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	namespace := auditTestNamespace(t, p, "cursor")
	cutoff := time.Now().UTC().Add(time.Second)

	baseline, baselineOldest, err := p.CountUnreconciledAdmissions(ctx, cutoff)
	if err != nil {
		t.Fatalf("CountUnreconciledAdmissions baseline: %v", err)
	}

	var seqIDs []uint64
	for i := 1; i <= 3; i++ {
		rec := admissionRecord(namespace, fmt.Sprintf("%s-request-%d", namespace, i))
		if err := p.AppendAudit(ctx, rec); err != nil {
			t.Fatalf("AppendAudit row %d: %v", i, err)
		}
		seqIDs = append(seqIDs, rec.ID)
	}

	all := listAuditAdmissionsThrough(t, ctx, p, cutoff, seqIDs[0]-1, seqIDs[2], 1, namespace)
	if len(all) != 3 {
		t.Fatalf("cursor first pass returned %d target rows, want 3", len(all))
	}
	for i, rec := range all {
		if rec.SeqID != seqIDs[i] {
			t.Fatalf("cursor row %d SeqID=%d, want %d", i, rec.SeqID, seqIDs[i])
		}
	}

	page2 := listAuditAdmissionsThrough(t, ctx, p, cutoff, seqIDs[0], seqIDs[2], 1, namespace)
	if len(page2) != 2 || page2[0].SeqID != seqIDs[1] || page2[1].SeqID != seqIDs[2] {
		t.Fatalf("cursor after first returned SeqIDs %v, want %v", auditSeqIDs(page2), seqIDs[1:])
	}
	page3, err := p.ListUnreconciledAdmissions(ctx, cutoff, seqIDs[2], 1)
	if err != nil {
		t.Fatalf("ListUnreconciledAdmissions after last: %v", err)
	}
	for _, rec := range page3 {
		if rec.Namespace == namespace {
			t.Fatalf("target row %d returned after final cursor %d", rec.SeqID, seqIDs[2])
		}
	}

	if got := countPendingForNamespace(t, ctx, p.DB(), namespace, cutoff); got != 3 {
		t.Fatalf("target namespace pending rows = %d, want 3", got)
	}
	pending, oldest, err := p.CountUnreconciledAdmissions(ctx, cutoff)
	if err != nil {
		t.Fatalf("CountUnreconciledAdmissions after inserts: %v", err)
	}
	if pending != baseline+3 {
		t.Fatalf("pending count delta = %d (baseline=%d after=%d), want exactly 3", pending-baseline, baseline, pending)
	}
	if oldest.IsZero() {
		t.Fatal("oldest pending timestamp is zero")
	}
	if baseline > 0 && !oldest.Equal(baselineOldest) {
		t.Fatalf("oldest changed with newer target rows: before=%s after=%s", baselineOldest, oldest)
	}
}

func newAuditTestProvider(t *testing.T) *sqlstore.Provider {
	t.Helper()
	p, err := mysqlstore.New(
		requireMySQL(t),
		mysqlstore.WithGormConfig(&gorm.Config{TranslateError: false}),
	)
	if err != nil {
		t.Fatalf("mysqlstore.New: %v", err)
	}
	sqlDB, err := p.DB().DB()
	if err != nil {
		t.Fatalf("get audit test sql.DB: %v", err)
	}
	t.Cleanup(func() {
		if err := sqlDB.Close(); err != nil {
			t.Errorf("close audit test database: %v", err)
		}
	})
	return p
}

func auditTestNamespace(t *testing.T, p *sqlstore.Provider, purpose string) string {
	t.Helper()
	namespace := fmt.Sprintf("audit-%s-%d", purpose, time.Now().UnixNano())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := p.DB().WithContext(ctx).Exec(
			"DELETE FROM xflow_audit_events WHERE namespace = ?", namespace,
		).Error; err != nil {
			t.Errorf("clean up audit namespace %q: %v", namespace, err)
		}
	})
	return namespace
}

func admissionRecord(namespace, requestID string) *store.AuditRecord {
	return &store.AuditRecord{
		RequestID: requestID,
		Principal: "audit-reconcile-test",
		Namespace: namespace,
		Operation: "workflow.create",
		Decision:  "allow",
		Outcome:   store.AuditOutcomeAdmitted,
		Phase:     store.AuditPhaseAdmission,
		Timestamp: time.Now().UTC(),
	}
}

func outcomeRecord(namespace, requestID string) *store.AuditRecord {
	return &store.AuditRecord{
		RequestID: requestID,
		Principal: "audit-reconcile-test",
		Namespace: namespace,
		Operation: "workflow.create",
		Decision:  "allow",
		Outcome:   store.AuditOutcomeReconciled,
		Phase:     store.AuditPhaseOutcome,
		Timestamp: time.Now().UTC(),
	}
}

func insertHistoricalNullOutcome(t *testing.T, ctx context.Context, db *gorm.DB, namespace, requestID string) {
	t.Helper()
	if err := db.WithContext(ctx).Exec(`
INSERT INTO xflow_audit_events
    (request_id, principal, namespace, operation, decision, outcome, ts, phase, phase_key)
VALUES (?, 'audit-history-test', ?, 'workflow.create', 'allow', ?, ?, ?, NULL)`,
		requestID, namespace, store.AuditOutcomeReconciled, time.Now().UTC(), store.AuditPhaseOutcome,
	).Error; err != nil {
		t.Fatalf("insert historical NULL outcome %q: %v", requestID, err)
	}
}

func assertAuditOutcomeCount(t *testing.T, ctx context.Context, db *gorm.DB, namespace, requestID string, want int64) {
	t.Helper()
	var got int64
	if err := db.WithContext(ctx).Table("xflow_audit_events").
		Where("namespace = ? AND request_id = ? AND phase = ?", namespace, requestID, store.AuditPhaseOutcome).
		Count(&got).Error; err != nil {
		t.Fatalf("count outcomes for %q/%q: %v", namespace, requestID, err)
	}
	if got != want {
		t.Fatalf("outcomes for %q/%q = %d, want %d", namespace, requestID, got, want)
	}
}

// listAuditAdmissionsThrough paginates the production cursor until it reaches
// the target test's largest ID, then filters in Go. It remains deterministic
// even when unrelated shared-backlog rows fill one or more pages.
func listAuditAdmissionsThrough(
	t *testing.T,
	ctx context.Context,
	p *sqlstore.Provider,
	cutoff time.Time,
	afterSeqID, throughSeqID uint64,
	limit int,
	namespace string,
) []*store.AuditRecord {
	t.Helper()
	cursor := afterSeqID
	var target []*store.AuditRecord
	for cursor < throughSeqID {
		page, err := p.ListUnreconciledAdmissions(ctx, cutoff, cursor, limit)
		if err != nil {
			t.Fatalf("ListUnreconciledAdmissions(afterSeqID=%d): %v", cursor, err)
		}
		if len(page) == 0 {
			break
		}
		for _, rec := range page {
			if rec.Namespace == namespace && rec.SeqID <= throughSeqID {
				target = append(target, rec)
			}
		}
		next := page[len(page)-1].SeqID
		if next <= cursor {
			t.Fatalf("audit cursor did not advance: current=%d next=%d", cursor, next)
		}
		cursor = next
	}
	return target
}

func countPendingForNamespace(t *testing.T, ctx context.Context, db *gorm.DB, namespace string, cutoff time.Time) int64 {
	t.Helper()
	var count int64
	if err := db.WithContext(ctx).Raw(`
SELECT COUNT(*)
FROM xflow_audit_events AS a
WHERE a.namespace = ?
  AND a.phase = ?
  AND a.outcome = ?
  AND a.created_at < ?
  AND NOT EXISTS (
      SELECT 1
      FROM xflow_audit_events AS b
      WHERE b.namespace = a.namespace
        AND b.request_id = a.request_id
        AND b.phase = ?
  )`, namespace, store.AuditPhaseAdmission, store.AuditOutcomeAdmitted, cutoff, store.AuditPhaseOutcome).Scan(&count).Error; err != nil {
		t.Fatalf("count pending admissions for namespace %q: %v", namespace, err)
	}
	return count
}

func auditRequestIDs(records []*store.AuditRecord) []string {
	ids := make([]string, len(records))
	for i, rec := range records {
		ids[i] = rec.RequestID
	}
	return ids
}

func auditSeqIDs(records []*store.AuditRecord) []uint64 {
	ids := make([]uint64, len(records))
	for i, rec := range records {
		ids[i] = rec.SeqID
	}
	return ids
}
