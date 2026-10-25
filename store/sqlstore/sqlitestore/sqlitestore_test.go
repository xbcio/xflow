package sqlitestore

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"github.com/ncruces/go-sqlite3"
	sqlitedriver "github.com/ncruces/go-sqlite3/driver"
	"github.com/ncruces/go-sqlite3/gormlite"
	"gorm.io/gorm"

	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/sqlstore"
	"github.com/xbcio/xflow/store/storecontract"
	"github.com/xbcio/xflow/store/storetest"
	"github.com/xbcio/xflow/types"
)

// newTestProvider opens a fresh, migrated SQLite file under t.TempDir().
func newTestProvider(t *testing.T) *sqlstore.Provider {
	t.Helper()
	p, err := New(filepath.Join(t.TempDir(), "xflow.db"), WithAutoMigrate())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		if sqlDB, err := p.DB().DB(); err == nil {
			_ = sqlDB.Close()
		}
	})
	return p
}

func TestNewCreatesSchemaAndAppliesPragmas(t *testing.T) {
	p := newTestProvider(t)
	db := p.DB()

	var journalMode string
	if err := db.Raw("PRAGMA journal_mode").Scan(&journalMode).Error; err != nil {
		t.Fatalf("PRAGMA journal_mode: %v", err)
	}
	if journalMode != "wal" {
		t.Errorf("journal_mode = %q, want wal", journalMode)
	}

	var busyTimeout int
	if err := db.Raw("PRAGMA busy_timeout").Scan(&busyTimeout).Error; err != nil {
		t.Fatalf("PRAGMA busy_timeout: %v", err)
	}
	if busyTimeout != int(defaultBusyTimeout.Milliseconds()) {
		t.Errorf("busy_timeout = %d, want %d", busyTimeout, defaultBusyTimeout.Milliseconds())
	}

	var foreignKeys int
	if err := db.Raw("PRAGMA foreign_keys").Scan(&foreignKeys).Error; err != nil {
		t.Fatalf("PRAGMA foreign_keys: %v", err)
	}
	if foreignKeys != 1 {
		t.Errorf("foreign_keys = %d, want 1", foreignKeys)
	}

	var tableCount int
	if err := db.Raw("SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'xflow_executions'").Scan(&tableCount).Error; err != nil {
		t.Fatalf("count xflow_executions: %v", err)
	}
	if tableCount != 1 {
		t.Errorf("xflow_executions table count = %d, want 1 (WithAutoMigrate must create the shared schema)", tableCount)
	}
}

func TestNewInMemory(t *testing.T) {
	p, err := New(":memory:", WithAutoMigrate())
	if err != nil {
		t.Fatalf("New(:memory:): %v", err)
	}
	t.Cleanup(func() {
		if sqlDB, err := p.DB().DB(); err == nil {
			_ = sqlDB.Close()
		}
	})

	ctx := context.Background()
	rec := &store.ExecutionRecord{
		ExecutionID: "mem-1", Namespace: "default",
		Status: types.ExecutionStatusSuccess, CreatedAt: time.Now().UTC(),
	}
	if err := p.CreateExecution(ctx, rec); err != nil {
		t.Fatalf("CreateExecution: %v", err)
	}
	got, err := p.GetExecution(ctx, "mem-1")
	if err != nil {
		t.Fatalf("GetExecution: %v", err)
	}
	if got.ExecutionID != "mem-1" {
		t.Fatalf("round trip = %q, want mem-1", got.ExecutionID)
	}
}

// TestNewInMemorySharedAcrossPool pins that ":memory:" is one database per New
// call no matter how many pooled connections serve it, and that two New calls
// never share data.
func TestNewInMemorySharedAcrossPool(t *testing.T) {
	ctx := context.Background()
	p, err := New(":memory:", WithAutoMigrate(), WithMaxOpenConns(4), WithMaxIdleConns(4))
	if err != nil {
		t.Fatalf("New(:memory:): %v", err)
	}
	sqlDB, err := p.DB().DB()
	if err != nil {
		t.Fatalf("resolve *sql.DB: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })

	// Hold several connections at once so the pool must open distinct ones.
	conns := make([]*sql.Conn, 4)
	for i := range conns {
		c, err := sqlDB.Conn(ctx)
		if err != nil {
			t.Fatalf("conn %d: %v", i, err)
		}
		conns[i] = c
	}
	for i, c := range conns {
		var n int
		if err := c.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'xflow_executions'").Scan(&n); err != nil {
			t.Fatalf("conn %d query: %v", i, err)
		}
		if n != 1 {
			t.Fatalf("conn %d sees %d xflow_executions tables, want 1", i, n)
		}
		_ = c.Close()
	}

	other, err := New(":memory:")
	if err != nil {
		t.Fatalf("second New(:memory:): %v", err)
	}
	otherDB, err := other.DB().DB()
	if err != nil {
		t.Fatalf("resolve second *sql.DB: %v", err)
	}
	t.Cleanup(func() { _ = otherDB.Close() })
	var n int
	if err := otherDB.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_master WHERE type = 'table'").Scan(&n); err != nil {
		t.Fatalf("second store query: %v", err)
	}
	if n != 0 {
		t.Fatalf("second :memory: store sees %d tables from the first, want 0", n)
	}
}

func TestReopenKeepsData(t *testing.T) {
	path := filepath.Join(t.TempDir(), "xflow.db")
	p, err := New(path, WithAutoMigrate())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()
	rec := &store.ExecutionRecord{
		ExecutionID: "reopen-1", Namespace: "default",
		Status: types.ExecutionStatusSuccess, CreatedAt: time.Now().UTC(),
	}
	if err := p.CreateExecution(ctx, rec); err != nil {
		t.Fatalf("CreateExecution: %v", err)
	}
	sqlDB, err := p.DB().DB()
	if err != nil {
		t.Fatalf("resolve *sql.DB: %v", err)
	}
	if err := sqlDB.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := New(path, WithAutoMigrate())
	if err != nil {
		t.Fatalf("reopen New: %v", err)
	}
	t.Cleanup(func() {
		if db, err := reopened.DB().DB(); err == nil {
			_ = db.Close()
		}
	})
	got, err := reopened.GetExecution(ctx, "reopen-1")
	if err != nil {
		t.Fatalf("GetExecution after reopen: %v", err)
	}
	if got.ExecutionID != "reopen-1" {
		t.Fatalf("round trip after reopen = %q, want reopen-1", got.ExecutionID)
	}
}

func TestIsSQLiteBusy(t *testing.T) {
	if !sqlstore.HasTransientClassifierFor("sqlite") {
		t.Fatal("no sqlite transient classifier registered")
	}

	// A plain error that merely mentions the condition must not be classified:
	// the predicate is type- and code-based, never substring-based.
	if isSQLiteBusy(errors.New("database is locked: SQLITE_BUSY")) {
		t.Fatal("plain errors.New was classified as a SQLite BUSY error")
	}
	if isSQLiteBusy(nil) {
		t.Fatal("nil was classified as a SQLite BUSY error")
	}

	path := filepath.Join(t.TempDir(), "busy.db")
	db, err := gorm.Open(gormliteOpenForTest(t, path), &gorm.Config{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("resolve *sql.DB: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	if err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)").Error; err != nil {
		t.Fatalf("create table: %v", err)
	}

	ctx := context.Background()
	holder, err := sqlDB.Conn(ctx)
	if err != nil {
		t.Fatalf("holder conn: %v", err)
	}
	t.Cleanup(func() { _ = holder.Close() })
	if _, err := holder.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		t.Fatalf("begin immediate: %v", err)
	}
	// The holder never commits; the competing write must fail immediately
	// because this scratch database sets busy_timeout(0).
	err = db.Exec("INSERT INTO t (v) VALUES ('x')").Error
	if err == nil {
		t.Fatal("competing write succeeded while another connection held a write lock")
	}
	if !isSQLiteBusy(err) {
		t.Fatalf("isSQLiteBusy(%v) = false, want true (type %T)", err, err)
	}
	if !errors.Is(err, sqlite3.BUSY) {
		t.Fatalf("error %v (%T) is not SQLITE_BUSY", err, err)
	}
	if _, err := holder.ExecContext(ctx, "ROLLBACK"); err != nil {
		t.Fatalf("rollback: %v", err)
	}
}

// gormliteOpenForTest opens a scratch SQLite database whose busy timeout is
// zero, so a lock conflict surfaces immediately instead of waiting 5s.
func gormliteOpenForTest(t *testing.T, path string) gorm.Dialector {
	t.Helper()
	sqlDB, err := sqlitedriver.Open("file:" + url.PathEscape(path) + "?_pragma=busy_timeout(0)")
	if err != nil {
		t.Fatalf("driver.Open: %v", err)
	}
	return gormlite.OpenDB(sqlDB)
}

func TestExecutionsContract(t *testing.T) {
	storetest.ExecutionsContract(t, newTestProvider(t), "sqliteexec")
}

func TestLatestExecutionsContract(t *testing.T) {
	storetest.LatestExecutionsContract(t, newTestProvider(t), "sqlitelatest")
}

// TestExecutionStatsContract runs the shared statistics contract against the
// same executionRepo statements a MySQL deployment runs, executed by SQLite —
// including the dialect bucket expression and the windowed percentile sample.
func TestExecutionStatsContract(t *testing.T) {
	storetest.ExecutionStatsContract(t, newTestProvider(t), "sqlitestats")
}

func TestNodesContract(t *testing.T) {
	storetest.NodesContract(t, newTestProvider(t))
}

func TestNodesTimingContract(t *testing.T) {
	storetest.NodesTimingContract(t, newTestProvider(t), types.ExecutionID("sqlite-node-timing"))
}

func TestSignalsContract(t *testing.T) {
	storetest.SignalsContract(t, newTestProvider(t))
}

func TestAuditContract(t *testing.T) {
	storetest.AuditContract(t, newTestProvider(t), "audit-contract-sqlite")
}

func TestSupplyContract(t *testing.T) {
	storetest.SupplyContract(t, newTestProvider(t), "sqlite")
}

func TestSupplyNamespaceNormContract(t *testing.T) {
	storetest.SupplyNamespaceNormContract(t, newTestProvider(t))
}

func TestScheduleOverrideContract(t *testing.T) {
	storetest.ScheduleOverrideContract(t, newTestProvider(t), "sqlitesched")
}

func TestScopedAuditReconcilerContract(t *testing.T) {
	storecontract.RunScopedAuditReconcilerContract(t, func(t *testing.T) storecontract.ScopedAuditStore {
		return newTestProvider(t)
	})
}

func TestRegistrationCodeStoreContract(t *testing.T) {
	storecontract.RunRegistrationCodeStoreContract(t, func(t *testing.T) store.RegistrationCodeStore {
		return sqlstore.NewRegistrationCodeStore(newTestProvider(t).DB())
	})
}

func TestIssuedIdentityStoreContract(t *testing.T) {
	storecontract.RunIssuedIdentityStoreContract(t, func(t *testing.T) store.IssuedIdentityStore {
		return sqlstore.NewIssuedIdentityStore(newTestProvider(t).DB())
	})
}

func TestRunnerPoolStoreContract(t *testing.T) {
	storecontract.RunRunnerPoolStoreContract(t, func(t *testing.T) store.RunnerPoolStore {
		return sqlstore.NewRunnerPoolStore(newTestProvider(t).DB())
	})
}

func TestScopedRunnerInstanceListerContract(t *testing.T) {
	storecontract.RunScopedRunnerInstanceListerContract(t, func(t *testing.T) storecontract.ScopedRunnerPoolStore {
		st := sqlstore.NewRunnerPoolStore(newTestProvider(t).DB())
		scoped, ok := st.(storecontract.ScopedRunnerPoolStore)
		if !ok {
			t.Fatalf("runner pool store %T does not implement ScopedRunnerInstanceLister", st)
		}
		return scoped
	})
}

// TestNewInMemorySurvivesPoolChurn pins that a ":memory:" store keeps its data
// when every pooled connection is closed and replaced.
func TestNewInMemorySurvivesPoolChurn(t *testing.T) {
	ctx := context.Background()
	p, err := New(":memory:", WithAutoMigrate())
	if err != nil {
		t.Fatalf("New(:memory:): %v", err)
	}
	sqlDB, err := p.DB().DB()
	if err != nil {
		t.Fatalf("resolve *sql.DB: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	rec := &store.ExecutionRecord{
		ExecutionID: "churn-1", Namespace: "default",
		Status: types.ExecutionStatusSuccess, CreatedAt: time.Now().UTC(),
	}
	if err := p.CreateExecution(ctx, rec); err != nil {
		t.Fatalf("CreateExecution: %v", err)
	}
	// Close the pool's only connection so the next query opens a new one: the
	// moment memdb would discard a database with no open reference.
	sqlDB.SetMaxIdleConns(0)
	sqlDB.SetMaxIdleConns(1)
	if _, err := p.GetExecution(ctx, "churn-1"); err != nil {
		t.Fatalf("GetExecution after pool churn: %v", err)
	}
	if got := sqlDB.Stats().MaxIdleClosed; got == 0 {
		t.Fatal("no pooled connection was closed; the test did not exercise churn")
	}
}

// TestNewDoesNotShareCallerGormConfigState pins that New opens on its own
// gorm.Config: a Config that was already opened elsewhere keeps its cache and
// maps untouched.
func TestNewDoesNotShareCallerGormConfigState(t *testing.T) {
	cfg := &gorm.Config{}
	first, err := New(":memory:", WithGormConfig(cfg), WithAutoMigrate())
	if err != nil {
		t.Fatalf("first New: %v", err)
	}
	second, err := New(":memory:", WithGormConfig(cfg), WithAutoMigrate())
	if err != nil {
		t.Fatalf("second New: %v", err)
	}
	for _, p := range []interface{ DB() *gorm.DB }{first, second} {
		if p.DB().Config == cfg {
			t.Fatal("New opened on the caller's *gorm.Config instead of a fresh one")
		}
		if db, err := p.DB().DB(); err == nil {
			_ = db.Close()
		}
	}
	if cfg.Dialector != nil || cfg.ClauseBuilders != nil || cfg.Plugins != nil {
		t.Fatal("caller's gorm.Config was mutated by New")
	}
}
