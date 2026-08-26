//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/sqlstore"
	"github.com/xbcio/xflow/store/sqlstore/mysqlstore"
	"github.com/xbcio/xflow/types"
)

// emptyJSON is minimal valid JSON for the WorkflowDef/Params/Runtime/Output/
// SignalConfig/Payload columns.
//
// It reads like a constraint being satisfied, and in production it is:
// db/xflow_schema.sql declares workflow_def and its siblings NOT NULL. But the
// database these tests actually open is not built from that file. requireMySQL
// points at the `xflow` schema, which sqlstore.AutoMigrate created, and
// AutoMigrate emits every one of these columns as nullable. Omitting emptyJSON
// here would NOT fail — the row would insert with NULL and the test would go
// green against a shape production rejects.
//
// So this is portability hygiene, not an enforced invariant, and no test may
// treat "it inserted" as evidence that the value was non-null. Assert the value
// you read back instead.
var emptyJSON = []byte(`{}`)

// newSQLStoreProvider opens a real MySQL connection (skips when MySQL is
// unavailable) and returns a ready *sqlstore.Provider implementing both
// store.Store and store.Transactor. Mirrors the setup in
// sqlstore_real_test.go: connection lifetime is owned by the test process.
func newSQLStoreProvider(t *testing.T) *sqlstore.Provider {
	t.Helper()
	dsn := requireMySQL(t)
	p, err := mysqlstore.New(dsn)
	if err != nil {
		t.Fatalf("mysqlstore.New(%q): %v", dsn, err)
	}
	return p
}

// newExecutionID builds a globally unique execution id short enough to fit
// the varchar(64) column: "sql-<suffix>-<unix-nano>". The suffix disambiguates
// within a test, the nanosecond timestamp across tests/runs.
func newExecutionID(t *testing.T, suffix string) types.ExecutionID {
	t.Helper()
	return types.ExecutionID(fmt.Sprintf("sql-%s-%d", suffix, time.Now().UnixNano()))
}

// newBaseExecution inserts a Running execution record and returns it. Many
// tests need a parent execution before they can UpsertNode / SaveSignal, since
// those reference the execution id.
func newBaseExecution(ctx context.Context, t *testing.T, p *sqlstore.Provider, execID types.ExecutionID) *store.ExecutionRecord {
	t.Helper()
	rec := &store.ExecutionRecord{
		ExecutionID:  execID,
		WorkflowName: "wf-sqlstore-test",
		WorkflowDef:  emptyJSON,
		Params:       emptyJSON,
		Runtime:      emptyJSON,
		Status:       types.ExecutionStatusRunning,
	}
	if err := p.CreateExecution(ctx, rec); err != nil {
		t.Fatalf("CreateExecution base %q: %v", execID, err)
	}
	return rec
}

// mustSaveSignal saves a signal and fatals on error.
func mustSaveSignal(ctx context.Context, t *testing.T, p *sqlstore.Provider, execID types.ExecutionID, name string, payload []byte) {
	t.Helper()
	rec := &store.SignalRecord{
		ExecutionID: execID,
		SignalName:  name,
		Payload:     payload,
	}
	if err := p.SaveSignal(ctx, rec); err != nil {
		t.Fatalf("SaveSignal %q/%q: %v", execID, name, err)
	}
}

// mustUpsertNode upserts a node and fatals on error.
func mustUpsertNode(ctx context.Context, t *testing.T, p *sqlstore.Provider, rec *store.NodeRecord) {
	t.Helper()
	if err := p.UpsertNode(ctx, rec); err != nil {
		t.Fatalf("UpsertNode %q/%q: %v", rec.ExecutionID, rec.NodeName, err)
	}
}

// jsonEqual compares two JSON byte slices by value (parses then DeepEqual),
// ignoring MySQL's whitespace normalization for JSON columns (e.g.
// `{"a":1}` round-trips as `{"a": 1}`).
func jsonEqual(a, b []byte) bool {
	var av, bv any
	if err := json.Unmarshal(a, &av); err != nil {
		return false
	}
	if err := json.Unmarshal(b, &bv); err != nil {
		return false
	}
	return reflect.DeepEqual(av, bv)
}
