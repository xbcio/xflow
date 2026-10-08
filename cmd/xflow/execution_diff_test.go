package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/xbcio/xflow/backend/providers/distributed"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// fakeExecutionStateReader is a hand-rolled executionStateReader for unit
// testing diffExecution's comparison logic without any real Redis or MySQL
// connection — this package's test discipline forbids a test depending on the
// shared local MySQL (not guaranteed running, and other sessions may be using
// it concurrently).
type fakeExecutionStateReader struct {
	view *executionView
	err  error
}

func (f fakeExecutionStateReader) readExecution(context.Context, namespace.Namespace, types.ExecutionID) (*executionView, error) {
	return f.view, f.err
}

func TestDiffExecutionNoDifference(t *testing.T) {
	redis := fakeExecutionStateReader{view: &executionView{Status: types.ExecutionStatusSuccess}}
	sql := fakeExecutionStateReader{view: &executionView{Status: types.ExecutionStatusSuccess}}

	result, err := diffExecution(context.Background(), redis, sql, namespace.Default, "exec-1")
	if err != nil {
		t.Fatalf("diffExecution: %v", err)
	}
	if result.Diverged() {
		t.Fatalf("Diverged() = true, want false; differences=%v", result.Differences)
	}
	if !result.RedisFound || !result.SQLFound {
		t.Fatalf("RedisFound=%v SQLFound=%v, want both true", result.RedisFound, result.SQLFound)
	}
}

func TestDiffExecutionStatusMismatch(t *testing.T) {
	redis := fakeExecutionStateReader{view: &executionView{Status: types.ExecutionStatusRunning}}
	sql := fakeExecutionStateReader{view: &executionView{Status: types.ExecutionStatusSuccess}}

	result, err := diffExecution(context.Background(), redis, sql, namespace.Default, "exec-1")
	if err != nil {
		t.Fatalf("diffExecution: %v", err)
	}
	if !result.Diverged() {
		t.Fatal("Diverged() = false, want true for a status mismatch")
	}
	if len(result.Differences) != 1 || !strings.Contains(result.Differences[0], "status mismatch") {
		t.Fatalf("Differences = %v, want exactly one status-mismatch entry", result.Differences)
	}
}

func TestDiffExecutionErrorMismatch(t *testing.T) {
	redis := fakeExecutionStateReader{view: &executionView{Status: types.ExecutionStatusFailed, Error: "boom"}}
	sql := fakeExecutionStateReader{view: &executionView{Status: types.ExecutionStatusFailed, Error: ""}}

	result, err := diffExecution(context.Background(), redis, sql, namespace.Default, "exec-1")
	if err != nil {
		t.Fatalf("diffExecution: %v", err)
	}
	if !result.Diverged() {
		t.Fatal("Diverged() = false, want true for an error-message mismatch")
	}
	found := false
	for _, d := range result.Differences {
		if strings.Contains(d, "error mismatch") {
			found = true
		}
	}
	if !found {
		t.Fatalf("Differences = %v, want an error-mismatch entry", result.Differences)
	}
}

func TestDiffExecutionMissingInSQL(t *testing.T) {
	redis := fakeExecutionStateReader{view: &executionView{Status: types.ExecutionStatusRunning}}
	sql := fakeExecutionStateReader{view: nil}

	result, err := diffExecution(context.Background(), redis, sql, namespace.Default, "exec-1")
	if err != nil {
		t.Fatalf("diffExecution: %v", err)
	}
	if !result.RedisFound || result.SQLFound {
		t.Fatalf("RedisFound=%v SQLFound=%v, want true/false", result.RedisFound, result.SQLFound)
	}
	if !result.Diverged() {
		t.Fatal("Diverged() = false, want true when the SQL projection is missing the row")
	}
}

func TestDiffExecutionMissingInRedis(t *testing.T) {
	redis := fakeExecutionStateReader{view: nil}
	sql := fakeExecutionStateReader{view: &executionView{Status: types.ExecutionStatusRunning}}

	result, err := diffExecution(context.Background(), redis, sql, namespace.Default, "exec-1")
	if err != nil {
		t.Fatalf("diffExecution: %v", err)
	}
	if result.RedisFound || !result.SQLFound {
		t.Fatalf("RedisFound=%v SQLFound=%v, want false/true", result.RedisFound, result.SQLFound)
	}
	if !result.Diverged() {
		t.Fatal("Diverged() = false, want true when the Redis side is missing the row")
	}
}

func TestDiffExecutionMissingOnBothSides(t *testing.T) {
	redis := fakeExecutionStateReader{view: nil}
	sql := fakeExecutionStateReader{view: nil}

	result, err := diffExecution(context.Background(), redis, sql, namespace.Default, "exec-1")
	if err != nil {
		t.Fatalf("diffExecution: %v", err)
	}
	if result.RedisFound || result.SQLFound {
		t.Fatalf("RedisFound=%v SQLFound=%v, want both false", result.RedisFound, result.SQLFound)
	}
	if !result.Diverged() {
		t.Fatal("Diverged() = false, want true when neither side has the execution")
	}
}

func TestDiffExecutionRedisReadErrorPropagates(t *testing.T) {
	wantErr := errors.New("redis down")
	redis := fakeExecutionStateReader{err: wantErr}
	sql := fakeExecutionStateReader{view: &executionView{Status: types.ExecutionStatusRunning}}

	_, err := diffExecution(context.Background(), redis, sql, namespace.Default, "exec-1")
	if !errors.Is(err, wantErr) {
		t.Fatalf("diffExecution error = %v, want it to wrap %v", err, wantErr)
	}
}

func TestDiffExecutionSQLReadErrorPropagates(t *testing.T) {
	wantErr := errors.New("sql down")
	redis := fakeExecutionStateReader{view: &executionView{Status: types.ExecutionStatusRunning}}
	sql := fakeExecutionStateReader{err: wantErr}

	_, err := diffExecution(context.Background(), redis, sql, namespace.Default, "exec-1")
	if !errors.Is(err, wantErr) {
		t.Fatalf("diffExecution error = %v, want it to wrap %v", err, wantErr)
	}
}

// fakeSQLExecutionGetter is a hand-rolled sqlExecutionGetter so
// TestSQLExecutionReader* never touches MySQL.
type fakeSQLExecutionGetter struct {
	rec *store.ExecutionRecord
	err error
}

func (f fakeSQLExecutionGetter) GetExecution(context.Context, types.ExecutionID) (*store.ExecutionRecord, error) {
	return f.rec, f.err
}

func TestSQLExecutionReaderMapsNotFoundToNilView(t *testing.T) {
	reader := sqlExecutionReader{getter: fakeSQLExecutionGetter{err: store.ErrNotFound}}
	view, err := reader.readExecution(context.Background(), namespace.Default, "exec-1")
	if err != nil {
		t.Fatalf("readExecution: %v", err)
	}
	if view != nil {
		t.Fatalf("view = %+v, want nil for store.ErrNotFound", view)
	}
}

func TestSQLExecutionReaderMapsRecord(t *testing.T) {
	reader := sqlExecutionReader{getter: fakeSQLExecutionGetter{rec: &store.ExecutionRecord{
		Status: types.ExecutionStatusFailed,
		Error:  "node x failed",
	}}}
	view, err := reader.readExecution(context.Background(), namespace.Default, "exec-1")
	if err != nil {
		t.Fatalf("readExecution: %v", err)
	}
	if view == nil || view.Status != types.ExecutionStatusFailed || view.Error != "node x failed" {
		t.Fatalf("view = %+v, want status=failed error=\"node x failed\"", view)
	}
}

func TestSQLExecutionReaderPropagatesOtherErrors(t *testing.T) {
	wantErr := errors.New("connection refused")
	reader := sqlExecutionReader{getter: fakeSQLExecutionGetter{err: wantErr}}
	_, err := reader.readExecution(context.Background(), namespace.Default, "exec-1")
	if !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want it to wrap %v", err, wantErr)
	}
}

// TestRedisExecutionReaderAgainstMiniredis seeds a real execution snapshot
// into a miniredis-backed distributed backend (through the same
// distributed.New(..., WithConsumer(false)) construction runExecutionDiff
// uses) and proves redisExecutionReader reads it back correctly, scoped to
// the right namespace. CreateExecution is used here only to seed the fixture
// — the production code path (runExecutionDiff) never calls it.
func TestRedisExecutionReaderAgainstMiniredis(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	b, err := distributed.New(mr.Addr(), nil, distributed.WithConsumer(false))
	if err != nil {
		t.Fatalf("distributed.New: %v", err)
	}
	defer closeRedis(b)

	ns := namespace.Namespace(fmt.Sprintf("diff-test-%d", time.Now().UnixNano()))
	execID := types.ExecutionID("exec-seed-1")

	seedCtx := namespace.WithNamespace(context.Background(), ns)
	if err := b.State().CreateExecution(seedCtx, &engine.ExecutionSnapshot{
		ID:     execID,
		Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("seed CreateExecution: %v", err)
	}

	reader := redisExecutionReader{state: b.State()}
	view, err := reader.readExecution(context.Background(), ns, execID)
	if err != nil {
		t.Fatalf("readExecution: %v", err)
	}
	if view == nil {
		t.Fatal("view = nil, want the seeded snapshot")
	}
	if view.Status != types.ExecutionStatusRunning {
		t.Fatalf("view.Status = %q, want running", view.Status)
	}

	// A different namespace must not see the same execution ID: proves the
	// namespace scoping (namespace.WithNamespace) is actually wired, not a
	// no-op that would make this command report false negatives across
	// tenants.
	otherNS := namespace.Namespace(fmt.Sprintf("diff-test-other-%d", time.Now().UnixNano()))
	otherView, err := reader.readExecution(context.Background(), otherNS, execID)
	if err != nil {
		t.Fatalf("readExecution (other namespace): %v", err)
	}
	if otherView != nil {
		t.Fatalf("otherView = %+v, want nil (namespace isolation must hold)", otherView)
	}
}

// TestExecutionDiffCommandRequiresFlags proves the CLI entry point validates
// its required flags before attempting any network connection, so
// `xflow execution diff` with missing flags fails fast and deterministically
// in this test (no live Redis/MySQL needed).
func TestExecutionDiffCommandRequiresFlags(t *testing.T) {
	cases := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{
			name:    "missing mysql-dsn",
			args:    []string{"execution", "diff", "--redis-addr", "127.0.0.1:1", "--namespace", "ns", "--execution-id", "e1"},
			wantErr: "--mysql-dsn",
		},
		{
			name:    "missing redis-addr",
			args:    []string{"execution", "diff", "--mysql-dsn", "root:x@tcp(127.0.0.1:1)/x", "--namespace", "ns", "--execution-id", "e1"},
			wantErr: "--redis-addr",
		},
		{
			name:    "missing namespace",
			args:    []string{"execution", "diff", "--mysql-dsn", "root:x@tcp(127.0.0.1:1)/x", "--redis-addr", "127.0.0.1:1", "--execution-id", "e1"},
			wantErr: "--namespace",
		},
		{
			name:    "missing execution-id",
			args:    []string{"execution", "diff", "--mysql-dsn", "root:x@tcp(127.0.0.1:1)/x", "--redis-addr", "127.0.0.1:1", "--namespace", "ns"},
			wantErr: "--execution-id",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			err := executeRootWith(&out, tc.args...)
			if err == nil {
				t.Fatalf("executeRootWith(%v) = nil error, want one mentioning %q", tc.args, tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("executeRootWith(%v) error = %v, want it to mention %q", tc.args, err, tc.wantErr)
			}
		})
	}
}

// TestExecutionDiffCommandRejectsInvalidNamespace proves an invalid namespace
// (one that would corrupt the Redis key schema) is rejected before any
// connection attempt, via the same namespace.Validate guard every other
// namespace-scoped Redis key builder relies on.
func TestExecutionDiffCommandRejectsInvalidNamespace(t *testing.T) {
	var out bytes.Buffer
	err := executeRootWith(&out, "execution", "diff",
		"--mysql-dsn", "root:x@tcp(127.0.0.1:1)/x",
		"--redis-addr", "127.0.0.1:1",
		"--namespace", "bad:ns",
		"--execution-id", "e1",
	)
	if err == nil {
		t.Fatal("executeRootWith = nil error, want a namespace validation error")
	}
	if !strings.Contains(err.Error(), "--namespace") {
		t.Fatalf("error = %v, want it to mention --namespace", err)
	}
}

// TestExecutionDiffHelp smoke-tests that `xflow execution diff --help` runs
// without needing any live Redis/MySQL connection, matching the task's manual
// smoke-test instruction.
func TestExecutionDiffHelp(t *testing.T) {
	var out bytes.Buffer
	if err := executeRootWith(&out, "execution", "diff", "--help"); err != nil {
		t.Fatalf("executeRootWith(execution diff --help): %v", err)
	}
	if !strings.Contains(out.String(), "read-only") {
		t.Fatalf("help output = %q, want it to mention the read-only guarantee", out.String())
	}
}

func TestExecutionDiffResultDivergedJSON(t *testing.T) {
	result := executionDiffResult{
		ExecutionID: "e1",
		Namespace:   "ns",
		RedisFound:  true,
		SQLFound:    true,
		Differences: []string{"status mismatch: redis=\"running\" sql=\"success\""},
	}
	if !result.Diverged() {
		t.Fatal("Diverged() = false, want true")
	}
	var out bytes.Buffer
	if err := writeJSONLines(&out, result); err != nil {
		t.Fatalf("writeJSONLines: %v", err)
	}
	if !strings.Contains(out.String(), `"execution_id":"e1"`) {
		t.Fatalf("json output = %q, missing execution_id", out.String())
	}
}
