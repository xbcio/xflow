package sqlstore

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"

	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// These tests assert the SQL shape of the statistics reader: that the
// aggregations happen in the database (GROUP BY / window functions, never a
// full-row read), that every caller value is a bind parameter, and that an
// unknown dialect is refused instead of approximated. The behavioural half of
// the same contract — counts agree with CountExecutions, buckets tile the
// window, percentiles are exact — lives in store/storetest and runs against
// memstore and the SQLite backend; a dry-run statement is what can show the
// difference between `WHERE namespace = ?` and a predicate that merely happens
// to return the right rows for one fixture.

// statsSQLCapture records both the raw statement (still carrying `?`) and the
// dialect-expanded statement (values interpolated), so one test can prove
// parameterization from the first and the dialect expression from the second.
type statsSQLCapture struct {
	db        *gorm.DB
	raw       []string
	explained []string
}

func newStatsSQLCapture(t *testing.T) *statsSQLCapture {
	t.Helper()
	base := newSQLCapture(t)
	c := &statsSQLCapture{db: base.db}
	raw := func(tx *gorm.DB) {
		c.raw = append(c.raw, tx.Statement.SQL.String())
	}
	explained := func(tx *gorm.DB) {
		c.explained = append(c.explained, tx.Explain(tx.Statement.SQL.String(), tx.Statement.Vars...))
	}
	// Scan goes through the Row processor, Find through Query; register on both
	// so the shape assertions do not depend on which finisher the repository
	// happens to use.
	for _, register := range []func(string, func(*gorm.DB)) error{
		c.db.Callback().Query().Register,
		c.db.Callback().Row().Register,
	} {
		if err := register("xflow_test:capture_stats_raw", raw); err != nil {
			t.Fatalf("register raw capture callback: %v", err)
		}
		if err := register("xflow_test:capture_stats_explained", explained); err != nil {
			t.Fatalf("register explained capture callback: %v", err)
		}
	}
	return c
}

// run resets the capture, runs one repository call, and returns the joined raw
// and explained statements. WorkflowExecutionStats issues two statements; both
// are joined so an assertion can search either.
//
// A non-nil call error is tolerated when SQL was captured: these repositories
// end in Scan, and GORM's DryRun mode refuses to scan ("dry run mode
// unsupported") only AFTER the statement is built and the query callbacks have
// run. An error with no statement means validation refused the request before
// reaching the database, which these shape tests treat as a failure.
func (c *statsSQLCapture) run(t *testing.T, name string, call func() error) (raw, explained string) {
	t.Helper()
	c.raw, c.explained = nil, nil
	err := call()
	if len(c.raw) == 0 {
		t.Fatalf("%s produced no SQL (error: %v); the query never reached the query callback", name, err)
	}
	return strings.Join(c.raw, "\n"), strings.Join(c.explained, "\n")
}

func statsTestFilter(base time.Time) store.ExecutionFilter {
	return store.ExecutionFilter{
		CreatedAfter:  base,
		CreatedBefore: base.Add(time.Hour),
		WorkflowID:    types.WorkflowID("wf-1"),
	}
}

func TestExecutionStatsCountsGroupInSQL(t *testing.T) {
	capture := newStatsSQLCapture(t)
	repo := &executionRepo{db: capture.db}
	ctx := context.Background()
	filter := statsTestFilter(time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC))

	raw, explained := capture.run(t, "CountExecutionsByStatus", func() error {
		_, err := repo.CountExecutionsByStatus(ctx, "tenant-a", filter)
		return err
	})

	// One GROUP BY statement, projecting only the status and its count: the
	// repository never reads a row's payload to count it.
	for _, want := range []string{"COUNT(*) AS cnt", "GROUP BY", "status"} {
		if !strings.Contains(explained, want) {
			t.Fatalf("count SQL is missing %q:\n%s", want, explained)
		}
	}
	if strings.Contains(explained, "SELECT *") {
		t.Fatalf("count SQL reads whole rows instead of aggregating:\n%s", explained)
	}
	// The scope and every filter are bind parameters, never interpolated text.
	for _, want := range []string{"namespace = ?", "workflow_id = ?", "created_at > ?", "created_at < ?"} {
		if !strings.Contains(raw, want) {
			t.Fatalf("count SQL is missing the parameterized predicate %q:\n%s", want, raw)
		}
	}
	if strings.Contains(raw, "tenant-a") || strings.Contains(raw, "wf-1") {
		t.Fatalf("count SQL inlines caller values instead of binding them:\n%s", raw)
	}
	if !strings.Contains(explained, "namespace = 'tenant-a'") {
		t.Fatalf("count SQL is not scoped to the requested namespace:\n%s", explained)
	}
}

func TestExecutionStatsHistogramUsesDialectBucketExpression(t *testing.T) {
	capture := newStatsSQLCapture(t)
	repo := &executionRepo{db: capture.db}
	ctx := context.Background()
	filter := statsTestFilter(time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC))

	raw, explained := capture.run(t, "ExecutionHistogram", func() error {
		_, err := repo.ExecutionHistogram(ctx, "tenant-a", filter, 5*time.Minute)
		return err
	})

	// The bucket index is a pure DATETIME subtraction from a bound UTC anchor.
	// UNIX_TIMESTAMP(created_at) would read the stored wall clock in the SESSION
	// time_zone, which the driver's write zone (loc=, UTC by default) does not
	// follow, so it shifts every row by the server's UTC offset.
	if !strings.Contains(raw, "TIMESTAMPDIFF(SECOND, ?, created_at) DIV ?") {
		t.Fatalf("histogram does not bind the anchor and bucket width into the MySQL bucket expression:\n%s", raw)
	}
	for _, sql := range []string{raw, explained} {
		if strings.Contains(sql, "UNIX_TIMESTAMP") {
			t.Fatalf("histogram depends on the session time_zone through UNIX_TIMESTAMP:\n%s", sql)
		}
	}
	if strings.Contains(raw, "strftime") {
		t.Fatalf("histogram used the SQLite bucket expression on a MySQL dialector:\n%s", raw)
	}
	// The anchor is the first bucket's UTC start, the 00:00 instant the window
	// (09-01 00:00 .. 01:00) falls in at 5-minute width, and the width is 300s.
	if !strings.Contains(explained, "TIMESTAMPDIFF(SECOND, '2026-09-01 00:00:00', created_at) DIV 300") {
		t.Fatalf("histogram anchor/width are not the first UTC bucket start and 300 seconds:\n%s", explained)
	}
	if !strings.Contains(explained, "GROUP BY") || !strings.Contains(explained, "bucket") {
		t.Fatalf("histogram does not group by bucket in SQL:\n%s", explained)
	}
	if !strings.Contains(explained, "COUNT(*) AS cnt") {
		t.Fatalf("histogram does not aggregate counts in SQL:\n%s", explained)
	}
	if strings.Contains(raw, "tenant-a") {
		t.Fatalf("histogram inlines the namespace instead of binding it:\n%s", raw)
	}
}

func TestExecutionStatsRecentExecutionsUseOneWindowedStatement(t *testing.T) {
	capture := newStatsSQLCapture(t)
	repo := &executionRepo{db: capture.db}
	ctx := context.Background()

	raw, explained := capture.run(t, "RecentExecutions", func() error {
		_, err := repo.RecentExecutions(ctx, "tenant-a",
			[]types.WorkflowID{"wf-1", "wf-2"}, store.MaxRecentExecutionsPerWorkflow)
		return err
	})

	for _, want := range []string{"ROW_NUMBER() OVER", "PARTITION BY workflow_id", "rn <= 10"} {
		if !strings.Contains(explained, want) {
			t.Fatalf("recent-executions SQL is missing %q:\n%s", want, explained)
		}
	}
	// One statement for the whole page: the per-workflow cap is a window
	// function, not a query per workflow.
	for _, want := range []string{"namespace = ?", "workflow_id IN (", "rn <= ?"} {
		if !strings.Contains(raw, want) {
			t.Fatalf("recent-executions SQL is missing the parameterized %q:\n%s", want, raw)
		}
	}
	if strings.Contains(raw, "tenant-a") || strings.Contains(raw, "wf-1") {
		t.Fatalf("recent-executions SQL inlines caller values:\n%s", raw)
	}
	// The projection stays narrow: no definition, params or error payload.
	for _, forbidden := range []string{"workflow_def", "params", "runtime", "error"} {
		if strings.Contains(explained, forbidden) {
			t.Fatalf("recent-executions SQL reads %q, which the projection must exclude:\n%s", forbidden, explained)
		}
	}
}

func TestExecutionStatsWorkflowCountsGroupInSQL(t *testing.T) {
	capture := newStatsSQLCapture(t)
	repo := &executionRepo{db: capture.db}
	ctx := context.Background()
	since := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)

	// DryRun returns no count rows, so WorkflowExecutionStats stops after its
	// first statement; the windowed sample that follows is exercised for real
	// by the SQLite backend's shared contract. What can be pinned here is that
	// the counts are one grouped statement over a parameterized IN list.
	raw, explained := capture.run(t, "WorkflowExecutionStats", func() error {
		_, err := repo.WorkflowExecutionStats(ctx, "tenant-a",
			[]types.WorkflowID{"wf-1", "wf-2"}, since, 100)
		return err
	})

	for _, want := range []string{"namespace = ?", "workflow_id IN (", "created_at > ?"} {
		if !strings.Contains(raw, want) {
			t.Fatalf("workflow-count SQL is missing the parameterized %q:\n%s", want, raw)
		}
	}
	if !strings.Contains(explained, "GROUP BY") || !strings.Contains(explained, "COUNT(*) AS cnt") {
		t.Fatalf("workflow counts are not aggregated in SQL:\n%s", explained)
	}
	if strings.Contains(raw, "tenant-a") || strings.Contains(raw, "wf-1") {
		t.Fatalf("workflow-count SQL inlines caller values:\n%s", raw)
	}
}

// TestExecutionBucketIndexExprIsZoneIndependent pins both dialect hooks: the
// MySQL one subtracts a bound anchor instead of reading the session zone, and
// the SQLite one subtracts the anchor's Unix second. Neither may reintroduce a
// function whose result depends on a server or session time zone.
func TestExecutionBucketIndexExprIsZoneIndependent(t *testing.T) {
	anchor := time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC)
	cases := []struct {
		dialect  string
		wantExpr string
		wantArgs []any
	}{
		{"mysql", "TIMESTAMPDIFF(SECOND, ?, created_at) DIV ?", []any{anchor, int64(3600)}},
		{"sqlite", "(CAST(strftime('%s', created_at) AS INTEGER) - ?) / ?", []any{anchor.Unix(), int64(3600)}},
	}
	for _, tc := range cases {
		hook, ok := executionBucketIndexExprs[tc.dialect]
		if !ok {
			t.Fatalf("no bucket expression for dialect %q", tc.dialect)
		}
		expr, args := hook(anchor, 3600)
		if expr != tc.wantExpr {
			t.Fatalf("%s bucket expression = %q, want %q", tc.dialect, expr, tc.wantExpr)
		}
		if len(args) != len(tc.wantArgs) {
			t.Fatalf("%s bucket args = %v, want %v", tc.dialect, args, tc.wantArgs)
		}
		for i := range args {
			if got, want := fmt.Sprint(args[i]), fmt.Sprint(tc.wantArgs[i]); got != want {
				t.Fatalf("%s bucket arg %d = %s, want %s", tc.dialect, i, got, want)
			}
		}
	}
}

// TestSQLStoreSourceHasNoZoneDependentSQLFunctions scans every string literal
// in this package's non-test sources for MySQL functions whose result depends
// on the session or server time_zone. Window boundaries and bucket anchors must
// be bound as Go times so they travel through the same driver zone as the
// stored created_at; a literal NOW() or UNIX_TIMESTAMP(col) silently shifts by
// the server's UTC offset, which only a MySQL with a non-UTC zone shows. The
// scan reads string literals only, so a comment explaining why a function is
// banned does not trip it; the match is upper-case, the way SQL text here is
// written, so ordinary prose in an error message is not mistaken for SQL.
func TestSQLStoreSourceHasNoZoneDependentSQLFunctions(t *testing.T) {
	banned := regexp.MustCompile(`\b(UNIX_TIMESTAMP|FROM_UNIXTIME|NOW|CURDATE|CURTIME|CURRENT_DATE|CURRENT_TIME|CURRENT_TIMESTAMP|LOCALTIME|LOCALTIMESTAMP|SYSDATE|UTC_TIMESTAMP|CONVERT_TZ)\b`)
	files, err := filepath.Glob("*.go")
	if err != nil || len(files) == 0 {
		t.Fatalf("glob sqlstore sources: %v (files=%d)", err, len(files))
	}
	fset := token.NewFileSet()
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			lit, ok := n.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			if m := banned.FindString(lit.Value); m != "" {
				t.Errorf("%s: SQL string literal uses time-zone dependent %s; bind a Go time (UTC) instead:\n%s",
					fset.Position(lit.Pos()), m, lit.Value)
			}
			return true
		})
	}
}

// renamedDialector renames an existing dialector so a test can exercise the
// unsupported-dialect branch without a second driver.
type renamedDialector struct {
	gorm.Dialector
	name string
}

func (d renamedDialector) Name() string { return d.name }

func TestExecutionStatsUnsupportedDialectIsRefused(t *testing.T) {
	capture := newStatsSQLCapture(t)
	capture.db.Dialector = renamedDialector{Dialector: capture.db.Dialector, name: "postgres"}
	repo := &executionRepo{db: capture.db}

	filter := statsTestFilter(time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC))
	_, err := repo.ExecutionHistogram(context.Background(), "tenant-a", filter, time.Hour)
	if !errors.Is(err, store.ErrExecutionStatsUnsupported) {
		t.Fatalf("ExecutionHistogram on an unknown dialect = %v, want store.ErrExecutionStatsUnsupported", err)
	}
	if len(capture.raw) != 0 {
		t.Fatalf("unsupported dialect still built a query:\n%s", strings.Join(capture.raw, "\n"))
	}
}
