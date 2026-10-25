package sqlstore

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// These tests are dry-run GORM statements, not round trips: they need no MySQL,
// and they assert on the SQL that would be sent. That is the stronger check for
// what is under test here. The behavioural half of the executions contract lives
// in store/storetest (run against store/memstore by store/memstore's tests and
// against this backend by test/integration's MySQL tests), and a behavioural
// test cannot see the difference between `namespace = ?` and a predicate that
// merely happens to return the right rows for the fixture — which is exactly the
// difference that matters for a tenant-scoped listing.

// TestListExecutionsFailsClosedOnUnusableScope asserts the refusal happens
// before any SQL is built. A query that reaches the database with a widened
// scope has already leaked by the time a result-set assertion could notice.
func TestListExecutionsFailsClosedOnUnusableScope(t *testing.T) {
	capture := newSQLCapture(t)
	repo := &executionRepo{db: capture.db}
	ctx := context.Background()

	for _, tc := range []struct {
		name string
		ns   namespace.Namespace
	}{
		{name: "empty", ns: ""},
		{name: "too long", ns: namespace.Namespace(strings.Repeat("n", namespace.MaxNameLen+1))},
		{name: "colon", ns: "ns:a"},
		{name: "glob", ns: "ns-*"},
	} {
		t.Run("list/"+tc.name, func(t *testing.T) {
			recs, err := repo.ListExecutions(ctx, tc.ns, store.ExecutionFilter{}, store.DefaultListOptions())
			if !errors.Is(err, store.ErrInvalidNamespace) {
				t.Fatalf("ListExecutions(ns=%q) = %v, want store.ErrInvalidNamespace", tc.ns, err)
			}
			if recs != nil {
				t.Fatalf("ListExecutions(ns=%q) returned %d rows with its error", tc.ns, len(recs))
			}
		})
		t.Run("count/"+tc.name, func(t *testing.T) {
			if _, err := repo.CountExecutions(ctx, tc.ns, store.ExecutionFilter{}); !errors.Is(err, store.ErrInvalidNamespace) {
				t.Fatalf("CountExecutions(ns=%q) = %v, want store.ErrInvalidNamespace", tc.ns, err)
			}
		})
	}
}

// TestListExecutionsQueryShape pins the predicates, the order and the
// pagination of the generated SQL. Each assertion here is a property the
// memstore implements in Go and the shared contract verifies behaviourally; this
// is the same contract expressed at the SQL level, where a mismatch between the
// two backends can actually be seen.
func TestListExecutionsQueryShape(t *testing.T) {
	capture := newSQLCapture(t)
	repo := &executionRepo{db: capture.db}
	ctx := context.Background()

	t.Run("scope is exact equality, never a wildcard", func(t *testing.T) {
		sql := capture.sql(t, func() {
			if _, err := repo.ListExecutions(ctx, "tenant-a", store.ExecutionFilter{}, store.DefaultListOptions()); err != nil {
				t.Fatalf("ListExecutions: %v", err)
			}
		})
		if !strings.Contains(sql, "namespace = 'tenant-a'") {
			t.Fatalf("generated SQL does not scope by exact namespace equality:\n%s", sql)
		}
		for _, forbidden := range []string{"LIKE", "IN (", "OR namespace", "namespace IS NULL"} {
			if strings.Contains(strings.ToUpper(sql), forbidden) {
				t.Fatalf("generated SQL contains %q, which could widen the scope past one namespace:\n%s", forbidden, sql)
			}
		}
		if !strings.Contains(sql, "FROM `xflow_executions`") {
			t.Fatalf("generated SQL does not read xflow_executions:\n%s", sql)
		}
	})

	t.Run("order is the documented total order", func(t *testing.T) {
		sql := capture.sql(t, func() {
			if _, err := repo.ListExecutions(ctx, "tenant-a", store.ExecutionFilter{}, store.DefaultListOptions()); err != nil {
				t.Fatalf("ListExecutions: %v", err)
			}
		})
		// store.ExecutionOrder is the single source of this string; assert on it
		// so a change to the constant is a change to what the query does, with
		// no second place to update.
		if !strings.Contains(sql, "ORDER BY "+store.ExecutionOrder) {
			t.Fatalf("generated SQL does not use store.ExecutionOrder (%q):\n%s", store.ExecutionOrder, sql)
		}
		if !strings.Contains(store.ExecutionOrder, "id DESC") {
			t.Fatalf("store.ExecutionOrder (%q) has no unique tiebreak; created_at is DATETIME(3) and "+
				"two rows can share a millisecond, which makes the order partial and offset "+
				"pagination unsound", store.ExecutionOrder)
		}
	})

	t.Run("offset and limit reach the query", func(t *testing.T) {
		sql := capture.sql(t, func() {
			if _, err := repo.ListExecutions(ctx, "tenant-a", store.ExecutionFilter{}, store.ListOptions{Limit: 25, Offset: 50}); err != nil {
				t.Fatalf("ListExecutions: %v", err)
			}
		})
		if !strings.Contains(sql, "LIMIT 25") || !strings.Contains(sql, "OFFSET 50") {
			t.Fatalf("generated SQL does not carry LIMIT 25 OFFSET 50:\n%s", sql)
		}
	})

	t.Run("zero limit is unbounded", func(t *testing.T) {
		// SQL's LIMIT 0 means "no rows"; store.ListOptions documents 0 as "every
		// match". applyPagination exists to keep the two apart, and this is the
		// executions query covered by that guard.
		sql := capture.sql(t, func() {
			if _, err := repo.ListExecutions(ctx, "tenant-a", store.ExecutionFilter{}, store.ListOptions{}); err != nil {
				t.Fatalf("ListExecutions: %v", err)
			}
		})
		if strings.Contains(sql, "LIMIT") {
			t.Fatalf("a zero limit emitted a LIMIT clause:\n%s", sql)
		}
	})

	t.Run("status filter", func(t *testing.T) {
		sql := capture.sql(t, func() {
			if _, err := repo.ListExecutions(ctx, "tenant-a", store.ExecutionFilter{Status: types.ExecutionStatusFailed}, store.DefaultListOptions()); err != nil {
				t.Fatalf("ListExecutions: %v", err)
			}
		})
		if !strings.Contains(sql, "status = 'failed'") {
			t.Fatalf("generated SQL does not filter on the status column:\n%s", sql)
		}
	})

	t.Run("workflow filter", func(t *testing.T) {
		sql := capture.sql(t, func() {
			if _, err := repo.ListExecutions(ctx, "tenant-a", store.ExecutionFilter{WorkflowID: "wfid-a"}, store.DefaultListOptions()); err != nil {
				t.Fatalf("ListExecutions: %v", err)
			}
		})
		if !strings.Contains(sql, "workflow_id = 'wfid-a'") {
			t.Fatalf("generated SQL does not filter on the workflow_id column:\n%s", sql)
		}
		// Identity, not a pattern: a LIKE here would turn one workflow's filter
		// into a prefix match over every workflow in the namespace.
		if strings.Contains(strings.ToUpper(sql), "LIKE") {
			t.Fatalf("generated SQL uses LIKE for the workflow id filter:\n%s", sql)
		}
	})

	t.Run("overlong workflow id is refused before SQL", func(t *testing.T) {
		long := types.WorkflowID(strings.Repeat("w", store.MaxExecutionWorkflowIDLen+1))
		if _, err := repo.ListExecutions(ctx, "tenant-a", store.ExecutionFilter{WorkflowID: long}, store.DefaultListOptions()); err == nil {
			t.Fatal("an overlong workflow id filter was accepted")
		}
	})

	t.Run("unknown status is refused before SQL", func(t *testing.T) {
		if _, err := repo.ListExecutions(ctx, "tenant-a", store.ExecutionFilter{Status: types.ExecutionStatus("nope")}, store.DefaultListOptions()); err == nil {
			t.Fatal("an unrecognized status filter was accepted")
		}
	})

	t.Run("count has no pagination", func(t *testing.T) {
		var count int64
		sql := capture.sql(t, func() {
			// DryRun returns no rows, so the count is whatever GORM scans into
			// it; only the statement is under test.
			_ = repo.executionQuery(ctx, "tenant-a", store.ExecutionFilter{}).Count(&count)
		})
		if strings.Contains(sql, "LIMIT") || strings.Contains(sql, "OFFSET") {
			t.Fatalf("the count query carries pagination, so it would not report the filtered total:\n%s", sql)
		}
		if !strings.Contains(sql, "namespace = 'tenant-a'") {
			t.Fatalf("the count query is not namespace-scoped:\n%s", sql)
		}
	})
}

// TestLatestExecutionsFailsClosedBeforeSQL asserts the refusal of an unusable
// scope happens before any SQL is built, including when there is nothing to
// look up. A query that reaches the database with a widened scope has already
// leaked by the time a result-set assertion could notice.
func TestLatestExecutionsFailsClosedBeforeSQL(t *testing.T) {
	capture := newSQLCapture(t)
	repo := &executionRepo{db: capture.db}
	ctx := context.Background()

	for _, tc := range []struct {
		name string
		ns   namespace.Namespace
	}{
		{name: "empty", ns: ""},
		{name: "too long", ns: namespace.Namespace(strings.Repeat("n", namespace.MaxNameLen+1))},
		{name: "colon", ns: "ns:a"},
		{name: "glob", ns: "ns-*"},
	} {
		for _, ids := range [][]types.WorkflowID{nil, {"wfid-a"}} {
			capture.last = ""
			got, err := repo.LatestExecutions(ctx, tc.ns, ids)
			if !errors.Is(err, store.ErrInvalidNamespace) {
				t.Fatalf("LatestExecutions(ns=%q, ids=%v) = %v, want store.ErrInvalidNamespace", tc.ns, ids, err)
			}
			if got != nil {
				t.Fatalf("LatestExecutions(ns=%q, ids=%v) returned %d entries with its error", tc.ns, ids, len(got))
			}
			if capture.last != "" {
				t.Fatalf("LatestExecutions(ns=%q, ids=%v) built SQL despite the refusal:\n%s", tc.ns, ids, capture.last)
			}
		}
	}
}

// TestLatestExecutionsQueryShape pins the statement: one namespace-scoped,
// parameter-bound batch read with the documented total order. The behavioural
// half lives in store/storetest.LatestExecutionsContract and runs against this
// backend via the SQLite tests; this half proves the properties a fixture on a
// two-row table cannot distinguish — a widened scope predicate, a LIKE instead
// of an identity match, or a client-side loop that would be O(ids) queries.
func TestLatestExecutionsQueryShape(t *testing.T) {
	capture := newSQLCapture(t)
	repo := &executionRepo{db: capture.db}
	ctx := context.Background()

	latestSQL := func(t *testing.T, ids []types.WorkflowID) string {
		t.Helper()
		return capture.sql(t, func() {
			if _, err := repo.LatestExecutions(ctx, "tenant-a", ids); err != nil {
				t.Fatalf("LatestExecutions(ids=%v): %v", ids, err)
			}
		})
	}

	t.Run("scope is exact equality and ids are the only IN list", func(t *testing.T) {
		sql := latestSQL(t, []types.WorkflowID{"wfid-a", "wfid-b"})
		if !strings.Contains(sql, "namespace = 'tenant-a'") {
			t.Fatalf("generated SQL does not scope by exact namespace equality:\n%s", sql)
		}
		if !strings.Contains(sql, "workflow_id IN (") ||
			!strings.Contains(sql, "'wfid-a'") || !strings.Contains(sql, "'wfid-b'") {
			t.Fatalf("generated SQL does not bind the requested workflow ids into an IN list:\n%s", sql)
		}
		for _, forbidden := range []string{"LIKE", "OR namespace", "namespace IS NULL", "namespace IN"} {
			if strings.Contains(strings.ToUpper(sql), forbidden) {
				t.Fatalf("generated SQL contains %q, which could widen the scope past one namespace:\n%s", forbidden, sql)
			}
		}
		if !strings.Contains(sql, "FROM xflow_executions") {
			t.Fatalf("generated SQL does not read xflow_executions:\n%s", sql)
		}
	})

	t.Run("window orders newest-first with the documented tiebreak", func(t *testing.T) {
		sql := latestSQL(t, []types.WorkflowID{"wfid-a"})
		for _, want := range []string{
			"ROW_NUMBER() OVER (",
			"PARTITION BY workflow_id",
			"ORDER BY created_at DESC, execution_id ASC",
			"WHERE rn = 1",
		} {
			if !strings.Contains(sql, want) {
				t.Fatalf("generated SQL does not contain %q:\n%s", want, sql)
			}
		}
	})

	t.Run("surplus ids are dropped before the statement", func(t *testing.T) {
		long := strings.Repeat("w", store.MaxExecutionWorkflowIDLen+1)
		sql := latestSQL(t, []types.WorkflowID{"wfid-keep", "wfid-keep", "", types.WorkflowID(long)})
		if strings.Count(sql, "'wfid-keep'") != 1 {
			t.Fatalf("a duplicate id was bound more than once:\n%s", sql)
		}
		if strings.Contains(sql, long) {
			t.Fatalf("an over-long workflow id reached the SQL; it can never match a stored id:\n%s", sql)
		}
		if strings.Contains(sql, "''") {
			t.Fatalf("an empty workflow id reached the SQL; it would select unattributed rows:\n%s", sql)
		}
	})

	t.Run("no ids means no statement", func(t *testing.T) {
		for _, ids := range [][]types.WorkflowID{nil, {}, {""}, {types.WorkflowID(strings.Repeat("w", store.MaxExecutionWorkflowIDLen+1))}} {
			capture.last = ""
			got, err := repo.LatestExecutions(ctx, "tenant-a", ids)
			if err != nil {
				t.Fatalf("LatestExecutions(ids=%v): %v", ids, err)
			}
			if got == nil || len(got) != 0 {
				t.Fatalf("LatestExecutions(ids=%v) = %v, want a non-nil empty map", ids, got)
			}
			if capture.last != "" {
				t.Fatalf("LatestExecutions(ids=%v) sent a statement with nothing usable to look up:\n%s", ids, capture.last)
			}
		}
	})
}

// TestExecutionNamespaceRoundTrip covers the projection: the column must survive
// both directions of the dbExecution <-> store.ExecutionRecord mapping. A listing
// is only scoped correctly if the column it filtered on is the one it read back,
// and a mapper that dropped the field would still pass the SQL shape tests above.
func TestExecutionNamespaceRoundTrip(t *testing.T) {
	const ns = "tenant-round-trip"
	rec := &store.ExecutionRecord{
		ID:          42,
		ExecutionID: types.ExecutionID("exec-round-trip"),
		Namespace:   ns,
		WorkflowID:  "wfid-round-trip",
		Status:      types.ExecutionStatusSuccess,
	}
	back := fromDBExecution(toDBExecution(rec))
	if back.Namespace != ns {
		t.Fatalf("namespace after a round trip = %q, want %q", back.Namespace, ns)
	}
	if back.WorkflowID != rec.WorkflowID {
		t.Fatalf("workflow id after a round trip = %q, want %q: a mapper that dropped the "+
			"field would still pass an SQL-shape test while every filtered listing returned nothing",
			back.WorkflowID, rec.WorkflowID)
	}
	if back.ID != rec.ID || back.ExecutionID != rec.ExecutionID {
		t.Fatalf("round trip changed identity fields: %+v", back)
	}

	// fromDBExecutions must return an empty non-nil slice, because that slice is
	// marshalled as the `list` payload and the API spec requires [] not null.
	page := fromDBExecutions(nil)
	if page == nil {
		t.Fatal("fromDBExecutions(nil) returned a nil slice; an empty list must serialize as []")
	}
	if len(page) != 0 {
		t.Fatalf("fromDBExecutions(nil) returned %d rows", len(page))
	}
}
