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
		Status:      types.ExecutionStatusSuccess,
	}
	back := fromDBExecution(toDBExecution(rec))
	if back.Namespace != ns {
		t.Fatalf("namespace after a round trip = %q, want %q", back.Namespace, ns)
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
