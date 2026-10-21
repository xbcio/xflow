package sqlstore

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"

	"github.com/xbcio/xflow/store"
)

// sqlCapture is a dry-run GORM handle plus the SQL of the last query built
// through it.
//
// Dry run is what makes this a plain unit test: SkipInitializeWithVersion keeps
// Open from probing the server for its version and database/sql connects
// lazily, so no MySQL has to exist. Asserting on the statement is also the
// stronger check — a `LIMIT 0` and an empty table both come back as an empty
// slice with a nil error, so counting rows cannot tell the defect from a
// correct query against no data.
type sqlCapture struct {
	db   *gorm.DB
	last string
}

func newSQLCapture(t *testing.T) *sqlCapture {
	t.Helper()
	db, err := gorm.Open(
		mysql.New(mysql.Config{
			DSN:                       "xflow:xflow@tcp(127.0.0.1:1)/xflow?parseTime=true",
			SkipInitializeWithVersion: true,
		}),
		// SkipDefaultTransaction is not a production posture: GORM wraps every
		// write in a transaction, and BeginTransaction dials even under DryRun
		// (it only checks SkipDefaultTransaction). The capture connection points
		// at 127.0.0.1:1 on purpose and must never be reached.
		&gorm.Config{DryRun: true, DisableAutomaticPing: true, SkipDefaultTransaction: true},
	)
	if err != nil {
		t.Fatalf("gorm.Open(dry run): %v", err)
	}
	c := &sqlCapture{db: db}
	// The repos call WithContext, which forks a session with its own Statement,
	// so the built SQL is not readable off this handle afterwards. A callback
	// runs on the forked session and can see it. Under DryRun the query callback
	// still builds the statement and only skips the round trip.
	//
	// Explain interpolates the bind variables back in. Without it the statement
	// reads `LIMIT ? OFFSET ?` and the assertion could not tell 25 from 0 — which
	// is the entire distinction under test.
	err = db.Callback().Query().After("gorm:query").Register("xflow_test:capture",
		func(tx *gorm.DB) {
			c.last = tx.Explain(tx.Statement.SQL.String(), tx.Statement.Vars...)
		})
	if err != nil {
		t.Fatalf("register capture callback: %v", err)
	}
	// Update statements (Updates) run through a different callback than
	// queries, so they need their own capture. Explain is applied here too: the
	// conditional repair's whole contract is which row the WHERE clause can
	// match, and that is only visible with the bind values interpolated.
	err = db.Callback().Update().After("gorm:update").Register("xflow_test:capture_update",
		func(tx *gorm.DB) {
			c.last = tx.Explain(tx.Statement.SQL.String(), tx.Statement.Vars...)
		})
	if err != nil {
		t.Fatalf("register update capture callback: %v", err)
	}
	return c
}

func (c *sqlCapture) sql(t *testing.T, run func()) string {
	t.Helper()
	c.last = ""
	run()
	if c.last == "" {
		t.Fatal("dry run produced no SQL; the query never reached the query callback")
	}
	return c.last
}

// updateSQL is sql's counterpart for writes: it captures the statement an
// Updates call builds. Under DryRun nothing is executed, so a caller must not
// read the returned RowsAffected as database behavior — the statement is what
// is under test.
func (c *sqlCapture) updateSQL(t *testing.T, run func()) string {
	t.Helper()
	c.last = ""
	run()
	if c.last == "" {
		t.Fatal("dry run produced no SQL; the write never reached the update callback")
	}
	return c.last
}

// TestPaginatedListsTreatZeroLimitAsUnbounded pins the ListOptions contract on
// the SQL side: a zero limit means "every match", so no LIMIT clause may reach
// the database.
//
// Passing the normalized zero straight to GORM does not mean "no limit" — its
// clause/limit.go emits the clause whenever the value is `>= 0`, so the query
// went out as `LIMIT 0` and MySQL correctly answered with nothing. All three
// paginated lists did that, which means store.ListOptions{} — the obvious thing
// to write when you want all of something — returned an empty slice and a nil
// error, while store/memstore returned every row for the same argument.
//
// The failure mode is worse than a wrong count. An empty result is
// indistinguishable from "nothing matched", so a test that lists rows and
// asserts none came back passes whether or not the code under test is correct:
// test/integration/transient_projection_real_test.go hit exactly that and had to
// pass an explicit Limit to keep its assertion meaningful.
func TestPaginatedListsTreatZeroLimitAsUnbounded(t *testing.T) {
	ctx := context.Background()
	c := newSQLCapture(t)
	nodes := &nodeRepo{db: c.db}
	signals := &signalRepo{db: c.db}
	artifacts := &artifactIndexRepo{db: c.db}

	for _, tc := range []struct {
		name string
		// innerLimits is how many LIMIT clauses the query legitimately contains
		// even when unbounded, because they come from a subquery rather than
		// from pagination. Asserting a count rather than absence keeps the test
		// able to catch a stray outer LIMIT on those queries too.
		innerLimits int
		run         func(store.ListOptions)
	}{
		{"ListNodes", 0, func(o store.ListOptions) {
			_, _ = nodes.ListNodes(ctx, "exec-1", o)
		}},
		{"ListExpiredSuspensions", 0, func(o store.ListOptions) {
			_, _ = nodes.ListExpiredSuspensions(ctx, time.Now(), o)
		}},
		{"ListSignalsByNames", 0, func(o store.ListOptions) {
			_, _ = signals.ListSignalsByNames(ctx, "exec-1", []string{"sig"}, o)
		}},
		{"ListLatestVersions", 1, func(o store.ListOptions) {
			// The one permitted LIMIT is latestRowPredicate's `LIMIT 1`, which
			// picks the newest row per filename and has nothing to do with
			// pagination.
			_, _ = artifacts.ListLatestVersions(ctx, "ns", o)
		}},
		{"ListVersions", 0, func(o store.ListOptions) {
			_, _ = artifacts.ListVersions(ctx, "ns", "a.wasm", o)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Each case runs the query twice. "No LIMIT appears" would also hold
			// for a build that dropped pagination altogether, so the second arm
			// is what keeps the first one honest.
			unbounded := c.sql(t, func() { tc.run(store.ListOptions{}) })
			if got := strings.Count(strings.ToUpper(unbounded), "LIMIT"); got != tc.innerLimits {
				t.Errorf("a zero limit must not add a LIMIT clause: store.ListOptions "+
					"documents zero as unbounded and store/memstore implements it that "+
					"way, so this query returns no rows where the memstore returns all "+
					"of them. want %d LIMIT clause(s) (all from subqueries), got %d.\nSQL: %s",
					tc.innerLimits, got, unbounded)
			}

			paged := c.sql(t, func() { tc.run(store.ListOptions{Limit: 25, Offset: 5}) })
			if !strings.Contains(paged, "LIMIT 25") {
				t.Errorf("an explicit limit must still reach the database.\nSQL: %s", paged)
			}
			if !strings.Contains(paged, "OFFSET 5") {
				t.Errorf("an explicit offset must still reach the database.\nSQL: %s", paged)
			}
		})
	}
}

// TestOffsetWithoutLimitGetsAnUnboundedLimit pins the offset-only arm of the
// ListOptions contract: skipping rows without sizing a page must still return
// every remaining match.
//
// SQL cannot express that directly. OFFSET is only grammatical as part of
// `LIMIT <n> OFFSET <m>`, so GORM's clause/limit.go writing a bare `OFFSET ?`
// produced a statement MySQL rejects with error 1064 (and SQLite with a syntax
// error). The contract tests that pass store.ListOptions{Offset: 1 << 30} —
// store/storetest/nodes.go and signals.go — failed on exactly that. The fix is
// an unreachable LIMIT; this test pins that the substitution appears with an
// offset, that the offset reaches the query at all, and that neither the
// no-pagination nor the explicit-limit path changed.
func TestOffsetWithoutLimitGetsAnUnboundedLimit(t *testing.T) {
	ctx := context.Background()
	c := newSQLCapture(t)
	nodes := &nodeRepo{db: c.db}
	signals := &signalRepo{db: c.db}
	artifacts := &artifactIndexRepo{db: c.db}

	for _, tc := range []struct {
		name string
		// innerLimits counts LIMIT clauses the query contains on its own,
		// before pagination is applied (ListLatestVersions' per-filename
		// `LIMIT 1` subquery).
		innerLimits int
		run         func(store.ListOptions)
	}{
		{"ListNodes", 0, func(o store.ListOptions) {
			_, _ = nodes.ListNodes(ctx, "exec-1", o)
		}},
		{"ListExpiredSuspensions", 0, func(o store.ListOptions) {
			_, _ = nodes.ListExpiredSuspensions(ctx, time.Now(), o)
		}},
		{"ListSignalsByNames", 0, func(o store.ListOptions) {
			_, _ = signals.ListSignalsByNames(ctx, "exec-1", []string{"sig"}, o)
		}},
		{"ListLatestVersions", 1, func(o store.ListOptions) {
			_, _ = artifacts.ListLatestVersions(ctx, "ns", o)
		}},
		{"ListVersions", 0, func(o store.ListOptions) {
			_, _ = artifacts.ListVersions(ctx, "ns", "a.wasm", o)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Zero offset, zero limit: no pagination at all. The missing LIMIT
			// is the correct half; the OFFSET must not exist on its own either.
			unbounded := c.sql(t, func() { tc.run(store.ListOptions{}) })
			if got := strings.Count(strings.ToUpper(unbounded), "LIMIT"); got != tc.innerLimits {
				t.Errorf("zero offset and zero limit must not add a LIMIT clause: "+
					"want %d (all from subqueries), got %d.\nSQL: %s",
					tc.innerLimits, got, unbounded)
			}
			if strings.Contains(strings.ToUpper(unbounded), "OFFSET") {
				t.Errorf("zero offset must not add an OFFSET clause.\nSQL: %s", unbounded)
			}

			// Offset only: the offset has to survive, preceded by the
			// unreachable bound rather than by nothing.
			offsetOnly := c.sql(t, func() { tc.run(store.ListOptions{Offset: 5}) })
			want := fmt.Sprintf("LIMIT %d OFFSET 5", unboundedOffsetLimit)
			if !strings.Contains(offsetOnly, want) {
				t.Errorf("an offset without a limit must produce %q; a bare OFFSET "+
					"is a syntax error on both MySQL and SQLite.\nSQL: %s", want, offsetOnly)
			}

			// An explicit limit keeps its own value and still implies no offset.
			limitOnly := c.sql(t, func() { tc.run(store.ListOptions{Limit: 25}) })
			if !strings.Contains(limitOnly, "LIMIT 25") {
				t.Errorf("an explicit limit must still reach the database.\nSQL: %s", limitOnly)
			}
			if strings.Contains(strings.ToUpper(limitOnly), "OFFSET") {
				t.Errorf("a zero offset must not add an OFFSET clause.\nSQL: %s", limitOnly)
			}
		})
	}
}
