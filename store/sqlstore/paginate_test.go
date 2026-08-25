package sqlstore

import (
	"context"
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
		&gorm.Config{DryRun: true, DisableAutomaticPing: true},
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
			c.last = tx.Dialector.Explain(tx.Statement.SQL.String(), tx.Statement.Vars...)
		})
	if err != nil {
		t.Fatalf("register capture callback: %v", err)
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

	for _, tc := range []struct {
		name string
		run  func(store.ListOptions)
	}{
		{"ListNodes", func(o store.ListOptions) {
			_, _ = nodes.ListNodes(ctx, "exec-1", o)
		}},
		{"ListExpiredSuspensions", func(o store.ListOptions) {
			_, _ = nodes.ListExpiredSuspensions(ctx, time.Now(), o)
		}},
		{"ListSignalsByNames", func(o store.ListOptions) {
			_, _ = signals.ListSignalsByNames(ctx, "exec-1", []string{"sig"}, o)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Each case runs the query twice. "No LIMIT appears" would also hold
			// for a build that dropped pagination altogether, so the second arm
			// is what keeps the first one honest.
			unbounded := c.sql(t, func() { tc.run(store.ListOptions{}) })
			if strings.Contains(strings.ToUpper(unbounded), "LIMIT") {
				t.Errorf("a zero limit must not emit a LIMIT clause: store.ListOptions "+
					"documents zero as unbounded and store/memstore implements it that "+
					"way, so this query returns no rows where the memstore returns all "+
					"of them.\nSQL: %s", unbounded)
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
