package sqlstore

import (
	"reflect"
	"strings"
	"testing"

	"gorm.io/gorm/clause"

	"github.com/xbcio/xflow/store"
)

// columnOf reports the SQL column dbNode maps a Go field to, by reading the
// gorm tag rather than guessing a snake_case transform. Timeout maps to
// timeout_at, so a mechanical transform would derive the wrong name and the
// comparison below would fail for a reason that has nothing to do with the
// contract.
func columnOf(t *testing.T, field string) string {
	t.Helper()
	f, ok := reflect.TypeFor[dbNode]().FieldByName(field)
	if !ok {
		t.Fatalf("dbNode has no field %q, but store.UpsertNodeUpdateFields names it; "+
			"the persistence type and the contract have diverged", field)
	}
	for _, part := range strings.Split(f.Tag.Get("gorm"), ";") {
		if name, found := strings.CutPrefix(part, "column:"); found {
			return name
		}
	}
	t.Fatalf("dbNode.%s has no gorm column tag; the column name cannot be derived", field)
	return ""
}

// TestUpsertNodeUpdateColumnsMatchTheContract binds the sqlstore half of the
// cross-backend UpsertNode field set to store.UpsertNodeUpdateFields.
//
// This is the half that had no test at all. The contract's own test compared
// the list against itself, and the doc comment said the sqlstore side was "kept
// in sync manually" — so deleting a column from the ON CONFLICT update set
// silently stopped refreshing it on every upsert, with the projection quietly
// serving a stale value and nothing anywhere going red.
//
// updated_at is expected in the SQL set and absent from the contract: each
// backend refreshes it on its own, and the contract documents that exclusion.
func TestUpsertNodeUpdateColumnsMatchTheContract(t *testing.T) {
	want := make(map[string]bool, len(store.UpsertNodeUpdateFields)+1)
	for _, field := range store.UpsertNodeUpdateFields {
		want[columnOf(t, field)] = true
	}
	want["updated_at"] = true
	// Write-once timing columns are refreshed by the same statement but not by
	// the "refresh every listed field" contract; COALESCE makes them a no-op
	// once set. They are pinned as their own set so moving one into the
	// refresh contract (a retry would move started_at) or dropping one from the
	// statement (finished_at would never be written after the INSERT) fails
	// here.
	for _, col := range upsertNodeWriteOnceColumns {
		want[col] = true
	}

	got := make(map[string]bool, len(upsertNodeUpdateColumns)+len(upsertNodeWriteOnceColumns))
	for _, col := range append(append([]string{}, upsertNodeUpdateColumns...), upsertNodeWriteOnceColumns...) {
		if got[col] {
			t.Errorf("upsert assignments list %q twice", col)
		}
		got[col] = true
	}

	for col := range want {
		if !got[col] {
			t.Errorf("column %q is in the UpsertNode contract but not in the ON CONFLICT "+
				"update set; an upsert on an existing row leaves it stale", col)
		}
	}
	for col := range got {
		if !want[col] {
			t.Errorf("column %q is updated on conflict but is not in the contract; either "+
				"add the field to store.UpsertNodeUpdateFields or stop updating the column "+
				"(the memstore does not refresh it, so the two backends disagree)", col)
		}
	}
}

// TestUpsertNodeTimingColumnsAreWriteOnce pins the shape the column-set test
// cannot see: a plain AssignmentColumns entry for started_at/finished_at would
// refresh them on every conflict, and a column missing from the statement would
// never be written after the INSERT. Both failures leave the column in the
// right set while producing the wrong lifecycle.
func TestUpsertNodeTimingColumnsAreWriteOnce(t *testing.T) {
	refresh := make(map[string]bool, len(upsertNodeUpdateColumns))
	for _, col := range upsertNodeUpdateColumns {
		refresh[col] = true
	}
	// Every dialect sqlstore.New accepts must produce a statement that parses:
	// MySQL has no `excluded` pseudo-table and Postgres/SQLite have no VALUES()
	// row reference, so a hard-coded spelling breaks half of them.
	for _, dialect := range []string{"mysql", "postgres", "sqlite"} {
		exprs := make(map[string]string, len(upsertNodeWriteOnceColumns))
		for _, a := range nodeUpsertAssignments(dialect) {
			if expr, ok := a.Value.(clause.Expr); ok {
				exprs[a.Column.Name] = expr.SQL
			}
		}
		for _, col := range upsertNodeWriteOnceColumns {
			if refresh[col] {
				t.Errorf("%q is in the refresh set; a retry would move it", col)
			}
			// The expected spelling is written out here rather than borrowed
			// from conflictRowExpr: computing it with the function under test
			// would move the expectation with the bug.
			conflictRow := "excluded." + col
			if dialect == "mysql" {
				conflictRow = "VALUES(" + col + ")"
			}
			want := "COALESCE(" + col + ", " + conflictRow + ")"
			if got := exprs[col]; got != want {
				t.Errorf("assignment for %q in dialect %q = %q, want %q: without "+
					"COALESCE the upsert overwrites the first write; with the wrong "+
					"conflict-row spelling the statement does not parse", col, dialect, got, want)
			}
		}
	}
}

// TestUpsertNodeDoesNotUpdateTheConflictKeyOrCreatedAt pins the columns that
// must stay out of the update set. Adding created_at would let a re-upsert
// rewrite the row's origin timestamp, and adding either half of the conflict
// key would make the ON CONFLICT clause update the very columns it matched on.
func TestUpsertNodeDoesNotUpdateTheConflictKeyOrCreatedAt(t *testing.T) {
	for _, col := range upsertNodeUpdateColumns {
		switch col {
		case "id", "execution_id", "node_name", "created_at":
			t.Errorf("upsertNodeUpdateColumns includes %q, which must never be rewritten "+
				"by an upsert on an existing row", col)
		}
	}
}
