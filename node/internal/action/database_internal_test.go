package action

import (
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

// TestBuildWhereJoinsClausesWithAND pins the join operator at
// database.go:369. Every WHERE fixture in the repo — database_test.go:16 and
// test/integration/action_parity_database_server_test.go:179 — uses a SINGLE
// key, and with one clause " AND " and " OR " produce byte-identical SQL. So
// the operator is currently unobservable.
//
// buildWhere feeds execSelect, execUpdate and execDelete. A workflow that
// scopes a DELETE by {"tenant_id": …, "user_id": …} means "this user, in this
// tenant"; under OR it becomes "this user OR anyone in this tenant", and the
// node still returns success with a rows_affected count. There is no error
// port, no log, and nothing downstream that could tell the difference.
//
// Map iteration randomizes clause order, so the assertion is written to be
// order-insensitive while still pinning that each argument stays paired with
// its own column — the same walk catches a clauses/args desync.
func TestBuildWhereJoinsClausesWithAND(t *testing.T) {
	where := map[string]any{"tenant_id": 7, "user_id": 3}

	clause, args, err := buildWhere(where)
	if err != nil {
		t.Fatalf("buildWhere() error = %v", err)
	}
	if strings.Contains(clause, " OR ") {
		t.Fatalf("buildWhere() = %q: an OR here widens every scoped UPDATE and "+
			"DELETE from \"all of these\" to \"any of these\"", clause)
	}
	parts := strings.Split(clause, " AND ")
	if len(parts) != len(where) {
		t.Fatalf("buildWhere() = %q, want %d clauses joined by AND", clause, len(where))
	}
	if len(args) != len(where) {
		t.Fatalf("buildWhere() args = %#v, want %d", args, len(where))
	}
	for i, part := range parts {
		column, found := strings.CutSuffix(part, " = ?")
		if !found {
			t.Fatalf("clause %q is not a parameterized equality — a literal here "+
				"would be a SQL injection sink", part)
		}
		want, ok := where[column]
		if !ok {
			t.Fatalf("clause %q names a column that was not in the WHERE map", part)
		}
		if args[i] != want {
			t.Fatalf("args[%d] = %v, want %v: argument %d does not belong to "+
				"clause %q, so every value is bound to the wrong column",
				i, args[i], want, i, part)
		}
	}
}

// TestBuildWhereRejectsInvalidColumnName covers the guard at database.go:359,
// which the comment there explains is deliberately a rejection rather than a
// silent drop. No test executed it: a dropped clause makes an UPDATE or DELETE
// broader than the workflow asked for, and the column name is also the one part
// of the statement that is concatenated rather than bound.
func TestBuildWhereRejectsInvalidColumnName(t *testing.T) {
	for _, column := range []string{"id = 1 OR 1", "1id", "", "user-id"} {
		_, _, err := buildWhere(map[string]any{column: 1})
		if err == nil {
			t.Fatalf("buildWhere(%q) error = nil, want a rejection", column)
		}
		if !types.IsPermanent(err) {
			t.Fatalf("buildWhere(%q) error = %v, want a permanent error: a bad "+
				"column name is an authoring mistake, retrying cannot fix it",
				column, err)
		}
	}
	// The valid shapes must keep working, or the guard above is satisfied by a
	// function that rejects everything.
	for _, column := range []string{"id", "tenant_id", "userID2"} {
		if _, _, err := buildWhere(map[string]any{column: 1}); err != nil {
			t.Fatalf("buildWhere(%q) error = %v, want accepted", column, err)
		}
	}
}
