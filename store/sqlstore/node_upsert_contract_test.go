package sqlstore

import (
	"reflect"
	"strings"
	"testing"

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

	got := make(map[string]bool, len(upsertNodeUpdateColumns))
	for _, col := range upsertNodeUpdateColumns {
		if got[col] {
			t.Errorf("upsertNodeUpdateColumns lists %q twice", col)
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
