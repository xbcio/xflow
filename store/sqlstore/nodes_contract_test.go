package sqlstore

import (
	"testing"

	"github.com/xbcio/xflow/store/storetest"
)

func TestSQLNodeRepoSatisfiesNodesContract(t *testing.T) {
	db := newEnrollTestDB(t)
	// newEnrollTestDB only clears the enroll tables. The nodes contract asserts
	// exact listings within its own execution ids, but ListExpiredSuspensions is
	// a table-wide scan, so leftovers from an earlier run would join the result.
	if err := db.Exec("DELETE FROM xflow_nodes").Error; err != nil {
		t.Fatalf("clear xflow_nodes: %v", err)
	}
	storetest.NodesContract(t, &nodeRepo{db: db})
}
