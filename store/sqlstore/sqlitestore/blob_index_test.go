package sqlitestore

import (
	"testing"

	"github.com/xbcio/xflow/store/blob"
	"github.com/xbcio/xflow/store/blob/blobtest"
	"github.com/xbcio/xflow/store/sqlstore"
)

// TestBlobIndexSQLiteSatisfiesContract runs the blob.Index contract against the
// SQL implementation on SQLite. The default pool holds one connection, so a
// concurrent Register waits for a running Purge's transaction by queueing for
// the connection rather than on a row lock; the contract's ordering
// guarantees hold either way.
func TestBlobIndexSQLiteSatisfiesContract(t *testing.T) {
	blobtest.RunIndexContract(t, sqliteIndexFactory)
}

func sqliteIndexFactory(t *testing.T) blobtest.IndexHarness {
	clock := blobtest.NewClock()
	idx, err := sqlstore.NewBlobIndex(newTestProvider(t).DB(), clock.Now)
	if err != nil {
		t.Fatalf("NewBlobIndex: %v", err)
	}
	return blobtest.IndexHarness{Index: blob.Index(idx), Clock: clock}
}

// TestCollectorScenariosOnSQLiteIndex runs the collector scenarios, with their
// deterministic fence races, on the SQL index over ObjectBacked and FSStore.
func TestCollectorScenariosOnSQLiteIndex(t *testing.T) {
	blobtest.RunGCScenarios(t, sqliteIndexFactory)
}

// TestCollectorStressOnSQLiteIndex is the randomized concurrency run against the
// SQL index. SQLite serialises every transaction on its single connection, so it
// proves the Collector and ObjectBacked protocol against real SQL, not row-lock
// concurrency; the MySQL run does that.
func TestCollectorStressOnSQLiteIndex(t *testing.T) {
	blobtest.RunGCStress(t, sqliteIndexFactory, 15)
}
