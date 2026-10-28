package mysqlstore

import (
	"testing"

	"github.com/xbcio/xflow/store/blob"
	"github.com/xbcio/xflow/store/blob/blobtest"
	"github.com/xbcio/xflow/store/sqlstore"
)

// blobIndexFactory builds a sqlstore.BlobIndex on a real MySQL. It lives in this
// package rather than in sqlstore because the index retries a transaction that
// MySQL aborted as a deadlock victim, and it recognises that through the
// classifier this package registers in init(): a test here runs the same wiring
// production does. Reaching MySQL goes through registrationTestDSN, and an
// unreachable server skips (or fails under XFLOW_REQUIRE_MYSQL_INTEGRATION=1)
// through skipOrFailRegistrationMySQL, like the rest of this package's tests.
//
// The two blob tables belong to this index alone, so emptying them before each
// case cannot disturb any other suite.
func blobIndexFactory(t *testing.T) blobtest.IndexHarness {
	t.Helper()
	provider, err := New(registrationTestDSN())
	if err != nil {
		skipOrFailRegistrationMySQL(t, "mysql unavailable at %s (run `make env-up && make env-migrate` for real-DB coverage): %v",
			registrationMySQLEndpoint(), err)
	}
	db := provider.DB()
	t.Cleanup(func() {
		if sqlDB, err := db.DB(); err == nil {
			_ = sqlDB.Close()
		}
	})
	if err := sqlstore.AutoMigrate(db); err != nil {
		t.Fatalf("AutoMigrate: %v", err)
	}
	for _, table := range []string{"xflow_blob_links", "xflow_blob_objects"} {
		if err := db.Exec("DELETE FROM " + table).Error; err != nil {
			t.Fatalf("clear %s: %v", table, err)
		}
	}
	clock := blobtest.NewClock()
	idx, err := sqlstore.NewBlobIndex(db, clock.Now)
	if err != nil {
		t.Fatalf("NewBlobIndex: %v", err)
	}
	return blobtest.IndexHarness{Index: blob.Index(idx), Clock: clock}
}

// TestBlobIndexMySQLSatisfiesContract runs the blob.Index contract against a
// real MySQL. This is the run that matters for the fence: row locks, the
// primary key's byte-wise collation, DATETIME(3) comparison, and the
// insert-then-lock ordering in Register only exist on a real InnoDB. Run it
// under several server time zones (--default-time-zone): every time in the
// index is a bound UTC value, so the result must not depend on the zone.
func TestBlobIndexMySQLSatisfiesContract(t *testing.T) {
	blobtest.RunIndexContract(t, blobIndexFactory)
}

// TestCollectorScenariosOnMySQLIndex runs the collector scenarios, with their
// deterministic fence races, over ObjectBacked, FSStore, and the MySQL index.
func TestCollectorScenariosOnMySQLIndex(t *testing.T) {
	blobtest.RunGCScenarios(t, blobIndexFactory)
}

// TestCollectorStressOnMySQLIndex is the randomized concurrency run on real row
// locks: writers and collectors contend for the same objects through InnoDB,
// including the gap-lock deadlocks MySQL reports to concurrent inserters.
func TestCollectorStressOnMySQLIndex(t *testing.T) {
	blobtest.RunGCStress(t, blobIndexFactory, 15)
}
