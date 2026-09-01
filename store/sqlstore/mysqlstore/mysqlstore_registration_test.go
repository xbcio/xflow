package mysqlstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"

	"github.com/xbcio/xflow/store"
)

// registrationTestDSN mirrors probeMySQLDSN in
// node/internal/action/database_mysql_test.go (same local test MySQL:
// test/env/docker-compose.yml, localhost:3306, db "xflow", root password
// "xflow"). Duplicated rather than imported because that helper lives in a
// different package (action_test) with its own reasons not to export it.
func registrationTestDSN() string {
	if dsn := os.Getenv("XFLOW_TEST_MYSQL_DSN"); dsn != "" {
		return dsn
	}
	port := envOrDefault("MYSQL_PORT", "3306")
	pw := envOrDefault("MYSQL_ROOT_PASSWORD", "xflow")
	dbname := envOrDefault("MYSQL_DATABASE", "xflow")
	return fmt.Sprintf("root:%s@tcp(127.0.0.1:%s)/%s?parseTime=true&multiStatements=true", pw, port, dbname)
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// TestNewRegistersMySQLDeadlockClassifier is task-56 fix-1.md Defect 3's
// Mutation R target: it proves this package's init() actually WIRES
// isMySQLDeadlock into sqlstore's dialect-agnostic core, not just that
// isMySQLDeadlock itself is correct in isolation (that contract is
// TestIsMySQLDeadlock_DeadlockIsTransient in mysqlstore_classifier_test.go).
//
// Named after New rather than init for continuity with its original
// pre-ruling-d form (task-56-fix-1.md's Defect 3 originally gated
// registration on New running instead of on package import; ruling-d
// retracted that -- see mysqlstore.go's package comment and
// store/sqlstore/errors.go's RegisterTransientClassifier doc comment for the
// full reasoning). The test still calls New (rather than sqlstore.New
// directly) because New is still this package's own public constructor and
// the one every real production caller uses; what changed is only where the
// registration that makes this test pass actually happens (init(), not
// inside New's body).
//
// It deliberately does NOT call sqlstore.RegisterTransientClassifier or
// isMySQLDeadlock directly -- doing so would only re-prove the contract a
// second time and say nothing about whether this package's init() actually
// ran. Instead it builds a *sqlstore.Provider via New and drives many
// goroutines through Provider.ArtifactIndex().Bind concurrently, all racing
// to insert the SAME never-before-seen (namespace, filename, version)
// triple.
//
// This is not a novel reproduction technique invented for this test: it is
// the same shape store/sqlstore/artifact.go's own Bind implementation
// documents ("two concurrent binds of the same triple both reach [the insert
// branch]"), and the same shape SAS's independent real-MySQL deadlock test
// (controller_deadlock_test.go, a different repo entirely) already relies on
// empirically -- first-time concurrent inserts racing the same unique key
// reliably produce MySQL's ER_LOCK_DEADLOCK (1213) under enough concurrency,
// because every racer takes a gap/next-key lock on the same index range
// before any of them can complete the insert.
//
// The judgment below deliberately separates two different questions that a
// naive "did anything satisfy errors.Is(_, store.ErrTransient)" check would
// conflate:
//  1. Did a real MySQL deadlock (driver error 1213) happen in this run at
//     all? Checked via errors.As against *mysqldriver.MySQLError -- which
//     survives even completely UNCLASSIFIED, because wrapDBErr's fallback
//     branch still wraps the original error with %w.
//  2. Of the real 1213s that happened, how many did this package's
//     registration actually get classified as store.ErrTransient?
//
// Collapsing these into one "count is zero" check would make this test
// SKIP -- not FAIL -- if a registration regression (e.g. Mutation R,
// commenting out init()'s sqlstore.RegisterTransientClassifier call) stopped
// classifying real 1213s as transient: real deadlocks would still occur,
// they just would not be recognized, and "zero classified" looks identical
// to "zero occurred" under a single counter. Splitting the two makes a
// present-but-unclassified deadlock a loud FAILURE instead of a silent SKIP.
//
// Requires a real local MySQL (test/env/docker-compose.yml); skips with an
// explicit reason if one is not reachable, following this repo's existing
// convention for non-integration-tagged real-DB tests (see
// node/internal/action/database_mysql_test.go).
func TestNewRegistersMySQLDeadlockClassifier(t *testing.T) {
	dsn := registrationTestDSN()

	provider, err := New(dsn)
	if err != nil {
		t.Skipf("mysql unavailable at %s (run `make env-up && make env-migrate` for real-DB coverage): %v", dsn, err)
	}

	const racers = 12
	namespace := fmt.Sprintf("mysqlstore-wiring-probe-%d", time.Now().UnixNano())
	filename := "probe.bin"
	version := "v1"

	var wg sync.WaitGroup
	errs := make([]error, racers)
	start := make(chan struct{})
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			errs[idx] = provider.ArtifactIndex().Bind(ctx, store.ArtifactIdentity{
				Namespace:   namespace,
				Filename:    filename,
				Version:     version,
				Digest:      "sha256:" + fmt.Sprintf("%064d", 1),
				ContentType: "application/octet-stream",
			})
		}(i)
	}
	close(start)
	wg.Wait()

	var rawDeadlockCount, classifiedTransientCount int
	for _, e := range errs {
		if e == nil {
			continue // the eventual winner, and any idempotent re-bind, succeeds outright
		}
		var mysqlErr *mysqldriver.MySQLError
		if errors.As(e, &mysqlErr) && mysqlErr.Number == mysqlErrDeadlock {
			rawDeadlockCount++
			if errors.Is(e, store.ErrTransient) {
				classifiedTransientCount++
			}
		}
	}

	if rawDeadlockCount == 0 {
		t.Skip("no real MySQL deadlock (driver error 1213) occurred across 12 concurrent " +
			"first-time Binds this run; this test verifies nothing about New's registration " +
			"wiring when that happens -- deadlock reproduction under concurrent inserts is " +
			"probabilistic, rerun")
	}
	if classifiedTransientCount != rawDeadlockCount {
		t.Fatalf("%d real MySQL deadlock(s) (driver error 1213) occurred, but only %d were "+
			"classified as errors.Is(_, store.ErrTransient) -- this package's "+
			"init() -> sqlstore.RegisterTransientClassifier(\"mysql\", isMySQLDeadlock) call is "+
			"not reaching wrapDBErr through the real Provider.ArtifactIndex().Bind call path",
			rawDeadlockCount, classifiedTransientCount)
	}
	t.Logf("reproduced %d/%d concurrent Binds as a real MySQL deadlock, all %d correctly "+
		"classified as store.ErrTransient -- this package's init() registration reaches wrapDBErr",
		rawDeadlockCount, racers, classifiedTransientCount)
}
