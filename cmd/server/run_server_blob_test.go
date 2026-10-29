package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store/blob"
	"github.com/xbcio/xflow/store/objectstore"
	"github.com/xbcio/xflow/store/sqlstore"
	"github.com/xbcio/xflow/store/sqlstore/mysqlstore"
)

// The blob layer is wired inside runServer, after the SQL store is open: the
// *gorm.DB the store is built on (enrollDB) is the only database the blob index
// can live in, and buildServerOptions runs from tests with no database at all,
// so appendBlobOption is called from runServer's own body. blob_options_test.go
// covers appendBlobOption with a database handed to it; nothing there can show
// that runServer hands it the right one, or any. These tests do, against a real
// Redis and a real MySQL.
//
// They are gated the way the rest of the real-service tests are: without
// XFLOW_TEST_REDIS_ADDR and XFLOW_TEST_MYSQL_DSN they skip, and under
// XFLOW_REQUIRE_REDIS_INTEGRATION=1 / XFLOW_REQUIRE_MYSQL_INTEGRATION=1 an
// unreachable service fails instead. The MySQL database must already hold
// db/xflow_schema.sql; runServer never migrates it.

// runServerBlobBackends returns the Redis address and MySQL DSN the tests use,
// or skips (fails under the REQUIRE switches) when either is unset or down.
func runServerBlobBackends(t *testing.T) (redisAddr, mysqlDSN string) {
	t.Helper()
	redisAddr = os.Getenv("XFLOW_TEST_REDIS_ADDR")
	mysqlDSN = os.Getenv("XFLOW_TEST_MYSQL_DSN")
	gate := func(require, format string, args ...any) {
		t.Helper()
		if os.Getenv(require) == "1" {
			t.Fatalf(require+"=1: "+format, args...)
		}
		t.Skipf(format, args...)
	}
	if redisAddr == "" {
		gate("XFLOW_REQUIRE_REDIS_INTEGRATION", "set XFLOW_TEST_REDIS_ADDR to run the runServer blob wiring tests")
	}
	if mysqlDSN == "" {
		gate("XFLOW_REQUIRE_MYSQL_INTEGRATION", "set XFLOW_TEST_MYSQL_DSN to run the runServer blob wiring tests")
	}
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer func() { _ = rdb.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		gate("XFLOW_REQUIRE_REDIS_INTEGRATION", "redis unavailable at the XFLOW_TEST_REDIS_ADDR address: %v", err)
	}
	return redisAddr, mysqlDSN
}

// runServerBlobArgs is the smallest real-service server: dev posture (so the
// production gate does not ask for tokens and a master key), Redis, MySQL, and
// the blob layer on a directory.
func runServerBlobArgs(redisAddr, mysqlDSN, httpAddr, blobDir string) []string {
	return []string{
		"-mode", "dev",
		"-management",
		"-redis", redisAddr,
		"-mysql-dsn", mysqlDSN,
		"-addr", httpAddr,
		"-grpc-addr", "127.0.0.1:0",
		"-blob-dir", blobDir,
		"-blob-enable-ref-writes",
	}
}

// TestRunServerWiresBlobLayerFromTheSQLStore starts the whole server with a blob
// directory and requires it to come up ready, with the startup line that
// appendBlobOption logs only after it was given a database. A runServer that
// passed nil would not get that far: buildBlobConfig refuses a nil database, so
// the server would return "blob store: ..." instead of serving.
func TestRunServerWiresBlobLayerFromTheSQLStore(t *testing.T) {
	redisAddr, mysqlDSN := runServerBlobBackends(t)
	logs := captureLog(t)
	blobDir := t.TempDir()

	const maxAttempts = 3
	var lastErr error
	for range maxAttempts {
		httpAddr := reserveLoopbackAddr(t)
		cfg, err := parseServerConfig(runServerBlobArgs(redisAddr, mysqlDSN, httpAddr, blobDir))
		if err != nil {
			t.Fatalf("parseServerConfig: %v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- runServer(ctx, cfg) }()

		if err := waitForReadyz(t, httpAddr, done); err != nil {
			// A start-up error that names the blob store is the wiring bug this
			// test exists for, not a lost port race: fail on it, do not retry.
			if strings.Contains(err.Error(), "blob store") {
				cancel()
				t.Fatalf("runServer did not wire the blob layer: %v", err)
			}
			// waitForReadyz has already taken runServer's return value off done
			// when it reports that runServer returned, so draining done again
			// would only wait out the shutdown timeout. Drain only when the
			// server may still be running (readiness never arrived).
			if !strings.HasPrefix(err.Error(), "runServer returned before serving") {
				cancel()
				select {
				case rerr := <-done:
					t.Logf("runServer after a failed start attempt returned: %v", rerr)
				case <-time.After(runServerShutdownTimeout):
					t.Fatalf("runServer did not return after a failed start attempt (%v)", err)
				}
			}
			lastErr = err
			continue
		}

		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("runServer(cancelled ctx) = %v, want nil", err)
			}
		case <-time.After(runServerShutdownTimeout):
			t.Fatalf("runServer did not return within %v of ctx cancellation", runServerShutdownTimeout)
		}

		out := logs.String()
		if !strings.Contains(out, "xflow-server: blob store enabled (ref_writes=true") {
			t.Fatalf("server came up ready but never logged the blob startup line, so the blob "+
				"layer was not wired from runServer; log:\n%s", out)
		}
		if strings.Contains(out, blobDir) {
			t.Fatalf("the startup log carries the blob directory, which must stay out of logs:\n%s", out)
		}
		return
	}
	t.Fatalf("runServer never reached a ready state after %d attempts: %v", maxAttempts, lastErr)
}

// TestRunServerRefusesAnUnusableBlobDir pins the other half: runServer returns
// the blob error instead of serving without the layer the operator asked for.
// The directory is one that does not exist, which probeBlobDir treats as a
// missing mount and never creates.
func TestRunServerRefusesAnUnusableBlobDir(t *testing.T) {
	redisAddr, mysqlDSN := runServerBlobBackends(t)
	missing := filepath.Join(t.TempDir(), "not-mounted")

	cfg, err := parseServerConfig(runServerBlobArgs(redisAddr, mysqlDSN, reserveLoopbackAddr(t), missing))
	if err != nil {
		t.Fatalf("parseServerConfig: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- runServer(t.Context(), cfg) }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("runServer = nil with a missing blob directory, want a start-up error")
		}
		if !strings.Contains(err.Error(), "blob store") {
			t.Fatalf("runServer error = %v, want it to come from the blob store step", err)
		}
		if strings.Contains(err.Error(), missing) {
			t.Fatalf("the start-up error carries the blob directory: %v", err)
		}
	case <-time.After(runServerReadyTimeout):
		t.Fatal("runServer kept running with a missing blob directory instead of failing at start-up")
	}
	if _, err := os.Stat(missing); !os.IsNotExist(err) {
		t.Fatalf("the missing blob directory was created (stat err = %v); a missing mount must not be created", err)
	}
}

// TestRunServerStartsTheBlobCollector shows that the blob option does not just
// get built but reaches the server: a blob that nothing references, written
// through the same database and directory, is reclaimed by the collector the
// server starts. A runServer that built the option and then dropped it would
// still log the startup line and serve, and leave this blob in place forever.
func TestRunServerStartsTheBlobCollector(t *testing.T) {
	redisAddr, mysqlDSN := runServerBlobBackends(t)
	blobDir := t.TempDir()

	provider, err := mysqlstore.New(mysqlDSN)
	if err != nil {
		t.Fatalf("mysqlstore.New: %v", err)
	}
	t.Cleanup(func() {
		if sqlDB, err := provider.DB().DB(); err == nil {
			_ = sqlDB.Close()
		}
	})
	db := provider.DB()
	idx, err := sqlstore.NewBlobIndex(db, nil)
	if err != nil {
		t.Fatalf("NewBlobIndex: %v", err)
	}
	// A namespace of its own, so a leftover row from another run is never
	// mistaken for this one, and the cleanup below touches nothing else.
	ns := namespace.Namespace("runserver-blob-gc")
	t.Cleanup(func() {
		_ = db.Exec("DELETE FROM xflow_blob_links WHERE namespace = ?", string(ns)).Error
		_ = db.Exec("DELETE FROM xflow_blob_objects WHERE namespace = ?", string(ns)).Error
	})
	store := blob.NewObjectBacked(objectstore.NewFSStore(blobDir), blob.ObjectBackedOptions{Index: idx})
	ref, err := store.Put(context.Background(), ns, []byte("an orphan nothing links to "+t.Name()))
	if err != nil {
		t.Fatalf("Put orphan: %v", err)
	}
	count := func() int64 {
		var n int64
		if err := db.Raw("SELECT COUNT(*) FROM xflow_blob_objects WHERE namespace = ? AND digest = ?",
			string(ns), ref.Digest).Scan(&n).Error; err != nil {
			t.Fatalf("count orphan row: %v", err)
		}
		return n
	}
	if count() != 1 {
		t.Fatalf("the orphan was not registered in xflow_blob_objects before the server started")
	}

	httpAddr := reserveLoopbackAddr(t)
	args := append(runServerBlobArgs(redisAddr, mysqlDSN, httpAddr, blobDir),
		// The shortest interval and grace the flags accept, so the first pass
		// comes within a moment and finds the orphan old enough.
		"-blob-gc-interval", "200ms", "-blob-gc-grace", "1ms")
	cfg, err := parseServerConfig(args)
	if err != nil {
		t.Fatalf("parseServerConfig: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runServer(ctx, cfg) }()
	t.Cleanup(func() {
		cancel()
		select {
		case rerr := <-done:
			t.Logf("runServer returned after cancellation: %v", rerr)
		case <-time.After(runServerShutdownTimeout):
			t.Logf("runServer did not return within %v of cancellation", runServerShutdownTimeout)
		}
	})

	deadline := time.Now().Add(runServerReadyTimeout)
	for count() != 0 {
		select {
		case err := <-done:
			t.Fatalf("runServer returned before the collector ran: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("the orphan blob was still registered %v after the server started with "+
				"-blob-gc-interval 200ms; the blob option never reached the backend", runServerReadyTimeout)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
