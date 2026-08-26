package resource

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/types"
)

// pool_defaults_test.go pins what normalizeConfig produces. This file pins the
// other half — that those values actually reach the *sql.DB — which nothing
// asserted on.
//
// The gap is structural, not an oversight of one test: sql.DBStats exposes
// neither MaxIdleConns nor ConnMaxLifetime, so there is no way to read the
// settings back off the handle. Every other reference to these two fields in
// the repo inspects the config struct, never the DB. Delete both
// db.SetMaxIdleConns and db.SetConnMaxLifetime from SQL() and the package, its
// importers, and the config assertions in pool_defaults_test.go all stay green
// while production silently runs on database/sql's own defaults: MaxIdleConns=2
// instead of the configured value, and ConnMaxLifetime=0, which means
// connections are never retired — the failure mode that turns a failed-over or
// restarted database into a pool of permanently dead handles.
//
// What DBStats does expose is the two counters below, which move only as a
// consequence of those settings. That makes them the only available evidence
// that the wiring exists.

type probeConn struct{}

func (probeConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (probeConn) Close() error                        { return nil }
func (probeConn) Begin() (driver.Tx, error)           { return nil, errors.New("probe: no transactions") }

type probeDriver struct{}

func (probeDriver) Open(string) (driver.Conn, error) { return probeConn{}, nil }

// slowCloseConn blocks in Close so that (*sql.DB).Close, and therefore
// defaultResourcePool.Close's closeAll goroutine, does not return promptly.
type slowCloseConn struct{ block time.Duration }

func (slowCloseConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (c slowCloseConn) Close() error                      { time.Sleep(c.block); return nil }
func (slowCloseConn) Begin() (driver.Tx, error) {
	return nil, errors.New("probe: no transactions")
}

type slowCloseDriver struct{ block time.Duration }

func (d slowCloseDriver) Open(string) (driver.Conn, error) { return slowCloseConn{block: d.block}, nil }

const (
	probeDriverName     = "xflow-resource-probe"
	slowCloseDriverName = "xflow-resource-probe-slowclose"
	// slowCloseBlock is far longer than the deadline the Close test gives the
	// pool, so "returned before this elapsed" is unambiguous.
	slowCloseBlock = 3 * time.Second
)

func init() {
	sql.Register(probeDriverName, probeDriver{})
	sql.Register(slowCloseDriverName, slowCloseDriver{block: slowCloseBlock})
}

// grab opens n connections at once and returns a func that releases them all.
// Holding them simultaneously is what forces the idle-limit decision: each
// release after the limit is reached closes a connection instead of parking it.
func grab(t *testing.T, db *sql.DB, n int) func() {
	t.Helper()
	conns := make([]*sql.Conn, 0, n)
	for i := 0; i < n; i++ {
		c, err := db.Conn(context.Background())
		if err != nil {
			t.Fatalf("db.Conn(#%d): %v", i, err)
		}
		conns = append(conns, c)
	}
	return func() {
		for _, c := range conns {
			_ = c.Close()
		}
	}
}

// TestSQLAppliesConfiguredMaxIdleConns pins db.SetMaxIdleConns(cfg.SQL.MaxIdleConns).
//
// The assertion is an exact count rather than "some connections were closed",
// because both the configured value and database/sql's default produce a
// non-zero count and only the exact number tells them apart. With three
// connections held at once and MaxIdleConns=1, releasing them parks one and
// closes two. Drop the SetMaxIdleConns call and the default of 2 parks two and
// closes one — still non-zero, still "connections were closed", and a `> 0`
// assertion would not notice.
func TestSQLAppliesConfiguredMaxIdleConns(t *testing.T) {
	p := NewDefaultResourcePool(types.ResourcePoolConfig{
		SQL: types.SQLPoolConfig{MaxOpenConns: 10, MaxIdleConns: 1, ConnMaxLifetime: time.Hour},
	})
	t.Cleanup(func() { _ = p.Close(context.Background()) })

	db, err := p.SQL(context.Background(), probeDriverName, "probe://idle")
	if err != nil {
		t.Fatalf("SQL(): %v", err)
	}

	release := grab(t, db, 3)
	release()

	got := db.Stats().MaxIdleClosed
	const want = 2 // 3 released, 1 parked (MaxIdleConns=1), 2 closed
	if got != want {
		t.Fatalf("MaxIdleClosed = %d after releasing 3 connections with MaxIdleConns=1, "+
			"want %d: the configured idle limit never reached the *sql.DB, so the pool "+
			"is running on database/sql's default of 2 (which would give %d here)",
			got, want, 1)
	}
}

// TestSQLAppliesConfiguredConnMaxLifetime pins db.SetConnMaxLifetime(cfg.SQL.ConnMaxLifetime).
//
// A connection parked past its lifetime is discarded at the next acquire rather
// than handed back out, and that discard is the only externally visible effect
// of the setting. database/sql's zero default means "never expire", so without
// the call the parked connection is reused and the counter stays at zero.
//
// The lifetime here is 1ms and the wait 50ms, both far below the connection
// cleaner's floor of one second, so the expiry observed below is the one
// detected on the acquire path — not a background sweep whose timing this test
// would otherwise be racing.
func TestSQLAppliesConfiguredConnMaxLifetime(t *testing.T) {
	p := NewDefaultResourcePool(types.ResourcePoolConfig{
		SQL: types.SQLPoolConfig{MaxOpenConns: 10, MaxIdleConns: 5, ConnMaxLifetime: time.Millisecond},
	})
	t.Cleanup(func() { _ = p.Close(context.Background()) })

	db, err := p.SQL(context.Background(), probeDriverName, "probe://lifetime")
	if err != nil {
		t.Fatalf("SQL(): %v", err)
	}

	release := grab(t, db, 1)
	release() // parks one connection

	if n := db.Stats().MaxLifetimeClosed; n != 0 {
		t.Fatalf("MaxLifetimeClosed = %d before the connection could expire, want 0", n)
	}

	time.Sleep(50 * time.Millisecond)

	release = grab(t, db, 1) // must discard the expired one and dial a fresh one
	release()

	got := db.Stats().MaxLifetimeClosed
	if got != 1 {
		t.Fatalf("MaxLifetimeClosed = %d after re-acquiring past a 1ms lifetime, want 1: "+
			"the configured lifetime never reached the *sql.DB, so connections are "+
			"never retired and a failed-over database leaves a pool of dead handles",
			got)
	}
}

// TestCloseReturnsOnContextDeadlineWithoutWaitingForSlowClosers pins the
// `case <-ctx.Done()` arm of Close's select, which its own doc comment sells as
// the reason a caller can bound shutdown: "on timeout the close continues in
// the background."
//
// Every existing Close test hands it a five-second context and resources that
// close instantly, so the ctx arm is never the one that fires. Collapse the
// select to a bare `e := <-errCh` and all of them stay green while Close
// becomes unbounded — a shutdown path whose duration is set by the slowest
// remote endpoint rather than by the caller.
//
// The two assertions are not redundant. Returning the right error proves the
// ctx arm was taken; returning early proves it was taken *instead of* waiting,
// rather than after the close had already finished.
func TestCloseReturnsOnContextDeadlineWithoutWaitingForSlowClosers(t *testing.T) {
	p := NewDefaultResourcePool(types.ResourcePoolConfig{
		SQL: types.SQLPoolConfig{MaxOpenConns: 10, MaxIdleConns: 5, ConnMaxLifetime: time.Hour},
	})

	db, err := p.SQL(context.Background(), slowCloseDriverName, "probe://slowclose")
	if err != nil {
		t.Fatalf("SQL(): %v", err)
	}
	// Park a connection so (*sql.DB).Close has something whose Close blocks.
	release := grab(t, db, 1)
	release()

	const deadline = 150 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()

	start := time.Now()
	err = p.Close(ctx)
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close() error = %v, want it to wrap context.DeadlineExceeded: the "+
			"caller's deadline does not bound shutdown", err)
	}
	// An upper bound on the fast path, not a timing assertion on the slow one:
	// anything at or beyond slowCloseBlock means Close waited for the closer.
	if elapsed >= slowCloseBlock {
		t.Fatalf("Close() returned after %v with a %v deadline, waiting out the %v "+
			"closer: shutdown duration is set by the resource, not by the caller",
			elapsed, deadline, slowCloseBlock)
	}
}
