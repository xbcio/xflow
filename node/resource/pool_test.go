package resource

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/xbcio/xflow/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

// poolDriverName must be a real driver. We use mysql, imported for its
// side-effect driver registration. We only need sql.Open to succeed; we
// never run a query, so even an unreachable DSN works because sql.Open
// validates only the DSN syntax (not the connection).
const (
	poolDriverName = "mysql"
	poolDSN        = "user:pass@tcp(127.0.0.1:1)/db?timeout=1s"
)

func TestResourcePool_SQLReusesHandle(t *testing.T) {
	p := NewDefaultResourcePool(types.DefaultResourcePoolConfig())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = p.Close(ctx)
	})

	db1, err := p.SQL(context.Background(), poolDriverName, poolDSN)
	if err != nil {
		t.Fatalf("SQL() error = %v", err)
	}
	db2, err := p.SQL(context.Background(), poolDriverName, poolDSN)
	if err != nil {
		t.Fatalf("SQL() error = %v", err)
	}
	if db1 != db2 {
		t.Fatal("SQL() returned different handles for same (driver, dsn) — pool not reusing")
	}
}

func TestResourcePool_SQLConcurrentSingleInit(t *testing.T) {
	p := NewDefaultResourcePool(types.DefaultResourcePoolConfig())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = p.Close(ctx)
	})

	const racers = 32
	results := make(chan any, racers)
	var wg sync.WaitGroup
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			db, err := p.SQL(context.Background(), poolDriverName, poolDSN)
			if err != nil {
				results <- err
				return
			}
			results <- db
		}()
	}
	wg.Wait()
	close(results)

	var first any
	for r := range results {
		if err, ok := r.(error); ok {
			t.Fatalf("racer returned error %v", err)
		}
		if first == nil {
			first = r
			continue
		}
		if first != r {
			t.Fatal("concurrent SQL() calls produced distinct handles — init not single-flight")
		}
	}
}

func TestResourcePool_GRPCReusesConnection(t *testing.T) {
	srv := startNoopGRPC(t)

	p := NewDefaultResourcePool(types.DefaultResourcePoolConfig())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = p.Close(ctx)
	})

	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	conn1, err := p.GRPC(context.Background(), srv.addr, false, opts...)
	if err != nil {
		t.Fatalf("GRPC() error = %v", err)
	}
	conn2, err := p.GRPC(context.Background(), srv.addr, false, opts...)
	if err != nil {
		t.Fatalf("GRPC() error = %v", err)
	}
	if conn1 != conn2 {
		t.Fatal("GRPC() returned different connections for same (host, secure) — pool not reusing")
	}
}

// TestResourcePool_GRPCKeyIncludesSecureFlag covers the other half of the
// cache key at pool.go:85 (`key := host + "|" + boolFlag(secure)`). The test
// above only ever passes secure=false, so it cannot tell a key that includes
// the secure flag from one that is just the bare host: dropping
// `+ "|" + boolFlag(secure)` entirely would still pass it, because every call
// in that test shares the same host AND the same secure value.
//
// Here the host is held fixed and only secure changes between the two calls.
//
// What makes this more than a cache-key nit is the one production caller.
// node/internal/action/grpc.go:135-142 derives BOTH things from the same
// useTLS value: it picks credentials.NewTLS or insecure.NewCredentials for
// the dial options, then passes that same bool to the pool as `secure`. The
// pool applies dial options only when it actually dials (pool.go:93-100) and
// returns the cached conn untouched otherwise. So if the key stopped
// distinguishing the flag, a workflow with tls:false to some host would cache
// a plaintext connection, and a later node with tls:true to that same host
// would be handed it back — its TLS credentials silently never applied. That
// is a transport downgrade, not a missed optimisation.
//
// Both calls here pass insecure credentials deliberately: the pool does not
// derive credentials from `secure` at all (it is a key input only), so
// varying the opts as well would confuse which input the assertion pins.
func TestResourcePool_GRPCKeyIncludesSecureFlag(t *testing.T) {
	srv := startNoopGRPC(t)

	p := NewDefaultResourcePool(types.DefaultResourcePoolConfig())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = p.Close(ctx)
	})

	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	insecureConn, err := p.GRPC(context.Background(), srv.addr, false, opts...)
	if err != nil {
		t.Fatalf("GRPC(secure=false) error = %v", err)
	}
	secureConn, err := p.GRPC(context.Background(), srv.addr, true, opts...)
	if err != nil {
		t.Fatalf("GRPC(secure=true) error = %v", err)
	}
	if insecureConn == secureConn {
		t.Fatal("GRPC() returned the SAME connection for secure=false and secure=true " +
			"at the same host: the cache key does not actually distinguish the " +
			"secure flag, so a secure request silently reuses a plaintext connection")
	}
}

func TestResourcePool_CloseIdempotent(t *testing.T) {
	p := NewDefaultResourcePool(types.DefaultResourcePoolConfig())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.Close(ctx); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := p.Close(ctx); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if _, err := p.SQL(context.Background(), poolDriverName, poolDSN); err == nil {
		t.Fatal("SQL() after Close should fail")
	}
}

func TestResourcePool_GRPCAfterCloseFails(t *testing.T) {
	p := NewDefaultResourcePool(types.DefaultResourcePoolConfig())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.Close(ctx); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if _, err := p.GRPC(context.Background(), "127.0.0.1:0", false, opts...); err == nil {
		t.Fatal("GRPC() after Close should fail")
	}
}

// TestResourcePool_ConcurrentGRPCAndClose exercises the race between GRPC
// callers and Close. Must pass under `go test -race`.
func TestResourcePool_ConcurrentGRPCAndClose(t *testing.T) {
	srv := startNoopGRPC(t)
	p := NewDefaultResourcePool(types.DefaultResourcePoolConfig())

	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}

	const racers = 32
	var wg sync.WaitGroup
	wg.Add(racers + 1)

	go func() {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = p.Close(ctx)
	}()

	for i := 0; i < racers; i++ {
		go func() {
			defer wg.Done()
			_, _ = p.GRPC(context.Background(), srv.addr, false, opts...)
		}()
	}

	wg.Wait()
}

// TestResourcePool_ConcurrentSQLAndClose exercises the race between SQL
// callers and Close. Must pass under `go test -race`.
func TestResourcePool_ConcurrentSQLAndClose(t *testing.T) {
	p := NewDefaultResourcePool(types.DefaultResourcePoolConfig())

	const racers = 32
	var wg sync.WaitGroup
	wg.Add(racers + 1)

	go func() {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = p.Close(ctx)
	}()

	for i := 0; i < racers; i++ {
		go func() {
			defer wg.Done()
			_, _ = p.SQL(context.Background(), poolDriverName, poolDSN)
		}()
	}

	wg.Wait()
}

// ---------------------------------------------------------------------------
// gRPC test fixture
// ---------------------------------------------------------------------------

type noopGRPC struct {
	addr string
	stop func()
}

func startNoopGRPC(t *testing.T) noopGRPC {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}
	srv := grpc.NewServer()
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() {
		srv.Stop()
	})
	return noopGRPC{addr: lis.Addr().String(), stop: srv.Stop}
}

// TestResourcePoolCloseIdempotent asserts that Close is safe to call more
// than once (regression 2026-07-21: double-close from deprecated Bind path).
// The second call must return nil and not panic.
func TestResourcePoolCloseIdempotent(t *testing.T) {
	p := NewDefaultResourcePool(types.DefaultResourcePoolConfig())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.Close(ctx); err != nil {
		t.Fatalf("first Close error = %v", err)
	}
	if err := p.Close(ctx); err != nil {
		t.Fatalf("second Close error = %v, want nil (idempotent)", err)
	}
	if err := p.Close(ctx); err != nil {
		t.Fatalf("third Close error = %v, want nil (idempotent)", err)
	}
}

// TestResourcePool_CloseActuallyClosesCachedResources checks the thing every
// other Close test takes on faith: that Close releases the resources it
// evicted from the maps.
//
// The existing coverage is all downstream of the p.closed flag —
// CloseIdempotent, GRPCAfterCloseFails and the two concurrency tests observe
// only that later calls are rejected — and TestCloseAllJoinsErrors calls
// closeAll directly rather than through Close. So replacing either
// `closeAll(dbs)` or `closeAll(conns)` in Close with a bare nil leaks every
// cached *sql.DB and *grpc.ClientConn (each with its own connection pool and
// background goroutines) while the whole package stays green: the pool still
// reports itself closed, so nothing downstream notices.
//
// Both resources expose their own post-close state, which is what makes this
// observable without reaching into the pool's internals: a closed *sql.DB
// rejects every call with "sql: database is closed" instead of attempting to
// dial, and a closed *grpc.ClientConn reports connectivity.Shutdown.
func TestResourcePool_CloseActuallyClosesCachedResources(t *testing.T) {
	// database/sql exports no way to ask a *sql.DB whether it is closed:
	// sql.ErrConnDone belongs to Conn/Tx, and the DB-level error (errDBClosed)
	// is unexported. Matching its message is the only handle available, and it
	// is a stable, documented string.
	const dbClosedMsg = "sql: database is closed"

	srv := startNoopGRPC(t)
	p := NewDefaultResourcePool(types.DefaultResourcePoolConfig())

	db, err := p.SQL(context.Background(), poolDriverName, poolDSN)
	if err != nil {
		t.Fatalf("SQL() error = %v", err)
	}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	conn, err := p.GRPC(context.Background(), srv.addr, false, opts...)
	if err != nil {
		t.Fatalf("GRPC() error = %v", err)
	}
	// Before Close the handle must NOT look closed, or the assertions below
	// would hold for a pool that never cached anything in the first place.
	// poolDSN is deliberately unreachable, so an open handle fails here with a
	// dial error -- a different failure, not the absence of one.
	if err := db.PingContext(context.Background()); err == nil || strings.Contains(err.Error(), dbClosedMsg) {
		t.Fatalf("handle already reports closed before Close(): %v", err)
	}
	if state := conn.GetState(); state == connectivity.Shutdown {
		t.Fatal("connection already reports Shutdown before Close()")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.Close(ctx); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if err := db.PingContext(context.Background()); err == nil || !strings.Contains(err.Error(), dbClosedMsg) {
		t.Errorf("after Close, db.PingContext() = %v, want %q: the cached *sql.DB "+
			"was evicted from the pool but never closed, leaking its connection "+
			"pool and connectionOpener goroutine for the process lifetime",
			err, dbClosedMsg)
	}
	if state := conn.GetState(); state != connectivity.Shutdown {
		t.Errorf("after Close, conn.GetState() = %v, want Shutdown: the cached "+
			"*grpc.ClientConn was evicted from the pool but never closed, leaking "+
			"its transport and keepalive goroutines", state)
	}
}

// fakeCloser is a closer whose Close returns a configured sentinel error.
type fakeCloser struct{ err error }

func (f fakeCloser) Close() error { return f.err }

var errCloseSentinelA = errors.New("close A failed")
var errCloseSentinelB = errors.New("close B failed")

// TestCloseAllJoinsErrors proves closeAll aggregates every close error (not
// just the first) and surfaces each via errors.Is, so a partial resource-pool
// shutdown no longer silently drops DB/gRPC close failures (A1 residual).
func TestCloseAllJoinsErrors(t *testing.T) {
	items := map[string]closer{
		"a": fakeCloser{err: errCloseSentinelA},
		"b": fakeCloser{err: errCloseSentinelB},
		"c": fakeCloser{err: nil},
	}
	err := closeAll(items)
	if err == nil {
		t.Fatal("closeAll returned nil, want joined error")
	}
	if !errors.Is(err, errCloseSentinelA) {
		t.Fatalf("joined error does not wrap errCloseSentinelA: %v", err)
	}
	if !errors.Is(err, errCloseSentinelB) {
		t.Fatalf("joined error does not wrap errCloseSentinelB: %v", err)
	}
}
