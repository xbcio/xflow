package resource

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestSQLReturnsTheOpenErrorInsteadOfDereferencingANilHandle pins
// pool.go:73-76:
//
//	db, err := sql.Open(driver, dsn)
//	if err != nil {
//		return nil, err
//	}
//	db.SetMaxOpenConns(...)
//
// No test in this package has ever passed a driver name that is not
// registered. pool_test.go and pool_defaults_test.go use poolDriverName
// ("mysql"); pool_wiring_test.go uses probeDriverName and slowCloseDriverName,
// both registered in that file's init. So the error check has never been
// evaluated as true.
//
// It is not a formality. sql.Open returns a nil *sql.DB together with its
// error — measured, not assumed: sql.Open("no-such-driver", ...) returns
// (nil, `sql: unknown driver "no-such-driver" (forgotten import?)`). The very
// next line calls a method on that handle, so without the check this is a nil
// dereference, not a late failure.
//
// The driver name is config-controlled. node/internal/action/database.go:108
// reads it straight off the credential (`driver := cast.ToString(cred["driver"])`,
// defaulting to "mysql" only when empty) and mysql is the only driver imported
// anywhere in the binary. A typo, or a credential copy-pasted from a stack that
// uses postgres, produces exactly this. There is no recover() on the node
// execution path — the only three in the repo are service/runner's activation
// tracker and acker and engine/hooks.go, none of which wrap a node handler — so
// the panic is not contained to one execution. It takes the runner process down.
//
// Stated plainly so the result is not over-read: under the mutation that
// removes this check the failure arrives as a panic, which aborts the whole
// package binary. That proves the line is load-bearing, but it means the
// assertion below is never reached in that run. The second half of the test —
// that a failed open leaves nothing cached under the key — is what has teeth
// against the weaker mutation of caching before checking.
func TestSQLReturnsTheOpenErrorInsteadOfDereferencingANilHandle(t *testing.T) {
	p := NewDefaultResourcePool(types.DefaultResourcePoolConfig())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = p.Close(ctx)
	})

	const badDriver = "xflow-no-such-driver"
	db, err := p.SQL(context.Background(), badDriver, "whatever://host/db")
	if err == nil {
		t.Fatalf("SQL(%q) returned no error: an unregistered driver name reached "+
			"the pool and was accepted, so the caller now holds a handle that "+
			"cannot open anything", badDriver)
	}
	if db != nil {
		t.Fatal("SQL() returned both a handle and an error; the caller cannot tell " +
			"which one to believe")
	}

	// A failed open must not leave anything behind under the key. If it did, the
	// second call would hit the `if db, ok := p.dbs[key]; ok` fast path at
	// pool.go:70 and hand back whatever was cached — with a nil error, which is
	// worse than the first failure because now nothing reports a problem.
	db, err = p.SQL(context.Background(), badDriver, "whatever://host/db")
	if err == nil {
		t.Fatal("a second SQL() with the same unregistered driver succeeded: the " +
			"failed open was cached, so the pool now answers a broken key with a " +
			"nil error forever")
	}
	if db != nil {
		t.Fatal("a second SQL() with the same unregistered driver returned a handle")
	}

	// The other direction: a registered driver must still work, so a mutation
	// that fails every open cannot pass either.
	good, err := p.SQL(context.Background(), probeDriverName, "probe://open-failure")
	if err != nil {
		t.Fatalf("SQL(%q) error = %v, want nil", probeDriverName, err)
	}
	if good == nil {
		t.Fatal("SQL() returned a nil handle with a nil error")
	}
}

// TestGRPCDoesNotCacheAConnectionItFailedToBuild pins pool.go:100-103:
//
//	conn, err := grpc.NewClient(host, dialOpts...)
//	if err != nil {
//		return nil, err
//	}
//	p.conns[key] = conn
//
// Every GRPC call in this package's tests passes
// grpc.WithTransportCredentials(insecure.NewCredentials()) and a real listener
// address, so grpc.NewClient has never returned an error here.
//
// Finding the trigger took measuring rather than guessing, and the answer
// narrows the claim: grpc.NewClient does not validate the target. It returned a
// nil error for every malformed target I tried — "", "bogus://x", ":::",
// "dns:///", "passthrough://" — because connection setup is lazy. The one
// thing that does make it fail is a missing transport-credentials option:
// "grpc: no transport security set". node/internal/action/grpc.go:135-139
// always supplies one, so the in-repo production caller does not reach this
// branch. It is reachable through types.ResourcePool, which is part of the
// public surface a custom node dials through.
//
// What makes it worth a test anyway is the cache key. pool.go:85 keys on
// host + "|" + boolFlag(secure) and does not include the dial options. So with
// the check removed, one caller that forgets credentials writes a nil
// *grpc.ClientConn under "host|0" — and every later caller for that same host,
// including ones that pass credentials correctly, is handed that nil out of the
// fast path at pool.go:91 with a nil error. The first RPC on it dereferences
// nil. One malformed call poisons the host for the rest of the process, and the
// call that crashes is not the one that was wrong.
//
// How the mutation actually fails, measured rather than predicted, because my
// first guess was wrong. The assertions below do fire — under the mutation the
// run prints this file's line 142 diagnostic, so the teeth are real. But the
// run does not end in a clean `--- FAIL`. Cleanup calls p.Close, Close hands
// the cached conns to closeAll in a goroutine (pool.go:138), and
// (*grpc.ClientConn).Close on a nil receiver dereferences at
// clientconn.go:1199. A panic in a goroutine cannot be recovered and kills the
// binary before `go test` flushes its per-test result lines, so without -v the
// failure is visible only as a panic with no FAIL line above it.
//
// That is worth stating twice over. It means a harness that counts `^--- FAIL`
// will score this mutation as zero failures even though the assertion caught
// it — and it means the poisoned cache entry is not merely a latent hazard for
// the next RPC: it also takes the process down at shutdown, in a goroutine
// where nothing can intercept it.
func TestGRPCDoesNotCacheAConnectionItFailedToBuild(t *testing.T) {
	srv := startNoopGRPC(t)

	p := NewDefaultResourcePool(types.DefaultResourcePoolConfig())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = p.Close(ctx)
	})

	// No transport credentials: the only input that makes grpc.NewClient itself
	// fail rather than defer the failure to the first RPC.
	conn, err := p.GRPC(context.Background(), srv.addr, false)
	if err == nil {
		t.Fatal("GRPC() without transport credentials returned no error: " +
			"grpc.NewClient's own rejection was discarded")
	}
	if conn != nil {
		t.Fatal("GRPC() returned both a connection and an error")
	}

	// Same host, same secure flag — the same cache key — but a correct call.
	// It must build its own connection, not inherit whatever the failed call
	// left behind.
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	good, err := p.GRPC(context.Background(), srv.addr, false, opts...)
	if err != nil {
		t.Fatalf("GRPC() with credentials error = %v, want nil", err)
	}
	if good == nil {
		t.Fatalf("GRPC() handed back a nil connection with a nil error for host %q: "+
			"the earlier credential-less call cached its failure under the same "+
			"key (host|secure, which does not include the dial options), so every "+
			"correct caller for this host now receives nil and dereferences it on "+
			"its first RPC", srv.addr)
	}

	// And the pool must still be reusing connections for the good key, so the
	// assertion above is about a real cache entry rather than about GRPC having
	// stopped caching altogether.
	again, err := p.GRPC(context.Background(), srv.addr, false, opts...)
	if err != nil {
		t.Fatalf("second GRPC() error = %v", err)
	}
	if again != good {
		t.Fatal("GRPC() built a second connection for the same (host, secure): the " +
			"nil-cache assertion above would be satisfied by a pool that never " +
			"caches anything")
	}
}
