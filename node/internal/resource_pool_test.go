package internal

import (
	"context"
	"net"
	"sync"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// poolDriverName must be a real driver. We use the embedded sqlite-shaped
// "register a noop driver" pattern: register once at init via sql.Register —
// but the stdlib doesn't ship one. Instead lean on the built-in driver
// already imported transitively (mysql). We only need sql.Open to succeed; we
// never run a query, so even an unreachable DSN works because sql.Open
// validates only the DSN syntax (not the connection).
const (
	poolDriverName = "mysql"
	poolDSN        = "user:pass@tcp(127.0.0.1:1)/db?timeout=1s"
)

func TestResourcePool_SQLReusesHandle(t *testing.T) {
	p := NewDefaultResourcePool(DefaultResourcePoolConfig())
	t.Cleanup(func() { _ = p.Close() })

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
	p := NewDefaultResourcePool(DefaultResourcePoolConfig())
	t.Cleanup(func() { _ = p.Close() })

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

	p := NewDefaultResourcePool(DefaultResourcePoolConfig())
	t.Cleanup(func() { _ = p.Close() })

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

func TestResourcePool_CloseIdempotent(t *testing.T) {
	p := NewDefaultResourcePool(DefaultResourcePoolConfig())
	if err := p.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if _, err := p.SQL(context.Background(), poolDriverName, poolDSN); err == nil {
		t.Fatal("SQL() after Close should fail")
	}
}

func TestResourcePoolFromContext_NilWhenAbsent(t *testing.T) {
	if got := ResourcePoolFromContext(context.Background()); got != nil {
		t.Fatalf("ResourcePoolFromContext() = %v, want nil", got)
	}
}

func TestResourcePoolFromContext_RoundtripsValue(t *testing.T) {
	p := NewDefaultResourcePool(DefaultResourcePoolConfig())
	t.Cleanup(func() { _ = p.Close() })
	ctx := WithResourcePool(context.Background(), p)
	if got := ResourcePoolFromContext(ctx); got != p {
		t.Fatal("ResourcePoolFromContext() did not return injected pool")
	}
}

func TestWithResourcePool_NilIsNoop(t *testing.T) {
	base := context.Background()
	if WithResourcePool(base, nil) != base {
		t.Fatal("WithResourcePool(ctx, nil) should return the original context unchanged")
	}
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
