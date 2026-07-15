package resource

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/types"
	_ "github.com/go-sql-driver/mysql"
	"google.golang.org/grpc"
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
