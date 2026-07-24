package types

import (
	"context"
	"database/sql"
	"testing"

	"google.golang.org/grpc"
)

func TestResourcePoolFromContext_NilWhenAbsent(t *testing.T) {
	if got := ResourcePoolFromContext(context.Background()); got != nil {
		t.Fatalf("ResourcePoolFromContext() = %v, want nil", got)
	}
}

func TestResourcePoolFromContext_NilContext(t *testing.T) {
	var nilContext context.Context
	if got := ResourcePoolFromContext(nilContext); got != nil {
		t.Fatalf("ResourcePoolFromContext(nil) = %v, want nil", got)
	}
}

func TestWithResourcePool_NilIsNoop(t *testing.T) {
	base := context.Background()
	if WithResourcePool(base, nil) != base {
		t.Fatal("WithResourcePool(ctx, nil) should return the original context unchanged")
	}
}

func TestWithResourcePool_RoundtripsValue(t *testing.T) {
	p := &stubPool{}
	ctx := WithResourcePool(context.Background(), p)
	got := ResourcePoolFromContext(ctx)
	if got != p {
		t.Fatal("ResourcePoolFromContext() did not return injected pool")
	}
}

// stubPool is a minimal ResourcePool for context plumbing tests.
type stubPool struct{}

func (*stubPool) SQL(context.Context, string, string) (*sql.DB, error) { return nil, nil }
func (*stubPool) GRPC(context.Context, string, bool, ...grpc.DialOption) (*grpc.ClientConn, error) {
	return nil, nil
}
func (*stubPool) Close(context.Context) error { return nil }
