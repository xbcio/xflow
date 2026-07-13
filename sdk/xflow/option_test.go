package xflow

import (
	"context"
	"database/sql"
	"testing"

	"github.com/xbcio/xflow/types"
	"google.golang.org/grpc"
)

// stubPool is a sentinel ResourcePool used to assert that resolveResourcePool
// returns the caller-supplied pool verbatim. Its SQL/GRPC methods are never
// invoked by these tests; only identity (==) is asserted.
type stubPool struct{ id string }

func (s stubPool) SQL(context.Context, string, string) (*sql.DB, error) { return nil, nil }
func (s stubPool) GRPC(context.Context, string, bool, ...grpc.DialOption) (*grpc.ClientConn, error) {
	return nil, nil
}
func (s stubPool) Close(context.Context) error { return nil }

// TestResolveResourcePool_Default pins the contract that an empty engineConfig
// (NewLocal with no pool options) yields the SDK-managed default pool. This is
// the regression guard for the silent deletion of WithResourcePoolConfig that
// Task 5 introduced and bd48159 restored.
func TestResolveResourcePool_Default(t *testing.T) {
	cfg := &engineConfig{}
	pool := resolveResourcePool(cfg)
	if pool == nil {
		t.Fatal("resolveResourcePool(empty cfg) = nil, want non-nil default pool")
	}
	// The default pool is built by resource.NewDefaultResourcePool; the concrete
	// type is unexported, so we only assert non-nil and that Close is callable.
	t.Cleanup(func() { _ = pool.Close(context.Background()) })
}

// TestResolveResourcePool_ExplicitPool pins the contract that
// WithResourcePool(p) makes resolveResourcePool return p verbatim — including
// when p is a custom stub. This guards against a future change that silently
// swaps a caller-supplied pool for the default.
func TestResolveResourcePool_ExplicitPool(t *testing.T) {
	want := stubPool{id: "explicit"}
	cfg := &engineConfig{resourcePool: want, resourcePoolSet: true}
	got := resolveResourcePool(cfg)
	if got != want {
		t.Fatalf("resolveResourcePool(explicit) = %T(%[1]v), want the same stubPool instance", got)
	}
}

// TestResolveResourcePool_CustomConfig pins the contract that
// WithResourcePoolConfig(cfg) produces a non-nil pool built from the supplied
// config. We cannot easily inspect the config the pool was built with without
// reaching into unexported fields, so we assert non-nil and that the pool is
// usable (Close-able). The load-bearing assertion is "non-nil" — the empty-cfg
// default and the explicit-pool case cover the other branches.
func TestResolveResourcePool_CustomConfig(t *testing.T) {
	cfg := &engineConfig{
		resourcePoolConfig: &types.ResourcePoolConfig{
			SQL: types.SQLPoolConfig{MaxOpenConns: 99, MaxIdleConns: 9},
		},
	}
	pool := resolveResourcePool(cfg)
	if pool == nil {
		t.Fatal("resolveResourcePool(custom config) = nil, want non-nil pool built from config")
	}
	t.Cleanup(func() { _ = pool.Close(context.Background()) })
}

// TestResolveResourcePool_NilOptOut pins the contract that
// WithResourcePool(nil) is an explicit opt-out: resolveResourcePool returns nil
// so resource-aware nodes (DatabaseNode/GRPCNode) error at runtime rather than
// falling back to a per-call resource. This is the branch that was most at risk
// during the Task 5 refactor.
func TestResolveResourcePool_NilOptOut(t *testing.T) {
	cfg := &engineConfig{resourcePool: nil, resourcePoolSet: true}
	got := resolveResourcePool(cfg)
	if got != nil {
		t.Fatalf("resolveResourcePool(opt-out) = %T, want nil", got)
	}
}

// TestResolveResourcePool_ExplicitPoolBeatsConfig pins the precedence rule
// documented on WithResourcePoolConfig: when both WithResourcePool and
// WithResourcePoolConfig are set, the explicit pool wins and the config is
// ignored. This matches the doc comment on option.go.
func TestResolveResourcePool_ExplicitPoolBeatsConfig(t *testing.T) {
	want := stubPool{id: "explicit-over-config"}
	cfg := &engineConfig{
		resourcePool:       want,
		resourcePoolSet:    true,
		resourcePoolConfig: &types.ResourcePoolConfig{SQL: types.SQLPoolConfig{MaxOpenConns: 99}},
	}
	got := resolveResourcePool(cfg)
	if got != want {
		t.Fatalf("resolveResourcePool(explicit+config) = %T, want the explicit stubPool", got)
	}
}

// TestNewLocal_OptionsDoNotPanic is a smoke test that the public options
// (WithResourcePool, WithResourcePoolConfig) are accepted by NewLocal without
// error across all three branches. The detailed three-way logic is pinned by
// the resolveResourcePool unit tests above; this guards the public surface
// stays wired through NewLocal.
func TestNewLocal_OptionsDoNotPanic(t *testing.T) {
	cases := []struct {
		name string
		opt  Option
	}{
		{name: "default", opt: nil},
		{name: "explicit-pool", opt: WithResourcePool(stubPool{id: "newlocal"})},
		{name: "config-pool", opt: WithResourcePoolConfig(types.ResourcePoolConfig{})},
		{name: "opt-out", opt: WithResourcePool(nil)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var opts []Option
			if tc.opt != nil {
				opts = append(opts, tc.opt)
			}
			eng, err := NewLocal(opts...)
			if err != nil {
				t.Fatalf("NewLocal(%s) error = %v", tc.name, err)
			}
			eng.Stop()
		})
	}
}

// compile-time guard: stubPool must satisfy types.ResourcePool.
var _ types.ResourcePool = stubPool{}
