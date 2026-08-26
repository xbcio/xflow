package xflow

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/xbcio/xflow/types"
	"google.golang.org/grpc"
)

// poolProbeDriver is a registered SQL driver that never connects, used only so
// a test can ask a resource pool for a *sql.DB and read back the limits the
// pool applied to it. sql.Open does not dial, so Open below is never reached.
const poolProbeDriver = "xflow-resource-pool-probe"

type probeDriver struct{}

func (probeDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("probe driver never connects")
}

func init() { sql.Register(poolProbeDriver, probeDriver{}) }

// sqlPoolMaxOpenConns reports the MaxOpenConns a pool actually applied, by
// taking a handle from it and reading the limit off the handle.
//
// This exists because the obvious assertion — that the pool is non-nil — is not
// one. resource.NewDefaultResourcePool returns a non-nil pool for every input
// including the ones that ignored the caller's config, so a test asserting only
// non-nil stays green when the line threading the config through is deleted,
// which is the line those tests are named after. The pool's config field is
// unexported and types.ResourcePool has no accessor, so the config has to be
// observed where it lands: on the *sql.DB.
//
// Reading it off the handle is also the stronger claim. An accessor would prove
// the value was stored; this proves it reached the connection limit that is the
// entire point of configuring it.
func sqlPoolMaxOpenConns(t *testing.T, pool types.ResourcePool) int {
	t.Helper()
	db, err := pool.SQL(context.Background(), poolProbeDriver, "probe-dsn")
	if err != nil {
		t.Fatalf("pool.SQL(%q): %v", poolProbeDriver, err)
	}
	return db.Stats().MaxOpenConnections
}

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
// WithResourcePoolConfig(cfg) produces a pool built from the supplied config —
// not merely a pool.
//
// The distinction is the whole test. resolveResourcePool ends every non-opt-out
// branch at resource.NewDefaultResourcePool, which returns non-nil whatever it
// is handed, so "the pool is non-nil" holds just as well for a version that
// dropped `poolCfg = *cfg.resourcePoolConfig` and defaulted everything. The
// assertion has to be on a value the caller chose.
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

	// 99, not the 25 normalizeConfig substitutes for an unset value, so a pool
	// that ignored the config fails here rather than passing on non-nil.
	if got := sqlPoolMaxOpenConns(t, pool); got != 99 {
		t.Errorf("pool MaxOpenConns = %d, want 99 from the supplied config "+
			"(25 means the config was dropped and defaults were used)", got)
	}
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
