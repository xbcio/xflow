package control

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
)

var prunerScopeNow = time.Date(2026, 10, 15, 3, 0, 0, 0, time.UTC)

// prunerScopeFixture holds three pools of different owners, each with one
// instance enrolled 48h ago. Instances are absent from the directory by
// default; a test that expects an instance to be judged idle must register it
// as stale explicitly.
type prunerScopeFixture struct {
	pools      RunnerPoolStore
	identities IssuedIdentityStore
	directory  *MemoryRunnerDirectory
}

func newPrunerScopeFixture(t *testing.T) prunerScopeFixture {
	t.Helper()
	f := prunerScopeFixture{
		pools:      NewMemoryRunnerPoolStore(),
		identities: NewMemoryIssuedIdentityStore(),
		directory:  NewMemoryRunnerDirectory(),
	}
	created := prunerScopeNow.Add(-72 * time.Hour)
	for _, p := range []RunnerPool{
		{ID: "pool-team", Name: "team", OwnerKind: store.PoolOwnerTenant, OwnerNamespace: "team-a", CreatedAt: created},
		{ID: "pool-local", Name: "local", OwnerKind: store.PoolOwnerTenant, OwnerNamespace: "local-jack", CreatedAt: created},
		{ID: "pool-plat", Name: "plat", OwnerKind: store.PoolOwnerPlatform, CreatedAt: created},
	} {
		if err := f.pools.CreatePool(context.Background(), p); err != nil {
			t.Fatalf("CreatePool(%s): %v", p.ID, err)
		}
	}
	enrollPrunerTestInstance(t, f.pools, "pool-team", "sys-team", "runner-team", prunerScopeNow.Add(-48*time.Hour))
	enrollPrunerTestInstance(t, f.pools, "pool-local", "sys-local", "runner-local", prunerScopeNow.Add(-48*time.Hour))
	enrollPrunerTestInstance(t, f.pools, "pool-plat", "sys-plat", "runner-plat", prunerScopeNow.Add(-48*time.Hour))
	return f
}

func (f prunerScopeFixture) pruner(owned []namespace.Namespace) *RunnerInstancePruner {
	return NewRunnerInstancePruner(RunnerInstancePrunerConfig{
		Pools: f.pools, IssuedIdentities: f.identities, Directory: f.directory,
		Leader: prunerTestLeader(true), IdleTTL: 24 * time.Hour,
		Clock: func() time.Time { return prunerScopeNow }, Namespaces: owned,
	})
}

// registerStale puts each instance in the directory with a heartbeat older
// than the idle TTL, which is the only condition that makes it idle.
func (f prunerScopeFixture) registerStale(t *testing.T, runnerIDs ...string) {
	t.Helper()
	for _, runnerID := range runnerIDs {
		registerPrunerTestRunner(t, f.directory, runnerID, prunerScopeNow.Add(-25*time.Hour), 1)
	}
}

func (f prunerScopeFixture) runnerIDs(t *testing.T, state store.InstanceState) []string {
	t.Helper()
	instances, err := f.pools.ListInstancesByState(context.Background(), state)
	if err != nil {
		t.Fatalf("ListInstancesByState(%s): %v", state, err)
	}
	ids := make([]string, 0, len(instances))
	for _, in := range instances {
		ids = append(ids, in.RunnerID)
	}
	sort.Strings(ids)
	return ids
}

func TestRunnerInstancePrunerScope(t *testing.T) {
	all := []string{"runner-team", "runner-local", "runner-plat"}
	tests := []struct {
		name       string
		owned      []namespace.Namespace
		register   []string
		wantPruned []string
		wantActive []string
	}{
		// Mutation: scoping the nil case would stop legacy deployments from
		// pruning pools outside a configured set.
		{"nil owned prunes every pool", nil, all, []string{"runner-local", "runner-plat", "runner-team"}, nil},
		// Mutation: keeping empty entries turns "no scope" into a scope that
		// matches nothing, so the legacy pass silently stops pruning.
		{"only-empty entries are unscoped", nsList(""), all, []string{"runner-local", "runner-plat", "runner-team"}, nil},
		// Mutation: listing without the scope drains instances that belong to
		// another deployment's pool.
		{"tenant scope prunes its own pool only", nsList("team-a"), all, []string{"runner-team"}, []string{"runner-local", "runner-plat"}},
		{"other tenant scope", nsList("local-jack"), all, []string{"runner-local"}, []string{"runner-plat", "runner-team"}},
		{"two tenants", nsList("team-a", "local-jack"), all, []string{"runner-local", "runner-team"}, []string{"runner-plat"}},
		// Mutation: draining on absence would revoke a live platform runner. The
		// pool belongs to this deployment, but a missing directory record is
		// unknown, never idle.
		{"default scope never drains an absent platform instance", nsList("default"), []string{"runner-team", "runner-local"}, nil, []string{"runner-local", "runner-plat", "runner-team"}},
		// The incident canary ran a scoped tenant directory: the live runner in
		// this deployment's own tenant pool was absent from a freshly replaced
		// store. Absence must not drain it, even under a tenant scope whose
		// directory is authoritative-looking. The other two are registered stale
		// so an unscoped fallback would be caught as well.
		{"tenant scope never drains an absent tenant instance", nsList("team-a"), []string{"runner-local", "runner-plat"}, nil, []string{"runner-local", "runner-plat", "runner-team"}},
		{"unowned scope touches nothing", nsList("other"), all, nil, []string{"runner-local", "runner-plat", "runner-team"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newPrunerScopeFixture(t)
			f.registerStale(t, tc.register...)
			if err := f.pruner(tc.owned).Sweep(context.Background()); err != nil {
				t.Fatalf("Sweep: %v", err)
			}
			if got := f.runnerIDs(t, store.InstancePruned); !reflect.DeepEqual(got, orEmpty(tc.wantPruned)) {
				t.Fatalf("pruned = %v, want %v", got, tc.wantPruned)
			}
			if got := f.runnerIDs(t, store.InstanceActive); !reflect.DeepEqual(got, orEmpty(tc.wantActive)) {
				t.Fatalf("active = %v, want %v", got, tc.wantActive)
			}
		})
	}
}

func orEmpty(in []string) []string {
	if in == nil {
		return []string{}
	}
	return in
}

// Absence is unknown, but a present stale record is still idle evidence: a
// platform runner that is registered here with a stale heartbeat is still idle
// by the ordinary rule.
func TestRunnerInstancePrunerScopeStillPrunesStalePlatformRunnerInDirectory(t *testing.T) {
	f := newPrunerScopeFixture(t)
	registerPrunerTestRunner(t, f.directory, "runner-plat", prunerScopeNow.Add(-25*time.Hour), 1)
	if err := f.pruner(nsList("default")).Sweep(context.Background()); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if got := f.runnerIDs(t, store.InstancePruned); !reflect.DeepEqual(got, []string{"runner-plat"}) {
		t.Fatalf("pruned = %v, want [runner-plat]", got)
	}
}

// Mutation: listing the draining pass unscoped lets a scoped pruner finish
// another deployment's saga (revoke its identity, remove its directory state).
func TestRunnerInstancePrunerScopeAppliesToDrainingPass(t *testing.T) {
	f := newPrunerScopeFixture(t)
	if err := f.pools.TransitionInstance(context.Background(), "runner-local", store.InstanceActive, store.InstanceDraining, prunerScopeNow); err != nil {
		t.Fatalf("TransitionInstance: %v", err)
	}
	if err := f.pruner(nsList("team-a")).Sweep(context.Background()); err != nil {
		t.Fatalf("scoped Sweep: %v", err)
	}
	if got := f.runnerIDs(t, store.InstanceDraining); !reflect.DeepEqual(got, []string{"runner-local"}) {
		t.Fatalf("draining after scoped sweep = %v, want [runner-local] untouched", got)
	}
	if err := f.pruner(nil).Sweep(context.Background()); err != nil {
		t.Fatalf("unscoped Sweep: %v", err)
	}
	if got := f.runnerIDs(t, store.InstanceDraining); len(got) != 0 {
		t.Fatalf("draining after unscoped sweep = %v, want none", got)
	}
}

// unscopableRunnerPoolStore hides the scoped lister of the wrapped store.
type unscopableRunnerPoolStore struct{ RunnerPoolStore }

// Mutation: falling back to the unscoped list when the store cannot scope
// drains every deployment's instances.
func TestRunnerInstancePrunerScopeWithoutScopedStoreFailsClosed(t *testing.T) {
	f := newPrunerScopeFixture(t)
	f.pools = unscopableRunnerPoolStore{f.pools}
	// Register all three as stale first: a silent fallback to the unscoped list
	// would drain them, so the "untouched" assertion below is not a no-op.
	f.registerStale(t, "runner-team", "runner-local", "runner-plat")
	err := f.pruner(nsList("team-a")).Sweep(context.Background())
	if !errors.Is(err, ErrRunnerInstanceScopeUnsupported) {
		t.Fatalf("Sweep error = %v, want ErrRunnerInstanceScopeUnsupported", err)
	}
	if got := f.runnerIDs(t, store.InstanceActive); len(got) != 3 {
		t.Fatalf("active = %v, want all three untouched", got)
	}
	// The legacy pass never needed the capability.
	if err := f.pruner(nil).Sweep(context.Background()); err != nil {
		t.Fatalf("unscoped Sweep: %v", err)
	}
	if got := f.runnerIDs(t, store.InstancePruned); len(got) != 3 {
		t.Fatalf("pruned = %v, want all three", got)
	}
}
