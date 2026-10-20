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
// instance enrolled 48h ago and (unless a test registers it) absent from the
// directory, so the only thing keeping an instance alive is the pruner's rule.
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
	tests := []struct {
		name       string
		owned      []namespace.Namespace
		wantPruned []string
		wantActive []string
	}{
		// Mutation: scoping the nil case would stop legacy deployments from
		// pruning pools outside a configured set.
		{"nil owned prunes every pool", nil, []string{"runner-local", "runner-plat", "runner-team"}, nil},
		// Mutation: keeping empty entries turns "no scope" into a scope that
		// matches nothing, so the legacy pass silently stops pruning.
		{"only-empty entries are unscoped", nsList(""), []string{"runner-local", "runner-plat", "runner-team"}, nil},
		// Mutation: listing without the scope drains instances that belong to
		// another deployment's pool.
		{"tenant scope prunes its own pool only", nsList("team-a"), []string{"runner-team"}, []string{"runner-local", "runner-plat"}},
		{"other tenant scope", nsList("local-jack"), []string{"runner-local"}, []string{"runner-plat", "runner-team"}},
		{"two tenants", nsList("team-a", "local-jack"), []string{"runner-local", "runner-team"}, []string{"runner-plat"}},
		// Mutation: removing the platform guard drains the platform runner on
		// the LastEnrolledAt fallback, although it is simply not in this
		// deployment's directory.
		{"default scope never drains an absent platform instance", nsList("default"), nil, []string{"runner-local", "runner-plat", "runner-team"}},
		{"unowned scope touches nothing", nsList("other"), nil, []string{"runner-local", "runner-plat", "runner-team"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newPrunerScopeFixture(t)
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

// The guard covers instances the directory cannot see, not instances it can:
// a platform runner that is registered here with a stale heartbeat is still
// idle by the ordinary rule.
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
