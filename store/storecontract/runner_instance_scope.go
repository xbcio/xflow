package storecontract

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
)

// ScopedRunnerPoolStore is what RunScopedRunnerInstanceListerContract needs
// from an implementation: the pool store plus the scoped lifecycle listing.
type ScopedRunnerPoolStore interface {
	store.RunnerPoolStore
	store.ScopedRunnerInstanceLister
}

// RunScopedRunnerInstanceListerContract holds every ScopedRunnerInstanceLister
// to the same scoping behavior: instances are matched through their pool's
// owner, platform pools belong to the default namespace's deployment alone, a
// soft-deleted pool's instances stay reachable, and an empty set fails closed.
// factory must return an empty store on every call.
func RunScopedRunnerInstanceListerContract(t *testing.T, factory func(t *testing.T) ScopedRunnerPoolStore) {
	t.Helper()
	ctx := context.Background()
	t0 := time.Date(2026, 10, 15, 1, 0, 0, 0, time.UTC)
	all := store.OwnerScope{All: true}

	// The fixture. Enrolled times differ per instance so the oldest-first order
	// is observable; d1 belongs to a pool that is soft-deleted afterwards.
	type inst struct {
		pool, system, runner string
		at                   time.Time
	}
	pools := []store.RunnerPool{
		{ID: "p-default", Name: "p-default", OwnerKind: store.PoolOwnerTenant, OwnerNamespace: "default"},
		{ID: "p-team-a", Name: "p-team-a", OwnerKind: store.PoolOwnerTenant, OwnerNamespace: "team-a"},
		{ID: "p-local", Name: "p-local", OwnerKind: store.PoolOwnerTenant, OwnerNamespace: "local-jack"},
		{ID: "p-deleted", Name: "p-deleted", OwnerKind: store.PoolOwnerTenant, OwnerNamespace: "local-jack"},
		{ID: "p-plat", Name: "p-plat", OwnerKind: store.PoolOwnerPlatform},
	}
	instances := []inst{
		{"p-default", "h-df1", "df1", t0},
		{"p-team-a", "h-a1", "a1", t0},
		{"p-team-a", "h-a2", "a2", t0},
		{"p-local", "h-l1", "l1", t0},
		{"p-local", "h-l2", "l2", t0.Add(time.Minute)},
		{"p-deleted", "h-d1", "d1", t0.Add(2 * time.Minute)},
		{"p-plat", "h-pl1", "pl1", t0},
	}
	seed := func(t *testing.T) ScopedRunnerPoolStore {
		t.Helper()
		st := factory(t)
		for _, p := range pools {
			p.CreatedAt = t0
			p.AllowedNamespaces = []string{"*"}
			p.AllowedNodeTypes = []string{"xflow.function"}
			if err := st.CreatePool(ctx, p); err != nil {
				t.Fatalf("CreatePool(%s): %v", p.ID, err)
			}
		}
		for _, in := range instances {
			if _, err := st.EnrollInstance(ctx, store.EnrollInstanceRequest{
				PoolID: in.pool, SystemID: in.system, InstanceUID: "uid-" + in.system,
				CandidateRunnerID: in.runner, Now: in.at,
			}); err != nil {
				t.Fatalf("EnrollInstance(%s): %v", in.runner, err)
			}
		}
		// a2 drains so the state filter has something to exclude.
		if err := st.TransitionInstance(ctx, "a2", store.InstanceActive, store.InstanceDraining, t0.Add(time.Hour)); err != nil {
			t.Fatalf("TransitionInstance(a2): %v", err)
		}
		// Soft-delete after enrolling: a deleted pool refuses new enrolls but its
		// instances still hold credentials the prune saga must clear.
		if err := st.DeletePool(ctx, "p-deleted", all); err != nil {
			t.Fatalf("DeletePool(p-deleted): %v", err)
		}
		return st
	}
	runnerIDs := func(in []store.ScopedRunnerInstance) []string {
		out := make([]string, 0, len(in))
		for _, i := range in {
			out = append(out, i.RunnerID)
		}
		return out
	}

	cases := []struct {
		name  string
		owned []namespace.Namespace
		want  []string // active instances, oldest state change first
	}{
		// The default deployment manages the platform pools and its own tenant
		// pools; nothing else.
		{"default", []namespace.Namespace{namespace.Default}, []string{"df1", "pl1"}},
		// A dedicated deployment sees its own pools — including the soft-deleted
		// one — and none of the platform ones.
		{"dedicated", []namespace.Namespace{"local-jack"}, []string{"l1", "l2", "d1"}},
		{"other", []namespace.Namespace{"team-a"}, []string{"a1"}},
		{"union", []namespace.Namespace{"local-jack", "team-a"}, []string{"a1", "l1", "l2", "d1"}},
		{"default and dedicated", []namespace.Namespace{namespace.Default, "local-jack"}, []string{"df1", "l1", "pl1", "l2", "d1"}},
		{"duplicated members", []namespace.Namespace{"team-a", "team-a"}, []string{"a1"}},
		{"unknown", []namespace.Namespace{"nobody"}, []string{}},
	}
	for _, tc := range cases {
		t.Run("scope/"+tc.name, func(t *testing.T) {
			st := seed(t)
			got, err := st.ListInstancesByStateIn(ctx, tc.owned, store.InstanceActive)
			if err != nil {
				t.Fatalf("ListInstancesByStateIn: %v", err)
			}
			if ids := runnerIDs(got); !reflect.DeepEqual(ids, tc.want) {
				t.Fatalf("active in %v = %v, want %v", tc.owned, ids, tc.want)
			}
		})
	}

	t.Run("state filter", func(t *testing.T) {
		st := seed(t)
		owned := []namespace.Namespace{"team-a"}
		draining, err := st.ListInstancesByStateIn(ctx, owned, store.InstanceDraining)
		if err != nil || !reflect.DeepEqual(runnerIDs(draining), []string{"a2"}) {
			t.Fatalf("draining = %v, err %v; want [a2]", runnerIDs(draining), err)
		}
		pruned, err := st.ListInstancesByStateIn(ctx, owned, store.InstancePruned)
		if err != nil || len(pruned) != 0 {
			t.Fatalf("pruned = %v, err %v; want none", runnerIDs(pruned), err)
		}
	})

	// The prune worker treats platform instances differently from tenant ones,
	// so the owner kind it reads back must be the pool's own.
	t.Run("owner kind comes from the pool", func(t *testing.T) {
		st := seed(t)
		got, err := st.ListInstancesByStateIn(ctx, []namespace.Namespace{namespace.Default}, store.InstanceActive)
		if err != nil {
			t.Fatalf("ListInstancesByStateIn: %v", err)
		}
		kinds := map[string]store.PoolOwnerKind{}
		for _, in := range got {
			kinds[in.RunnerID] = in.PoolOwnerKind
		}
		want := map[string]store.PoolOwnerKind{"df1": store.PoolOwnerTenant, "pl1": store.PoolOwnerPlatform}
		if !reflect.DeepEqual(kinds, want) {
			t.Fatalf("owner kinds = %v, want %v", kinds, want)
		}
		if got[0].PoolID == "" || got[0].SystemID == "" {
			t.Fatalf("embedded instance fields not populated: %+v", got[0])
		}
	})

	// Mutation note: expanding an empty set to "every pool" must turn this red —
	// a caller whose configured set came out empty would then drain and revoke
	// the whole shared fleet.
	t.Run("empty set fails closed", func(t *testing.T) {
		st := seed(t)
		for name, owned := range map[string][]namespace.Namespace{"nil": nil, "empty": {}} {
			got, err := st.ListInstancesByStateIn(ctx, owned, store.InstanceActive)
			if err != nil || len(got) != 0 {
				t.Fatalf("%s = %v, err %v; want none", name, runnerIDs(got), err)
			}
		}
	})

	// The deployments partition the fleet: the scopes of every namespace that
	// owns a pool, listed separately, must reproduce the unscoped listing.
	t.Run("scopes partition the unscoped listing", func(t *testing.T) {
		st := seed(t)
		unscoped, err := st.ListInstancesByState(ctx, store.InstanceActive)
		if err != nil {
			t.Fatalf("ListInstancesByState: %v", err)
		}
		seen := map[string]int{}
		for _, ns := range []namespace.Namespace{namespace.Default, "local-jack", "team-a"} {
			part, err := st.ListInstancesByStateIn(ctx, []namespace.Namespace{ns}, store.InstanceActive)
			if err != nil {
				t.Fatalf("scope %s: %v", ns, err)
			}
			for _, in := range part {
				seen[in.RunnerID]++
			}
		}
		if len(unscoped) != 6 || len(seen) != len(unscoped) {
			t.Fatalf("unscoped = %d rows, scoped union = %v; want the fixture's 6", len(unscoped), seen)
		}
		for _, in := range unscoped {
			if seen[in.RunnerID] != 1 {
				t.Fatalf("runner %s appears in %d scopes, want exactly 1", in.RunnerID, seen[in.RunnerID])
			}
		}
	})
}
