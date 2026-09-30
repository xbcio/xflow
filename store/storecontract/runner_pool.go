package storecontract

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/store"
)

type runnerPoolContractStateSetter interface {
	SetRunnerInstanceStateForContract(context.Context, string, string, store.InstanceState) error
}

// RunRunnerPoolStoreContract holds every RunnerPoolStore implementation to the
// same visibility, immutability, uniqueness, and atomic enrollment behavior.
func RunRunnerPoolStoreContract(t *testing.T, factory func(t *testing.T) store.RunnerPoolStore) {
	t.Helper()
	ctx := context.Background()
	createdAt := time.Date(2026, 9, 29, 1, 0, 0, 0, time.UTC)
	all := store.OwnerScope{All: true}

	create := func(t *testing.T, st store.RunnerPoolStore, pool store.RunnerPool) {
		t.Helper()
		if pool.CreatedAt.IsZero() {
			pool.CreatedAt = createdAt
		}
		if err := st.CreatePool(ctx, pool); err != nil {
			t.Fatalf("CreatePool(%q): %v", pool.ID, err)
		}
	}
	tenantPool := func(id, owner string) store.RunnerPool {
		return store.RunnerPool{
			ID: id, Name: "pool-" + id, OwnerKind: store.PoolOwnerTenant,
			OwnerNamespace: owner, AllowedNamespaces: []string{owner},
			AllowedNodeTypes: []string{"xflow.function"}, Labels: map[string]string{"region": "east"},
			CreatedAt: createdAt,
		}
	}
	enroll := func(poolID, systemID, runnerID string, at time.Time) store.EnrollInstanceRequest {
		return store.EnrollInstanceRequest{
			PoolID: poolID, SystemID: systemID, InstanceUID: "uid-" + systemID,
			CandidateRunnerID: runnerID, Now: at,
		}
	}

	t.Run("pool values are cloned across the store boundary", func(t *testing.T) {
		st := factory(t)
		pool := tenantPool("clone", "team-a")
		create(t, st, pool)
		pool.AllowedNamespaces[0] = "mutated-input"
		pool.AllowedNodeTypes[0] = "mutated-input"
		pool.Labels["region"] = "mutated-input"

		got, err := st.GetPool(ctx, "clone", all)
		if err != nil {
			t.Fatalf("GetPool: %v", err)
		}
		if got.AllowedNamespaces[0] != "team-a" || got.AllowedNodeTypes[0] != "xflow.function" || got.Labels["region"] != "east" {
			t.Fatalf("CreatePool retained caller aliases: %+v", got)
		}
		got.AllowedNamespaces[0] = "mutated-output"
		got.AllowedNodeTypes[0] = "mutated-output"
		got.Labels["region"] = "mutated-output"
		again, err := st.GetPool(ctx, "clone", all)
		if err != nil {
			t.Fatalf("second GetPool: %v", err)
		}
		if again.AllowedNamespaces[0] != "team-a" || again.AllowedNodeTypes[0] != "xflow.function" || again.Labels["region"] != "east" {
			t.Fatalf("GetPool exposed stored aliases: %+v", again)
		}
		listed, err := st.ListPools(ctx, all)
		if err != nil || len(listed) != 1 {
			t.Fatalf("ListPools = (%d, %v), want (1, nil)", len(listed), err)
		}
		listed[0].Labels["region"] = "mutated-list"
		last, _ := st.GetPool(ctx, "clone", all)
		if last.Labels["region"] != "east" {
			t.Fatalf("ListPools exposed stored map: %+v", last.Labels)
		}
	})

	t.Run("visibility distinguishes platform tenant and deleted pools", func(t *testing.T) {
		st := factory(t)
		platform := store.RunnerPool{ID: "platform", Name: "platform", OwnerKind: store.PoolOwnerPlatform, CreatedAt: createdAt}
		teamA := tenantPool("team-a", "team-a")
		teamB := tenantPool("team-b", "team-b")
		create(t, st, platform)
		create(t, st, teamA)
		create(t, st, teamB)

		if _, err := st.GetPool(ctx, platform.ID, store.OwnerScope{Namespace: "team-a"}); !errors.Is(err, store.ErrRunnerPoolNotFound) {
			t.Fatalf("tenant GetPool(platform) = %v, want ErrRunnerPoolNotFound", err)
		}
		if _, err := st.GetPool(ctx, teamB.ID, store.OwnerScope{Namespace: "team-a"}); !errors.Is(err, store.ErrRunnerPoolNotFound) {
			t.Fatalf("cross-tenant GetPool = %v, want ErrRunnerPoolNotFound", err)
		}
		visible, err := st.ListPools(ctx, store.OwnerScope{Namespace: "team-a"})
		if err != nil {
			t.Fatalf("ListPools(team-a): %v", err)
		}
		if len(visible) != 1 || visible[0].ID != teamA.ID {
			t.Fatalf("ListPools(team-a) = %v, want [team-a]", runnerPoolIDs(visible))
		}
		global, err := st.ListPools(ctx, all)
		if err != nil || len(global) != 3 {
			t.Fatalf("ListPools(all) = (%v, %v), want three pools", runnerPoolIDs(global), err)
		}
		if err := st.DeletePool(ctx, teamA.ID, store.OwnerScope{Namespace: "team-a"}); err != nil {
			t.Fatalf("DeletePool: %v", err)
		}
		if _, err := st.GetPool(ctx, teamA.ID, all); !errors.Is(err, store.ErrRunnerPoolNotFound) {
			t.Fatalf("GetPool(deleted) = %v, want ErrRunnerPoolNotFound", err)
		}
		global, err = st.ListPools(ctx, all)
		if err != nil || len(global) != 2 {
			t.Fatalf("ListPools after delete = (%v, %v), want two pools", runnerPoolIDs(global), err)
		}
		if err := st.DeletePool(ctx, teamA.ID, all); !errors.Is(err, store.ErrRunnerPoolNotFound) {
			t.Fatalf("second DeletePool = %v, want ErrRunnerPoolNotFound", err)
		}
	})

	t.Run("update changes only mutable fields", func(t *testing.T) {
		st := factory(t)
		original := tenantPool("update", "team-a")
		create(t, st, original)
		replacement := store.RunnerPool{
			ID: original.ID, Name: "new", OwnerKind: store.PoolOwnerPlatform,
			OwnerNamespace: "team-b", AllowedNamespaces: []string{"narrow"},
			AllowedNodeTypes: []string{"xflow.http"}, Labels: map[string]string{"zone": "one"},
			MaxInstances: 4, InheritNamespaces: true, Paused: true,
			CreatedAt: createdAt.Add(time.Hour), DeletedAt: createdAt.Add(2 * time.Hour),
		}
		if err := st.UpdatePool(ctx, replacement, store.OwnerScope{Namespace: "team-a"}); err != nil {
			t.Fatalf("UpdatePool: %v", err)
		}
		got, err := st.GetPool(ctx, original.ID, store.OwnerScope{Namespace: "team-a"})
		if err != nil {
			t.Fatalf("GetPool after update: %v", err)
		}
		if got.ID != original.ID || got.OwnerKind != original.OwnerKind || got.OwnerNamespace != original.OwnerNamespace || !got.CreatedAt.Equal(original.CreatedAt) || !got.DeletedAt.IsZero() {
			t.Fatalf("UpdatePool changed immutable fields: %+v", got)
		}
		if got.Name != replacement.Name || !reflect.DeepEqual(got.AllowedNamespaces, replacement.AllowedNamespaces) || !reflect.DeepEqual(got.AllowedNodeTypes, replacement.AllowedNodeTypes) || !reflect.DeepEqual(got.Labels, replacement.Labels) || got.MaxInstances != 4 || !got.InheritNamespaces || !got.Paused {
			t.Fatalf("UpdatePool did not replace mutable fields: %+v", got)
		}
		replacement.Labels["zone"] = "mutated"
		again, _ := st.GetPool(ctx, original.ID, all)
		if again.Labels["zone"] != "one" {
			t.Fatalf("UpdatePool retained caller map: %+v", again.Labels)
		}
		if err := st.UpdatePool(ctx, replacement, store.OwnerScope{Namespace: "team-b"}); !errors.Is(err, store.ErrRunnerPoolNotFound) {
			t.Fatalf("cross-tenant UpdatePool = %v, want ErrRunnerPoolNotFound", err)
		}
	})

	t.Run("concurrent idempotent enroll creates one stable runner", func(t *testing.T) {
		st := factory(t)
		create(t, st, tenantPool("concurrent", "team-a"))
		const workers = 32
		release := make(chan struct{})
		results := make([]store.EnrollInstanceResult, workers)
		errs := make([]error, workers)
		var wg sync.WaitGroup
		wg.Add(workers)
		for i := 0; i < workers; i++ {
			go func(i int) {
				defer wg.Done()
				<-release
				results[i], errs[i] = st.EnrollInstance(ctx, enroll("concurrent", "host-1", fmt.Sprintf("candidate-%d", i), createdAt.Add(time.Duration(i)*time.Second)))
			}(i)
		}
		close(release)
		wg.Wait()
		created := 0
		runnerID := ""
		for i := range results {
			if errs[i] != nil {
				t.Fatalf("EnrollInstance worker %d: %v", i, errs[i])
			}
			if results[i].Created {
				created++
			}
			if runnerID == "" {
				runnerID = results[i].Instance.RunnerID
			}
			if results[i].Instance.RunnerID != runnerID {
				t.Fatalf("worker %d RunnerID = %q, want stable %q", i, results[i].Instance.RunnerID, runnerID)
			}
			if !results[i].Instance.StateChangedAt.Equal(results[i].Instance.CreatedAt) {
				t.Fatalf("worker %d state changed at %v, want created at %v", i, results[i].Instance.StateChangedAt, results[i].Instance.CreatedAt)
			}
		}
		if created != 1 {
			t.Fatalf("Created count = %d, want exactly 1", created)
		}
	})

	t.Run("runner id is globally unique", func(t *testing.T) {
		st := factory(t)
		create(t, st, tenantPool("unique-a", "team-a"))
		create(t, st, tenantPool("unique-b", "team-b"))
		if _, err := st.EnrollInstance(ctx, enroll("unique-a", "host-a", "runner-shared", createdAt)); err != nil {
			t.Fatalf("first EnrollInstance: %v", err)
		}
		if _, err := st.EnrollInstance(ctx, enroll("unique-b", "host-b", "runner-shared", createdAt)); err == nil {
			t.Fatal("second EnrollInstance reused runner ID, want uniqueness error")
		}
		instances, err := st.ListInstances(ctx, "unique-b", all)
		if err != nil || len(instances) != 0 {
			t.Fatalf("failed unique insert left instances = (%+v, %v), want empty", instances, err)
		}
	})

	t.Run("max instances counts active instances atomically", func(t *testing.T) {
		st := factory(t)
		pool := tenantPool("limited", "team-a")
		pool.MaxInstances = 2
		create(t, st, pool)
		for i := 0; i < 2; i++ {
			if _, err := st.EnrollInstance(ctx, enroll("limited", fmt.Sprintf("host-%d", i), fmt.Sprintf("runner-%d", i), createdAt)); err != nil {
				t.Fatalf("EnrollInstance #%d: %v", i+1, err)
			}
		}
		if _, err := st.EnrollInstance(ctx, enroll("limited", "host-3", "runner-3", createdAt)); !errors.Is(err, store.ErrRunnerPoolInstanceLimit) {
			t.Fatalf("EnrollInstance over limit = %v, want ErrRunnerPoolInstanceLimit", err)
		}
		later := createdAt.Add(time.Hour)
		res, err := st.EnrollInstance(ctx, store.EnrollInstanceRequest{PoolID: "limited", SystemID: "host-0", InstanceUID: "uid-new", CandidateRunnerID: "ignored", Now: later})
		if err != nil || res.Created {
			t.Fatalf("reenroll at limit = (Created=%v, %v), want existing success", res.Created, err)
		}
		if res.Instance.RunnerID != "runner-0" || res.Instance.InstanceUID != "uid-new" || !res.Instance.LastEnrolledAt.Equal(later) || res.Instance.State != store.InstanceActive {
			t.Fatalf("reenroll did not preserve identity/update process fields: %+v", res.Instance)
		}
	})

	t.Run("non-active instance cannot reenroll", func(t *testing.T) {
		st := factory(t)
		setter, ok := st.(runnerPoolContractStateSetter)
		if !ok {
			t.Skip("store contract fixture cannot transition instance state")
		}
		create(t, st, tenantPool("inactive", "team-a"))
		initial := enroll("inactive", "host", "runner", createdAt)
		if _, err := st.EnrollInstance(ctx, initial); err != nil {
			t.Fatalf("first EnrollInstance: %v", err)
		}
		if err := setter.SetRunnerInstanceStateForContract(ctx, "inactive", "host", store.InstanceDraining); err != nil {
			t.Fatalf("SetRunnerInstanceStateForContract: %v", err)
		}
		later := initial
		later.InstanceUID = "uid-new"
		later.Now = createdAt.Add(time.Hour)
		if _, err := st.EnrollInstance(ctx, later); !errors.Is(err, store.ErrRunnerInstanceNotActive) {
			t.Fatalf("EnrollInstance(draining) = %v, want ErrRunnerInstanceNotActive", err)
		}
		instances, err := st.ListInstances(ctx, "inactive", all)
		if err != nil || len(instances) != 1 {
			t.Fatalf("ListInstances = (%+v, %v), want one instance", instances, err)
		}
		if instances[0].InstanceUID != initial.InstanceUID || !instances[0].LastEnrolledAt.Equal(initial.Now) {
			t.Fatalf("rejected reenroll mutated instance: %+v", instances[0])
		}
	})

	t.Run("pruned instance reenroll creates a new runner id", func(t *testing.T) {
		st := factory(t)
		pool := tenantPool("recycle", "team-a")
		pool.MaxInstances = 1
		create(t, st, pool)
		initial := enroll("recycle", "host", "runner-old", createdAt)
		if _, err := st.EnrollInstance(ctx, initial); err != nil {
			t.Fatalf("first EnrollInstance: %v", err)
		}
		if err := st.TransitionInstance(ctx, "runner-old", store.InstanceActive, store.InstanceDraining, createdAt.Add(time.Minute)); err != nil {
			t.Fatalf("Transition active->draining: %v", err)
		}
		if _, err := st.EnrollInstance(ctx, enroll("recycle", "host", "runner-while-draining", createdAt.Add(2*time.Minute))); !errors.Is(err, store.ErrRunnerInstanceNotActive) {
			t.Fatalf("EnrollInstance(draining) = %v, want ErrRunnerInstanceNotActive", err)
		}
		prunedAt := createdAt.Add(3 * time.Minute)
		if err := st.TransitionInstance(ctx, "runner-old", store.InstanceDraining, store.InstancePruned, prunedAt); err != nil {
			t.Fatalf("Transition draining->pruned: %v", err)
		}
		if _, err := st.EnrollInstance(ctx, enroll("recycle", "host", "runner-old", createdAt.Add(4*time.Minute))); err == nil {
			t.Fatal("EnrollInstance(pruned) reused the revoked runner ID")
		}
		recycledAt := createdAt.Add(4 * time.Minute)
		res, err := st.EnrollInstance(ctx, enroll("recycle", "host", "runner-new", recycledAt))
		if err != nil {
			t.Fatalf("EnrollInstance(pruned): %v", err)
		}
		if !res.Created || res.Instance.RunnerID != "runner-new" || res.Instance.RunnerID == "runner-old" || res.Instance.State != store.InstanceActive {
			t.Fatalf("recycled instance = %+v Created=%v", res.Instance, res.Created)
		}
		if !res.Instance.CreatedAt.Equal(recycledAt) || !res.Instance.StateChangedAt.Equal(recycledAt) {
			t.Fatalf("recycled timestamps = created %v state %v, want %v", res.Instance.CreatedAt, res.Instance.StateChangedAt, recycledAt)
		}
	})

	t.Run("pruned recycle obeys max instances", func(t *testing.T) {
		st := factory(t)
		pool := tenantPool("recycle-limit", "team-a")
		pool.MaxInstances = 1
		create(t, st, pool)
		if _, err := st.EnrollInstance(ctx, enroll("recycle-limit", "host-old", "runner-old", createdAt)); err != nil {
			t.Fatalf("EnrollInstance(old): %v", err)
		}
		if err := st.TransitionInstance(ctx, "runner-old", store.InstanceActive, store.InstanceDraining, createdAt.Add(time.Minute)); err != nil {
			t.Fatalf("Transition old draining: %v", err)
		}
		if err := st.TransitionInstance(ctx, "runner-old", store.InstanceDraining, store.InstancePruned, createdAt.Add(2*time.Minute)); err != nil {
			t.Fatalf("Transition old pruned: %v", err)
		}
		if _, err := st.EnrollInstance(ctx, enroll("recycle-limit", "host-live", "runner-live", createdAt.Add(3*time.Minute))); err != nil {
			t.Fatalf("EnrollInstance(live): %v", err)
		}
		if _, err := st.EnrollInstance(ctx, enroll("recycle-limit", "host-old", "runner-new", createdAt.Add(4*time.Minute))); !errors.Is(err, store.ErrRunnerPoolInstanceLimit) {
			t.Fatalf("recycle over limit = %v, want ErrRunnerPoolInstanceLimit", err)
		}
	})

	t.Run("transition compare-and-swap and state listing", func(t *testing.T) {
		st := factory(t)
		create(t, st, tenantPool("states", "team-a"))
		if _, err := st.EnrollInstance(ctx, enroll("states", "host-a", "runner-a", createdAt)); err != nil {
			t.Fatalf("EnrollInstance(a): %v", err)
		}
		if _, err := st.EnrollInstance(ctx, enroll("states", "host-b", "runner-b", createdAt)); err != nil {
			t.Fatalf("EnrollInstance(b): %v", err)
		}
		active, err := st.ListInstancesByState(ctx, store.InstanceActive)
		if err != nil {
			t.Fatalf("ListInstancesByState(active): %v", err)
		}
		if _, ok := runnerInstanceByID(active, "runner-a"); !ok {
			t.Fatalf("active listing missing runner-a: %+v", active)
		}
		changedAt := createdAt.Add(time.Minute)
		if err := st.TransitionInstance(ctx, "runner-a", store.InstanceActive, store.InstanceDraining, changedAt); err != nil {
			t.Fatalf("TransitionInstance: %v", err)
		}
		if err := st.TransitionInstance(ctx, "runner-a", store.InstanceActive, store.InstancePruned, changedAt.Add(time.Minute)); !errors.Is(err, store.ErrRunnerInstanceStateConflict) {
			t.Fatalf("stale TransitionInstance = %v, want ErrRunnerInstanceStateConflict", err)
		}
		if err := st.TransitionInstance(ctx, "missing", store.InstanceActive, store.InstanceDraining, changedAt); !errors.Is(err, store.ErrRunnerInstanceNotFound) {
			t.Fatalf("missing TransitionInstance = %v, want ErrRunnerInstanceNotFound", err)
		}
		draining, err := st.ListInstancesByState(ctx, store.InstanceDraining)
		if err != nil {
			t.Fatalf("ListInstancesByState(draining): %v", err)
		}
		got, ok := runnerInstanceByID(draining, "runner-a")
		if !ok || !got.StateChangedAt.Equal(changedAt) {
			t.Fatalf("draining listing = %+v, want runner-a changed at %v", draining, changedAt)
		}
		if _, ok := runnerInstanceByID(draining, "runner-b"); ok {
			t.Fatalf("draining listing unexpectedly contains runner-b: %+v", draining)
		}
	})

	t.Run("list instances hides inaccessible pools", func(t *testing.T) {
		st := factory(t)
		create(t, st, tenantPool("instances", "team-a"))
		if _, err := st.EnrollInstance(ctx, enroll("instances", "host", "runner", createdAt)); err != nil {
			t.Fatalf("EnrollInstance: %v", err)
		}
		if _, err := st.ListInstances(ctx, "instances", store.OwnerScope{Namespace: "team-b"}); !errors.Is(err, store.ErrRunnerPoolNotFound) {
			t.Fatalf("cross-tenant ListInstances = %v, want ErrRunnerPoolNotFound", err)
		}
		if _, err := st.ListInstances(ctx, "absent", all); !errors.Is(err, store.ErrRunnerPoolNotFound) {
			t.Fatalf("absent ListInstances = %v, want ErrRunnerPoolNotFound", err)
		}
		got, err := st.ListInstances(ctx, "instances", store.OwnerScope{Namespace: "team-a"})
		if err != nil || len(got) != 1 || got[0].RunnerID != "runner" {
			t.Fatalf("owner ListInstances = (%+v, %v), want runner", got, err)
		}
	})
}

func runnerPoolIDs(pools []store.RunnerPool) []string {
	out := make([]string, 0, len(pools))
	for _, pool := range pools {
		out = append(out, pool.ID)
	}
	return out
}

// runnerInstanceByID finds one instance in an unscoped lifecycle listing.
func runnerInstanceByID(instances []store.RunnerInstance, runnerID string) (store.RunnerInstance, bool) {
	for _, instance := range instances {
		if instance.RunnerID == runnerID {
			return instance, true
		}
	}
	return store.RunnerInstance{}, false
}
