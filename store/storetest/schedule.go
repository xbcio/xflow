package storetest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// ScheduleOverrideContract exercises one backend's ScheduleOverrideStore.
//
// nsPrefix disambiguates rows between backends and between reruns against a
// persistent database, matching the other contracts in this package: a MySQL
// run must not see a previous run's rows, and the isolation assertions below
// would otherwise be satisfied by leftovers.
//
// What it pins, and why each is a contract rather than an implementation
// detail:
//
//   - nil is not empty. Every overridable field round-trips as nil when it was
//     never set. A backend that stored nil as "" would turn "follow the
//     definition" into "override it with nothing", which is a schedule that
//     stops firing rather than one that follows its definition.
//   - Revision is the CAS token, bumped on every accepted write — including a
//     write that changes nothing. Two operators editing one schedule must not
//     both succeed because the second happened to write the same values.
//   - Namespace is a required scope, not a filter: an empty one is refused
//     rather than widened, and two namespaces holding the same (workflow, node)
//     are two independent schedules.
//   - The cascade removes exactly one workflow's rows and reports the count a
//     caller needs to log the effect of a delete.
func ScheduleOverrideContract(t *testing.T, s store.ScheduleOverrideStore, nsPrefix string) {
	t.Helper()
	ctx := context.Background()
	nsA := namespace.Namespace(nsPrefix + "-a")
	nsB := namespace.Namespace(nsPrefix + "-b")
	const wfA types.WorkflowID = "wf-contract-a"
	const wfB types.WorkflowID = "wf-contract-b"
	nodeA := "nightly"

	str := func(v string) *string { return &v }
	boolean := func(v bool) *bool { return &v }

	t.Run("refuses an invalid scope", func(t *testing.T) {
		if _, err := s.GetScheduleOverride(ctx, "", wfA, nodeA); !errors.Is(err, store.ErrInvalidNamespace) {
			t.Fatalf("GetScheduleOverride(\"\") error = %v, want ErrInvalidNamespace", err)
		}
		if _, err := s.ListScheduleOverrides(ctx, "", wfA); !errors.Is(err, store.ErrInvalidNamespace) {
			t.Fatalf("ListScheduleOverrides(\"\") error = %v, want ErrInvalidNamespace", err)
		}
		if _, err := s.PutScheduleOverride(ctx, &store.ScheduleOverride{
			Namespace: "", WorkflowID: wfA, Node: nodeA,
		}, nil); !errors.Is(err, store.ErrInvalidNamespace) {
			t.Fatalf("PutScheduleOverride(\"\") error = %v, want ErrInvalidNamespace", err)
		}
		if err := s.DeleteScheduleOverride(ctx, "", wfA, nodeA, nil); !errors.Is(err, store.ErrInvalidNamespace) {
			t.Fatalf("DeleteScheduleOverride(\"\") error = %v, want ErrInvalidNamespace", err)
		}
		if _, err := s.DeleteScheduleOverridesForWorkflow(ctx, "", wfA); !errors.Is(err, store.ErrInvalidNamespace) {
			t.Fatalf("DeleteScheduleOverridesForWorkflow(\"\") error = %v, want ErrInvalidNamespace", err)
		}
	})

	t.Run("absent overlay is not found", func(t *testing.T) {
		if _, err := s.GetScheduleOverride(ctx, nsA, wfA, "never-written"); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("GetScheduleOverride error = %v, want ErrNotFound", err)
		}
		if err := s.DeleteScheduleOverride(ctx, nsA, wfA, "never-written", nil); err != nil {
			t.Fatalf("deleting an absent overlay: %v, want nil (the asked-for state holds)", err)
		}
	})

	t.Run("nil fields stay nil", func(t *testing.T) {
		stored, err := s.PutScheduleOverride(ctx, &store.ScheduleOverride{
			Namespace:  nsA,
			WorkflowID: wfA,
			Node:       nodeA,
			Expression: str("0 3 * * *"),
			Paused:     boolean(true),
			UpdatedBy:  "operator@example",
		}, nil)
		if err != nil {
			t.Fatalf("PutScheduleOverride: %v", err)
		}
		if stored.Revision == 0 {
			t.Fatal("first write left Revision at 0; If-Match cannot distinguish create from update")
		}
		if stored.UpdatedAt.IsZero() {
			t.Fatal("UpdatedAt was not set by the store")
		}

		got, err := s.GetScheduleOverride(ctx, nsA, wfA, nodeA)
		if err != nil {
			t.Fatalf("GetScheduleOverride: %v", err)
		}
		if got.Expression == nil || *got.Expression != "0 3 * * *" {
			t.Fatalf("Expression = %v, want the value that was written", got.Expression)
		}
		if got.Paused == nil || !*got.Paused {
			t.Fatalf("Paused = %v, want true", got.Paused)
		}
		if got.Timezone != nil || got.Misfire != nil || got.Overlap != nil {
			t.Fatalf("unset fields came back non-nil: timezone=%v misfire=%v overlap=%v — "+
				"a read that cannot tell \"not overridden\" from \"overridden with empty\" "+
				"makes DELETE .../override impossible to honour", got.Timezone, got.Misfire, got.Overlap)
		}
		if got.UpdatedBy != "operator@example" {
			t.Fatalf("UpdatedBy = %q, want the writer", got.UpdatedBy)
		}

		// A second write that changes nothing still moves the revision: it is the
		// CAS token, not a content version.
		again, err := s.PutScheduleOverride(ctx, got, nil)
		if err != nil {
			t.Fatalf("second PutScheduleOverride: %v", err)
		}
		if again.Revision <= stored.Revision {
			t.Fatalf("revision after an identical write = %d, want > %d", again.Revision, stored.Revision)
		}
	})

	t.Run("if-match is enforced", func(t *testing.T) {
		cur, err := s.GetScheduleOverride(ctx, nsA, wfA, nodeA)
		if err != nil {
			t.Fatalf("GetScheduleOverride: %v", err)
		}

		// Create-only against an existing row is a conflict.
		if _, err := s.PutScheduleOverride(ctx, &store.ScheduleOverride{
			Namespace: nsA, WorkflowID: wfA, Node: nodeA, Expression: str("* * * * *"),
		}, uint64Ptr(0)); !errors.Is(err, store.ErrRevisionConflict) {
			t.Fatalf("create-only over an existing row: error = %v, want ErrRevisionConflict", err)
		}

		// A stale revision is a conflict AND leaves the row untouched.
		if _, err := s.PutScheduleOverride(ctx, &store.ScheduleOverride{
			Namespace: nsA, WorkflowID: wfA, Node: nodeA, Expression: str("stale"),
		}, uint64Ptr(cur.Revision+7)); !errors.Is(err, store.ErrRevisionConflict) {
			t.Fatalf("stale If-Match: error = %v, want ErrRevisionConflict", err)
		}
		unchanged, err := s.GetScheduleOverride(ctx, nsA, wfA, nodeA)
		if err != nil {
			t.Fatalf("GetScheduleOverride after a refused write: %v", err)
		}
		if unchanged.Revision != cur.Revision || unchanged.Expression == nil || *unchanged.Expression != "0 3 * * *" {
			t.Fatalf("a refused write changed the row: revision %d (want %d), expression %v", unchanged.Revision, cur.Revision, unchanged.Expression)
		}

		// The current revision is accepted.
		next, err := s.PutScheduleOverride(ctx, &store.ScheduleOverride{
			Namespace: nsA, WorkflowID: wfA, Node: nodeA, Expression: str("0 4 * * *"),
			Misfire: str("fire_once"), Timezone: str("Asia/Shanghai"), Overlap: str("skip"),
		}, uint64Ptr(cur.Revision))
		if err != nil {
			t.Fatalf("PutScheduleOverride with the current revision: %v", err)
		}
		if next.Revision != cur.Revision+1 {
			t.Fatalf("revision = %d, want %d", next.Revision, cur.Revision+1)
		}

		// Delete honours If-Match too.
		if err := s.DeleteScheduleOverride(ctx, nsA, wfA, nodeA, uint64Ptr(cur.Revision)); !errors.Is(err, store.ErrRevisionConflict) {
			t.Fatalf("delete with a stale If-Match: error = %v, want ErrRevisionConflict", err)
		}
		if err := s.DeleteScheduleOverride(ctx, nsA, wfA, nodeA, uint64Ptr(next.Revision)); err != nil {
			t.Fatalf("delete with the current If-Match: %v", err)
		}
		if _, err := s.GetScheduleOverride(ctx, nsA, wfA, nodeA); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("after delete, GetScheduleOverride error = %v, want ErrNotFound", err)
		}
	})

	t.Run("listing is per workflow and per namespace", func(t *testing.T) {
		for _, node := range []string{"zeta", "alpha", "mid"} {
			if _, err := s.PutScheduleOverride(ctx, &store.ScheduleOverride{
				Namespace: nsA, WorkflowID: wfB, Node: node, Expression: str("* * * * *"),
			}, nil); err != nil {
				t.Fatalf("PutScheduleOverride(%s): %v", node, err)
			}
		}
		// The same (workflow, node) in another namespace is a different schedule.
		if _, err := s.PutScheduleOverride(ctx, &store.ScheduleOverride{
			Namespace: nsB, WorkflowID: wfB, Node: "alpha", Expression: str("0 0 * * *"), Paused: boolean(true),
		}, nil); err != nil {
			t.Fatalf("PutScheduleOverride in the second namespace: %v", err)
		}
		// wfA's only row was deleted above, so its list must be empty and must
		// not carry over wfB's.
		empty, err := s.ListScheduleOverrides(ctx, nsA, wfA)
		if err != nil {
			t.Fatalf("ListScheduleOverrides: %v", err)
		}
		if len(empty) != 0 {
			t.Fatalf("wfA listed %d overlays, want 0", len(empty))
		}

		list, err := s.ListScheduleOverrides(ctx, nsA, wfB)
		if err != nil {
			t.Fatalf("ListScheduleOverrides: %v", err)
		}
		if len(list) != 3 {
			t.Fatalf("wfB listed %d overlays, want 3", len(list))
		}
		for i, want := range []string{"alpha", "mid", "zeta"} {
			if list[i].Node != want {
				t.Fatalf("list[%d].Node = %q, want %q (lists must be ordered by node)", i, list[i].Node, want)
			}
			if list[i].Namespace != nsA {
				t.Fatalf("list[%d] came from namespace %q, want %q", i, list[i].Namespace, nsA)
			}
		}

		other, err := s.ListScheduleOverrides(ctx, nsB, wfB)
		if err != nil {
			t.Fatalf("ListScheduleOverrides(nsB): %v", err)
		}
		if len(other) != 1 || other[0].Node != "alpha" || other[0].Paused == nil || !*other[0].Paused {
			t.Fatalf("nsB list = %+v, want only its own paused alpha row", other)
		}
	})

	t.Run("cascade delete takes exactly one workflow", func(t *testing.T) {
		if _, err := s.PutScheduleOverride(ctx, &store.ScheduleOverride{
			Namespace: nsA, WorkflowID: wfA, Node: "resurrected", Expression: str("* * * * *"),
		}, nil); err != nil {
			t.Fatalf("PutScheduleOverride: %v", err)
		}
		removed, err := s.DeleteScheduleOverridesForWorkflow(ctx, nsA, wfB)
		if err != nil {
			t.Fatalf("DeleteScheduleOverridesForWorkflow: %v", err)
		}
		if removed != 3 {
			t.Fatalf("removed = %d, want 3", removed)
		}
		if gone, err := s.ListScheduleOverrides(ctx, nsA, wfB); err != nil || len(gone) != 0 {
			t.Fatalf("after the cascade, wfB listed %d overlays (err %v), want 0", len(gone), err)
		}
		// The other namespace's row and the other workflow's row survive.
		if kept, err := s.ListScheduleOverrides(ctx, nsB, wfB); err != nil || len(kept) != 1 {
			t.Fatalf("the cascade crossed a namespace boundary: nsB listed %d overlays (err %v), want 1", len(kept), err)
		}
		if kept, err := s.ListScheduleOverrides(ctx, nsA, wfA); err != nil || len(kept) != 1 {
			t.Fatalf("the cascade took another workflow's row: wfA listed %d overlays (err %v), want 1", len(kept), err)
		}

		// A second cascade removes nothing and still reports the count it removed.
		removed, err = s.DeleteScheduleOverridesForWorkflow(ctx, nsA, wfB)
		if err != nil {
			t.Fatalf("second DeleteScheduleOverridesForWorkflow: %v", err)
		}
		if removed != 0 {
			t.Fatalf("second cascade removed = %d, want 0", removed)
		}

		// Clean up so a rerun against a persistent database starts where this one did.
		for _, wf := range []types.WorkflowID{wfA, wfB} {
			for _, ns := range []namespace.Namespace{nsA, nsB} {
				if _, err := s.DeleteScheduleOverridesForWorkflow(ctx, ns, wf); err != nil {
					t.Fatalf("cleanup cascade: %v", err)
				}
			}
		}
	})

	// A timestamp is stored with sub-second precision or better; a backend that
	// truncated it would make two writes inside the same second indistinguishable
	// in the audit trail the API also writes.
	t.Run("updated_at moves with each write", func(t *testing.T) {
		first, err := s.PutScheduleOverride(ctx, &store.ScheduleOverride{
			Namespace: nsA, WorkflowID: wfA, Node: "clock", Expression: str("* * * * *"),
		}, nil)
		if err != nil {
			t.Fatalf("PutScheduleOverride: %v", err)
		}
		time.Sleep(2 * time.Millisecond)
		second, err := s.PutScheduleOverride(ctx, &store.ScheduleOverride{
			Namespace: nsA, WorkflowID: wfA, Node: "clock", Expression: str("*/2 * * * *"),
		}, nil)
		if err != nil {
			t.Fatalf("second PutScheduleOverride: %v", err)
		}
		if !second.UpdatedAt.After(first.UpdatedAt) {
			t.Fatalf("UpdatedAt did not advance: %v then %v", first.UpdatedAt, second.UpdatedAt)
		}
		if _, err := s.DeleteScheduleOverridesForWorkflow(ctx, nsA, wfA); err != nil {
			t.Fatalf("cleanup: %v", err)
		}
	})
}

func uint64Ptr(v uint64) *uint64 { return &v }
