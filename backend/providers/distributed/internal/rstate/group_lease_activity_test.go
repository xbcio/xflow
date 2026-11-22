package rstate

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// TestGroupActivityClockRefreshOnLeaseRenewAndCommit pins the write points of
// the execution activity clock on the group-unit lease path: acquiring a group
// lease, renewing it, and an accepted group commit each stamp activity_at_ms.
// The key starts at a stale sentinel before every step, so a fresh value can
// only come from that step; this is the group counterpart of
// TestEngineActivityClockRefreshOnLeaseRenewAndCommit.
func TestGroupActivityClockRefreshOnLeaseRenewAndCommit(t *testing.T) {
	state, _, rdb := newTestRedisState(t)
	ctx := context.Background()
	id := types.ExecutionID("group-activity-clock")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: twoNodeGraph(t), Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}

	activityKey := activityAtKey(namespace.Default, id)
	stale := func() {
		t.Helper()
		if err := rdb.Set(ctx, activityKey, 1, 0).Err(); err != nil {
			t.Fatalf("seed stale activity clock: %v", err)
		}
	}
	activityMs := func() int64 {
		t.Helper()
		ms, err := rdb.Get(ctx, activityKey).Int64()
		if err != nil {
			t.Fatalf("read activity clock: %v", err)
		}
		return ms
	}
	freshAfter := func(step string) {
		t.Helper()
		if ms := activityMs(); ms <= time.Now().Add(-time.Minute).UnixMilli() {
			t.Fatalf("%s did not refresh activity_at_ms: %d is not recent", step, ms)
		}
	}
	// The activity key must carry a TTL, or the age anchor would outlive the
	// execution it describes. On acquire and commit it is written by the same
	// script that rewrites the unit status key, so the two must agree; renewal
	// extends only the activity key and the lease index (the unit keys keep the
	// TTL the acquisition gave them), so there only the positive TTL is pinned.
	assertTTL := func(step string) {
		t.Helper()
		if activityTTL := rdb.TTL(ctx, activityKey).Val(); activityTTL <= 0 {
			t.Fatalf("%s left activity_at_ms without a TTL (%v)", step, activityTTL)
		}
	}
	assertTTLMatchesStatus := func(step string) {
		t.Helper()
		assertTTL(step)
		activityTTL := rdb.TTL(ctx, activityKey).Val()
		statusTTL := rdb.TTL(ctx, groupUnitStatusKey(namespace.Default, id, 0)).Val()
		if activityTTL != statusTTL {
			t.Fatalf("%s activity TTL = %v, want the unit status TTL %v", step, activityTTL, statusTTL)
		}
	}

	lease := &engine.GroupLease{
		LeaseID: "activity-lease", LeaseToken: "activity-token", Attempt: 1,
		ExecutionID: id, GroupUnitIdx: 0, GroupName: "group", EntryNodeIdx: 0,
		IssuedAt: time.Now().UTC(), TTL: time.Minute,
	}
	stale()
	acquired, err := state.AcquireGroupLease(ctx, lease)
	if err != nil || !acquired {
		t.Fatalf("AcquireGroupLease() acquired=%v err=%v, want true/nil", acquired, err)
	}
	freshAfter("group lease acquisition")
	assertTTLMatchesStatus("group lease acquisition")

	stale()
	renewed, err := state.RenewGroupLease(ctx, id, 0, lease.LeaseToken, time.Now().UTC().Add(2*time.Minute))
	if err != nil || !renewed {
		t.Fatalf("RenewGroupLease() renewed=%v err=%v, want true/nil", renewed, err)
	}
	freshAfter("group lease renewal")
	assertTTL("group lease renewal")

	stale()
	committed, err := state.CommitGroup(ctx, engine.GroupCommitRequest{
		ExecutionID:  id,
		GroupUnitIdx: 0,
		GroupID:      "group",
		LeaseID:      lease.LeaseID,
		LeaseToken:   lease.LeaseToken,
		Attempt:      lease.Attempt,
		Outcome:      engine.GroupOutcomeSuccess,
	})
	if err != nil {
		t.Fatalf("CommitGroup() error = %v", err)
	}
	if !committed.Applied || committed.ExecutionDone {
		t.Fatalf("CommitGroup() = %+v, want an applied commit that leaves the execution running", committed)
	}
	freshAfter("accepted group commit")
	assertTTLMatchesStatus("accepted group commit")
}
