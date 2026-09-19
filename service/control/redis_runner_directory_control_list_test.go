package control

import (
	"context"
	"reflect"
	"testing"
)

// TestListLiveRunnersProjectsControlInOneLedgerPass pins the list projection's
// cost model: the fleet-wide handoff and deactivation ledgers are keyed by
// claim/obligation, so a list must read them a fixed number of times no matter
// how many runners it projects. The previous shape called RunnerControl once
// per runner, and each of those was itself a pipeline carrying the same four
// whole-hash reads -- O(runners) round-trips and O(runners x fleet debt).
//
// The counter-check is that the projection stays identical to the per-runner
// read, so a lower ledger count cannot be bought by reporting less.
func TestListLiveRunnersProjectsControlInOneLedgerPass(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)
	hook := newLedgerHGetAllHook(ledgerKeys(directory)...)
	rdb.AddHook(hook)

	wantLedgerReads := len(ledgerKeys(directory))

	for _, runnerID := range []string{"runner-a", "runner-b", "runner-c"} {
		registerRedisDirectoryRunner(t, ctx, directory, runnerID, 2)
	}
	if _, err := directory.SetRunnerControl(ctx, drainRunnerControlRequest("runner-b", "req-drain-b")); err != nil {
		t.Fatalf("SetRunnerControl() error = %v", err)
	}

	hook.reset()
	snapshots := directory.ListLiveRunners(ctx)
	if n := hook.count(); n != wantLedgerReads {
		t.Fatalf("ledger HGetAll reads for a 3-runner list = %d, want %d", n, wantLedgerReads)
	}
	requireListMatchesPerRunnerProjection(t, ctx, directory, snapshots, 3)

	// Growing the fleet must not grow the ledger reads. This is the part that
	// distinguished the old per-runner projection from the batched one.
	for _, runnerID := range []string{"runner-d", "runner-e", "runner-f"} {
		registerRedisDirectoryRunner(t, ctx, directory, runnerID, 2)
	}
	hook.reset()
	snapshots = directory.ListLiveRunners(ctx)
	if n := hook.count(); n != wantLedgerReads {
		t.Fatalf("ledger HGetAll reads for a 6-runner list = %d, want %d", n, wantLedgerReads)
	}
	requireListMatchesPerRunnerProjection(t, ctx, directory, snapshots, 6)
}

func requireListMatchesPerRunnerProjection(t *testing.T, ctx context.Context, directory *RedisRunnerDirectory, snapshots []RunnerSnapshot, want int) {
	t.Helper()

	if len(snapshots) != want {
		t.Fatalf("ListLiveRunners() returned %d snapshots, want %d", len(snapshots), want)
	}
	seen := make(map[string]struct{}, len(snapshots))
	for _, snap := range snapshots {
		seen[snap.RunnerID] = struct{}{}
		if snap.Control == nil {
			t.Fatalf("ListLiveRunners() control for %q = nil, want projection", snap.RunnerID)
		}
		full, found, err := directory.RunnerControl(ctx, snap.RunnerID)
		if err != nil || !found {
			t.Fatalf("RunnerControl(%q) found=%v err=%v, want projection", snap.RunnerID, found, err)
		}
		if !reflect.DeepEqual(*snap.Control, full) {
			t.Fatalf("ListLiveRunners() control for %q = %+v, RunnerControl = %+v", snap.RunnerID, *snap.Control, full)
		}
	}
	for _, runnerID := range []string{"runner-a", "runner-b", "runner-c", "runner-d", "runner-e", "runner-f"}[:want] {
		if _, ok := seen[runnerID]; !ok {
			t.Fatalf("ListLiveRunners() omitted %q", runnerID)
		}
	}
}
