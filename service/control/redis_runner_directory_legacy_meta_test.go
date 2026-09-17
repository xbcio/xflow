package control

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

// legacyLeaseMetaField is the unit under test's only safe output shape: the
// reaper must be able to name a field without ever surfacing its value.
func assertLegacyLeaseMetaFieldPresent(t *testing.T, server *miniredis.Miniredis, directory *RedisRunnerDirectory, assignmentID string) {
	t.Helper()

	if server.HGet(directory.keys.assignmentLeaseMetaLegacy, assignmentID) == "" {
		t.Fatalf("legacy lease metadata field %q is gone; it is still reachable", assignmentID)
	}
}

func assertLegacyLeaseMetaFieldAbsent(t *testing.T, server *miniredis.Miniredis, directory *RedisRunnerDirectory, assignmentID string) {
	t.Helper()

	if server.HGet(directory.keys.assignmentLeaseMetaLegacy, assignmentID) != "" {
		t.Fatalf("legacy lease metadata field %q survived", assignmentID)
	}
}

func seedLegacyLeaseMetaField(t *testing.T, server *miniredis.Miniredis, directory *RedisRunnerDirectory, assignmentID, payload string) {
	t.Helper()

	server.HSet(directory.keys.assignmentLeaseMetaLegacy, assignmentID, payload)
}

// TestRedisRunnerDirectoryReapsOnlyUnreachableLegacyLeaseMetaFields is the
// safety proof for the online reaper, expressed as one subtest per shared
// record a previous-version instance reads before it can reach the legacy
// field. Any one of them still present means the field is reachable, so the
// reaper must keep it; only a field that has none of them is residue.
func TestRedisRunnerDirectoryReapsOnlyUnreachableLegacyLeaseMetaFields(t *testing.T) {
	ctx := context.Background()

	liveHashes := []struct {
		name   string
		stream func(keys redisRunnerDirectoryKeys) string
	}{
		{"assignmentData", func(k redisRunnerDirectoryKeys) string { return k.assignmentData }},
		{"assignmentState", func(k redisRunnerDirectoryKeys) string { return k.assignmentState }},
		{"assignmentClaim", func(k redisRunnerDirectoryKeys) string { return k.assignmentClaim }},
		{"assignmentRunner", func(k redisRunnerDirectoryKeys) string { return k.assignmentRunner }},
		{"assignmentSession", func(k redisRunnerDirectoryKeys) string { return k.assignmentSession }},
		{"assignmentLeaseID", func(k redisRunnerDirectoryKeys) string { return k.assignmentLeaseID }},
		{"assignmentLeaseToken", func(k redisRunnerDirectoryKeys) string { return k.assignmentLeaseToken }},
	}

	for _, live := range liveHashes {
		live := live
		t.Run(live.name, func(t *testing.T) {
			server, rdb := newRedisRunnerDirectoryTestClient(t)
			directory := NewRedisRunnerDirectory(rdb)
			seedLegacyLeaseMetaField(t, server, directory, "reachable", "lease-payload")
			seedLegacyLeaseMetaField(t, server, directory, "residue", "lease-payload")
			server.HSet(live.stream(directory.keys), "reachable", "x")

			reaped, err := directory.ReapOrphanedLegacyAssignmentLeaseMeta(ctx, 16)
			if err != nil {
				t.Fatalf("ReapOrphanedLegacyAssignmentLeaseMeta() error = %v", err)
			}
			if reaped != 1 {
				t.Fatalf("reaped = %d, want 1 (only the field with no %s record)", reaped, live.name)
			}
			assertLegacyLeaseMetaFieldPresent(t, server, directory, "reachable")
			assertLegacyLeaseMetaFieldAbsent(t, server, directory, "residue")
		})
	}
}

// TestRedisRunnerDirectoryLegacyLeaseMetaReapTreatsPayloadAsOpaque pins the
// reason the reaper is safe to run against metadata that may contain secrets:
// the decision is made from the field name and the shared per-assignment
// records alone. A field whose value is not even decodable as a lease is still
// residue, so the reaper must not try to interpret it — and it must not
// surface it either.
func TestRedisRunnerDirectoryLegacyLeaseMetaReapTreatsPayloadAsOpaque(t *testing.T) {
	const secret = "legacy-payload-that-must-never-be-returned"

	ctx := context.Background()
	server, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)
	seedLegacyLeaseMetaField(t, server, directory, "opaque", secret)

	reaped, err := directory.ReapOrphanedLegacyAssignmentLeaseMeta(ctx, 16)
	if err != nil {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("reap error leaked the legacy payload: %v", err)
		}
		t.Fatalf("ReapOrphanedLegacyAssignmentLeaseMeta() error = %v", err)
	}
	if reaped != 1 {
		t.Fatalf("reaped = %d, want 1: the reaper must not need a decodable payload", reaped)
	}
	assertLegacyLeaseMetaFieldAbsent(t, server, directory, "opaque")
}

// TestRedisRunnerDirectoryLegacyLeaseMetaReapIsBounded keeps the reaper from
// becoming an unbounded walk of a hash whose size is exactly what went wrong.
func TestRedisRunnerDirectoryLegacyLeaseMetaReapIsBounded(t *testing.T) {
	ctx := context.Background()
	server, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		seedLegacyLeaseMetaField(t, server, directory, id, "lease-payload")
	}

	reaped, err := directory.ReapOrphanedLegacyAssignmentLeaseMeta(ctx, 2)
	if err != nil {
		t.Fatalf("ReapOrphanedLegacyAssignmentLeaseMeta() error = %v", err)
	}
	if reaped != 2 {
		t.Fatalf("reaped = %d, want exactly the limit of 2", reaped)
	}

	// A second call makes progress on what the first deliberately left behind.
	reaped, err = directory.ReapOrphanedLegacyAssignmentLeaseMeta(ctx, 16)
	if err != nil {
		t.Fatalf("second ReapOrphanedLegacyAssignmentLeaseMeta() error = %v", err)
	}
	if reaped != 3 {
		t.Fatalf("second reaped = %d, want the remaining 3", reaped)
	}
	if server.Exists(directory.keys.assignmentLeaseMetaLegacy) {
		t.Fatal("legacy lease metadata hash still exists after every field was reaped")
	}
}

func TestRedisRunnerDirectoryLegacyLeaseMetaReapWithoutLegacyKey(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb)

	reaped, err := directory.ReapOrphanedLegacyAssignmentLeaseMeta(ctx, 16)
	if err != nil {
		t.Fatalf("ReapOrphanedLegacyAssignmentLeaseMeta() error = %v", err)
	}
	if reaped != 0 {
		t.Fatalf("reaped = %d, want 0 when the legacy key was never written", reaped)
	}
}

// TestRedisRunnerDirectoryClearAssignmentDrainsLegacyLeaseMeta covers the
// other half of the design: the reaper only removes residue for assignments
// that are already gone, so the terminal path has to drop the legacy field for
// the assignments it terminates, or a mixed-version window would keep growing
// the hash faster than the reaper drains it.
func TestRedisRunnerDirectoryClearAssignmentDrainsLegacyLeaseMeta(t *testing.T) {
	ctx := context.Background()
	server, rdb := newRedisRunnerDirectoryTestClient(t)
	directory := NewRedisRunnerDirectory(rdb, WithRedisRunnerDirectoryClaimTTL(time.Second))
	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-legacy-clear", 1)

	assignment := redisDirectoryTestAssignment("exec-legacy-clear/node/activation-1")
	lease := redisRunnerDirectoryLeaseMetaTestLease(assignment, "lease-legacy-clear", time.Minute)
	finalizeRedisRunnerDirectoryLeaseMetaTestAssignment(t, ctx, directory, session, assignment, lease)

	// The previous version wrote its field for this assignment before the
	// upgrade; the new version finalized the claim under the per-assignment key.
	assignmentID := string(assignment.AssignmentID)
	seedLegacyLeaseMetaField(t, server, directory, assignmentID, "previous-version-lease-payload")

	if err := directory.ClearAssignment(ctx, assignment.AssignmentID); err != nil {
		t.Fatalf("ClearAssignment() error = %v", err)
	}
	assertLegacyLeaseMetaFieldAbsent(t, server, directory, assignmentID)
	if server.Exists(directory.keys.assignmentLeaseMetaKey(assignmentID)) {
		t.Fatalf("per-assignment lease metadata %q survived ClearAssignment", assignmentID)
	}
}
