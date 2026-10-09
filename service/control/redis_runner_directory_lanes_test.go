package control

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/xbcio/xflow/service/protocol"
)

func TestNormalizeLaneNodeTypesTrimsDedupesAndKeepsOrder(t *testing.T) {
	got := normalizeLaneNodeTypes([]string{
		" xflow.sas.sink ",
		"",
		"xflow.sas.ulp-result",
		"xflow.sas.sink",
		"   ",
		// The reserved legacy label is not a node type and is dropped: as a
		// lane it would report its depth under the same label the legacy queue
		// uses.
		QueueLaneLegacy,
		"xflow.group",
	})
	want := []string{"xflow.sas.sink", "xflow.sas.ulp-result", "xflow.group"}
	if !slices.Equal(got, want) {
		t.Fatalf("normalizeLaneNodeTypes() = %q, want %q", got, want)
	}
}

func TestWithRedisRunnerDirectoryLanesCopiesTheSlice(t *testing.T) {
	source := []string{"xflow.sas.sink"}
	directory := NewRedisRunnerDirectory(nil, WithRedisRunnerDirectoryLanes(source))

	source[0] = "mutated-after-construction"
	if !slices.Equal(directory.lanes, []string{"xflow.sas.sink"}) {
		t.Fatalf("lanes = %q, want the option to have copied its slice", directory.lanes)
	}
}

func TestLaneWriteModeOrDefault(t *testing.T) {
	lanes := []string{"xflow.sas.sink"}
	cases := []struct {
		name  string
		lanes []string
		mode  LaneWriteMode
		want  LaneWriteMode
	}{
		{"no lanes keeps the zero mode legacy-only", nil, "", LaneWriteLegacyOnly},
		{"no lanes degrades lane-only", nil, LaneWriteLaneOnly, LaneWriteLegacyOnly},
		{"no lanes degrades dual", nil, LaneWriteDual, LaneWriteLegacyOnly},
		{"lanes with the zero mode stay legacy-only", lanes, "", LaneWriteLegacyOnly},
		{"lanes with dual keep dual", lanes, LaneWriteDual, LaneWriteDual},
		{"lanes with lane-only keep lane-only", lanes, LaneWriteLaneOnly, LaneWriteLaneOnly},
		{"lanes with an unrecognized mode fall back to legacy-only", lanes, LaneWriteMode("duall"), LaneWriteLegacyOnly},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			directory := NewRedisRunnerDirectory(nil,
				WithRedisRunnerDirectoryLanes(tc.lanes),
				WithRedisRunnerDirectoryLaneWriteMode(tc.mode),
			)
			if got := directory.laneWriteModeOrDefault(); got != tc.want {
				t.Fatalf("laneWriteModeOrDefault() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestResolveQueueLane(t *testing.T) {
	lanes := []string{"xflow.sas.webscan-sink", "xflow.sas.ulp-result"}
	cases := []struct {
		name     string
		lanes    []string
		nodeType string
		wantLane string
		wantOK   bool
	}{
		{"configured lane resolves to itself", lanes, "xflow.sas.webscan-sink", "xflow.sas.webscan-sink", true},
		{"second configured lane resolves", lanes, "xflow.sas.ulp-result", "xflow.sas.ulp-result", true},
		{"unconfigured sink stays on legacy", lanes, "xflow.sas.sink", "", false},
		{"synthetic group type stays on legacy", lanes, "xflow.group", "", false},
		{"subgraph body type stays on legacy", lanes, "xflow.subgraph", "", false},
		{"empty node type stays on legacy", lanes, "", "", false},
		{"match is exact, not trimmed", lanes, " xflow.sas.webscan-sink", "", false},
		{"no lanes configured resolves nothing", nil, "xflow.sas.webscan-sink", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lane, ok := resolveQueueLane(tc.lanes, tc.nodeType)
			if ok != tc.wantOK || lane != tc.wantLane {
				t.Fatalf("resolveQueueLane(%q) = (%q, %v), want (%q, %v)", tc.nodeType, lane, ok, tc.wantLane, tc.wantOK)
			}
		})
	}
}

// TestLaneKeysShareTheDirectoryHashTag guards the single-slot property: the
// directory's Lua transitions mix the lane queue keys with the legacy keys in
// one script, which Redis Cluster only allows inside one hash slot.
func TestLaneKeysShareTheDirectoryHashTag(t *testing.T) {
	directory := NewRedisRunnerDirectory(nil)
	keys := []string{
		directory.keys.queue,
		directory.keys.laneQueueKey("xflow.sas.webscan-sink"),
		directory.keys.assignmentLane,
	}
	for _, key := range keys {
		if !strings.Contains(key, "{control}") {
			t.Fatalf("key %q does not carry the directory hash tag", key)
		}
	}
	if got := directory.keys.laneQueueKey("xflow.sas.webscan-sink"); got != "xflow:runner-directory:{control}:queue:lane:xflow.sas.webscan-sink" {
		t.Fatalf("laneQueueKey() = %q", got)
	}
}

func TestLaneQueueKeysOrderLegacyLast(t *testing.T) {
	directory := NewRedisRunnerDirectory(nil, WithRedisRunnerDirectoryLanes([]string{"a.b", "c.d"}))
	want := []string{
		directory.keys.laneQueueKey("a.b"),
		directory.keys.laneQueueKey("c.d"),
		directory.keys.queue,
	}
	if got := directory.laneQueueKeys(); !slices.Equal(got, want) {
		t.Fatalf("laneQueueKeys() = %q, want %q", got, want)
	}
	bare := NewRedisRunnerDirectory(nil)
	if got := bare.laneQueueKeys(); !slices.Equal(got, []string{bare.keys.queue}) {
		t.Fatalf("laneQueueKeys() without lanes = %q, want the legacy queue only", got)
	}
}

func TestEnqueueAcrossQueueLanes(t *testing.T) {
	laneType := "xflow.sas.webscan-sink"
	lanes := []string{laneType}
	cases := []struct {
		name             string
		lanes            []string
		mode             LaneWriteMode
		nodeType         string
		wantLaneCopies   int64
		wantLegacyCopies int64
		wantMarker       string
		wantNoMarkerKey  bool
	}{
		{"no lanes stays on legacy and writes no marker", nil, "", laneType, 0, 1, "", true},
		{"dual writes both copies and the lane marker", lanes, LaneWriteDual, laneType, 1, 1, "lane", false},
		{"dual keeps an unowned type on legacy and marks legacy", lanes, LaneWriteDual, "xflow.sas.sink", 0, 1, "legacy", false},
		{"lane-only writes the lane copy and the lane marker", lanes, LaneWriteLaneOnly, laneType, 1, 0, "lane", false},
		{"lane-only keeps an unowned type on legacy and marks legacy", lanes, LaneWriteLaneOnly, "xflow.sas.sink", 0, 1, "legacy", false},
		{"legacy-only keeps a lane type on legacy and writes no marker", lanes, LaneWriteLegacyOnly, laneType, 0, 1, "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			_, rdb := newRedisRunnerDirectoryTestClient(t)
			directory := NewRedisRunnerDirectory(rdb,
				WithRedisRunnerDirectoryLanes(tc.lanes),
				WithRedisRunnerDirectoryLaneWriteMode(tc.mode),
			)
			assignment := redisDirectoryTestAssignment("exec-lanes/enqueue/activation-1")
			assignment.Routing.NodeType = tc.nodeType
			mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)

			laneCopies, err := rdb.LLen(ctx, directory.keys.laneQueueKey(laneType)).Result()
			if err != nil {
				t.Fatal(err)
			}
			if laneCopies != tc.wantLaneCopies {
				t.Fatalf("lane queue copies = %d, want %d", laneCopies, tc.wantLaneCopies)
			}
			legacyCopies, err := rdb.LLen(ctx, directory.keys.queue).Result()
			if err != nil {
				t.Fatal(err)
			}
			if legacyCopies != tc.wantLegacyCopies {
				t.Fatalf("legacy queue copies = %d, want %d", legacyCopies, tc.wantLegacyCopies)
			}

			marker, _ := rdb.HGet(ctx, directory.keys.assignmentLane, string(assignment.AssignmentID)).Result()
			wantMarker := tc.wantMarker
			switch wantMarker {
			case "lane":
				wantMarker = directory.keys.laneQueueKey(laneType)
			case "legacy":
				wantMarker = directory.keys.queue
			}
			if marker != wantMarker {
				t.Fatalf("lane marker = %q, want %q", marker, wantMarker)
			}
			exists, err := rdb.Exists(ctx, directory.keys.assignmentLane).Result()
			if err != nil {
				t.Fatal(err)
			}
			if tc.wantNoMarkerKey && exists != 0 {
				t.Fatalf("marker hash exists, want it never created")
			}
			if !tc.wantNoMarkerKey && exists == 0 {
				t.Fatalf("marker hash missing, want one marker field")
			}
		})
	}
}

func TestEnqueueClearsStaleCopiesFromEveryCandidate(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	laneType := "xflow.sas.webscan-sink"
	directory := NewRedisRunnerDirectory(rdb,
		WithRedisRunnerDirectoryLanes([]string{laneType}),
		WithRedisRunnerDirectoryLaneWriteMode(LaneWriteDual),
	)
	id := AssignmentID("exec-lanes/stale/activation-1")
	assignment := redisDirectoryTestAssignment(id)
	assignment.Routing.NodeType = laneType

	// A previous placement left one copy on the lane and one on the legacy
	// queue, and the stale-token release that followed left the record in
	// 'released' with its seen mark kept.
	if err := rdb.RPush(ctx, directory.keys.laneQueueKey(laneType), string(id)).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.RPush(ctx, directory.keys.queue, string(id)).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.HSet(ctx, directory.keys.assignmentState, string(id), "released").Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.SAdd(ctx, directory.keys.seen, string(id)).Err(); err != nil {
		t.Fatal(err)
	}

	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)

	laneCopies, err := rdb.LRange(ctx, directory.keys.laneQueueKey(laneType), 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(laneCopies, []string{string(id)}) {
		t.Fatalf("lane queue = %q, want exactly one fresh copy", laneCopies)
	}
	legacyCopies, err := rdb.LRange(ctx, directory.keys.queue, 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(legacyCopies, []string{string(id)}) {
		t.Fatalf("legacy queue = %q, want exactly one fresh copy", legacyCopies)
	}
}

// seedLaneRequeueClaim writes the claim-time bookkeeping redisClaimAssignmentLua
// would have left for an already-enqueued assignment. The requeue transitions
// only need an ID and read the rest off these keys, so seeding them drives every
// requeue path without a claim walk — which, until the multi-target walk lands,
// can only ever find an entry on the legacy queue.
func seedLaneRequeueClaim(
	t *testing.T,
	ctx context.Context,
	rdb *redis.Client,
	directory *RedisRunnerDirectory,
	runnerID string,
	assignment Assignment,
	claimID ClaimID,
	handoffState string,
	claimExpired bool,
) {
	t.Helper()
	id := string(assignment.AssignmentID)
	seeds := []struct{ key, field, value string }{
		{directory.keys.assignmentState, id, "claimed"},
		{directory.keys.assignmentClaim, id, string(claimID)},
		{directory.keys.claimsAssignment, string(claimID), id},
		{directory.keys.claimsRunner, string(claimID), runnerID},
		{directory.keys.handoffState, string(claimID), handoffState},
		{directory.keys.handoffClaim, string(claimID), id},
	}
	for _, seed := range seeds {
		if err := rdb.HSet(ctx, seed.key, seed.field, seed.value).Err(); err != nil {
			t.Fatalf("seed %s: %v", seed.key, err)
		}
	}
	// The expiry score puts the claim in or out of ReclaimExpiredClaims' reach.
	// Only its own driver may have it expired: both ReleaseClaim and
	// ClaimForRunner run that scan first, and an expired claim would be
	// reclaimed out from under the transition under test.
	score := float64(time.Now().Add(time.Hour).UnixMilli())
	if claimExpired {
		score = 0
	}
	if err := rdb.ZAdd(ctx, directory.keys.claimsExpiry, redis.Z{Score: score, Member: string(claimID)}).Err(); err != nil {
		t.Fatalf("seed claimsExpiry: %v", err)
	}
}

// assertLaneQueuePlacement checks how many copies of id sit on the lane and on
// the legacy queue, and what the lane marker says. markerWant is "lane",
// "legacy" or "" (no marker field).
func assertLaneQueuePlacement(
	t *testing.T,
	ctx context.Context,
	rdb *redis.Client,
	directory *RedisRunnerDirectory,
	laneType string,
	id AssignmentID,
	laneCopies int,
	legacyCopies int,
	markerWant string,
) {
	t.Helper()
	gotLane, err := rdb.LRange(ctx, directory.keys.laneQueueKey(laneType), 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	wantLane := slices.Repeat([]string{string(id)}, laneCopies)
	if !slices.Equal(gotLane, wantLane) {
		t.Fatalf("lane queue = %q, want %q", gotLane, wantLane)
	}
	gotLegacy, err := rdb.LRange(ctx, directory.keys.queue, 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	wantLegacy := slices.Repeat([]string{string(id)}, legacyCopies)
	if !slices.Equal(gotLegacy, wantLegacy) {
		t.Fatalf("legacy queue = %q, want %q", gotLegacy, wantLegacy)
	}
	gotMarker, err := rdb.HGet(ctx, directory.keys.assignmentLane, string(id)).Result()
	if errors.Is(err, redis.Nil) {
		gotMarker = ""
	} else if err != nil {
		t.Fatal(err)
	}
	wantMarker := ""
	switch markerWant {
	case "lane":
		wantMarker = directory.keys.laneQueueKey(laneType)
	case "legacy":
		wantMarker = directory.keys.queue
	}
	if gotMarker != wantMarker {
		t.Fatalf("lane marker = %q, want %q", gotMarker, wantMarker)
	}
}

// TestRequeueAcrossQueueLanes drives every transition that returns an
// assignment to a queue — the reclaim scan, ReleaseClaim, SettleClaimHandoff
// and RegisterRunner's session replacement — with lanes configured, and checks
// the exact copy placement each write mode owes.
func TestRequeueAcrossQueueLanes(t *testing.T) {
	laneType := "xflow.sas.webscan-sink"
	decommissionedLaneType := "xflow.sas.ulp-result"
	lanes := []string{laneType}
	runnerID := "runner-requeue-lanes"
	claimID := ClaimID("claim-requeue-lanes")

	drivers := []struct {
		name         string
		handoffState string
		claimExpired bool
		supportsDrop bool
		run          func(t *testing.T, ctx context.Context, directory *RedisRunnerDirectory, claimID ClaimID, disposition HandoffDisposition)
	}{
		{
			name:         "settle-handoff",
			handoffState: "lease_may_exist",
			supportsDrop: true,
			run: func(t *testing.T, ctx context.Context, directory *RedisRunnerDirectory, claimID ClaimID, disposition HandoffDisposition) {
				if err := directory.SettleClaimHandoff(ctx, claimID, disposition); err != nil {
					t.Fatalf("SettleClaimHandoff(%q) error = %v", disposition, err)
				}
			},
		},
		{
			name:         "release-claim",
			handoffState: "finalized",
			supportsDrop: true,
			run: func(t *testing.T, ctx context.Context, directory *RedisRunnerDirectory, claimID ClaimID, disposition HandoffDisposition) {
				reason := ReleaseClaimRequeue
				if disposition == HandoffDispositionDrop {
					reason = ReleaseClaimDrop
				}
				if err := directory.ReleaseClaim(ctx, claimID, reason); err != nil {
					t.Fatalf("ReleaseClaim(%q) error = %v", reason, err)
				}
			},
		},
		{
			name:         "recover-expired",
			handoffState: "reserved",
			claimExpired: true,
			run: func(t *testing.T, ctx context.Context, directory *RedisRunnerDirectory, _ ClaimID, _ HandoffDisposition) {
				if err := directory.ReclaimExpiredClaims(ctx); err != nil {
					t.Fatalf("ReclaimExpiredClaims() error = %v", err)
				}
			},
		},
		{
			name:         "register",
			handoffState: "finalized",
			run: func(t *testing.T, ctx context.Context, directory *RedisRunnerDirectory, _ ClaimID, _ HandoffDisposition) {
				registerRedisDirectoryRunner(t, ctx, directory, runnerID, 1)
			},
		},
	}

	placements := []struct {
		name         string
		mode         LaneWriteMode
		nodeType     string
		marker       string // "", "missing", "decommissioned"
		laneCopies   int
		legacyCopies int
		markerWant   string
		// consumeLaneCopy seeds the state a claim leaves behind — the lane copy
		// already taken — so the requeue must put it back rather than confirm a
		// shape that was never disturbed. Without such a row, a placement whose
		// written shape equals its requeued shape also passes for a driver that
		// does nothing at all.
		consumeLaneCopy bool
	}{
		{"legacy-only keeps a lane type on legacy and writes no marker", LaneWriteLegacyOnly, laneType, "", 0, 1, "", false},
		{"dual writes both copies and follows the lane marker", LaneWriteDual, laneType, "", 1, 1, "lane", false},
		{"dual requeues a lane copy the claim already took", LaneWriteDual, laneType, "", 1, 1, "lane", true},
		{"dual keeps an unowned node type on legacy", LaneWriteDual, "xflow.function", "", 0, 1, "legacy", false},
		{"dual falls back to legacy when the marker is missing", LaneWriteDual, laneType, "missing", 0, 1, "legacy", false},
		{"dual falls back to legacy when the marker names a decommissioned lane", LaneWriteDual, laneType, "decommissioned", 0, 1, "legacy", false},
		{"lane-only writes the lane copy alone", LaneWriteLaneOnly, laneType, "", 1, 0, "lane", false},
		{"lane-only requeues a lane copy the claim already took", LaneWriteLaneOnly, laneType, "", 1, 0, "lane", true},
		{"lane-only keeps an unowned node type on legacy", LaneWriteLaneOnly, "xflow.function", "", 0, 1, "legacy", false},
		{"lane-only falls back to legacy when the marker names a decommissioned lane", LaneWriteLaneOnly, laneType, "decommissioned", 0, 1, "legacy", false},
	}

	for _, driver := range drivers {
		dispositions := []HandoffDisposition{HandoffDispositionRequeue}
		if driver.supportsDrop {
			dispositions = append(dispositions, HandoffDispositionDrop)
		}
		for _, disposition := range dispositions {
			for _, placement := range placements {
				t.Run(driver.name+"/"+string(disposition)+"/"+placement.name, func(t *testing.T) {
					ctx := context.Background()
					_, rdb := newRedisRunnerDirectoryTestClient(t)
					directory := NewRedisRunnerDirectory(rdb,
						WithRedisRunnerDirectoryLanes(lanes),
						WithRedisRunnerDirectoryLaneWriteMode(placement.mode),
					)
					assignment := redisDirectoryTestAssignment(AssignmentID("exec-lanes/requeue/activation-1"))
					assignment.Routing.NodeType = placement.nodeType
					mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)
					seedLaneRequeueClaim(t, ctx, rdb, directory, runnerID, assignment, claimID, driver.handoffState, driver.claimExpired)

					id := string(assignment.AssignmentID)
					switch placement.marker {
					case "missing":
						if err := rdb.HDel(ctx, directory.keys.assignmentLane, id).Err(); err != nil {
							t.Fatal(err)
						}
					case "decommissioned":
						if err := rdb.HSet(ctx, directory.keys.assignmentLane, id, directory.keys.laneQueueKey(decommissionedLaneType)).Err(); err != nil {
							t.Fatal(err)
						}
					}
					if placement.consumeLaneCopy {
						// What a claim leaves behind: the lane copy is taken
						// from the queue; the legacy copy and the marker stay.
						if err := rdb.LRem(ctx, directory.keys.laneQueueKey(laneType), 0, id).Err(); err != nil {
							t.Fatal(err)
						}
					}

					driver.run(t, ctx, directory, claimID, disposition)

					laneCopies, legacyCopies, markerWant := placement.laneCopies, placement.legacyCopies, placement.markerWant
					if disposition == HandoffDispositionDrop {
						laneCopies, legacyCopies, markerWant = 0, 0, ""
					}
					assertLaneQueuePlacement(t, ctx, rdb, directory, laneType, assignment.AssignmentID, laneCopies, legacyCopies, markerWant)
				})
			}
		}
	}
}

// TestRequeueEndToEndThroughARealClaim keeps one requeue path honest against
// real claim bookkeeping: a runner claims a dual-written lane assignment, the
// handoff goes recoverable, and settling it must land the entry per the write
// mode. The seeded matrix above would still pass if the IDs the transitions
// derive stopped matching what a real claim writes.
func TestRequeueEndToEndThroughARealClaim(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	laneType := "xflow.sas.webscan-sink"
	directory := NewRedisRunnerDirectory(rdb,
		WithRedisRunnerDirectoryLanes([]string{laneType}),
		WithRedisRunnerDirectoryLaneWriteMode(LaneWriteDual),
	)
	session, err := directory.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-lane-e2e",
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: laneType}},
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{laneType}},
		Now:          time.Unix(10, 0),
	})
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	assignment := redisDirectoryTestAssignment(AssignmentID("exec-lanes/e2e/activation-1"))
	assignment.Routing.NodeType = laneType
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)
	claim := claimRedisDirectoryAssignment(t, ctx, directory, session, 1)
	if err := directory.MarkClaimLeaseMayExist(ctx, claim.ClaimID); err != nil {
		t.Fatalf("MarkClaimLeaseMayExist() error = %v", err)
	}
	if err := directory.SettleClaimHandoff(ctx, claim.ClaimID, HandoffDispositionRequeue); err != nil {
		t.Fatalf("SettleClaimHandoff(requeue) error = %v", err)
	}
	assertLaneQueuePlacement(t, ctx, rdb, directory, laneType, assignment.AssignmentID, 1, 1, "lane")
}

// TestRequeueFollowsTheMarkerOntoEveryConfiguredLane proves the marker, not the
// legacy queue, decides the lane when several lanes are configured — the entry
// must land on its own lane and never on a sibling.
func TestRequeueFollowsTheMarkerOntoEveryConfiguredLane(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedisRunnerDirectoryTestClient(t)
	sinkLane := "xflow.sas.webscan-sink"
	ulpLane := "xflow.sas.ulp-result"
	directory := NewRedisRunnerDirectory(rdb,
		WithRedisRunnerDirectoryLanes([]string{sinkLane, ulpLane}),
		WithRedisRunnerDirectoryLaneWriteMode(LaneWriteLaneOnly),
	)
	assignment := redisDirectoryTestAssignment(AssignmentID("exec-lanes/multi/activation-1"))
	assignment.Routing.NodeType = ulpLane
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, assignment)
	seedLaneRequeueClaim(t, ctx, rdb, directory, "runner-multi-lane", assignment, ClaimID("claim-multi-lane"), "finalized", false)
	if err := directory.ReleaseClaim(ctx, ClaimID("claim-multi-lane"), ReleaseClaimRequeue); err != nil {
		t.Fatalf("ReleaseClaim(requeue) error = %v", err)
	}

	id := assignment.AssignmentID
	if got, err := rdb.LRange(ctx, directory.keys.laneQueueKey(ulpLane), 0, -1).Result(); err != nil {
		t.Fatal(err)
	} else if !slices.Equal(got, []string{string(id)}) {
		t.Fatalf("ulp lane = %q, want exactly one copy", got)
	}
	if got, err := rdb.LRange(ctx, directory.keys.laneQueueKey(sinkLane), 0, -1).Result(); err != nil {
		t.Fatal(err)
	} else if len(got) != 0 {
		t.Fatalf("sink lane = %q, want no copy: the entry belongs to the ulp lane", got)
	}
	if got, err := rdb.LRange(ctx, directory.keys.queue, 0, -1).Result(); err != nil {
		t.Fatal(err)
	} else if len(got) != 0 {
		t.Fatalf("legacy queue = %q, want no copy in lane-only mode", got)
	}
}
