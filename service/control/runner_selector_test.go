package control

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/protocol"
)

func TestSelector_IsLive(t *testing.T) {
	sel := DefaultRunnerSelector()
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name          string
		lastHeartbeat time.Time
		want          bool
	}{
		{"recent heartbeat", now.Add(-5 * time.Second), true},
		{"at boundary", now.Add(-30 * time.Second), true},
		{"just expired", now.Add(-31 * time.Second), false},
		{"long dead", now.Add(-5 * time.Minute), false},
		{"zero (never heartbeated)", time.Time{}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snap := RunnerSnapshot{
				RunnerID:      "r1",
				LastHeartbeat: tt.lastHeartbeat,
			}
			if got := sel.IsLive(snap, now); got != tt.want {
				t.Errorf("IsLive() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSelector_MatchLabels(t *testing.T) {
	tests := []struct {
		name         string
		runnerLabels map[string]string
		required     map[string]string
		want         bool
	}{
		{"nil required matches any", map[string]string{"a": "1"}, nil, true},
		{"empty required matches any", map[string]string{"a": "1"}, map[string]string{}, true},
		{"exact match", map[string]string{"region": "us-east-1"}, map[string]string{"region": "us-east-1"}, true},
		{"subset match", map[string]string{"region": "us-east-1", "pool": "gpu"}, map[string]string{"region": "us-east-1"}, true},
		{"mismatch value", map[string]string{"region": "eu-west-1"}, map[string]string{"region": "us-east-1"}, false},
		{"missing key", map[string]string{"pool": "gpu"}, map[string]string{"region": "us-east-1"}, false},
		{"nil runner labels", nil, map[string]string{"region": "us-east-1"}, false},
		{"both nil", nil, nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := MatchLabels(tt.runnerLabels, tt.required); got != tt.want {
				t.Errorf("MatchLabels() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSelector_CanAssign_DeadRunnerRejected(t *testing.T) {
	sel := DefaultRunnerSelector()
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)

	snap := RunnerSnapshot{
		RunnerID:      "r1",
		Capabilities:  []protocol.Capability{{NodeType: "http.request", NodeVersion: 1}},
		LastHeartbeat: now.Add(-5 * time.Minute),
	}
	policy := RunnerPolicy{AllowedNodeTypes: []string{"http.request"}}
	routing := engine.TaskRouting{NodeType: "http.request", NodeVersion: 1}

	if sel.CanAssign(snap, policy, routing, nil, now) {
		t.Error("dead runner should not be assignable")
	}
}

func TestSelector_CanAssign_LiveRunnerAccepted(t *testing.T) {
	sel := DefaultRunnerSelector()
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)

	snap := RunnerSnapshot{
		RunnerID:      "r1",
		Capabilities:  []protocol.Capability{{NodeType: "http.request", NodeVersion: 1}},
		Labels:        map[string]string{"region": "us-east-1"},
		LastHeartbeat: now.Add(-3 * time.Second),
	}
	policy := RunnerPolicy{AllowedNodeTypes: []string{"http.request"}}
	routing := engine.TaskRouting{NodeType: "http.request", NodeVersion: 1}

	if !sel.CanAssign(snap, policy, routing, nil, now) {
		t.Error("live runner with matching caps should be assignable")
	}
	if !sel.CanAssign(snap, policy, routing, map[string]string{"region": "us-east-1"}, now) {
		t.Error("live runner with matching labels should be assignable")
	}
	if sel.CanAssign(snap, policy, routing, map[string]string{"region": "eu-west-1"}, now) {
		t.Error("runner with wrong label value should not be assignable")
	}
}

func TestSelector_FallbackGrace(t *testing.T) {
	sel := RunnerSelector{LiveTTL: 10 * time.Second, FallbackGrace: 5 * time.Second}
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)

	// Within live TTL.
	snap := RunnerSnapshot{RunnerID: "r1", LastHeartbeat: now.Add(-8 * time.Second)}
	if !sel.IsLive(snap, now) {
		t.Error("8s old heartbeat with 10s TTL should be live")
	}

	// Just past TTL.
	snap.LastHeartbeat = now.Add(-11 * time.Second)
	if sel.IsLive(snap, now) {
		t.Error("11s old heartbeat with 10s TTL should NOT be live")
	}
}

func TestMatchCapabilities_WithFeature(t *testing.T) {
	caps := []protocol.Capability{
		{NodeType: "http.request", NodeVersion: 1},
		{NodeType: "xflow.group", NodeVersion: 0, Features: []string{"group.exec.v1"}},
	}

	routing := engine.TaskRouting{
		NodeType:    "xflow.group",
		NodeVersion: 0,
		Requirements: []engine.CapabilityRequirement{
			{NodeType: "xflow.group", Feature: "group.exec.v1"},
		},
	}
	if !MatchCapabilities(caps, routing) {
		t.Error("runner with group.exec.v1 feature should match group routing")
	}

	// Runner without the feature should not match.
	capsNoFeature := []protocol.Capability{
		{NodeType: "http.request", NodeVersion: 1},
		{NodeType: "xflow.group", NodeVersion: 0},
	}
	if MatchCapabilities(capsNoFeature, routing) {
		t.Error("runner without group.exec.v1 feature should NOT match group routing")
	}
}

func TestMatchCapabilities_NoRequirements(t *testing.T) {
	caps := []protocol.Capability{{NodeType: "http.request", NodeVersion: 1}}
	routing := engine.TaskRouting{NodeType: "http.request", NodeVersion: 1}

	if !MatchCapabilities(caps, routing) {
		t.Error("basic routing without requirements should match")
	}
}

func TestMatchCapabilities_VersionMismatch(t *testing.T) {
	caps := []protocol.Capability{{NodeType: "http.request", NodeVersion: 2}}
	routing := engine.TaskRouting{
		NodeType:    "http.request",
		NodeVersion: 1,
		Requirements: []engine.CapabilityRequirement{
			{NodeType: "http.request", NodeVersion: 1},
		},
	}

	if MatchCapabilities(caps, routing) {
		t.Error("version mismatch on requirement should reject")
	}
}

// TestRedisMatureGraceCandidatesDegradesWhenTheStampReadFails pins the failure
// direction of the advisory stamp read: when Redis cannot answer the batched
// HMGET/TIME at all, the page degrades to "everything is still waiting" instead
// of failing the walk. A walk error there would strand every entry on the page
// — including ones that already waited out their window — for as long as the
// command misbehaves.
func TestRedisMatureGraceCandidatesDegradesWhenTheStampReadFails(t *testing.T) {
	dead := redis.NewClient(&redis.Options{
		Addr:        "127.0.0.1:1",
		DialTimeout: 250 * time.Millisecond,
		MaxRetries:  -1,
	})
	t.Cleanup(func() { _ = dead.Close() })
	directory := NewRedisRunnerDirectory(dead, WithRedisRunnerDirectorySelector(RunnerSelector{FallbackGrace: time.Minute}))

	candidates := []redisClaimCandidate{
		{assignmentID: "a", assignment: immatureLabelMissAssignment("a")},
		{assignmentID: "b", assignment: immatureLabelMissAssignment("b")},
	}
	matured, pending, trips, err := directory.matureGraceCandidates(context.Background(), candidates)
	if err != nil {
		t.Fatalf("a failed stamp read failed the walk (%v); the page would be stranded while Redis misbehaves on this command", err)
	}
	if len(matured) != 0 {
		t.Fatalf("matured = %+v, want none: with no readable stamp nothing has proven its window elapsed", matured)
	}
	if len(pending) != len(candidates) {
		t.Fatalf("pending = %+v, want every candidate still pending", pending)
	}
	if trips != 2 {
		t.Fatalf("roundTrips = %d, want 2 (the batched HMGET + TIME attempt)", trips)
	}
}
