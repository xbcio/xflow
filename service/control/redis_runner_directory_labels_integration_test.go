package control

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// TestRedisRunnerDirectoryRegisterSnapshotSurvivesForgedPollMetadata proves
// that registration, rather than a subsequent Poll request, remains the
// routing authority in real Redis. A local runner can forge the labels and
// capabilities of the standalone workload in its Poll payload, but that must
// neither overwrite the stored registration snapshot nor make it eligible for
// that workload's tasks.
func TestRedisRunnerDirectoryRegisterSnapshotSurvivesForgedPollMetadata(t *testing.T) {
	addr := os.Getenv("XFLOW_TEST_REDIS_ADDR")
	if addr == "" {
		if os.Getenv("XFLOW_REQUIRE_REDIS_INTEGRATION") == "1" {
			t.Fatal("XFLOW_REQUIRE_REDIS_INTEGRATION=1: XFLOW_TEST_REDIS_ADDR not set (use 127.0.0.1:6380)")
		}
		t.Skip("XFLOW_TEST_REDIS_ADDR not set; skipping the real-Redis runner-label test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		t.Fatalf("ping real Redis at %s: %v", addr, err)
	}

	directory := newRealRedisRunnerDirectory(t, rdb)
	registeredLabels := map[string]string{"workload": "local"}
	registeredCapabilities := []protocol.Capability{{NodeType: "xflow.sas.sink"}}
	session, err := directory.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-registration-snapshot",
		Capacity:     3,
		Labels:       registeredLabels,
		Capabilities: registeredCapabilities,
		// Allow both node types so this test proves capability matching rather
		// than passing only because policy blocks xflow.map.
		Policy: RunnerPolicy{AllowedNodeTypes: []string{"xflow.sas.sink", "xflow.map"}},
		Now:    time.Now(),
	})
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	forgedPoll := ClaimRequest{
		RunnerID:     session.RunnerID,
		SessionID:    session.SessionID,
		Capacity:     99,
		Labels:       map[string]string{"workload": "sas-runner"},
		Capabilities: []protocol.Capability{{NodeType: "xflow.map"}},
		Now:          time.Now(),
	}

	// First issue the forged metadata with no assignment available, then inspect
	// Redis directly. This pins the persisted authority, not just the current
	// claim result.
	if _, ok, err := directory.ClaimForRunner(ctx, forgedPoll); err != nil {
		t.Fatalf("ClaimForRunner() forged metadata error = %v", err)
	} else if ok {
		t.Fatal("ClaimForRunner() claimed an assignment from an empty queue")
	}

	storedLabelsRaw, err := rdb.HGet(ctx, directory.keys.runnerLabels, session.RunnerID).Result()
	if err != nil {
		t.Fatalf("HGet(%s, %s) labels: %v", directory.keys.runnerLabels, session.RunnerID, err)
	}
	var storedLabels map[string]string
	if err := json.Unmarshal([]byte(storedLabelsRaw), &storedLabels); err != nil {
		t.Fatalf("decode stored labels %q: %v", storedLabelsRaw, err)
	}
	if !reflect.DeepEqual(storedLabels, registeredLabels) {
		t.Fatalf("stored labels = %v, want registered snapshot %v", storedLabels, registeredLabels)
	}

	storedCapabilitiesRaw, err := rdb.HGet(ctx, directory.keys.runnerCapabilities, session.RunnerID).Result()
	if err != nil {
		t.Fatalf("HGet(%s, %s) capabilities: %v", directory.keys.runnerCapabilities, session.RunnerID, err)
	}
	var storedCapabilities []protocol.Capability
	if err := json.Unmarshal([]byte(storedCapabilitiesRaw), &storedCapabilities); err != nil {
		t.Fatalf("decode stored capabilities %q: %v", storedCapabilitiesRaw, err)
	}
	if !reflect.DeepEqual(storedCapabilities, registeredCapabilities) {
		t.Fatalf("stored capabilities = %v, want registered snapshot %v", storedCapabilities, registeredCapabilities)
	}

	wrongWorkload := redisDirectoryTestAssignment("exec-labels/wrong-workload/activation-1")
	wrongWorkload.Routing.NodeType = "xflow.sas.sink"
	wrongWorkload.Routing.RunnerSelector = &types.RunnerSelector{
		Mode:        types.RunnerSelectorModeRequired,
		MatchLabels: map[string]string{"workload": "sas-runner"},
	}
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, wrongWorkload)
	if claim, ok, err := directory.ClaimForRunner(ctx, forgedPoll); err != nil {
		t.Fatalf("ClaimForRunner() wrong-workload error = %v", err)
	} else if ok {
		t.Fatalf("local runner claimed %q after forging workload=sas-runner", claim.Assignment.AssignmentID)
	}

	wrongCapability := redisDirectoryTestAssignment("exec-labels/wrong-capability/activation-1")
	wrongCapability.Routing.NodeType = "xflow.map"
	wrongCapability.Routing.RunnerSelector = &types.RunnerSelector{
		Mode:        types.RunnerSelectorModeRequired,
		MatchLabels: map[string]string{"workload": "local"},
	}
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, wrongCapability)
	if claim, ok, err := directory.ClaimForRunner(ctx, forgedPoll); err != nil {
		t.Fatalf("ClaimForRunner() wrong-capability error = %v", err)
	} else if ok {
		t.Fatalf("runner without xflow.map claimed %q after forging that capability", claim.Assignment.AssignmentID)
	}

	matching := redisDirectoryTestAssignment("exec-labels/matching-registration/activation-1")
	matching.Routing.NodeType = "xflow.sas.sink"
	matching.Routing.RunnerSelector = &types.RunnerSelector{
		Mode:        types.RunnerSelectorModeRequired,
		MatchLabels: map[string]string{"workload": "local"},
	}
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, matching)
	claim, ok, err := directory.ClaimForRunner(ctx, forgedPoll)
	if err != nil {
		t.Fatalf("ClaimForRunner() matching-registration error = %v", err)
	}
	if !ok {
		t.Fatal("registered local runner did not claim its matching assignment")
	}
	if claim.Assignment.AssignmentID != matching.AssignmentID {
		t.Fatalf("claimed %q, want matching registered assignment %q", claim.Assignment.AssignmentID, matching.AssignmentID)
	}
}

// TestRedisRunnerDirectoryCleanupDeletesEveryKeyItCreates checks the real-Redis
// tests' own teardown, which is the one piece of this package that no
// production assertion covers and that fails silently when it is wrong.
//
// It fails at the commit that introduces it: redisRunnerDirectoryAllKeys listed
// 24 of the 26 keys newRedisRunnerDirectoryKeys builds, omitting runnerLabels
// and runnerNamespaces. Those hashes carry no TTL — nothing in
// redis_runner_directory.go ever calls EXPIRE — so every real-Redis run left
// two keys behind under a prefix no later run reuses, accumulating forever in
// whatever Redis the harness pointed at. It was invisible because the uuid in
// the prefix means a leak never collides with anything, so no test ever got the
// wrong answer because of it; the instance just grows.
//
// A synthetic mutation would be the weaker evidence here. The list is test
// infrastructure, not production code, and this check earns its place by having
// caught a real omission that was already in the tree rather than one
// introduced to make it go red.
func TestRedisRunnerDirectoryCleanupDeletesEveryKeyItCreates(t *testing.T) {
	keys := newRedisRunnerDirectoryKeys("xflow:runner-directory:{cleanup-audit}")

	deleted := make(map[string]string, 32)
	for _, key := range redisRunnerDirectoryAllKeys(keys) {
		if previous, dup := deleted[key]; dup {
			t.Fatalf("redisRunnerDirectoryAllKeys lists %q twice (first as %s)", key, previous)
		}
		deleted[key] = ""
	}

	value := reflect.ValueOf(keys)
	structType := value.Type()
	var missing []string
	for i := range structType.NumField() {
		field := structType.Field(i)
		// prefix is the namespace the other keys are built from, never a key
		// that is written to Redis on its own.
		if field.Name == "prefix" {
			continue
		}
		key := value.Field(i).String()
		if key == "" {
			t.Fatalf("newRedisRunnerDirectoryKeys left %s empty", field.Name)
		}
		if _, ok := deleted[key]; !ok {
			missing = append(missing, field.Name)
			continue
		}
		deleted[key] = field.Name
	}
	if len(missing) > 0 {
		t.Fatalf("redisRunnerDirectoryAllKeys omits %v: every real-Redis test using "+
			"newRealRedisRunnerDirectory leaks those keys, and none of them expire",
			missing)
	}
	for key, owner := range deleted {
		if owner == "" {
			t.Fatalf("redisRunnerDirectoryAllKeys deletes %q, which "+
				"newRedisRunnerDirectoryKeys does not build", key)
		}
	}
}
