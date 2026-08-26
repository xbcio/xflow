package control

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/types"
)

// TestRedisRunnerDirectoryPersistsPolledLabelsForLaterPolls drives the two
// label-aware branches of ClaimForRunner against a real Redis:
// the HSet refresh at redis_runner_directory.go:279 and the RunnerSelector
// filter at :306.
//
// Neither has ever run against Redis. All five tests in runner_labels_test.go
// build NewMemoryRunnerDirectory(), whose labels live in a Go map, and
// runner_selector_test.go:40 calls MatchLabels directly with two literal maps.
// So the parts this exercises — that a poll's labels are marshalled, written to
// the runnerLabels hash under the runner's ID, and read back by
// runnerForClaim:943 on a later poll — had no coverage in either directory
// implementation.
//
// The third poll is where the teeth are. Dropping the HSet entirely would not
// break the second poll: effectiveLabels is a local variable, already set from
// req.Labels, so this poll routes correctly whether or not the write happens.
// The write only matters to the NEXT poll, and a runner sends Labels on the
// polls where they changed, not on every one. So the failure mode being pinned
// is a runner that routes correctly once and then stops matching its own
// selector — which looks like a scheduling problem, not a persistence one.
//
// Measured against all ten packages whose test-inclusive dependency closure
// contains service/control, with XFLOW_TEST_REDIS_ADDR set so this test really
// ran rather than skipping, and each mutation also run with this file removed:
//
//   - the HSet at :279 removed: the direct read below goes red first, since it
//     runs earlier. The third poll was then measured on its own, with that read
//     deleted, and reddens by itself ("claimed nothing") — so the behavioural
//     assertion carries the teeth this comment credits it with, rather than
//     riding on the storage one. Without this file the whole scope stays green.
//   - the MatchLabels filter at :306 removed: the first poll goes red (it
//     claims an assignment whose selector it does not satisfy); without this
//     file the whole scope stays green.
func TestRedisRunnerDirectoryPersistsPolledLabelsForLaterPolls(t *testing.T) {
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
	// Registered with no labels at all, which is the honest starting state: a
	// runner that has not yet reported any. Register stores "{}" for it.
	session := registerRedisDirectoryRunner(t, ctx, directory, "runner-labels", 3)

	north := func() *types.RunnerSelector {
		return &types.RunnerSelector{
			Mode:        types.RunnerSelectorModeRequired,
			MatchLabels: map[string]string{"zone": "cn-north"},
		}
	}

	first := redisDirectoryTestAssignment("exec-labels/node-a/activation-1")
	first.Routing.RunnerSelector = north()
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, first)

	// Poll 1: the runner reports no labels, so it does not satisfy the
	// selector and must be handed nothing.
	if claim, ok, err := directory.ClaimForRunner(ctx, labelledClaimRequest(session, 3, nil)); err != nil {
		t.Fatalf("ClaimForRunner() unlabelled error = %v", err)
	} else if ok {
		t.Fatalf("an unlabelled runner claimed %q, whose selector requires zone=cn-north: "+
			"the selector filter did not run", claim.Assignment.AssignmentID)
	}

	// Poll 2: the runner reports the label. It should claim, and it would
	// claim even if the write below never happened.
	claimed := mustClaim(t, ctx, directory, labelledClaimRequest(session, 3, map[string]string{"zone": "cn-north"}))
	if claimed.Assignment.AssignmentID != first.AssignmentID {
		t.Fatalf("claimed %q, want %q", claimed.Assignment.AssignmentID, first.AssignmentID)
	}

	// Name the storage location too, so a failure below says which of the two
	// halves broke rather than only that routing stopped working.
	stored, err := rdb.HGet(ctx, directory.keys.runnerLabels, session.RunnerID).Result()
	if err != nil {
		t.Fatalf("HGet(%s, %s) = %v, want the labels this poll reported",
			directory.keys.runnerLabels, session.RunnerID, err)
	}
	var decoded map[string]string
	if err := json.Unmarshal([]byte(stored), &decoded); err != nil {
		t.Fatalf("stored labels %q are not a JSON object: %v", stored, err)
	}
	if decoded["zone"] != "cn-north" {
		t.Fatalf("stored labels = %v, want zone=cn-north", decoded)
	}

	// Poll 3: no labels reported this time, which is the normal steady state.
	// The runner must still match, because the directory is supposed to
	// remember what it said last time.
	second := redisDirectoryTestAssignment("exec-labels/node-b/activation-1")
	second.Routing.RunnerSelector = north()
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, second)

	claimed = mustClaim(t, ctx, directory, labelledClaimRequest(session, 3, nil))
	if claimed.Assignment.AssignmentID != second.AssignmentID {
		t.Fatalf("claimed %q, want %q", claimed.Assignment.AssignmentID, second.AssignmentID)
	}

	// And the remembered labels must still be able to say no. A refresh that
	// stored something over-permissive would pass every assertion above.
	third := redisDirectoryTestAssignment("exec-labels/node-c/activation-1")
	third.Routing.RunnerSelector = &types.RunnerSelector{
		Mode:        types.RunnerSelectorModeRequired,
		MatchLabels: map[string]string{"zone": "cn-south"},
	}
	mustEnqueueRedisDirectoryAssignment(t, ctx, directory, third)

	if claim, ok, err := directory.ClaimForRunner(ctx, labelledClaimRequest(session, 3, nil)); err != nil {
		t.Fatalf("ClaimForRunner() mismatched-selector error = %v", err)
	} else if ok {
		t.Fatalf("a zone=cn-north runner claimed %q, which requires zone=cn-south",
			claim.Assignment.AssignmentID)
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

func labelledClaimRequest(session RunnerSession, capacity int, labels map[string]string) ClaimRequest {
	req := redisDirectoryClaimRequest(session, capacity)
	req.Labels = labels
	return req
}

func mustClaim(t *testing.T, ctx context.Context, directory *RedisRunnerDirectory, req ClaimRequest) Claim {
	t.Helper()

	claim, ok, err := directory.ClaimForRunner(ctx, req)
	if err != nil {
		t.Fatalf("ClaimForRunner() error = %v", err)
	}
	if !ok {
		t.Fatal("ClaimForRunner() claimed nothing, want the queued assignment: " +
			"the runner's labels satisfy its selector")
	}
	return claim
}
