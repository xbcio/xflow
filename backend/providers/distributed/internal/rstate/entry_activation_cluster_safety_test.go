package rstate

import (
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// TestEntryActivationLuaScriptKeyCounts pins the Redis Cluster audit that the
// assignment-transition design rests on: a legacy snapshot reaches
// assign/renew/fence as ARGV, never as a second KEY, so each of those scripts
// touches one slot only. Until now that claim lived solely in the doc comment
// on prepareEntryActivationTransitionLua. Reintroducing a legacy KEY would keep
// every other test in this package green, because miniredis and a single-node
// client both accept cross-slot KEYS without complaint — the failure would
// first appear as CROSSSLOT in a real cluster.
//
// The audited multi-key scripts are the ones carrying a second, co-located
// index key:
//
//   - assign/fence/renew take the per-workflow activation index (KEYS[2]),
//     which carries the same workflow digest hash tag as the record and the
//     watermark (asserted by
//     TestEntryActivationIndexKeysShareTheWorkflowSlot);
//   - upsert takes the watermark, the activation hash (KEYS[2]), and the same
//     per-workflow index (KEYS[3]) — all three on the workflow digest hash tag,
//     which TestRedisEntryActivationWorkflowTagIsSafe asserts holds even for
//     adversarial namespace and workflow IDs.
//
// Modeled on TestTriggerLuaScriptsAreSingleKey in internal/trigger.
func TestEntryActivationLuaScriptKeyCounts(t *testing.T) {
	scripts := map[string]struct {
		src     string
		maxKeys int
	}{
		"assignEntryActivationLua":                  {assignEntryActivationLuaSrc, 2},
		"fenceEntryActivationLua":                   {fenceEntryActivationLuaSrc, 2},
		"renewEntryActivationLua":                   {renewEntryActivationLuaSrc, 2},
		"advanceEntryActivationWorkflowRevisionLua": {advanceEntryActivationWorkflowRevisionLuaSrc, 1},
		"upsertEntryActivationLua":                  {upsertEntryActivationLuaSrc, 3},
		"releaseEntryActivationIndexRebuildLockLua": {releaseEntryActivationIndexRebuildLockLuaSrc, 1},
	}

	keysPattern := regexp.MustCompile(`KEYS\[(\d+)\]`)

	for name, script := range scripts {
		t.Run(name, func(t *testing.T) {
			seen := 0
			for _, match := range keysPattern.FindAllStringSubmatch(script.src, -1) {
				idx, err := strconv.Atoi(match[1])
				if err != nil {
					t.Fatalf("cannot parse KEYS index %q: %v", match[1], err)
				}
				if idx > seen {
					seen = idx
				}
				if idx > script.maxKeys {
					t.Errorf("references KEYS[%d]; the cluster-safety audit allows %d. Either pass the extra data as ARGV, or give the new key the same hash tag and record why here", idx, script.maxKeys)
				}
			}
			if seen == 0 {
				t.Fatalf("no KEYS reference found; the script source constant is probably not the one this script runs")
			}

			// Dynamic key construction inside redis.call / redis.pcall escapes the
			// declared KEYS and is a CROSSSLOT risk unless the constructed prefix
			// carries the same hash tag.
			for line := range strings.SplitSeq(script.src, "\n") {
				line = strings.TrimSpace(line)
				if (strings.Contains(line, "redis.call") || strings.Contains(line, "redis.pcall")) && strings.Contains(line, "..") {
					t.Errorf("constructs a key dynamically with '..': %s", line)
				}
			}
		})
	}
}

// TestEntryActivationIndexKeysShareTheWorkflowSlot pins the co-location the Lua
// key-count audit above relies on: the per-workflow activation index key must
// resolve to the same cluster slot as the modern record and watermark it is
// maintained with, or every SADD/EXPIRE the scripts perform would be a
// cross-slot operation in a real cluster. miniredis cannot catch that, so the
// assertion computes the slot both ways: the hash tag is checked literally and
// the CRC16 slot is computed for the whole key.
//
// The adversarial case blends braces (namespace and workflow both carry one)
// with the glob metacharacter that makes the legacy scan pattern
// over-inclusive.
func TestEntryActivationIndexKeysShareTheWorkflowSlot(t *testing.T) {
	cases := []struct {
		name string
		ns   namespace.Namespace
		wf   types.WorkflowID
	}{
		{name: "plain", ns: "index-slot-ns", wf: "wf-slot"},
		{name: "adversarial", ns: "tenant{attacker}*", wf: "wf{attacker}"},
		{name: "metacharacters", ns: "ns[with]?glob", wf: "wf/slash:colon"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			recordKey := workflowScopedEntryActivationRedisKey(tc.ns, tc.wf, "v1", "entry", 0)
			watermarkKey := entryActivationWorkflowRevisionRedisKey(tc.ns, tc.wf)
			indexKey := entryActivationWorkflowIndexRedisKey(tc.ns, tc.wf)
			wantTag := entryActivationWorkflowTag(tc.ns, tc.wf)
			wantSlot := entryActivationTestRedisSlot(recordKey)

			for name, key := range map[string]string{
				"record":    recordKey,
				"watermark": watermarkKey,
				"index":     indexKey,
			} {
				if got := firstRedisHashTag(key); got != wantTag {
					t.Errorf("%s key %q carries hash tag %q, want the workflow digest tag %q", name, key, got, wantTag)
				}
				if got := entryActivationTestRedisSlot(key); got != wantSlot {
					t.Errorf("%s key %q maps to slot %d, want the record slot %d", name, key, got, wantSlot)
				}
			}
		})
	}
}
