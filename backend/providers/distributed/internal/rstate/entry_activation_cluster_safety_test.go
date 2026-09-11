package rstate

import (
	"regexp"
	"strconv"
	"strings"
	"testing"
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
// upsertEntryActivationLua is the audited exception at two keys: the watermark
// and the activation hash co-locate on a workflow digest hash tag, which
// TestRedisEntryActivationWorkflowTagIsSafe asserts holds even for adversarial
// namespace and workflow IDs.
//
// Modeled on TestTriggerLuaScriptsAreSingleKey in internal/trigger.
func TestEntryActivationLuaScriptKeyCounts(t *testing.T) {
	scripts := map[string]struct {
		src     string
		maxKeys int
	}{
		"assignEntryActivationLua":                  {assignEntryActivationLuaSrc, 1},
		"fenceEntryActivationLua":                   {fenceEntryActivationLuaSrc, 1},
		"renewEntryActivationLua":                   {renewEntryActivationLuaSrc, 1},
		"advanceEntryActivationWorkflowRevisionLua": {advanceEntryActivationWorkflowRevisionLuaSrc, 1},
		"upsertEntryActivationLua":                  {upsertEntryActivationLuaSrc, 2},
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
