package control

import (
	"slices"
	"strings"
	"testing"
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
