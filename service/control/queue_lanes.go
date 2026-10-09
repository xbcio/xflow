package control

import "strings"

// LaneWriteMode selects which queue keys a new assignment is written to while
// queue lanes roll out. It only takes effect when lanes are configured; with
// an empty lane list every mode degenerates to the legacy single-queue write
// this directory has always performed.
type LaneWriteMode string

const (
	// LaneWriteLegacyOnly writes new assignments to the shared legacy queue
	// only. This is the zero value and the exact pre-lane behavior.
	LaneWriteLegacyOnly LaneWriteMode = "legacy_only"
	// LaneWriteDual writes new assignments to both their lane queue and the
	// legacy queue. This is the rolling-upgrade window: every reader, old or
	// new, can still find every assignment.
	LaneWriteDual LaneWriteMode = "dual"
	// LaneWriteLaneOnly writes new assignments to their lane queue only. The
	// post-upgrade steady state, once no old reader remains.
	LaneWriteLaneOnly LaneWriteMode = "lane_only"
)

// normalizeLaneNodeTypes trims and deduplicates the configured lane node
// types, keeping first-occurrence order: the configured order is the claim
// walk's priority order, smallest lane first being expressed by declaring it
// first. The reserved legacy label (QueueLaneLegacy) is dropped rather than
// configured: it is not a node type, and a lane named after it would report
// its depth under the legacy queue's own label — one metric label silently
// summing two different queues.
func normalizeLaneNodeTypes(lanes []string) []string {
	normalized := make([]string, 0, len(lanes))
	seen := make(map[string]struct{}, len(lanes))
	for _, lane := range lanes {
		lane = strings.TrimSpace(lane)
		if lane == "" || lane == QueueLaneLegacy {
			continue
		}
		if _, dup := seen[lane]; dup {
			continue
		}
		seen[lane] = struct{}{}
		normalized = append(normalized, lane)
	}
	return normalized
}

// resolveQueueLane reports whether the routing node type is owned by a queue
// lane under the given whitelist. The returned lane is the node type itself:
// the queue key is composed from it by the key constructor, and the marker
// hash stores that resolved lane. Both the write side (which keys a new
// assignment is offered to) and the read side (which keys a runner walks) must
// resolve through this one function so the two sets can never disagree.
func resolveQueueLane(lanes []string, nodeType string) (string, bool) {
	for _, lane := range lanes {
		if lane == nodeType {
			return nodeType, true
		}
	}
	return "", false
}

// resolveLaneWriteMode applies the one gate every write mode shares: a mode
// that splits writes onto lanes only takes effect when lanes are configured, so
// an empty whitelist and an unrecognized mode both resolve to legacy-only. A
// config typo can then never split writes onto a lane that no configured reader
// walks.
func resolveLaneWriteMode(lanes []string, mode LaneWriteMode) LaneWriteMode {
	switch mode {
	case LaneWriteDual, LaneWriteLaneOnly:
		if len(lanes) > 0 {
			return mode
		}
	}
	return LaneWriteLegacyOnly
}

// lanePlacement is where one assignment's queue entry is offered: the lanes it
// is written to and what the marker records.
//
// A "" lane name is the legacy queue everywhere in this type — it is the
// fallback every mode keeps, the lane the marker names for a node type no lane
// owns. The marker is a pair rather than a bare name because "no marker" is a
// distinct state from "a marker naming the legacy queue": the two behave
// identically on the requeue path, but only the second is what the dual and
// lane-only modes write for a node type no lane owns, and the Redis marker hash
// cannot un-write that distinction once a reader may observe it.
type lanePlacement struct {
	targets   []string
	marker    string
	hasMarker bool
}

// resolveLanePlacement resolves the placement of one assignment from the lane
// whitelist, the effective write mode and the routing node type. Both
// directories resolve through this one function, so the in-memory double cannot
// disagree with the Redis implementation about where an entry goes.
func resolveLanePlacement(lanes []string, mode LaneWriteMode, nodeType string) lanePlacement {
	if len(lanes) == 0 {
		return lanePlacement{targets: []string{""}}
	}
	lane, inLane := resolveQueueLane(lanes, nodeType)
	switch {
	case mode == LaneWriteDual && inLane:
		return lanePlacement{targets: []string{lane, ""}, marker: lane, hasMarker: true}
	case mode == LaneWriteLaneOnly && inLane:
		return lanePlacement{targets: []string{lane}, marker: lane, hasMarker: true}
	case inLane:
		// legacy-only with a lane-owned node type: everything stays on the
		// legacy queue and no marker is written.
		return lanePlacement{targets: []string{""}}
	case mode == LaneWriteLegacyOnly:
		// Not a lane-owned node type in legacy-only mode: no marker either.
		return lanePlacement{targets: []string{""}}
	default:
		// Dual and lane-only are the modes that write markers, so a node type
		// no lane owns is marked as legacy explicitly.
		return lanePlacement{targets: []string{""}, marker: "", hasMarker: true}
	}
}
