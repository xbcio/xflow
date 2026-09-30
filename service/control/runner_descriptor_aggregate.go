package control

import (
	"context"
	"encoding/json"
	"sort"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
)

// AggregatedDescriptor is one (type, version) as the live runner fleet
// reports it. JSON is the winning runner's canonical types.Descriptor
// encoding. Pools is the sorted set of non-empty pool names that report the
// pair (every reporter, not only the winner); Namespaces is the sorted union
// of the reporting runners' effective namespaces. DistinctHashes above 1
// means reporters disagree on the descriptor; it is never put on the public
// wire.
type AggregatedDescriptor struct {
	Type           string
	Version        int
	Hash           string
	JSON           json.RawMessage
	Pools          []string
	Namespaces     []namespace.Namespace
	DistinctHashes int
}

// RunnerDescriptorConflictObserver receives, per node type, how many
// (type, version) descriptor conflicts the latest aggregation saw. Values are
// gauges: 0 means the type is consistent.
type RunnerDescriptorConflictObserver interface {
	OnRunnerDescriptorConflicts(ctx context.Context, nodeType string, conflicts int)
}

// FilterRunnerDescriptorRecords keeps the records whose effective namespaces
// contain ns or "*". Filter before aggregating: a conflict winner, and the
// pool list, must only ever come from runners the namespace may see, or one
// tenant's schema could be served to another.
func FilterRunnerDescriptorRecords(records []RunnerDescriptorRecord, ns namespace.Namespace) []RunnerDescriptorRecord {
	var out []RunnerDescriptorRecord
	for _, record := range records {
		for _, candidate := range record.Namespaces {
			if candidate == ns || candidate == "*" {
				out = append(out, record)
				break
			}
		}
	}
	return out
}

// AggregateRunnerDescriptors merges live runner records by (type, version).
// When every reporter carries the same hash the result is that descriptor.
// When hashes differ — a mixed-version rolling deploy is the usual cause —
// the most recently registered runner wins, ties going to the smaller runner
// ID, so the result (and any ETag over it) is deterministic. The result is
// sorted by type then version. It is a pure function: see
// ReportRunnerDescriptorConflicts for the log and metric.
func AggregateRunnerDescriptors(records []RunnerDescriptorRecord) []AggregatedDescriptor {
	type key struct {
		nodeType string
		version  int
	}
	type group struct {
		winner       RunnerNodeDescriptor
		winnerRunner string
		winnerAt     int64
		hashes       map[string]struct{}
		pools        map[string]struct{}
		namespaces   map[namespace.Namespace]struct{}
	}
	groups := make(map[key]*group)
	for _, record := range records {
		at := record.RegisteredAt.UnixNano()
		for _, d := range record.Descriptors {
			k := key{nodeType: d.Type, version: d.Version}
			g := groups[k]
			if g == nil {
				g = &group{
					winner:       d,
					winnerRunner: record.RunnerID,
					winnerAt:     at,
					hashes:       make(map[string]struct{}),
					pools:        make(map[string]struct{}),
					namespaces:   make(map[namespace.Namespace]struct{}),
				}
				groups[k] = g
			} else if at > g.winnerAt || (at == g.winnerAt && record.RunnerID < g.winnerRunner) {
				g.winner, g.winnerRunner, g.winnerAt = d, record.RunnerID, at
			}
			g.hashes[d.Hash] = struct{}{}
			if record.PoolName != "" {
				g.pools[record.PoolName] = struct{}{}
			}
			for _, ns := range record.Namespaces {
				g.namespaces[ns] = struct{}{}
			}
		}
	}
	out := make([]AggregatedDescriptor, 0, len(groups))
	for k, g := range groups {
		out = append(out, AggregatedDescriptor{
			Type:           k.nodeType,
			Version:        k.version,
			Hash:           g.winner.Hash,
			JSON:           append(json.RawMessage(nil), g.winner.JSON...),
			Pools:          sortedStringSet(g.pools),
			Namespaces:     sortedNamespaceSet(g.namespaces),
			DistinctHashes: len(g.hashes),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Type != out[j].Type {
			return out[i].Type < out[j].Type
		}
		return out[i].Version < out[j].Version
	})
	return out
}

// ReportRunnerDescriptorConflicts logs a warning for every conflicting
// (type, version) and sets the per-type conflict gauge for every type in the
// aggregation, 0 included, so a resolved conflict reads as resolved. Both
// logger and observer may be nil; an observer panic is swallowed.
func ReportRunnerDescriptorConflicts(ctx context.Context, logger engine.Logger, observer RunnerDescriptorConflictObserver, aggregated []AggregatedDescriptor) {
	perType := make(map[string]int)
	var order []string
	for _, a := range aggregated {
		if _, seen := perType[a.Type]; !seen {
			order = append(order, a.Type)
			perType[a.Type] = 0
		}
		if a.DistinctHashes <= 1 {
			continue
		}
		perType[a.Type]++
		if logger != nil {
			logger.Warn("runner_descriptor_conflict",
				"node_type", a.Type, "node_version", a.Version,
				"distinct_hashes", a.DistinctHashes, "winner_hash", a.Hash, "pools", a.Pools)
		}
	}
	if observer == nil {
		return
	}
	defer func() { _ = recover() }()
	for _, nodeType := range order {
		observer.OnRunnerDescriptorConflicts(ctx, nodeType, perType[nodeType])
	}
}

func sortedStringSet(set map[string]struct{}) []string {
	if len(set) == 0 {
		return nil
	}
	out := make([]string, 0, len(set))
	for v := range set {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

func sortedNamespaceSet(set map[namespace.Namespace]struct{}) []namespace.Namespace {
	if len(set) == 0 {
		return nil
	}
	out := make([]namespace.Namespace, 0, len(set))
	for v := range set {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
