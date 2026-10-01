package control

import (
	"context"
	"encoding/json"
	"sort"
	"sync"

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
// (type, version) descriptor conflicts the live fleet reports. Values are
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
// runnerDescriptorConflictReporter for the log and metric.
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

// runnerDescriptorConflictReporter turns aggregations of the whole live
// fleet into the conflict log and gauge. It must only ever be fed unfiltered
// aggregations: the gauge has no namespace label, so a namespace-filtered
// view would make it read whichever namespace was aggregated last.
//
// It remembers which node types it has gauged. A type that drops out of a
// later aggregation (its last reporter went away) is set back to 0 once and
// then forgotten, so a resolved or vanished conflict never keeps its last
// value. It also remembers the set of conflicting (type, version) pairs and
// logs only when that set changes: a mixed-version rolling deploy is the
// normal cause of a conflict, so a warning per read would fire steadily at
// editor request rate for the whole deploy. Safe for concurrent use; logger
// and observer may be nil, and an observer panic is swallowed.
type runnerDescriptorConflictReporter struct {
	logger   engine.Logger
	observer RunnerDescriptorConflictObserver

	mu         sync.Mutex
	gauged     map[string]struct{}
	conflicted map[runnerDescriptorConflictKey]struct{}
}

type runnerDescriptorConflictKey struct {
	nodeType string
	version  int
}

func newRunnerDescriptorConflictReporter(logger engine.Logger, observer RunnerDescriptorConflictObserver) *runnerDescriptorConflictReporter {
	return &runnerDescriptorConflictReporter{logger: logger, observer: observer}
}

// report sets the per-type conflict gauge for every type in the fleet-wide
// aggregation, 0 included, plus 0 for every previously gauged type no longer
// present. When the set of conflicting (type, version) pairs differs from the
// previous report it logs a warning for every current conflict, or one info
// line when the last conflict resolved.
func (r *runnerDescriptorConflictReporter) report(ctx context.Context, aggregated []AggregatedDescriptor) {
	if r == nil {
		return
	}
	perType := make(map[string]int)
	var order []string
	var conflicts []AggregatedDescriptor
	conflicted := make(map[runnerDescriptorConflictKey]struct{})
	for _, a := range aggregated {
		if _, seen := perType[a.Type]; !seen {
			order = append(order, a.Type)
			perType[a.Type] = 0
		}
		if a.DistinctHashes <= 1 {
			continue
		}
		perType[a.Type]++
		conflicts = append(conflicts, a)
		conflicted[runnerDescriptorConflictKey{nodeType: a.Type, version: a.Version}] = struct{}{}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if !sameRunnerDescriptorConflicts(r.conflicted, conflicted) {
		hadConflicts := len(r.conflicted) > 0
		r.conflicted = conflicted
		if r.logger != nil {
			for _, a := range conflicts {
				r.logger.Warn("runner_descriptor_conflict",
					"node_type", a.Type, "node_version", a.Version,
					"distinct_hashes", a.DistinctHashes, "winner_hash", a.Hash, "pools", a.Pools)
			}
			if len(conflicts) == 0 && hadConflicts {
				r.logger.Info("runner_descriptor_conflicts_resolved")
			}
		}
	}
	var vanished []string
	for nodeType := range r.gauged {
		if _, ok := perType[nodeType]; !ok {
			vanished = append(vanished, nodeType)
		}
	}
	sort.Strings(vanished)
	r.gauged = make(map[string]struct{}, len(order))
	for _, nodeType := range order {
		r.gauged[nodeType] = struct{}{}
	}
	if r.observer == nil {
		return
	}
	defer func() { _ = recover() }()
	for _, nodeType := range order {
		r.observer.OnRunnerDescriptorConflicts(ctx, nodeType, perType[nodeType])
	}
	for _, nodeType := range vanished {
		r.observer.OnRunnerDescriptorConflicts(ctx, nodeType, 0)
	}
}

func sameRunnerDescriptorConflicts(a, b map[runnerDescriptorConflictKey]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if _, ok := b[k]; !ok {
			return false
		}
	}
	return true
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
