package types

import (
	"context"
	"sort"
	"sync"
)

// ArtifactUse records that one node executed one content-addressed artifact.
//
// It is host-attested: the only writer is Record, which the node layer calls
// with its own NodeName and the digest it actually resolved code from. A guest
// script cannot reach a collector — it lives in the context, and a guest sees
// only its globals and its return value.
type ArtifactUse struct {
	Node   string
	Digest string
}

// ArtifactUseCollector accumulates the distinct artifacts executed under one
// context. It is created by the host at a batch (or single-node) boundary, read
// by the host when it assembles that boundary's result, and discarded.
//
// Deduplication is what keeps the payload O(distinct artifacts) rather than
// O(items): a map body running 5000 items through decode+clean records 10000
// times and yields two entries.
//
// A map body runs its items concurrently, so Record must be safe for concurrent
// use.
type ArtifactUseCollector struct {
	mu   sync.Mutex
	seen map[ArtifactUse]struct{}
}

type artifactUseCollectorKey struct{}

// WithArtifactUseCollector returns a context carrying a fresh collector, and
// the collector itself. The caller keeps the returned pointer: reading the
// collector back out of the context works too, but holding it makes the
// producer/consumer pair visible at the call site instead of implicit.
func WithArtifactUseCollector(ctx context.Context) (context.Context, *ArtifactUseCollector) {
	c := &ArtifactUseCollector{seen: make(map[ArtifactUse]struct{})}
	return context.WithValue(ctx, artifactUseCollectorKey{}, c), c
}

// ArtifactUseCollectorFrom returns the collector carried by ctx, or nil when
// there is none. Nil is the normal case for any path that does not assemble a
// boundary result, and every method tolerates it.
func ArtifactUseCollectorFrom(ctx context.Context) *ArtifactUseCollector {
	c, _ := ctx.Value(artifactUseCollectorKey{}).(*ArtifactUseCollector)
	return c
}

// ContextWithArtifactUseCollector puts an EXISTING collector on ctx, as opposed
// to WithArtifactUseCollector which creates a fresh one. It is what lets a
// collector survive the local queue's context.Background() reset: the value
// travels as data on the task lease and is re-seeded here, on the far side of
// the boundary, exactly like namespace does (execution/runner.go:144-146).
//
// A nil collector returns ctx unchanged rather than seeding a nil entry, so a
// caller need not branch and ArtifactUseCollectorFrom keeps returning nil for
// "nobody is collecting" rather than a non-nil interface holding nil.
func ContextWithArtifactUseCollector(ctx context.Context, c *ArtifactUseCollector) context.Context {
	if c == nil {
		return ctx
	}
	return context.WithValue(ctx, artifactUseCollectorKey{}, c)
}

// Record notes that node executed the artifact identified by digest.
//
// A nil receiver is a no-op: handlers call this unconditionally, and a path
// with no collector is a legitimate configuration, not a fault. An empty digest
// is also ignored — it means the node ran inline code rather than an artifact,
// and a {node, ""} entry would name nothing a consumer could resolve.
func (c *ArtifactUseCollector) Record(node, digest string) {
	if c == nil || digest == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seen[ArtifactUse{Node: node, Digest: digest}] = struct{}{}
}

// Uses returns the distinct recorded uses, ordered by node then digest.
//
// The order is deterministic because this value lands on a node's output, and
// an output that reorders between two identical runs makes every downstream
// equality assertion flaky.
func (c *ArtifactUseCollector) Uses() []ArtifactUse {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.seen) == 0 {
		return nil
	}
	out := make([]ArtifactUse, 0, len(c.seen))
	for u := range c.seen {
		out = append(out, u)
	}
	sortArtifactUses(out)
	return out
}

// ArtifactUsesAsData converts uses into the shape carried on node output.
//
// It is deliberately []any of map[string]any rather than []ArtifactUse: node
// output round-trips through JSON on the durable path, so a typed slice would
// arrive as this shape anyway. Producing it from the start keeps the in-process
// and durable paths byte-identical, so a test on the local backend asserts the
// shape production actually produces.
func ArtifactUsesAsData(uses []ArtifactUse) []any {
	if len(uses) == 0 {
		return nil
	}
	out := make([]any, 0, len(uses))
	for _, u := range uses {
		out = append(out, map[string]any{"node": u.Node, "digest": u.Digest})
	}
	return out
}

// ArtifactUsesFromData is the inverse of ArtifactUsesAsData, used when merging
// the per-batch lists the expansion barrier stored. Entries it cannot read are
// skipped rather than failing the merge: the barrier's results are whatever a
// previous version of this code wrote, and dropping an unreadable entry loses
// one provenance record while failing would lose the whole map node's output.
func ArtifactUsesFromData(v any) []ArtifactUse {
	list, ok := v.([]any)
	if !ok {
		return nil
	}
	var out []ArtifactUse
	for _, entry := range list {
		m, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		digest, _ := m["digest"].(string)
		if digest == "" {
			continue
		}
		node, _ := m["node"].(string)
		out = append(out, ArtifactUse{Node: node, Digest: digest})
	}
	return out
}

func sortArtifactUses(uses []ArtifactUse) {
	sort.Slice(uses, func(i, j int) bool {
		if uses[i].Node != uses[j].Node {
			return uses[i].Node < uses[j].Node
		}
		return uses[i].Digest < uses[j].Digest
	})
}
