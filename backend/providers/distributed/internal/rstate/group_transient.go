package rstate

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// defaultGroupCompletionTTL is the retention a transient group's artifacts get
// once the execution that owns them is terminal, for the case where neither the
// workflow nor the deployment named one.
//
// A default exists because the mode must not silently do nothing. groups[].mode
// is a workflow-author declaration and is independent of the deployment's
// execution mode: a control plane running no WithTransientMode has no
// store-wide completion TTL, and without a fallback `mode: transient` would
// retain everything for the full durable TTL — a no-op dressed as a
// fire-and-forget group, which is the one outcome the mode's compile-time
// validation (unknown values rejected) exists to prevent.
//
// The value mirrors sdk/xflow/execution_mode.go's defaultTransientCompletionTTL,
// the retention an execution that opted into transient mode gets when it names
// none. The backend cannot import sdk/xflow (the SDK sits above the provider
// layer), so the value is repeated and pinned by a test rather than shared.
const defaultGroupCompletionTTL = 30 * time.Second

// shortenTransientGroupCompletionTTL applies the completion retention to every
// transient group's Redis keys. It is called from each execution-terminal
// transition, next to shortenTransientCompletionTTL.
//
// It is a separate pass rather than part of shortenTransientCompletionTTL for
// two reasons:
//
//  1. That helper is gated on isTransient — the EXECUTION's mode. The case this
//     exists for is a transient group inside a durable workflow, where
//     isTransient is false and the helper returns before doing anything.
//  2. transientExecutionKeys enumerates per-node keys by node name and per-unit
//     counters by unit index, but never the group unit's own fence keys
//     (group:<idx>:status, group:<idx>:meta). Those keep the full active TTL
//     even in a transient WORKFLOW today, so this pass closes that gap too.
//
// Same best-effort shape as the execution-level shortening: the terminal
// transition is already durable when this runs, so a failure is logged by
// shortenTransientGroupCompletionTTLBestEffort rather than propagated.
//
// The enumeration needs the graph, so an execution that outlived its :graph key
// (possible for a durable execution whose wall-clock exceeds the exec TTL: the
// status key is re-EXPIREd on every transition, the graph key is not) degrades
// to "nothing shortened" instead of enumerating the wrong keys — the same
// degradation shortenTransientCompletionTTL makes for its node keys, and the
// safe direction: the artifacts keep the longer retention.
//
// The mode is read off the persisted graph rather than off
// GroupCommitRequest.Transient, even though the commit carries it. The commit is
// not where the decision is applied: a group's artifacts are shortened when the
// EXECUTION becomes terminal, which may happen in a later transition, on another
// replica, or inside a script (a node commit, a cancel) where no commit request
// exists. The graph is the compiled artifact both views derive from, it is
// already persisted, and it covers the groups that never committed at all — a
// canceled execution's leased group, whose keys are exactly the ones left
// behind. The request flag remains the engine-side contract for backends that
// consume it at commit time.
func (s *Store) shortenTransientGroupCompletionTTL(ctx context.Context, id types.ExecutionID) error {
	g, _ := s.LoadGraph(ctx, id)
	if g == nil {
		return nil
	}
	var groups []graph.GroupMeta
	for _, gm := range g.Groups() {
		if gm.Transient() {
			groups = append(groups, gm)
		}
	}
	if len(groups) == 0 {
		return nil
	}
	ttl := s.transientGroupCompletionTTL(ctx, id, g)
	if ttl <= 0 {
		return nil
	}

	t := namespace.FromContext(ctx)
	pipe := s.rdb.Pipeline()
	for _, gm := range groups {
		statusKey := groupUnitStatusKey(t, id, gm.UnitIdx)
		metaKey := groupUnitMetaKey(t, id, gm.UnitIdx)
		// The fence keys — the unit status and the lease token/attempt hash — are
		// floored at one full lease window so the fence cannot lapse while a
		// still-outstanding lease could present a token. In practice a late
		// commit for a terminal execution is rejected before this: both backends
		// check the execution status first and answer execution_inactive once it
		// is terminal — but that verdict depends on exec:status still being in
		// Redis, and in a transient WORKFLOW it is shortened in this very
		// transition. The floor keeps the group's own duplicate-detection record
		// (committed_lease_token) alive for at least as long as any lease that
		// could still be redeemed, which is what the design's "fence TTL >= lease
		// TTL plus margin" requires. transientMarkerTTLFactor is reused as the
		// margin for the same reason it exists there: it is already this store's
		// definition of "one full window plus safety".
		fenceTTL := ttl
		if leaseTTL := s.groupLeaseTTL(ctx, metaKey); leaseTTL > 0 {
			if floored := transientMarkerTTLFactor * leaseTTL; floored > fenceTTL {
				fenceTTL = floored
			}
		}
		pipe.Expire(ctx, statusKey, fenceTTL)
		pipe.Expire(ctx, metaKey, fenceTTL)
		for _, name := range transientGroupArtifactNames(g, gm) {
			// Exits and their node-meta pair. nodeMetaKey carries the private
			// output marker, so shortening the output without its meta would leave
			// a long-lived privacy bit — or, worse, let the bit expire before the
			// output it qualifies.
			pipe.Expire(ctx, outputKey(t, id, name), ttl)
			pipe.Expire(ctx, nodeMetaKey(t, id, name), ttl)
		}
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return fmt.Errorf("shorten transient group completion ttl %q: %w", id, err)
	}
	return nil
}

// shortenTransientGroupCompletionTTLBestEffort is the post-commit form of the
// pass above. See shortenTransientCompletionTTLBestEffort for why it cannot
// return an error and why the failure is logged instead of swallowed.
func (s *Store) shortenTransientGroupCompletionTTLBestEffort(ctx context.Context, id types.ExecutionID) {
	if err := s.shortenTransientGroupCompletionTTL(ctx, id); err != nil && s.logger != nil {
		s.logger.Error("transient_group_completion_ttl_shorten_failed", "execution_id", string(id), "err", err)
	}
}

// transientGroupCompletionTTL resolves the retention for a transient group's
// artifacts, mirroring how execution-level transient retention resolves:
//
//  1. the workflow's own TransientCompletionTTL override (Options), which is
//     taken unconditionally — the field is a workflow's way of saying how long
//     its ephemeral artifacts live, and a group marked transient is exactly
//     such an artifact. Reading it only under Options.Transient would make the
//     override inert for the very shape it fits, a transient group in an
//     otherwise durable workflow;
//  2. the per-execution marker or the store-wide transient completion TTL;
//  3. defaultGroupCompletionTTL, so the mode has an effect in a deployment that
//     configured no transient retention at all.
//
// The result is capped at the execution's active TTL: shortening must never
// extend, and a completion TTL longer than the key's own lifetime (a store
// configured with a very short execTTL, or a test store) would do exactly that.
func (s *Store) transientGroupCompletionTTL(ctx context.Context, id types.ExecutionID, g *graph.Graph) time.Duration {
	var ttl time.Duration
	if g != nil {
		ttl = g.TransientCompletionTTL()
	}
	if ttl <= 0 {
		ttl = s.getTransientCompletionTTL(ctx, id)
	}
	if ttl <= 0 {
		ttl = defaultGroupCompletionTTL
	}
	if active := s.getExecTTL(ctx, id); active > 0 && ttl > active {
		ttl = active
	}
	return ttl
}

// groupLeaseTTL reads the lease TTL the group's last acquisition recorded
// (acquireGroupLeaseLua ARGV 11, which survives the commit's HSET — the commit
// clears the token fields, not this one). Zero means the group never held a
// lease — a trigger-entry group seeded directly by the admission path — in
// which case there is no lease window to floor the fence against.
func (s *Store) groupLeaseTTL(ctx context.Context, metaKey string) time.Duration {
	ms, err := s.rdb.HGet(ctx, metaKey, "lease_ttl_ms").Int64()
	if err != nil || ms <= 0 {
		return 0
	}
	return time.Duration(ms) * time.Millisecond
}

// transientGroupArtifactNames returns the names whose output/node-meta keys
// belong to the group: every member, plus the group's own name — a group routed
// to error_output stores that payload under the group name (engine/group_exec.go
// writes NodeName: meta.Name / "error"), so the group name is a real output key,
// not a label.
func transientGroupArtifactNames(g *graph.Graph, gm graph.GroupMeta) []string {
	names := make([]string, 0, len(gm.Members)+1)
	for _, memberIdx := range gm.Members {
		if memberIdx < 0 || memberIdx >= g.NodeCount() {
			continue
		}
		names = append(names, g.NodeAt(memberIdx).Name)
	}
	return append(names, gm.Name)
}
