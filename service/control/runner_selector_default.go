package control

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// The default-mode selector fallback is a single rule with two enforcement
// points: entry activation placement (EntryActivationReconciler.chooseRunner →
// fallbackChooseRunner) and the claim path each runner directory walks. Both
// must agree on what "default mode" means and on what the grace window is, or
// the same match_labels declaration would be a soft preference at activation
// time and a hard filter at claim time — which is exactly the divergence these
// helpers exist to end. They live here, next to MatchLabels, because
// service/control is the only package both points can import.

// selectorIsDefault reports whether a selector uses "default" mode semantics:
// its labels are a placement preference whose grace window, once expired,
// stops being enforced.
//
// A nil selector means "any capable runner" — there are no labels to relax, so
// default-mode semantics do not apply to it (false). An empty Mode is
// RunnerSelectorModeDefault: per engine/graph/compile.go:resolveRunnerSelector
// the compiler defaults to "default" when no explicit mode is set, so a DSL
// author who wrote only `match_labels` gets default semantics, at both
// enforcement points.
func selectorIsDefault(sel *types.RunnerSelector) bool {
	if sel == nil {
		return false
	}
	return sel.Mode == types.RunnerSelectorModeDefault || sel.Mode == ""
}

// selectorEffectiveGrace is the grace window in force for s. A non-positive
// FallbackGrace means "unset", not "no grace": explicit zero is treated as
// unset everywhere RunnerSelector is built (see DefaultRunnerSelector).
func (s RunnerSelector) selectorEffectiveGrace() time.Duration {
	if s.FallbackGrace <= 0 {
		return DefaultSelectorFallback
	}
	return s.FallbackGrace
}

// selectorGraceElapsed reports whether a default-mode assignment whose labels
// this runner does not satisfy may stop enforcing them at now. False while the
// grace window is still running (and false for a required-mode selector and a
// nil one, which never relax), true once it has elapsed.
//
// queuedAt is where the window is anchored: the moment the assignment last
// entered a queue (memory: the queuedAt map; redis: the assignment:queued_at
// hash). The claim path deliberately anchors on that existing stamp rather
// than tracking a separate no-match-since map the way entry activation does —
// the stamp already survives a control-plane restart, and it is the same
// quantity the assignment queue oldest-age report exposes, so "why is this
// stuck" has one answer instead of two. A zero queuedAt (an entry an older
// control plane wrote before stamps existed) does NOT start an already-elapsed
// window: it is treated as still pending, so an unstamped entry cannot jump
// ahead of a stamped one.
func (s RunnerSelector) selectorGraceElapsed(sel *types.RunnerSelector, queuedAt, now time.Time) bool {
	if !selectorIsDefault(sel) {
		return false
	}
	if queuedAt.IsZero() {
		return false
	}
	return now.Sub(queuedAt) >= s.selectorEffectiveGrace()
}

// Selector rejection reasons. These are metric label values
// (xflow_runner_selector_rejected_total{reason}), so they are part of the
// observable surface and must stay bounded and stable.
const (
	// SelectorRejectLabelPending is a default-mode assignment whose labels no
	// runner has satisfied for less than the grace window so far. A rising
	// count with a flat claim rate is the "waiting on placement, not on
	// capacity" signal — precisely the one the queues' depth/age series could
	// not distinguish on their own.
	SelectorRejectLabelPending = "label_pending"
	// SelectorRejectLabelRequired is a "required"-mode assignment whose labels
	// no runner satisfies. It never falls back; it waits for a matching runner
	// to be deployed.
	SelectorRejectLabelRequired = "label_required"
	// SelectorRejectCapability is a runner that cannot host the node type at
	// all (or lacks a required feature/version).
	SelectorRejectCapability = "capability"
	// SelectorRejectPolicy is a runner whose policy does not allow the node
	// type.
	SelectorRejectPolicy = "policy"
	// SelectorRejectNamespace is a runner outside the assignment's namespace.
	// Namespace membership is an authorization boundary, never a preference.
	SelectorRejectNamespace = "namespace"
)

// RunnerSelectorRejectObserver is the optional RunnerClaimObserver extension
// that receives one event per claim-path selector rejection, classified by
// reason. It is discovered from the same observer the directories already
// accept (WithRedisRunnerDirectoryObserver /
// WithMemoryRunnerDirectoryObserver), so a host that installs an observer for
// one extension gets the others with no extra wiring — the pattern
// SweepQueueDepthObserver established.
//
// It exists because a label rejection used to be a bare `continue`: a queue
// whose head no live runner may claim looked exactly like a queue with no
// capacity, and the only way to tell them apart was to read runner
// registrations by hand.
type RunnerSelectorRejectObserver interface {
	// OnRunnerSelectorRejected reports one assignment rejected for this runner.
	// reason is one of the SelectorReject* values; ctx carries the namespace.
	// Implementations must be non-blocking.
	OnRunnerSelectorRejected(ctx context.Context, reason string)
}

// observeSelectorReject reports one rejection to the observer, when it
// implements the extension. Failures inside an observer must never break a
// claim walk, so the call is recovered — the contract every other observer
// seam in this package carries.
func (d *RedisRunnerDirectory) observeSelectorReject(ctx context.Context, reason string) {
	observer, ok := d.observer.(RunnerSelectorRejectObserver)
	if !ok {
		return
	}
	defer func() { _ = recover() }()
	observer.OnRunnerSelectorRejected(ctx, reason)
}

func (d *MemoryRunnerDirectory) observeSelectorReject(ctx context.Context, reason string) {
	observer, ok := d.observer.(RunnerSelectorRejectObserver)
	if !ok {
		return
	}
	defer func() { _ = recover() }()
	observer.OnRunnerSelectorRejected(ctx, reason)
}

// logSelectorGraceFallback emits the one structured line that records a claim
// path grace fallback: a default-mode selector's window elapsed and the
// assignment is being handed to a runner whose labels do not satisfy it. The
// counter says how often this happens; this says what it happened to. One line
// per fallback claim, not per (selector, runner) pair — the event being
// recorded is the claim, and a claim happens once.
//
// Nil logger and nil receivers are no-ops, mirroring every other optional
// logger in this package.
func (d *RedisRunnerDirectory) logSelectorGraceFallback(ctx context.Context, assignment Assignment) {
	if d == nil || d.log == nil {
		return
	}
	d.log.Warn("control: default-mode runner selector grace elapsed; claiming on a non-matching runner",
		"assignment_id", string(assignment.AssignmentID),
		"execution_id", string(assignment.Task.ExecutionID),
		"node", assignment.Task.NodeName,
		"node_type", assignment.Routing.NodeType,
		"namespace", string(namespace.FromContext(ctx)),
	)
}

func (d *MemoryRunnerDirectory) logSelectorGraceFallback(ctx context.Context, assignment Assignment) {
	if d == nil || d.log == nil {
		return
	}
	d.log.Warn("control: default-mode runner selector grace elapsed; claiming on a non-matching runner",
		"assignment_id", string(assignment.AssignmentID),
		"execution_id", string(assignment.Task.ExecutionID),
		"node", assignment.Task.NodeName,
		"node_type", assignment.Routing.NodeType,
		"namespace", string(namespace.FromContext(ctx)),
	)
}

// redisClaimCandidate is one page entry the walk resolved far enough to
// judge: its id, its raw payload (what the claim transition must match on),
// and the decoded assignment.
type redisClaimCandidate struct {
	assignmentID string
	raw          string
	assignment   Assignment
}

// maturedAssignments projects a candidate slice's assignments, for the
// liveness probe only. A page's candidate list and its assignment slice are
// always parallel.
func maturedAssignments(candidates []redisClaimCandidate) []Assignment {
	out := make([]Assignment, len(candidates))
	for i, c := range candidates {
		out[i] = c.assignment
	}
	return out
}

// matureGraceCandidates applies the default-mode grace rule to a page's
// label-missed candidates and splits them into the ones whose window has
// elapsed (claimable now, on capability/policy/namespace alone) and the ones
// still waiting.
//
// The window is anchored on the per-assignment queued-at stamp the enqueue and
// requeue transitions write, and measured against the Redis server's clock, in
// one round trip (HMGET + TIME). Reading the queue's clock is what keeps the
// verdict identical on every control-plane replica; a local clock would let
// two replicas disagree about a window by their skew. A candidate whose stamp
// is missing — an entry an older control plane queued before stamps existed —
// counts as still pending: it must not jump a stamped entry.
//
// Every candidate passed here is already default-mode (the caller holds back
// only those; required-mode misses are rejected inline), so the maturity test
// is the stamp comparison alone.
func (d *RedisRunnerDirectory) matureGraceCandidates(ctx context.Context, candidates []redisClaimCandidate) (matured, pending []redisClaimCandidate, roundTrips int, err error) {
	ids := make([]string, len(candidates))
	for i, c := range candidates {
		ids[i] = c.assignmentID
	}
	pipe := d.rdb.Pipeline()
	stamps := pipe.HMGet(ctx, d.keys.assignmentQueuedAt, ids...)
	serverTime := pipe.Time(ctx)
	roundTrips = 2 // HMGET + TIME
	if _, execErr := pipe.Exec(ctx); execErr != nil && !errors.Is(execErr, redis.Nil) {
		return nil, nil, roundTrips, fmt.Errorf("read assignment queued-at stamps: %w", execErr)
	}
	// A failed TIME degrades to the directory clock rather than failing the
	// walk: the grace verdict is advisory-soft (the fallback is the permitted
	// outcome), so a skew-sized error moves a window by milliseconds, while a
	// walk error would strand the entry entirely.
	now := d.clock()
	if t, timeErr := serverTime.Result(); timeErr == nil && !t.IsZero() {
		now = t
	}
	values := stamps.Val()
	for i, c := range candidates {
		var stamp string
		if i < len(values) {
			stamp, _ = values[i].(string)
		}
		queuedAtMs, parseErr := strconv.ParseInt(stamp, 10, 64)
		if parseErr != nil || queuedAtMs <= 0 {
			pending = append(pending, c)
			continue
		}
		// selectorGraceElapsed is the shared predicate, so this path and entry
		// activation cannot drift on what the window means.
		if !d.selector.selectorGraceElapsed(c.assignment.Routing.RunnerSelector, time.UnixMilli(queuedAtMs), now) {
			pending = append(pending, c)
			continue
		}
		matured = append(matured, c)
	}
	return matured, pending, roundTrips, nil
}
