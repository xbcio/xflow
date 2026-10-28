package control

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
)

// ErrRunnerDirectoryIncompatible reports that a runner directory supports no
// enumeration this package can classify against. It exists so an unsupported
// directory answers with an error instead of an empty snapshot list, which
// would otherwise look identical to "no runners registered".
var ErrRunnerDirectoryIncompatible = errors.New("control: runner directory does not enumerate runner snapshots")

// RunnerSnapshotLister is an optional RunnerDirectory capability: enumerate
// every registered runner's snapshot, propagating infrastructure failures.
//
// It exists because ActivationRunnerLister.ListLiveRunners deliberately swallows
// every storage error and returns nil, which is right for its metric and
// reconciler callers (a missed scrape must not fail a request) but wrong for a
// caller that must distinguish "the directory is unreachable" from "no runner
// is registered". The claim-availability check is such a caller.
type RunnerSnapshotLister interface {
	ListRunnerSnapshots(ctx context.Context) ([]RunnerSnapshot, error)
}

// EnumerateRunnerSnapshots reads every registered runner's snapshot from a
// directory, preferring the error-propagating RunnerSnapshotLister and falling
// back to ActivationRunnerLister (whose errors are invisible). A directory
// implementing neither yields ErrRunnerDirectoryIncompatible.
//
// Callers apply liveness themselves: the list is of registered runners, the
// same view the reconciler and the metrics collector enumerate.
func EnumerateRunnerSnapshots(ctx context.Context, directory RunnerDirectory) ([]RunnerSnapshot, error) {
	if directory == nil {
		return nil, ErrRunnerDirectoryIncompatible
	}
	if lister, ok := directory.(RunnerSnapshotLister); ok {
		return lister.ListRunnerSnapshots(ctx)
	}
	if lister, ok := directory.(ActivationRunnerLister); ok {
		return lister.ListLiveRunners(ctx), nil
	}
	return nil, fmt.Errorf("%w: %T", ErrRunnerDirectoryIncompatible, directory)
}

// ClassifyUnitClaim reports whether at least one live, non-draining runner that
// serves ns could claim an assignment routing to routing, under STRICT label
// semantics.
//
// It reuses the dispatch path's own predicates — IsLive, canServeNamespace,
// MatchCapabilities and MatchLabels — rather than re-implementing rules, so the
// verdict cannot drift from what ClaimForRunner will do. It is strict where the
// claim path is soft: a "default"-mode selector whose labels no live runner
// satisfies is reported as a gap (SelectorRejectLabelPending) even though the
// claim path would hand the assignment to any capable runner once the grace
// window has elapsed. That is the point — the caller is deciding whether work
// may start NOW and where it would land, and the fallback lands it on a runner
// that does not match the declared placement.
//
// claimable is true as soon as one runner passes every gate. When none does,
// reason is the first gate with no survivors, ordered the way the claim path
// judges runners:
//
//   - SelectorRejectNamespace: no live, non-draining runner serves this
//     namespace — none is registered, every registrant is outside the
//     namespace, or every in-namespace registrant's heartbeat has expired.
//   - SelectorRejectCapability: a runner serves the namespace but none can
//     host the unit's node type/requirements.
//   - SelectorRejectLabelPending / SelectorRejectLabelRequired: a runner serves
//     the namespace and can host the unit, but none satisfies the selector's
//     labels; the mode picks the reason (default -> pending, required ->
//     required), never a grace window.
//
// SelectorRejectPolicy is never returned: a RunnerSnapshot carries no
// RunnerPolicy (it lives in the directory's per-runner record and the claim
// path reads it only when it claims), and re-implementing the policy read would
// duplicate directory internals. The gap this leaves is provably small:
// Core.register refuses a registration whose declared capabilities or
// namespaces fall outside the runner's authenticated policy, so for any runner
// the directory knows about, passing capability and namespace already means
// passing policy.
//
// Capacity and in-flight load are not considered: a fully busy runner is still
// a candidate. Draining runners are not: neither directory hands a new claim to
// a runner the control plane has told to drain.
func ClassifyUnitClaim(snapshots []RunnerSnapshot, routing engine.TaskRouting, ns namespace.Namespace, now time.Time) (claimable bool, reason string) {
	selector := DefaultRunnerSelector()
	var required map[string]string
	if routing.RunnerSelector != nil {
		required = routing.RunnerSelector.MatchLabels
	}
	var servingNamespace, capable bool
	for _, snap := range snapshots {
		if !canServeNamespace(snap.Namespaces, ns) {
			continue
		}
		if !selector.IsLive(snap, now) {
			continue
		}
		if snap.Control != nil && snap.Control.DesiredState == RunnerDesiredStateDraining {
			continue
		}
		servingNamespace = true
		if !MatchCapabilities(snap.Capabilities, routing) {
			continue
		}
		capable = true
		if MatchLabels(snap.Labels, required) {
			return true, ""
		}
	}
	switch {
	case !servingNamespace:
		return false, SelectorRejectNamespace
	case !capable:
		return false, SelectorRejectCapability
	case selectorIsDefault(routing.RunnerSelector):
		return false, SelectorRejectLabelPending
	default:
		return false, SelectorRejectLabelRequired
	}
}
