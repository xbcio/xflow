package control

import (
	"context"

	"github.com/xbcio/xflow/engine"
)

// ReportRejected* name the fence that refused a runner result report. They are a
// closed set and the only values ReportRejectionObserver.OnReportRejected takes,
// so they are safe as metric label values.
//
// The distinction is the point: all four surface to a runner as one HTTP 409,
// and the reason the R6 investigation could not attribute the observed 409s is
// that three of them require a directory lookup to FAIL while the fourth
// requires it to SUCCEED. Counting them together loses exactly the information
// needed to tell "the directory lost the lease" from "the engine re-leased it
// under a directory record that is still there".
const (
	// ReportRejectedDirectoryUnavailable: the runner directory does not
	// implement LeaseLookup, so the report path fails closed. Configuration
	// defect, never a lease race.
	ReportRejectedDirectoryUnavailable = "directory_unavailable"
	// ReportRejectedDirectoryLeaseNotFound: the directory resolved no finalized
	// lease for this (runner, session, lease identity). Covers a released lease,
	// a wrong runner/session, and an expired lease-metadata key.
	ReportRejectedDirectoryLeaseNotFound = "directory_lease_not_found"
	// ReportRejectedDirectoryImmutableMismatch: the directory resolved a lease
	// but its immutable fields disagree with what the runner echoed, so the
	// directory holds a record for a DIFFERENT lease under the same address.
	ReportRejectedDirectoryImmutableMismatch = "directory_immutable_mismatch"
	// ReportRejectedEngineStaleToken: the directory resolved the lease and the
	// engine refused its token at commit. This is the only reason that is
	// evidence of the two lease views disagreeing.
	ReportRejectedEngineStaleToken = "engine_stale_token"
)

// ReportRejectionObserver receives one event per rejected runner result report.
// Implementations must be non-blocking and must not use runner, lease,
// execution, or node identifiers as metric labels.
type ReportRejectionObserver interface {
	OnReportRejected(ctx context.Context, reason string)
	// OnReportRejectionDivergence fires only for the subset where the directory
	// resolved the lease and the engine refused its token anyway.
	OnReportRejectionDivergence(ctx context.Context)
}

// observeReportRejected reports one rejection. Nil-safe: an unset observer
// leaves the report path byte-identical to before.
func (c *Core) observeReportRejected(ctx context.Context, reason string) {
	if c == nil || c.reportRejectionObserver == nil {
		return
	}
	// A metrics adapter must never be able to fail a report.
	defer func() { _ = recover() }()
	c.reportRejectionObserver.OnReportRejected(ctx, reason)
}

// observeReportDivergence reports the directory/engine disagreement. Called only
// after the directory has been re-asked and has confirmed it still resolves the
// lease, so a non-zero count is measured rather than inferred.
func (c *Core) observeReportDivergence(ctx context.Context) {
	if c == nil || c.reportRejectionObserver == nil {
		return
	}
	defer func() { _ = recover() }()
	c.reportRejectionObserver.OnReportRejectionDivergence(ctx)
}

// reportLeaseStillResolvable re-asks the directory whether the SAME lease the
// runner echoed — same three credentials, not merely the same assignment — is
// still resolvable, for the (runner, session) pair the report came in on.
//
// It exists solely to attribute an engine-rejected commit. The engine has just
// said the token no longer fences its node, so the two ways that can be true are
// worth separating:
//
//   - the directory still resolves the ECHOED identity. Both views hold the same
//     lease and disagree about whether it is live. This is the divergence: the
//     directory record is real, the engine's node has moved past it.
//   - the directory no longer resolves it, or now resolves a DIFFERENT
//     identity for that address. Only one view was ever holding this lease by
//     the time the commit ran, which is the ordinary superseded-report shape.
//
// LeaseID and LeaseToken are checked individually rather than compared to each
// other: an empty-value comparison would pass vacuously whenever a caller
// omitted one of the two, and a hit on an empty credential proves nothing.
//
// Best effort by design. The rejection is already decided, so any error from the
// probe (or a directory without the capability) reports "not resolvable" and
// costs only a missing data point.
func (c *Core) reportLeaseStillResolvable(ctx context.Context, runnerID, sessionID string, echoed *engine.TaskLease) bool {
	if runnerID == "" || sessionID == "" || echoed == nil || c.runners == nil {
		return false
	}
	if echoed.LeaseID == "" && echoed.LeaseToken == "" {
		return false
	}
	lookup, ok := c.runners.(LeaseLookup)
	if !ok {
		return false
	}
	resolved, found, err := lookup.LookupLease(ctx, runnerID, sessionID, LeaseLookupKey{
		AssignmentID: BuildAssignmentID(&echoed.Task),
		LeaseID:      echoed.LeaseID,
		LeaseToken:   echoed.LeaseToken,
	})
	if err != nil || !found || resolved == nil {
		return false
	}
	if echoed.LeaseToken != "" && resolved.LeaseToken != echoed.LeaseToken {
		return false
	}
	if echoed.LeaseID != "" && resolved.LeaseID != echoed.LeaseID {
		return false
	}
	return true
}
