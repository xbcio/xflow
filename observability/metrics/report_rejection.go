package metrics

import (
	"context"
)

// Control-plane report-path metric names.
//
// Before these existed, every rejection on the report path collapsed into one
// HTTP 409 with one body, so an operator could see that reports were being
// refused but not which of two structurally different things had happened:
//
//   - the DIRECTORY could not resolve the lease (no record, or the echoed
//     identity does not match the record). The runner's work is unfenced
//     because the directory lost track of it.
//   - the ENGINE refused the token at commit. The directory DID resolve the
//     lease (the lookup succeeded), and the engine's node still carries a
//     different lease. This is the directory/engine divergence.
//
// The two need opposite responses — the first is a directory/lease-retention
// problem, the second is a re-lease/redelivery problem — and telling them apart
// used to require a log post-mortem with a DEBUG level guarantee.
const (
	metricReportRejections = "xflow_report_rejections_total"
	metricLeaseDivergence  = "xflow_lease_view_divergence_total"
)

// reportRejectionObserver mirrors service/control.ReportRejectionObserver
// locally, the way the other adapters in this package do, so the mirror fails
// to compile here rather than silently detaching from the producer.
type reportRejectionObserver interface {
	OnReportRejected(ctx context.Context, reason string)
	OnReportRejectionDivergence(ctx context.Context)
}

var _ reportRejectionObserver = ReportRejectionMetrics{}

// ReportRejectionMetrics counts rejected runner result reports.
type ReportRejectionMetrics struct {
	Metrics *Metrics
}

func NewReportRejectionMetrics(metrics *Metrics) ReportRejectionMetrics {
	return ReportRejectionMetrics{Metrics: metrics}
}

// OnReportRejected counts one rejection. reason is one of the ReportRejected*
// constants in service/control: a small closed set, never a runner or lease
// identifier.
func (r ReportRejectionMetrics) OnReportRejected(ctx context.Context, reason string) {
	r.Metrics.Inc(metricReportRejections, withNamespace(ctx, map[string]string{"reason": reason}))
}

// OnReportRejectionDivergence counts the subset where the directory resolved the
// lease and the engine still refused its token.
//
// This is the state the R6 investigation could not observe: the two views of the
// same lease disagree, nothing logs it, and it surfaces only as a 409. It is
// derived from the directory's own verdict on a follow-up lookup, not assumed
// from the engine's answer, so a non-zero series is direct evidence of a live
// directory record for a token the engine no longer honours — not of a release.
func (r ReportRejectionMetrics) OnReportRejectionDivergence(ctx context.Context) {
	r.Metrics.Inc(metricLeaseDivergence, withNamespace(ctx, nil))
}
