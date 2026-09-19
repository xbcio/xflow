package metrics

import (
	"context"
	"time"

	"github.com/xbcio/xflow/types"
)

// Entry-seed admission metric names.
const (
	metricEntrySeedAdmission      = "xflow_entry_seed_admission_total"
	metricEntrySeedAdmissionDelay = "xflow_entry_seed_admission_duration_seconds"
)

// EntrySeedMetrics observes the entry-seed admission round trip — the POST to
// /v1/executions that admits one trigger batch's results.
//
// This is the only series that separates a TIMEOUT from the other ways that
// call can fail. The batch layer's xflow_trigger_batch_admissions_total folds
// every transport failure into state="error", reason="seed_transport", so a
// deployment whose batch size has outgrown the admission window is
// indistinguishable there from one hitting connection resets — and the two call
// for opposite responses: the first means lower the batch or raise the deadline,
// the second means look at the network. Here the timeout is its own outcome.
//
// The duration histogram is what makes the deadline a tuning decision rather
// than a cliff. A batch whose admissions sit at 9s against a 15s deadline is one
// traffic spike from redelivering everything, and only the distribution says
// so; the rate alone reads as healthy right up until it is not.
type EntrySeedMetrics struct {
	Metrics *Metrics
}

// NewEntrySeedMetrics creates an EntrySeedMetrics bound to the shared Metrics
// registry.
func NewEntrySeedMetrics(m *Metrics) EntrySeedMetrics { return EntrySeedMetrics{Metrics: m} }

// OnEntrySeedAdmission records one entry-seed admission attempt: the counter
// partitioned by outcome, and the duration histogram.
//
// One call in, both series out, so the count and the distribution can never
// disagree about how many attempts happened — the reason this is a single
// method and not a counter method plus a duration method. The same fan-out
// shape as GroupObserverAdapter.OnGroupAdmission.
//
// It satisfies types.EntrySeedObserver rather than being called from inside
// observability, so the producer (protocol.HTTPEntrySeedRuntime) needs no
// dependency on this package.
//
// Namespace is a label here, as on every runner-side trigger metric: a host
// that admits for several namespaces can attribute a timeout rate to the one
// whose batch grew. It is not usable on the group path (there is no ctx
// namespace at that call site) but this path always has one.
func (e EntrySeedMetrics) OnEntrySeedAdmission(ctx context.Context, outcome string, d time.Duration) {
	labels := map[string]string{"outcome": outcome}
	withNamespace(ctx, labels)
	e.Metrics.Inc(metricEntrySeedAdmission, labels)
	e.Metrics.Observe(metricEntrySeedAdmissionDelay, labels, d)
}

var _ types.EntrySeedObserver = EntrySeedMetrics{}
