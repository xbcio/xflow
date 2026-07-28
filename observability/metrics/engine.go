package metrics

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// Engine metric names.
const (
	metricNodeStarted               = "xflow_node_started_total"
	metricNodeCompleted             = "xflow_node_completed_total"
	metricNodeDuration              = "xflow_node_duration_seconds"
	metricNodeSuspended             = "xflow_node_suspended_total"
	metricNodeTimedOut              = "xflow_node_timed_out_total"
	metricNodeRetried               = "xflow_node_retried_total"
	metricExecutionCompleted        = "xflow_execution_completed_total"
	metricCommitOutcomes            = "xflow_commit_outcomes_total"
	metricOutboxRetries             = "xflow_outbox_retries_total"
	metricOutboxDeadLettersTotal    = "xflow_outbox_dead_letters_total"
	metricOutboxDeadLetters         = "xflow_outbox_dead_letters"
	metricOutboxDeadLettersReplayed = "xflow_outbox_dead_letters_replayed_total"
	metricOutboxPending             = "xflow_outbox_pending"
	metricOutboxOldestPendingAge    = "xflow_outbox_oldest_pending_age_seconds"
	metricOutboxErrors              = "xflow_outbox_errors_total"
)

// MetricsHooks turns engine lifecycle hooks into xflow_ Prometheus counters.
type MetricsHooks struct {
	Metrics *Metrics

	started sync.Map // "exec\x00node" -> time.Time
}

func NewMetricsHooks(metrics *Metrics) *MetricsHooks {
	return &MetricsHooks{Metrics: metrics}
}

func (h *MetricsHooks) OnNodeStart(ctx context.Context, id types.ExecutionID, name string) {
	h.Metrics.Inc(metricNodeStarted, h.nodeLabels(ctx, id, name, ""))
	h.started.Store(h.nodeKey(id, name), time.Now())
}

func (h *MetricsHooks) OnNodeComplete(ctx context.Context, id types.ExecutionID, name string, status types.NodeStatus) {
	labels := h.nodeLabels(ctx, id, name, string(status))
	h.Metrics.Inc(metricNodeCompleted, labels)
	if started, ok := h.started.LoadAndDelete(h.nodeKey(id, name)); ok {
		h.Metrics.Observe(metricNodeDuration, labels, time.Since(started.(time.Time)))
	}
}

func (h *MetricsHooks) OnNodeSuspended(ctx context.Context, id types.ExecutionID, name string) {
	h.Metrics.Inc(metricNodeSuspended, h.nodeLabels(ctx, id, name, ""))
}

func (h *MetricsHooks) OnExecutionComplete(ctx context.Context, _ types.ExecutionID, status types.ExecutionStatus) {
	h.Metrics.Inc(metricExecutionCompleted, withNamespace(ctx, map[string]string{"status": string(status)}))
}

func (h *MetricsHooks) OnSignalDelivered(context.Context, types.ExecutionID, string, map[string]any) {
}
func (h *MetricsHooks) OnSignalRevoked(context.Context, types.ExecutionID, string) {}

func (h *MetricsHooks) OnNodeTimeout(ctx context.Context, id types.ExecutionID, nodeName string) {
	h.Metrics.Inc(metricNodeTimedOut, h.nodeLabels(ctx, id, nodeName, ""))
}

func (h *MetricsHooks) OnNodeRetry(ctx context.Context, id types.ExecutionID, name string, _ int, _ time.Duration) {
	h.Metrics.Inc(metricNodeRetried, h.nodeLabels(ctx, id, name, ""))
}

func (h *MetricsHooks) nodeLabels(ctx context.Context, _ types.ExecutionID, name string, status string) map[string]string {
	labels := map[string]string{"node": name}
	if status != "" {
		labels["status"] = status
	}
	return withNamespace(ctx, labels)
}

func (*MetricsHooks) nodeKey(id types.ExecutionID, name string) string {
	return fmt.Sprintf("%s\x00%s", id, name)
}

var _ engine.Hooks = (*MetricsHooks)(nil)

// CommitMetrics observes structured runner result-commit outcomes.
type CommitMetrics struct {
	Metrics *Metrics
}

// NewCommitMetrics creates a commit outcome observer backed by Metrics.
func NewCommitMetrics(metrics *Metrics) CommitMetrics {
	return CommitMetrics{Metrics: metrics}
}

// OnCommitOutcome records one stable low-cardinality commit classification.
func (c CommitMetrics) OnCommitOutcome(ctx context.Context, outcome engine.CommitOutcome) {
	c.Metrics.Inc(metricCommitOutcomes, withNamespace(ctx, map[string]string{"outcome": string(outcome)}))
}

var _ engine.CommitObserver = CommitMetrics{}

// OutboxMetrics observes durable outbox delivery failures and periodic backlog
// snapshots. It intentionally does not expose entry, execution, or error IDs.
type OutboxMetrics struct {
	Metrics *Metrics
}

// NewOutboxMetrics creates a durable outbox observer backed by Metrics.
func NewOutboxMetrics(metrics *Metrics) OutboxMetrics {
	return OutboxMetrics{Metrics: metrics}
}

// OnOutboxRetry records a retryable task-queue handoff failure.
func (o OutboxMetrics) OnOutboxRetry(ctx context.Context, _ int) {
	o.Metrics.Inc(metricOutboxRetries, withNamespace(ctx, nil))
}

// OnOutboxDeadLetter records an entry moved to durable dead-letter storage.
func (o OutboxMetrics) OnOutboxDeadLetter(ctx context.Context) {
	o.Metrics.Inc(metricOutboxDeadLettersTotal, withNamespace(ctx, nil))
}

// OnOutboxReplayed records a dead-letter replay attempt, partitioned by
// outcome (replayed/not_found/rejected_terminal/rejected_inactive).
func (o OutboxMetrics) OnOutboxReplayed(ctx context.Context, outcome engine.DeadLetterReplayOutcome) {
	o.Metrics.Inc(metricOutboxDeadLettersReplayed, withNamespace(ctx, map[string]string{"outcome": string(outcome)}))
}

// OnOutboxPending records the current pending and dead-letter backlog gauges
// and observes the age of the oldest pending entry when one exists.
func (o OutboxMetrics) OnOutboxPending(ctx context.Context, pending int, deadLettered int, oldestAge time.Duration) {
	o.Metrics.Set(metricOutboxPending, withNamespace(ctx, nil), float64(pending))
	o.Metrics.Set(metricOutboxDeadLetters, withNamespace(ctx, nil), float64(deadLettered))
	if pending > 0 {
		o.Metrics.Observe(metricOutboxOldestPendingAge, withNamespace(ctx, nil), oldestAge)
	}
}

// OnOutboxError records an outbox operation failure without using error text
// as a metric label.
func (o OutboxMetrics) OnOutboxError(ctx context.Context, operation string, _ error) {
	o.Metrics.Inc(metricOutboxErrors, withNamespace(ctx, map[string]string{"op": operation}))
}

var _ engine.OutboxObserver = OutboxMetrics{}
