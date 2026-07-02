package observability

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/observability/metrics"
	"github.com/xbcio/xflow/types"
)

// NodeTypeFunc resolves a node type for metrics labels. Return an empty string
// when the type is unknown.
type NodeTypeFunc func(types.ExecutionID, string) string

// HookChain fans engine lifecycle hooks out to multiple receivers.
type HookChain []engine.Hooks

func (c HookChain) OnNodeStart(ctx context.Context, id types.ExecutionID, name string) {
	c.each(ctx, func(h engine.Hooks, ctx context.Context) { h.OnNodeStart(ctx, id, name) })
}
func (c HookChain) OnNodeComplete(ctx context.Context, id types.ExecutionID, name string, status types.NodeStatus) {
	c.each(ctx, func(h engine.Hooks, ctx context.Context) { h.OnNodeComplete(ctx, id, name, status) })
}
func (c HookChain) OnNodeSuspended(ctx context.Context, id types.ExecutionID, name string) {
	c.each(ctx, func(h engine.Hooks, ctx context.Context) { h.OnNodeSuspended(ctx, id, name) })
}
func (c HookChain) OnExecutionComplete(ctx context.Context, id types.ExecutionID, status types.ExecutionStatus) {
	c.each(ctx, func(h engine.Hooks, ctx context.Context) { h.OnExecutionComplete(ctx, id, status) })
}
func (c HookChain) OnSignalDelivered(ctx context.Context, id types.ExecutionID, signalName string, data map[string]any) {
	c.each(ctx, func(h engine.Hooks, ctx context.Context) { h.OnSignalDelivered(ctx, id, signalName, data) })
}
func (c HookChain) OnSignalRevoked(ctx context.Context, id types.ExecutionID, signalName string) {
	c.each(ctx, func(h engine.Hooks, ctx context.Context) { h.OnSignalRevoked(ctx, id, signalName) })
}
func (c HookChain) OnNodeTimeout(ctx context.Context, id types.ExecutionID, nodeName string) {
	c.each(ctx, func(h engine.Hooks, ctx context.Context) { h.OnNodeTimeout(ctx, id, nodeName) })
}
func (c HookChain) OnNodeRetry(ctx context.Context, id types.ExecutionID, name string, attempt int, delay time.Duration) {
	c.each(ctx, func(h engine.Hooks, ctx context.Context) { h.OnNodeRetry(ctx, id, name, attempt, delay) })
}

func (c HookChain) each(ctx context.Context, fn func(engine.Hooks, context.Context)) {
	for _, h := range c {
		if h == nil {
			continue
		}
		engine.SafeHook(ctx, nil, func(ctx context.Context) { fn(h, ctx) })
	}
}

// MetricsHooks turns engine lifecycle hooks into xflow_ Prometheus counters.
type MetricsHooks struct {
	Metrics  *metrics.Metrics
	NodeType NodeTypeFunc

	started sync.Map // "exec\x00node" -> time.Time
}

func NewMetricsHooks(metrics *metrics.Metrics) *MetricsHooks {
	return &MetricsHooks{Metrics: metrics}
}

func (h *MetricsHooks) OnNodeStart(_ context.Context, id types.ExecutionID, name string) {
	h.Metrics.Inc("xflow_node_started_total", h.nodeLabels(id, name, ""))
	h.started.Store(h.nodeKey(id, name), time.Now())
}

func (h *MetricsHooks) OnNodeComplete(_ context.Context, id types.ExecutionID, name string, status types.NodeStatus) {
	labels := h.nodeLabels(id, name, string(status))
	h.Metrics.Inc("xflow_node_completed_total", labels)
	if started, ok := h.started.LoadAndDelete(h.nodeKey(id, name)); ok {
		h.Metrics.Observe("xflow_node_duration_seconds", labels, time.Since(started.(time.Time)))
	}
}

func (h *MetricsHooks) OnNodeSuspended(_ context.Context, id types.ExecutionID, name string) {
	h.Metrics.Inc("xflow_node_suspended_total", h.nodeLabels(id, name, ""))
}

func (h *MetricsHooks) OnExecutionComplete(_ context.Context, _ types.ExecutionID, status types.ExecutionStatus) {
	h.Metrics.Inc("xflow_execution_completed_total", map[string]string{"status": string(status)})
}

func (h *MetricsHooks) OnSignalDelivered(context.Context, types.ExecutionID, string, map[string]any) {
}
func (h *MetricsHooks) OnSignalRevoked(context.Context, types.ExecutionID, string) {}

func (h *MetricsHooks) OnNodeTimeout(_ context.Context, id types.ExecutionID, nodeName string) {
	h.Metrics.Inc("xflow_node_timed_out_total", h.nodeLabels(id, nodeName, ""))
}

func (h *MetricsHooks) OnNodeRetry(_ context.Context, id types.ExecutionID, name string, _ int, _ time.Duration) {
	h.Metrics.Inc("xflow_node_retried_total", h.nodeLabels(id, name, ""))
}

func (h *MetricsHooks) nodeLabels(id types.ExecutionID, name string, status string) map[string]string {
	labels := map[string]string{"node": name}
	nodeType := ""
	if h.NodeType != nil {
		nodeType = h.NodeType(id, name)
	}
	if nodeType == "" {
		nodeType = "unknown"
	}
	labels["node_type"] = nodeType
	if status != "" {
		labels["status"] = status
	}
	return labels
}

func (*MetricsHooks) nodeKey(id types.ExecutionID, name string) string {
	return fmt.Sprintf("%s\x00%s", id, name)
}

// AuditMetrics observes backend/asynq audit-store dual-write outcomes.
type AuditMetrics struct {
	Metrics *metrics.Metrics
}

func NewAuditMetrics(metrics *metrics.Metrics) AuditMetrics {
	return AuditMetrics{Metrics: metrics}
}

func (a AuditMetrics) OnAuditOK(op string) {
	a.Metrics.Inc("xflow_audit_write_total", map[string]string{"op": op, "result": "ok"})
}

func (a AuditMetrics) OnAuditFailed(op string, _ error) {
	a.Metrics.Inc("xflow_audit_write_total", map[string]string{"op": op, "result": "failed"})
}

// SweepMetrics observes lease sweeper outcomes.
type SweepMetrics struct {
	Metrics *metrics.Metrics
}

func NewSweepMetrics(metrics *metrics.Metrics) SweepMetrics {
	return SweepMetrics{Metrics: metrics}
}

func (s SweepMetrics) OnSweepReclaim(_, _ string, ageMs int64) {
	s.Metrics.Inc("xflow_lease_sweep_reclaimed_total", map[string]string{"result": "reclaimed"})
	s.Metrics.Observe("xflow_lease_age_seconds", map[string]string{"result": "reclaimed"}, time.Duration(ageMs)*time.Millisecond)
}

func (s SweepMetrics) OnSweepRace(_, _ string) {
	s.Metrics.Inc("xflow_lease_sweep_reclaimed_total", map[string]string{"result": "race"})
}

func (s SweepMetrics) OnSweepError(_, _ string, _ error) {
	s.Metrics.Inc("xflow_lease_sweep_errors_total", map[string]string{"reason": "reclaim_error"})
}

// DispatcherMetrics observes retryable dispatcher placement failures.
type DispatcherMetrics struct {
	Metrics *metrics.Metrics
}

func NewDispatcherMetrics(metrics *metrics.Metrics) DispatcherMetrics {
	return DispatcherMetrics{Metrics: metrics}
}

func (d DispatcherMetrics) OnDispatchTransient(reason string) {
	d.Metrics.Inc("xflow_dispatch_transient_total", map[string]string{"reason": reason})
}

// AuthMetrics observes runner-protocol auth decisions.
type AuthMetrics struct {
	Metrics *metrics.Metrics
}

func NewAuthMetrics(metrics *metrics.Metrics) AuthMetrics {
	return AuthMetrics{Metrics: metrics}
}

func (a AuthMetrics) OnAuthDecision(_, result, authMode string) {
	a.Metrics.Inc("xflow_runner_auth_decisions_total", map[string]string{"result": result, "auth_mode": authMode})
}
