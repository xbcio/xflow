package metrics

import (
	"context"
	"time"

	"github.com/xbcio/xflow/types"
)

// Execution drift repair metric names.
const (
	metricExecutionDriftScan         = "xflow_execution_drift_scan_total"
	metricExecutionDriftScanDuration = "xflow_execution_drift_scan_duration_seconds"
	metricExecutionDriftRepaired     = "xflow_execution_drift_repaired_total"
	metricExecutionDriftSkipped      = "xflow_execution_drift_skipped_total"
	metricExecutionDriftErrors       = "xflow_execution_drift_errors_total"
)

// executionDriftObserver is the local mirror of control.ExecutionDriftObserver,
// kept here to avoid an import cycle with service/control (which depends on
// observability/metrics). The method set must match exactly.
type executionDriftObserver interface {
	OnExecutionDriftScan(ctx context.Context, candidates int, elapsed time.Duration, err error)
	OnExecutionDriftRepaired(ctx context.Context, id types.ExecutionID, status types.ExecutionStatus, age time.Duration, applied bool)
	OnExecutionDriftSkipped(ctx context.Context, reason string)
	OnExecutionDriftError(ctx context.Context, id types.ExecutionID, err error)
}

// ExecutionDriftMetrics observes the execution drift repair worker: per-sweep
// scan counters/durations, repaired rows by terminal status, skipped rows by
// authority verdict (still running, other non-terminal, or gone), and
// per-candidate errors. A rising repaired rate means Redis and the SQL
// projection are diverging; a rising skipped/not_found rate is expected as
// executions age out, and is the signal that distinguishes "nothing to repair"
// from "nothing can be repaired".
type ExecutionDriftMetrics struct {
	Metrics *Metrics
}

// NewExecutionDriftMetrics wires the drift repair worker's observer into the
// Prometheus registry.
func NewExecutionDriftMetrics(metrics *Metrics) ExecutionDriftMetrics {
	return ExecutionDriftMetrics{Metrics: metrics}
}

func (d ExecutionDriftMetrics) OnExecutionDriftScan(ctx context.Context, _ int, elapsed time.Duration, err error) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	labels := withNamespace(ctx, map[string]string{"result": result})
	d.Metrics.Inc(metricExecutionDriftScan, labels)
	d.Metrics.Observe(metricExecutionDriftScanDuration, labels, elapsed)
}

func (d ExecutionDriftMetrics) OnExecutionDriftRepaired(ctx context.Context, _ types.ExecutionID, status types.ExecutionStatus, _ time.Duration, applied bool) {
	result := "repaired"
	if !applied {
		result = "dry_run"
	}
	d.Metrics.Inc(metricExecutionDriftRepaired, withNamespace(ctx, map[string]string{
		"status": string(status),
		"result": result,
	}))
}

func (d ExecutionDriftMetrics) OnExecutionDriftSkipped(ctx context.Context, reason string) {
	d.Metrics.Inc(metricExecutionDriftSkipped, withNamespace(ctx, map[string]string{"reason": reason}))
}

func (d ExecutionDriftMetrics) OnExecutionDriftError(ctx context.Context, _ types.ExecutionID, _ error) {
	d.Metrics.Inc(metricExecutionDriftErrors, withNamespace(ctx, nil))
}

var _ executionDriftObserver = ExecutionDriftMetrics{}
