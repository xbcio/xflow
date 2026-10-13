package metrics

import (
	"context"
	"time"
)

// Script metric names.
const (
	metricScriptExecute             = "xflow_script_execute_total"
	metricScriptExecuteDuration     = "xflow_script_execute_duration_seconds"
	metricScriptOutputBytes         = "xflow_script_output_bytes"
	metricScriptBatchSerialFallback = "xflow_script_batch_serial_fallback_total"
)

// Local mirror interface avoids importing the internal script package.
type scriptObserver interface {
	OnScriptExecute(ctx context.Context, language, runtime, outcome string, duration time.Duration)
	OnScriptOutputBytes(ctx context.Context, language, runtime string, size int)
}

// scriptBatchSerialFallbackObserver mirrors the script package's optional
// BatchFallbackObserver extension: a batch-shaped input whose engine implements
// no batch fast path, so the records were evaluated one by one.
type scriptBatchSerialFallbackObserver interface {
	OnScriptBatchSerialFallback(ctx context.Context, language, runtime string)
}

// ScriptMetrics observes xflow.script executions.
type ScriptMetrics struct {
	Metrics *Metrics
}

// NewScriptMetrics creates a script execution observer backed by Metrics.
func NewScriptMetrics(metrics *Metrics) ScriptMetrics {
	return ScriptMetrics{Metrics: metrics}
}

// OnScriptExecute records a script execution counter and duration histogram.
func (s ScriptMetrics) OnScriptExecute(ctx context.Context, language, runtime, outcome string, duration time.Duration) {
	labels := withNamespace(ctx, map[string]string{
		"language": language,
		"runtime":  runtime,
		"outcome":  outcome,
	})
	s.Metrics.Inc(metricScriptExecute, labels)
	s.Metrics.Observe(metricScriptExecuteDuration, labels, duration)
}

// OnScriptOutputBytes records the JSON-encoded result size histogram.
func (s ScriptMetrics) OnScriptOutputBytes(ctx context.Context, language, runtime string, size int) {
	s.Metrics.ObserveBytes(metricScriptOutputBytes, withNamespace(ctx, map[string]string{
		"language": language,
		"runtime":  runtime,
	}), size)
}

// OnScriptBatchSerialFallback counts one batch execution that ran serially
// because its runtime implements no batch fast path.
//
// It exists to tell "this deployment is on a serial batch runtime" apart from
// "this deployment is slow": the batch duration histogram cannot, because a
// serial batch of N records and a batch-capable runtime on a slow batch produce
// the same shape. The labels are the configured (language, runtime), so the
// counter also answers which runtimes are still taking that path — the fact the
// wazero legacy runtime's documentation now warns about.
func (s ScriptMetrics) OnScriptBatchSerialFallback(ctx context.Context, language, runtime string) {
	s.Metrics.Inc(metricScriptBatchSerialFallback, withNamespace(ctx, map[string]string{
		"language": language,
		"runtime":  runtime,
	}))
}

var (
	_ scriptObserver                    = ScriptMetrics{}
	_ scriptBatchSerialFallbackObserver = ScriptMetrics{}
)
