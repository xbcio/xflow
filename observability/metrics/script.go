package metrics

import "time"

// Script metric names.
const (
	metricScriptExecute         = "xflow_script_execute_total"
	metricScriptExecuteDuration = "xflow_script_execute_duration_seconds"
	metricScriptOutputBytes     = "xflow_script_output_bytes"
)

// Local mirror interface avoids importing the internal script package.
type scriptObserver interface {
	OnScriptExecute(language, runtime, outcome string, duration time.Duration)
	OnScriptOutputBytes(language, runtime string, size int)
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
func (s ScriptMetrics) OnScriptExecute(language, runtime, outcome string, duration time.Duration) {
	labels := map[string]string{
		"language": language,
		"runtime":  runtime,
		"outcome":  outcome,
	}
	s.Metrics.Inc(metricScriptExecute, labels)
	s.Metrics.Observe(metricScriptExecuteDuration, labels, duration)
}

// OnScriptOutputBytes records the JSON-encoded result size histogram.
func (s ScriptMetrics) OnScriptOutputBytes(language, runtime string, size int) {
	s.Metrics.ObserveBytes(metricScriptOutputBytes, map[string]string{
		"language": language,
		"runtime":  runtime,
	}, size)
}

var _ scriptObserver = ScriptMetrics{}
