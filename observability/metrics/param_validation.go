package metrics

// Param validation metric name.
const metricParamValidationSkipped = "xflow_param_validation_skipped_total"

// maxParamValidationTypeLabel bounds the type label. The value comes from a
// submitted definition, so an unbounded one would let a caller mint arbitrarily
// long label values.
const maxParamValidationTypeLabel = 128

// ParamValidationMetrics counts nodes the control plane's ParamSpec validator
// could not check because their type is not registered in the server process
// (a custom type registered only on a runner, or a typo). Such nodes are
// skipped, never rejected, so this series is the only signal that validation
// coverage is partial.
//
// The type label is the node type as submitted. Its cardinality is bounded by
// the set of distinct unknown types callers submit, which is small for a
// working fleet; a sudden rise is itself worth an alert.
type ParamValidationMetrics struct {
	Metrics *Metrics
}

// NewParamValidationMetrics creates an observer backed by m. A nil Metrics
// counts nothing.
func NewParamValidationMetrics(m *Metrics) ParamValidationMetrics {
	return ParamValidationMetrics{Metrics: m}
}

// ObserveSkipped counts one node skipped because nodeType is unknown.
func (p ParamValidationMetrics) ObserveSkipped(nodeType string) {
	if len(nodeType) > maxParamValidationTypeLabel {
		nodeType = nodeType[:maxParamValidationTypeLabel]
	}
	p.Metrics.Inc(metricParamValidationSkipped, map[string]string{"type": nodeType})
}
