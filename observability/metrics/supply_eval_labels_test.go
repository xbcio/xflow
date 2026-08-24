package metrics

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestSupplyMetricsOnEvalLabelsTheNode pins that eval cost lands on a series an
// operator can act on.
//
// Both wasm eval series carried only a namespace, so a runner hosting a decode
// node and a clean node reported their costs merged. "Something in this
// namespace is burning CPU" was already visible from the process CPU; the
// question the metric exists to answer is which node, and it could not.
func TestSupplyMetricsOnEvalLabelsTheNode(t *testing.T) {
	m := New()
	s := NewSupplyMetrics(m)
	ctx := context.Background()

	s.OnEval(ctx, "sas-collect", "decode", 8000, 3*time.Millisecond)
	s.OnEval(ctx, "sas-collect", "clean", 2000, time.Millisecond)

	body := gatherMetricsBody(t, m)
	for _, want := range []string{
		`xflow_wasm_eval_duration_seconds_count{namespace="default",node="decode",workflow="sas-collect"} 1`,
		`xflow_wasm_eval_duration_seconds_count{namespace="default",node="clean",workflow="sas-collect"} 1`,
		`xflow_wasm_eval_stdin_bytes_count{namespace="default",node="decode",workflow="sas-collect"} 1`,
		`xflow_wasm_eval_stdin_bytes_count{namespace="default",node="clean",workflow="sas-collect"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics body missing %q:\n%s", want, body)
		}
	}
}

// TestSupplyMetricsOnEvalKeepsTheLabelsWhenTheNodeIsUnknown is the guard that
// keeps the series from disappearing.
//
// A Prometheus vec is keyed by its label NAMES: registering a second one under
// the same metric name with a different set fails, and this package's registration
// path swallows that into a log line and returns nil. So a "cleaner" version that
// omitted the labels when the identity is empty would silently drop every
// unattributed eval from /metrics — a whole class of eval invisible, with nothing
// in the output saying so.
//
// Empty label values are the honest encoding: node="" is a legible "nothing named
// this one", and it cannot be confused with a workflow that has a node called
// "unknown".
func TestSupplyMetricsOnEvalKeepsTheLabelsWhenTheNodeIsUnknown(t *testing.T) {
	m := New()
	s := NewSupplyMetrics(m)
	ctx := context.Background()

	s.OnEval(ctx, "sas-collect", "decode", 8000, 3*time.Millisecond)
	s.OnEval(ctx, "", "", 500, time.Millisecond)

	body := gatherMetricsBody(t, m)
	for _, want := range []string{
		`xflow_wasm_eval_duration_seconds_count{namespace="default",node="decode",workflow="sas-collect"} 1`,
		`xflow_wasm_eval_duration_seconds_count{namespace="default",node="",workflow=""} 1`,
		`xflow_wasm_eval_stdin_bytes_count{namespace="default",node="",workflow=""} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics body missing %q; an unattributed eval must still be "+
				"counted, not dropped:\n%s", want, body)
		}
	}
}
