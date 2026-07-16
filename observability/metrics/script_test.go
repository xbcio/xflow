package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestScriptMetricsEmitsExecuteAndBytes(t *testing.T) {
	metrics := New()
	script := NewScriptMetrics(metrics)

	script.OnScriptExecute("js", "goja", "main", 5*time.Millisecond)
	script.OnScriptOutputBytes("js", "goja", 2048)

	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, req)
	body := rec.Body.String()

	want := []string{
		`xflow_script_execute_total{language="js",outcome="main",runtime="goja"} 1`,
		`xflow_script_execute_duration_seconds_count{language="js",outcome="main",runtime="goja"} 1`,
		`xflow_script_output_bytes_bucket{language="js",runtime="goja",le="4096"} 1`,
	}
	for _, w := range want {
		if !strings.Contains(body, w) {
			t.Fatalf("metrics body missing %q:\n%s", w, body)
		}
	}
}

func TestScriptMetricsEmitsErrorAndConfigOutcomes(t *testing.T) {
	metrics := New()
	script := NewScriptMetrics(metrics)

	script.OnScriptExecute("js", "goja", "error", time.Millisecond)
	script.OnScriptExecute("js", "goja", "config", time.Millisecond)

	body := gatherMetricsBody(t, metrics)
	for _, want := range []string{
		`xflow_script_execute_total{language="js",outcome="error",runtime="goja"} 1`,
		`xflow_script_execute_total{language="js",outcome="config",runtime="goja"} 1`,
		`xflow_script_execute_duration_seconds_count{language="js",outcome="error",runtime="goja"} 1`,
		`xflow_script_execute_duration_seconds_count{language="js",outcome="config",runtime="goja"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics body missing %q:\n%s", want, body)
		}
	}
}

func TestScriptMetricsNegativeBytesIgnored(t *testing.T) {
	metrics := New()
	script := NewScriptMetrics(metrics)

	script.OnScriptOutputBytes("js", "goja", -1)

	body := gatherMetricsBody(t, metrics)
	if strings.Contains(body, "xflow_script_output_bytes") {
		t.Fatalf("negative byte size should not emit histogram, got:\n%s", body)
	}
}

func gatherMetricsBody(t *testing.T, metrics *Metrics) string {
	t.Helper()
	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, req)
	return rec.Body.String()
}

func TestObserveBytesUsesByteBuckets(t *testing.T) {
	metrics := New()
	metrics.ObserveBytes("xflow_script_output_bytes", map[string]string{"language": "js"}, 512)

	family := gatherMetricFamily(t, metrics, "xflow_script_output_bytes")
	buckets := family.GetMetric()[0].GetHistogram().GetBucket()
	if len(buckets) == 0 {
		t.Fatal("no buckets found")
	}
	// First bucket must be 1024 (bytes), not a duration bucket like 0.005.
	if got := buckets[0].GetUpperBound(); got != 1024 {
		t.Fatalf("first bucket upper bound = %v, want 1024", got)
	}
}
