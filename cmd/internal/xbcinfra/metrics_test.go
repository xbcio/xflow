package xbcinfra

import (
	"testing"

	xbcmetrics "github.com/xbcio/xbc/extensions/observability/metrics"
)

func TestNewMetricsBridgesXflowCollectorIntoTheXBCRegistry(t *testing.T) {
	plugin, collector, err := NewMetrics(xbcmetrics.DefaultConfig())
	if err != nil {
		t.Fatalf("NewMetrics() = %v", err)
	}
	if plugin == nil || collector == nil {
		t.Fatal("NewMetrics() returned a nil plugin or collector")
	}
	if collector.Registry() != nil {
		t.Fatal("the xflow collector must not invent a registry of its own")
	}
	if collector.Gatherer() != plugin.Registry() {
		t.Fatal("the xflow collector must gather from the xbc registry")
	}

	collector.Inc("xflow_xbcinfra_total", map[string]string{"result": "ok"})

	families, err := plugin.Registry().Gather()
	if err != nil {
		t.Fatalf("Gather() = %v", err)
	}
	for _, family := range families {
		if family.GetName() != "xflow_xbcinfra_total" {
			continue
		}
		metrics := family.GetMetric()
		if len(metrics) != 1 {
			t.Fatalf("metric count = %d, want 1", len(metrics))
		}
		if got := metrics[0].GetCounter().GetValue(); got != 1 {
			t.Fatalf("counter value = %v, want 1", got)
		}
		return
	}
	t.Fatalf("xflow_xbcinfra_total not found in %#v", families)
}

func TestNewMetricsRejectsInvalidConfig(t *testing.T) {
	cfg := xbcmetrics.DefaultConfig()
	cfg.Namespace = "not-a-valid-name!"
	if _, _, err := NewMetrics(cfg); err == nil {
		t.Fatal("NewMetrics() accepted an invalid namespace")
	}
}
