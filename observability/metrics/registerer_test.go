package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestNewWithRegistererUsesTheHostRegistry(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := NewWithRegisterer(registry, registry)
	if metrics.Registry() != nil {
		t.Fatal("Registry() must be nil when the registry is host-owned")
	}
	if metrics.Gatherer() != registry {
		t.Fatal("Gatherer() must be the host gatherer")
	}

	metrics.Inc("xflow_host_registry_total", map[string]string{"result": "ok"})

	families, err := metrics.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	found := false
	for _, family := range families {
		if family.GetName() == "xflow_host_registry_total" {
			found = true
			if got := family.GetMetric()[0].GetCounter().GetValue(); got != 1 {
				t.Fatalf("counter value = %v, want 1", got)
			}
		}
	}
	if !found {
		t.Fatalf("xflow_host_registry_total not found in %#v", families)
	}

	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, req)
	if !strings.Contains(rec.Body.String(), "xflow_host_registry_total") {
		t.Fatalf("Handler() did not serve the host registry: %s", rec.Body.String())
	}
}

func TestNewWithRegistererDerivesGathererFromTheRegisterer(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := NewWithRegisterer(registry, nil)
	if metrics.Gatherer() != registry {
		t.Fatal("Gatherer() must fall back to a registerer that implements prometheus.Gatherer")
	}

	metrics.Inc("xflow_derived_gatherer_total", nil)
	families, err := metrics.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	if len(families) == 0 {
		t.Fatal("Gather() returned no families")
	}
}

func TestNewWithRegistererNilCreatesAnIsolatedRegistry(t *testing.T) {
	metrics := NewWithRegisterer(nil, nil)
	if metrics.Registry() == nil {
		t.Fatal("Registry() must expose the isolated registry created for a nil registerer")
	}
	if metrics.Gatherer() == nil {
		t.Fatal("Gatherer() must gather from the isolated registry")
	}

	metrics.Inc("xflow_isolated_total", nil)
	families, err := metrics.Registry().Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	if len(families) == 0 {
		t.Fatal("Gather() returned no families")
	}
}

func TestHandlerWithoutGathererIsEmpty(t *testing.T) {
	registerer := registerOnly{prometheus.NewRegistry()}
	metrics := NewWithRegisterer(registerer, nil)
	if metrics.Gatherer() != nil {
		t.Fatal("Gatherer() must stay nil for a registerer that cannot gather")
	}

	metrics.Inc("xflow_unserved_total", nil)

	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, req)
	if body := rec.Body.String(); body != "" {
		t.Fatalf("Handler() with no gatherer wrote %q, want an empty body", body)
	}
}

// registerOnly deliberately implements prometheus.Registerer without
// prometheus.Gatherer, so the constructor cannot derive one.
type registerOnly struct {
	prometheus.Registerer
}
