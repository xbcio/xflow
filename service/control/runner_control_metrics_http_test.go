package control_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	backendlocal "github.com/xbcio/xflow/backend/providers/local"
	observabilitymetrics "github.com/xbcio/xflow/observability/metrics"
	"github.com/xbcio/xflow/service/apiserver"
	"github.com/xbcio/xflow/service/control"
)

// TestManagementRunnerControlHTTPRecordsMetrics exercises the public management
// route rather than calling RunnerControlDirectory directly. It proves that the
// accessor used by apiserver reaches the control-plane decorator and publishes
// both the operation result and a post-mutation fleet snapshot.
func TestManagementRunnerControlHTTPRecordsMetrics(t *testing.T) {
	ctx := context.Background()
	directory := control.NewMemoryRunnerDirectory()
	if _, err := directory.Register(ctx, control.RegisterRunnerRequest{
		RunnerID: "runner-http-metrics",
		Capacity: 1,
		Now:      time.Now().UTC(),
	}); err != nil {
		t.Fatalf("register runner: %v", err)
	}

	registry := observabilitymetrics.New()
	cp, err := control.NewControlPlane(control.Config{
		Backend:         backendlocal.New(),
		RunnerDirectory: directory,
		Metrics:         registry,
	})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}

	const token = "0123456789abcdef0123456789abcdef"
	api, err := apiserver.New(apiserver.Config{
		Metrics: registry,
		PrincipalAuth: apiserver.NewBearerPrincipalAuth(token, "metrics-test-operator", []string{
			"management.runner.drain",
			apiserver.ScopeManagementRunnerControlGlobal,
		}),
		Authorizer: apiserver.ScopeAuthorizer{},
		AuditSink:  apiserver.NewInMemoryAuditSink(),
	}, apiserver.WithControlPlane(cp), apiserver.WithManagement())
	if err != nil {
		t.Fatalf("apiserver.New: %v", err)
	}

	httpServer := httptest.NewServer(api.Handler())
	defer httpServer.Close()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		httpServer.URL+"/v1/management/runners/runner-http-metrics/drain",
		bytes.NewBufferString(`{"reason":"planned maintenance"}`))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Request-Id", "runner-control-metrics-http")
	resp, err := httpServer.Client().Do(req)
	if err != nil {
		t.Fatalf("POST management drain: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST management drain status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	transitions := runnerControlHTTPMetricFamily(t, registry, "xflow_runner_control_transitions_total")
	transition := runnerControlHTTPMetric(transitions, map[string]string{
		"action": "drain",
		"result": "transitioned",
	})
	if transition == nil || transition.GetCounter().GetValue() != 1 {
		t.Fatalf("HTTP drain transition = %v, want one drain/transitioned metric", transition)
	}

	draining := runnerControlHTTPMetricFamily(t, registry, "xflow_runner_draining_count")
	drainingMetric := runnerControlHTTPMetric(draining, nil)
	if drainingMetric == nil || drainingMetric.GetGauge().GetValue() != 1 {
		t.Fatalf("HTTP drain fleet draining count = %v, want 1", drainingMetric)
	}
	if got := len(drainingMetric.GetLabel()); got != 0 {
		t.Fatalf("draining metric labels = %v, want none", drainingMetric.GetLabel())
	}

	blockers := runnerControlHTTPMetricFamily(t, registry, "xflow_runner_drain_blockers")
	runnerAck := runnerControlHTTPMetric(blockers, map[string]string{"kind": "runner_ack"})
	if runnerAck == nil || runnerAck.GetGauge().GetValue() != 1 {
		t.Fatalf("HTTP drain runner_ack blocker = %v, want 1", runnerAck)
	}
}

func runnerControlHTTPMetricFamily(t *testing.T, registry *observabilitymetrics.Metrics, name string) *dto.MetricFamily {
	t.Helper()
	families, err := registry.Registry().Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, family := range families {
		if family.GetName() == name {
			return family
		}
	}
	t.Fatalf("metric family %q not found", name)
	return nil
}

func runnerControlHTTPMetric(family *dto.MetricFamily, labels map[string]string) *dto.Metric {
	for _, metric := range family.GetMetric() {
		if len(metric.GetLabel()) != len(labels) {
			continue
		}
		matched := true
		for _, label := range metric.GetLabel() {
			if labels[label.GetName()] != label.GetValue() {
				matched = false
				break
			}
		}
		if matched {
			return metric
		}
	}
	return nil
}
