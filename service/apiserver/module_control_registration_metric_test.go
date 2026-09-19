package apiserver

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/observability/metrics"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/types"
)

// newRegistrationMetricServer builds an APIServer over the in-memory local
// backend with a metrics collector attached, so the registration counter can be
// read back through the registry after driving the real in-process path an
// embedded host uses (APIServer.RegisterWorkflow / ReplaceWorkflow).
func newRegistrationMetricServer(t *testing.T) (*APIServer, *metrics.Metrics) {
	t.Helper()
	be := local.New()
	cp, err := control.NewControlPlane(control.Config{Backend: be})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	m := metrics.New()
	srv, err := New(Config{Concurrency: 1, Metrics: m}, WithControlPlane(cp))
	if err != nil {
		t.Fatalf("apiserver.New: %v", err)
	}
	return srv, m
}

// registrationOutcomes reads xflow_workflow_registration_total back as
// "operation/outcome" -> count. Reading it through the registry rather than
// through the observer is what proves a real series was created.
func registrationOutcomes(t *testing.T, m *metrics.Metrics) map[string]float64 {
	t.Helper()
	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	counts := map[string]float64{}
	for _, family := range families {
		if family.GetName() != "xflow_workflow_registration_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, kv := range metric.GetLabel() {
				labels[kv.GetName()] = kv.GetValue()
			}
			counts[labels["operation"]+"/"+labels["outcome"]] = metric.GetCounter().GetValue()
		}
	}
	return counts
}

// TestRegisterWorkflowCountsEveryAttemptByOutcome drives the four shapes a
// startup registration can take — success, a definition the compiler rejects, a
// key another definition occupies, and a successful replacement — through the
// in-process path an embedded host uses, and asserts each lands on its own
// series.
//
// This is the R4 acceptance criterion at the counter: a host that never manages
// to register publishes no outcome="registered" sample while its API keeps
// serving, which is the state that used to be invisible.
func TestRegisterWorkflowCountsEveryAttemptByOutcome(t *testing.T) {
	srv, m := newRegistrationMetricServer(t)
	ctx := context.Background()
	ns := namespace.Namespace("tenant-a")

	if _, _, err := srv.RegisterWorkflow(ctx, ns, validWorkflow()); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}
	if got := registrationOutcomes(t, m)["add/registered"]; got != 1 {
		t.Fatalf("add/registered = %v, want 1 (all: %v)", got, registrationOutcomes(t, m))
	}

	// A definition the compiler rejects: nodes are required.
	_, _, err := srv.RegisterWorkflow(ctx, ns, &types.WorkflowDef{Name: "broken"})
	if err == nil {
		t.Fatal("RegisterWorkflow accepted a definition with no nodes")
	}
	var compileErr *WorkflowCompileError
	if !errors.As(err, &compileErr) {
		t.Fatalf("error = %v, want *WorkflowCompileError", err)
	}

	// A DIFFERENT definition under a key that is already taken.
	conflicting := validWorkflow()
	conflicting.Nodes = append(conflicting.Nodes, types.NodeDef{Name: "extra", Type: "test.extra"})
	if _, _, err := srv.RegisterWorkflow(ctx, ns, conflicting); err == nil {
		t.Fatal("RegisterWorkflow accepted a changed definition under a taken key")
	} else if !errors.Is(err, backend.ErrWorkflowConflict) {
		t.Fatalf("error = %v, want backend.ErrWorkflowConflict", err)
	}

	// The same changed definition through the retry-safe call.
	if _, _, err := srv.ReplaceWorkflow(ctx, ns, conflicting); err != nil {
		t.Fatalf("ReplaceWorkflow: %v", err)
	}

	got := registrationOutcomes(t, m)
	for key, want := range map[string]float64{
		"add/registered":         1,
		"add/invalid_definition": 1,
		"add/conflict":           1,
		"replace/registered":     1,
	} {
		if got[key] != want {
			t.Errorf("%s = %v, want %v (all: %v)", key, got[key], want, got)
		}
	}
	// Nothing may be counted as a transient backend error here: every failure
	// above is deterministic, and an "error" outcome is what tells an operator to
	// retry.
	if n := got["add/error"]; n != 0 {
		t.Errorf("add/error = %v, want 0: a compile failure and a key conflict are "+
			"not retryable backend errors (all: %v)", n, got)
	}
}

// TestRegisterWorkflowHTTPRouteCountsTheSameSeries pins that the counter is not
// an in-process-only signal: POST /v1/workflows lands on the same two series, so
// a host that registers over the wire reads its failures from the same query as
// one that registers in-process.
func TestRegisterWorkflowHTTPRouteCountsTheSameSeries(t *testing.T) {
	be := local.New()
	cp, err := control.NewControlPlane(control.Config{Backend: be})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	m := metrics.New()
	srv, err := New(Config{
		Concurrency: 1,
		Metrics:     m,
		PrincipalAuth: NewBearerPrincipalAuthMulti([]TokenPrincipalMapping{
			{Token: "tok-full", Subject: "op-full", Namespace: "namespaceA", Scopes: []string{"workflow"}},
		}),
		Authorizer: NamespaceAwareAuthorizer{},
		AuditSink:  NewInMemoryAuditSink(),
	}, WithControlPlane(cp))
	if err != nil {
		t.Fatalf("apiserver.New: %v", err)
	}
	httpSrv := httptest.NewServer(srv.Handler())
	t.Cleanup(httpSrv.Close)

	resp := postRegister(t, httpSrv.URL, "tok-full", validWorkflow())
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201", resp.StatusCode)
	}
	var out registerWorkflowResponse
	decodeRegisterData(t, resp, &out)

	if got := registrationOutcomes(t, m)["add/registered"]; got != 1 {
		t.Fatalf("add/registered = %v, want 1 (all: %v)", got, registrationOutcomes(t, m))
	}

	// PUT /v1/workflows/{id} is the third operation value; it must be counted
	// too, or an operator reading the series would see a replacement that the
	// registry accepted and the counter never mentioned.
	replacement := validWorkflow()
	replacement.Nodes = append(replacement.Nodes, types.NodeDef{Name: "extra", Type: "test.extra"})
	putResp := putWorkflow(t, httpSrv.URL, "tok-full", string(out.WorkflowID), replacement)
	defer func() { _ = putResp.Body.Close() }()
	if putResp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status = %d, want 200", putResp.StatusCode)
	}
	if got := registrationOutcomes(t, m)["replace_by_id/registered"]; got != 1 {
		t.Fatalf("replace_by_id/registered = %v, want 1 (all: %v)", got, registrationOutcomes(t, m))
	}
}

// TestRegisterWorkflowMetricCarriesTheTargetNamespace pins that the in-process
// path labels the namespace it registered INTO. The namespace arrives as an
// argument rather than in the context, so reading ctx would silently label every
// embedded host's registrations "default".
func TestRegisterWorkflowMetricCarriesTheTargetNamespace(t *testing.T) {
	srv, m := newRegistrationMetricServer(t)

	if _, _, err := srv.RegisterWorkflow(context.Background(), namespace.Namespace("tenant-b"), validWorkflow()); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}

	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	found := false
	for _, family := range families {
		if family.GetName() != "xflow_workflow_registration_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, kv := range metric.GetLabel() {
				if kv.GetName() == "namespace" {
					found = true
					if kv.GetValue() != "tenant-b" {
						t.Fatalf("namespace label = %q, want tenant-b", kv.GetValue())
					}
				}
			}
		}
	}
	if !found {
		t.Fatal("registration counter carried no namespace label at all")
	}
}
