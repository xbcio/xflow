package apiserver

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"

	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/namespace"
	_ "github.com/xbcio/xflow/node" // registers the builtin node types the validator looks up
	"github.com/xbcio/xflow/observability/metrics"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/types"
)

// newParamValidationServer builds an APIServer in the given mode with metrics.
func newParamValidationServer(t *testing.T, mode types.ParamValidationMode) (*httptest.Server, *APIServer, *metrics.Metrics) {
	t.Helper()
	cp, err := control.NewControlPlane(control.Config{Backend: local.New()})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	m := metrics.New()
	cfg := Config{
		Concurrency: 1,
		Metrics:     m,
		PrincipalAuth: NewBearerPrincipalAuthMulti([]TokenPrincipalMapping{
			{Token: "tok-full", Subject: "op-full", Namespace: "namespaceA", Scopes: []string{"workflow", "execution"}},
		}),
		Authorizer:      NamespaceAwareAuthorizer{},
		AuditSink:       NewInMemoryAuditSink(),
		ParamValidation: mode,
	}
	srv, err := New(cfg, WithControlPlane(cp))
	if err != nil {
		t.Fatalf("apiserver.New: %v", err)
	}
	httpSrv := httptest.NewServer(srv.Handler())
	t.Cleanup(httpSrv.Close)
	return httpSrv, srv, m
}

// invalidParamsWorkflow compiles, but its xflow.wait node is in timer mode with
// neither duration nor until (two RequiredWhen errors), and it carries a node
// type this process has not registered.
func invalidParamsWorkflow(name string) *types.WorkflowDef {
	return &types.WorkflowDef{
		Name:    name,
		Version: "v1",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "pause", Type: "xflow.wait", Parameters: map[string]any{"mode": "timer"}},
			{Name: "custom", Type: "custom.unregistered", Parameters: map[string]any{"anything": 1}},
		},
		Connections: types.Connections{
			"start": {"main": types.PortConnections{Targets: []types.Connection{{Node: "pause", Input: "main"}}}},
			"pause": {"main": types.PortConnections{Targets: []types.Connection{{Node: "custom", Input: "main"}}}},
		},
	}
}

// warningOnlyWorkflow has one advisory finding: xflow.http accepts any verb.
func warningOnlyWorkflow(name string) *types.WorkflowDef {
	return &types.WorkflowDef{
		Name:    name,
		Version: "v1",
		Nodes: []types.NodeDef{
			{Name: "call", Type: "xflow.http", Parameters: map[string]any{"method": "OPTIONS", "url": "https://example.test"}},
		},
	}
}

func issueSummary(issues []graph.ParamIssue) []string {
	out := make([]string, len(issues))
	for i, is := range issues {
		out[i] = is.Node + " " + is.Path + " " + is.Code + " " + is.Severity
	}
	sort.Strings(out)
	return out
}

func assertSummary(t *testing.T, issues []graph.ParamIssue, want ...string) {
	t.Helper()
	got := issueSummary(issues)
	sort.Strings(want)
	if len(got) != len(want) {
		t.Fatalf("param_issues = %q, want %q", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("param_issues = %q, want %q", got, want)
		}
	}
}

var waitTimerIssues = []string{
	"pause /parameters/duration required error",
	"pause /parameters/until required error",
}

func counterValue(t *testing.T, m *metrics.Metrics, name string, labels map[string]string) float64 {
	t.Helper()
	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
	metricLoop:
		for _, metric := range family.GetMetric() {
			got := map[string]string{}
			for _, kv := range metric.GetLabel() {
				got[kv.GetName()] = kv.GetValue()
			}
			for k, v := range labels {
				if got[k] != v {
					continue metricLoop
				}
			}
			return metric.GetCounter().GetValue()
		}
	}
	return 0
}

// submitCountingFacade records whether Submit was reached.
type submitCountingFacade struct {
	*fakeControlFacade
	submits int
}

func (f *submitCountingFacade) Submit(ctx context.Context, g *graph.Graph, params map[string]any, rt ...*types.Runtime) (types.ExecutionID, error) {
	f.submits++
	return f.fakeControlFacade.Submit(ctx, g, params, rt...)
}

type paramInvalidBody struct {
	ParamIssues []graph.ParamIssue `json:"param_issues"`
}

func TestRegisterWarnModeReturnsParamIssues(t *testing.T) {
	srv, _, m := newParamValidationServer(t, "") // default: warn

	resp := postRegister(t, srv.URL, "tok-full", invalidParamsWorkflow("warn-register"))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201 (warn never rejects)", resp.StatusCode)
	}
	var out registerWorkflowResponse
	decodeRegisterData(t, resp, &out)
	assertSummary(t, out.ParamIssues, waitTimerIssues...)
	for _, w := range out.Warnings {
		for _, is := range out.ParamIssues {
			if w == is.Message {
				t.Fatalf("param issue leaked into compile warnings: %q", w)
			}
		}
	}
	if got := counterValue(t, m, "xflow_param_validation_skipped_total", map[string]string{"type": "custom.unregistered"}); got != 1 {
		t.Fatalf("skipped counter = %v, want 1", got)
	}
}

func TestInlineExecuteWarnModeReturnsParamIssues(t *testing.T) {
	f := &fakeControlFacade{submitID: "exec-1"}
	mux := newControlMux(f) // a module built without New: mode "" = warn

	resp := doJSON(t, mux, http.MethodPost, "/v1/workflows/execute", executeWorkflowRequest{Workflow: invalidParamsWorkflow("warn-exec")})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var out executeWorkflowResponse
	decodeEnvelope(t, resp, &out)
	if out.ExecutionID != "exec-1" {
		t.Fatalf("execution_id = %q", out.ExecutionID)
	}
	assertSummary(t, out.ParamIssues, waitTimerIssues...)
}

func TestRegisterEnforceModeRejectsWithParamInvalid(t *testing.T) {
	srv, _, m := newParamValidationServer(t, types.ParamValidationEnforce)

	resp := postRegister(t, srv.URL, "tok-full", invalidParamsWorkflow("enforce-register"))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	var body paramInvalidBody
	env := decodeEnvelope(t, resp, &body)
	if env.Success || env.Code != "workflow_param_invalid" {
		t.Fatalf("envelope = %+v, want workflow_param_invalid", env)
	}
	assertSummary(t, body.ParamIssues, waitTimerIssues...)
	// The unknown type is skipped, not rejected, even under enforce.
	for _, is := range body.ParamIssues {
		if is.Node == "custom" {
			t.Fatalf("unknown type produced an issue: %+v", is)
		}
	}
	if got := counterValue(t, m, "xflow_param_validation_skipped_total", map[string]string{"type": "custom.unregistered"}); got != 1 {
		t.Fatalf("skipped counter = %v, want 1", got)
	}
	if got := counterValue(t, m, "xflow_workflow_registration_total", map[string]string{"operation": "add", "outcome": "invalid_definition"}); got != 1 {
		t.Fatalf("registration invalid_definition = %v, want 1", got)
	}
}

func TestInlineExecuteEnforceModeRejectsWithParamInvalid(t *testing.T) {
	f := &submitCountingFacade{fakeControlFacade: &fakeControlFacade{submitID: "exec-1"}}
	m := &workflowControlModule{eng: f, paramValidation: types.ParamValidationEnforce}
	mux := http.NewServeMux()
	m.RegisterHTTP(mux)

	resp := doJSON(t, mux, http.MethodPost, "/v1/workflows/execute", executeWorkflowRequest{Workflow: invalidParamsWorkflow("enforce-exec")})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	var body paramInvalidBody
	env := decodeEnvelope(t, resp, &body)
	if env.Code != "workflow_param_invalid" {
		t.Fatalf("code = %q, want workflow_param_invalid", env.Code)
	}
	assertSummary(t, body.ParamIssues, waitTimerIssues...)
	if f.submits != 0 {
		t.Fatal("a rejected inline definition must not be submitted")
	}
}

func TestReplaceEnforceModeRejectsWithParamInvalid(t *testing.T) {
	srv, _, _ := newParamValidationServer(t, types.ParamValidationEnforce)

	resp := postRegister(t, srv.URL, "tok-full", validWorkflow())
	var created registerWorkflowResponse
	decodeRegisterData(t, resp, &created)
	_ = resp.Body.Close()

	bad := invalidParamsWorkflow(validWorkflow().Name)
	resp = putWorkflow(t, srv.URL, "tok-full", string(created.WorkflowID), bad)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	env := decodeEnvelope(t, resp, nil)
	if env.Code != "workflow_param_invalid" {
		t.Fatalf("code = %q, want workflow_param_invalid", env.Code)
	}
}

func TestEnforceModeAcceptsWarningOnlyIssues(t *testing.T) {
	srv, _, _ := newParamValidationServer(t, types.ParamValidationEnforce)

	resp := postRegister(t, srv.URL, "tok-full", warningOnlyWorkflow("warning-only"))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201: an advisory finding never rejects", resp.StatusCode)
	}
	var out registerWorkflowResponse
	decodeRegisterData(t, resp, &out)
	assertSummary(t, out.ParamIssues, "call /parameters/method enum warning")
}

func TestOffModeSkipsParamValidation(t *testing.T) {
	srv, _, m := newParamValidationServer(t, types.ParamValidationOff)

	resp := postRegister(t, srv.URL, "tok-full", invalidParamsWorkflow("off"))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201", resp.StatusCode)
	}
	var out registerWorkflowResponse
	decodeRegisterData(t, resp, &out)
	if len(out.ParamIssues) != 0 {
		t.Fatalf("off mode returned param_issues %q", issueSummary(out.ParamIssues))
	}
	if got := counterValue(t, m, "xflow_param_validation_skipped_total", nil); got != 0 {
		t.Fatalf("off mode counted skips: %v", got)
	}
}

func TestRegisterWorkflowReportExposesParamIssues(t *testing.T) {
	ctx := context.Background()
	ns := namespace.Namespace("tenant-a")

	_, warn, _ := newParamValidationServer(t, types.ParamValidationWarn)
	res, err := warn.RegisterWorkflowReport(ctx, ns, invalidParamsWorkflow("report-warn"))
	if err != nil {
		t.Fatalf("RegisterWorkflowReport: %v", err)
	}
	if res.ID == "" {
		t.Fatal("RegisterWorkflowReport must return the id")
	}
	assertSummary(t, res.ParamIssues, waitTimerIssues...)
	// The legacy signature still works and still omits the issues.
	if _, _, err := warn.RegisterWorkflow(ctx, ns, invalidParamsWorkflow("report-warn-legacy")); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}

	_, enforce, _ := newParamValidationServer(t, types.ParamValidationEnforce)
	_, err = enforce.ReplaceWorkflowReport(ctx, ns, invalidParamsWorkflow("report-enforce"))
	var paramErr *execution.ParamIssuesError
	if !errors.As(err, &paramErr) {
		t.Fatalf("enforce error = %v, want *execution.ParamIssuesError", err)
	}
	assertSummary(t, paramErr.Issues, waitTimerIssues...)
	if registrationOutcome(err) != "invalid_definition" {
		t.Fatalf("registrationOutcome = %q, want invalid_definition", registrationOutcome(err))
	}
}

func TestNewRejectsInvalidParamValidationMode(t *testing.T) {
	cp, err := control.NewControlPlane(control.Config{Backend: local.New()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New(Config{Concurrency: 1, ParamValidation: "strict"}, WithControlPlane(cp)); err == nil {
		t.Fatal("New must reject an unknown ParamValidation mode")
	}
}
