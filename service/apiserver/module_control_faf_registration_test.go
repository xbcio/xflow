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
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/types"
)

func newFAFRegistrationTestServer(t *testing.T) (*APIServer, *httptest.Server, *control.ControlPlane, *InMemoryAuditSink) {
	t.Helper()
	cp, err := control.NewControlPlane(control.Config{Backend: local.New()})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	audit := NewInMemoryAuditSink()
	api, err := New(Config{
		Concurrency: 1,
		PrincipalAuth: NewBearerPrincipalAuthMulti([]TokenPrincipalMapping{
			{Token: "tok-full", Subject: "op-full", Namespace: "namespaceA", Scopes: []string{"workflow"}},
		}),
		Authorizer: NamespaceAwareAuthorizer{},
		AuditSink:  audit,
	}, WithControlPlane(cp))
	if err != nil {
		t.Fatalf("apiserver.New: %v", err)
	}
	httpSrv := httptest.NewServer(api.Handler())
	t.Cleanup(httpSrv.Close)
	return api, httpSrv, cp, audit
}

func fafRegistrationDefinition() *types.WorkflowDef {
	return &types.WorkflowDef{
		Name:    "faf",
		Options: &types.WorkflowOptions{FAF: true},
	}
}

func assertNoRegisteredWorkflows(t *testing.T, cp *control.ControlPlane) {
	t.Helper()
	ids, err := cp.WorkflowRegistry().ListWorkflows(context.Background(), namespace.Namespace("namespaceA"), backend.WorkflowListOptions{})
	if err != nil {
		t.Fatalf("ListWorkflows: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("registered workflow ids = %v, want none", ids)
	}
}

func assertFAFRegistrationFailure(t *testing.T, resp *http.Response) {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	env := decodeEnvelope(t, resp, nil)
	if env.Success || env.Code != "workflow_compile_failed" {
		t.Fatalf("envelope = %+v, want workflow_compile_failed", env)
	}
}

func TestHTTPWorkflowRegistrationRejectsFAFBeforeAdmission(t *testing.T) {
	_, httpSrv, cp, audit := newFAFRegistrationTestServer(t)

	assertFAFRegistrationFailure(t, postRegister(t, httpSrv.URL, "tok-full", fafRegistrationDefinition()))
	if events := audit.Events(); len(events) != 0 {
		t.Fatalf("FAF registration wrote audit events before rejection: %+v", events)
	}
	assertNoRegisteredWorkflows(t, cp)
}

func TestHTTPWorkflowReplacementRejectsFAFBeforeAdmission(t *testing.T) {
	_, httpSrv, cp, audit := newFAFRegistrationTestServer(t)

	created := postRegister(t, httpSrv.URL, "tok-full", validWorkflow())
	var registered registerWorkflowResponse
	decodeRegisterData(t, created, &registered)
	_ = created.Body.Close()
	if registered.WorkflowID == "" {
		t.Fatal("initial workflow id is empty")
	}
	before := len(audit.Events())

	assertFAFRegistrationFailure(t, putWorkflow(t, httpSrv.URL, "tok-full", string(registered.WorkflowID), fafRegistrationDefinition()))
	if events := audit.Events(); len(events) != before {
		t.Fatalf("FAF replacement changed audit events from %d to %d: %+v", before, len(events), events)
	}
	record, err := cp.WorkflowRegistry().GetWorkflow(context.Background(), registered.WorkflowID)
	if err != nil {
		t.Fatalf("GetWorkflow: %v", err)
	}
	if record.Definition.Options != nil && record.Definition.Options.FAF {
		t.Fatalf("persisted workflow was replaced by FAF definition: %+v", record.Definition.Options)
	}
}

func TestControlPlaneRegistrationRejectsFAFBeforePersistence(t *testing.T) {
	api, _, cp, audit := newFAFRegistrationTestServer(t)
	for name, register := range map[string]func(context.Context, namespace.Namespace, *types.WorkflowDef) (types.WorkflowID, []string, error){
		"register": api.RegisterWorkflow,
		"replace":  api.ReplaceWorkflow,
	} {
		t.Run(name, func(t *testing.T) {
			id, _, err := register(context.Background(), namespace.Namespace("namespaceA"), fafRegistrationDefinition())
			if id != "" {
				t.Fatalf("workflow id = %q, want empty", id)
			}
			var compileErr *WorkflowCompileError
			if !errors.As(err, &compileErr) {
				t.Fatalf("error = %v, want *WorkflowCompileError", err)
			}
			assertNoRegisteredWorkflows(t, cp)
			if events := audit.Events(); len(events) != 0 {
				t.Fatalf("in-process FAF registration wrote audit events: %+v", events)
			}
		})
	}
}
