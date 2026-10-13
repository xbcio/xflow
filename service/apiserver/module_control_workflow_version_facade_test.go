package apiserver

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/service/control"
)

// This file covers the in-process APIServer entry points the embedded SDK
// delegates to (ListWorkflowVersions / GetWorkflowVersion / RollbackWorkflow).
// They share the HTTP routes' implementation; what needs pinning here is that
// they are reachable without a request, that they still guard their inputs, and
// that they answer with the registry's own error taxonomy rather than a wire
// code.

func newWorkflowVersionFacadeAPI(t *testing.T) (*APIServer, backend.WorkflowRegistry) {
	t.Helper()
	provider := local.New()
	cp, err := control.NewControlPlane(control.Config{
		Backend:          provider,
		WorkflowRegistry: provider.WorkflowRegistry(),
	})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	api, err := New(Config{
		Concurrency:   1,
		PrincipalAuth: NewBearerPrincipalAuthMulti(listTestPrincipals()),
		Authorizer:    NamespaceAwareAuthorizer{},
		AuditSink:     NewInMemoryAuditSink(),
	}, WithControlPlane(cp))
	if err != nil {
		t.Fatalf("apiserver.New: %v", err)
	}
	return api, provider.WorkflowRegistry()
}

// TestAPIServerWorkflowVersionFacadesListRegisteredWorkflow is the positive
// path: a workflow registered through the in-process RegisterWorkflow facade is
// listable through the version facade, current-first, with no HTTP involved.
func TestAPIServerWorkflowVersionFacadesListRegisteredWorkflow(t *testing.T) {
	api, _ := newWorkflowVersionFacadeAPI(t)
	ctx := context.Background()

	id, _, err := api.RegisterWorkflow(ctx, "namespaceA", listTestWorkflow("facade-versions", "v1"))
	if err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}
	rows, err := api.ListWorkflowVersions(ctx, "namespaceA", id)
	if err != nil {
		t.Fatalf("ListWorkflowVersions: %v", err)
	}
	if len(rows) != 1 || rows[0].Version != "v1" || !rows[0].Current {
		t.Fatalf("versions = %+v, want a single current v1", rows)
	}

	rec, err := api.GetWorkflowVersion(ctx, "namespaceA", id, "v1")
	if err != nil {
		t.Fatalf("GetWorkflowVersion(v1): %v", err)
	}
	if rec.Version != "v1" || rec.DefinitionHash != rows[0].DefinitionHash {
		t.Fatalf("current version record = version %q hash %q, want v1 %q", rec.Version, rec.DefinitionHash, rows[0].DefinitionHash)
	}

	// Rolling back to the current version is the idempotent no-op even on the
	// in-process path: an embedded host replaying a rollback must not write.
	res, err := api.RollbackWorkflow(ctx, "namespaceA", id, "v1")
	if err != nil {
		t.Fatalf("RollbackWorkflow(current): %v", err)
	}
	if !res.Unchanged || res.WorkflowID != id || res.Version != "v1" {
		t.Fatalf("rollback result = %+v, want unchanged current v1", res)
	}
}

// TestAPIServerReplaceWorkflowSameVersionIDChangeKeepsHistory pins the
// in-process replace path an embedded host uses, in the shape where the
// definition changes but its (namespace, name, version) key does not:
// APIServer.ReplaceWorkflow gives the changed definition a NEW workflow id,
// and the retired versions of the id it supersedes must move to that new id.
// A fix that migrates history only when the version label changes strands them
// on the retired id — v1 then answers not-found while the list shows only the
// current v2.
func TestAPIServerReplaceWorkflowSameVersionIDChangeKeepsHistory(t *testing.T) {
	api, _ := newWorkflowVersionFacadeAPI(t)
	ctx := context.Background()
	httpSrv := httptest.NewServer(api.Handler())
	defer httpSrv.Close()

	id, _, err := api.RegisterWorkflow(ctx, "namespaceA", listTestWorkflow("facade-idchange", "v1"))
	if err != nil {
		t.Fatalf("RegisterWorkflow(v1): %v", err)
	}
	// Publish v2 under the SAME id, so the lineage holds [v2 current, v1 retired].
	publishRun(t, httpSrv.URL, "tok-a", string(id), "facade-idchange", "v2")

	// The same key with a changed definition is what ReplaceWorkflow exists for.
	changed := listTestWorkflow("facade-idchange", "v2")
	changed.Nodes[1].Parameters = map[string]any{"marker": "rewrite"}
	newID, _, err := api.ReplaceWorkflow(ctx, "namespaceA", changed)
	if err != nil {
		t.Fatalf("ReplaceWorkflow(changed v2): %v", err)
	}
	if newID == id {
		t.Fatalf("same-version replace kept id %q, want a new id for the rewritten definition", id)
	}

	rows, err := api.ListWorkflowVersions(ctx, "namespaceA", newID)
	if err != nil {
		t.Fatalf("ListWorkflowVersions after replace: %v", err)
	}
	if len(rows) != 2 || rows[0].Version != "v2" || !rows[0].Current || rows[1].Version != "v1" || !rows[1].Archived {
		t.Fatalf("version rows = %+v, want current v2 plus archived v1 carried to the new id", rows)
	}

	archived, err := api.GetWorkflowVersion(ctx, "namespaceA", newID, "v1")
	if err != nil {
		t.Fatalf("GetWorkflowVersion(v1) after the id change error = %v; the retired version was stranded on the superseded id", err)
	}
	if archived.DefinitionHash != rows[1].DefinitionHash {
		t.Fatalf("archived v1 hash = %q, want the listed %q", archived.DefinitionHash, rows[1].DefinitionHash)
	}
}

// TestAPIServerWorkflowVersionFacadesGuardTheirInputs pins the empty-argument
// answers: they are registry errors, so an embedded caller sees the same
// taxonomy the HTTP routes translate to 404s.
func TestAPIServerWorkflowVersionFacadesGuardTheirInputs(t *testing.T) {
	api, _ := newWorkflowVersionFacadeAPI(t)
	ctx := context.Background()

	if _, err := api.ListWorkflowVersions(ctx, "namespaceA", ""); !errors.Is(err, backend.ErrWorkflowNotFound) {
		t.Fatalf("ListWorkflowVersions(empty id) error = %v, want backend.ErrWorkflowNotFound", err)
	}
	if _, err := api.GetWorkflowVersion(ctx, "namespaceA", "", "v1"); !errors.Is(err, backend.ErrWorkflowNotFound) {
		t.Fatalf("GetWorkflowVersion(empty id) error = %v, want backend.ErrWorkflowNotFound", err)
	}
	if _, err := api.GetWorkflowVersion(ctx, "namespaceA", "wf-1", ""); !errors.Is(err, backend.ErrWorkflowVersionNotFound) {
		t.Fatalf("GetWorkflowVersion(empty version) error = %v, want backend.ErrWorkflowVersionNotFound", err)
	}
	if _, err := api.RollbackWorkflow(ctx, "namespaceA", "", "v1"); !errors.Is(err, backend.ErrWorkflowNotFound) {
		t.Fatalf("RollbackWorkflow(empty id) error = %v, want backend.ErrWorkflowNotFound", err)
	}

	// Cross-namespace resolution is refused by the shared implementation, not
	// only by the HTTP authz wrapper.
	id, _, err := api.RegisterWorkflow(ctx, "namespaceA", listTestWorkflow("facade-ns", "v1"))
	if err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}
	if _, err := api.ListWorkflowVersions(ctx, "namespaceB", id); !errors.Is(err, backend.ErrWorkflowNotFound) {
		t.Fatalf("ListWorkflowVersions(other namespace) error = %v, want backend.ErrWorkflowNotFound", err)
	}
	if _, err := api.RollbackWorkflow(ctx, "namespaceB", id, "v1"); !errors.Is(err, backend.ErrWorkflowNotFound) {
		t.Fatalf("RollbackWorkflow(other namespace) error = %v, want backend.ErrWorkflowNotFound", err)
	}
}
