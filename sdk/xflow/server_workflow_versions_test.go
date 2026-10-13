package xflow

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/namespace"
)

// This file covers the embedded Server face of version management (§2.11):
// ListWorkflowVersions / GetWorkflowVersion / RollbackWorkflow, all three of
// which delegate to the implementation the HTTP routes serve, so an embedded
// host and an HTTP client cannot observe different behavior.
//
// The properties pinned here are the ones a mistake would not surface as a
// compile error:
//
//   - the list is current-first then newest-retirement-first, and a row carries
//     the version's hash and revision but no definition;
//   - a retired version's definition is readable through GetWorkflowVersion
//     after it has been replaced;
//   - a rollback installs the retired version under the SAME id and reports
//     the request that changed nothing as Unchanged;
//   - the namespace is the one ctx carries, exactly as Engine.Invoke resolves a
//     workflow id — so a host addressing another namespace sets it on the
//     context, and one that does not gets backend.ErrWorkflowNotFound.

func TestServerWorkflowVersionListDetailAndRollback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	srv, err := NewServer(ServerConfig{}, WithServerInsecureNoRunnerAuth())
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	defer func() { _ = srv.Shutdown(context.Background()) }()

	id, err := srv.AddWorkflow(ctx, pinVersionWorkflow("srv-versions", "v1", "only-v1"))
	if err != nil {
		t.Fatalf("AddWorkflow: %v", err)
	}
	publishWorkflowVersion(t, ctx, srv.Engine().workflowRegistry, id, pinVersionWorkflow("srv-versions", "v2", "only-v2"))

	rows, err := srv.ListWorkflowVersions(ctx, id)
	if err != nil {
		t.Fatalf("ListWorkflowVersions: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("versions = %+v, want 2 rows (current v2, archived v1)", rows)
	}
	if rows[0].Version != "v2" || !rows[0].Current || rows[0].Archived {
		t.Fatalf("row 0 = %+v, want current v2", rows[0])
	}
	if rows[1].Version != "v1" || !rows[1].Archived || rows[1].Current {
		t.Fatalf("row 1 = %+v, want archived v1", rows[1])
	}
	if rows[0].DefinitionHash == "" || rows[1].DefinitionHash == "" {
		t.Fatalf("rows carry no definition hash: %+v", rows)
	}

	// The retired payload is readable after the replacement that retired it.
	rec, err := srv.GetWorkflowVersion(ctx, id, "v1")
	if err != nil {
		t.Fatalf("GetWorkflowVersion(v1): %v", err)
	}
	if rec.Version != "v1" || rec.Definition == nil || rec.Definition.Version != "v1" {
		t.Fatalf("retired record = version %q definition %+v, want the v1 payload", rec.Version, rec.Definition)
	}
	if rec.DefinitionHash != rows[1].DefinitionHash {
		t.Fatalf("retired hash = %q, want the listed %q", rec.DefinitionHash, rows[1].DefinitionHash)
	}

	// Rollback: v1 becomes current under the same id, v2 moves into history.
	res, err := srv.RollbackWorkflow(ctx, id, "v1")
	if err != nil {
		t.Fatalf("RollbackWorkflow(v1): %v", err)
	}
	if res.WorkflowID != id || res.Version != "v1" || res.Unchanged {
		t.Fatalf("rollback result = %+v, want id %q version v1 changed", res, id)
	}
	rows, err = srv.ListWorkflowVersions(ctx, id)
	if err != nil {
		t.Fatalf("ListWorkflowVersions after rollback: %v", err)
	}
	if len(rows) != 2 || rows[0].Version != "v1" || !rows[0].Current || rows[1].Version != "v2" || !rows[1].Archived {
		t.Fatalf("versions after rollback = %+v, want [v1 current, v2 archived]", rows)
	}

	// Replaying the rollback is the idempotent no-op, not a second revision.
	res, err = srv.RollbackWorkflow(ctx, id, "v1")
	if err != nil {
		t.Fatalf("RollbackWorkflow(v1) replay: %v", err)
	}
	if !res.Unchanged || res.Version != "v1" {
		t.Fatalf("replayed rollback = %+v, want unchanged v1", res)
	}
	if rows, err = srv.ListWorkflowVersions(ctx, id); err != nil || len(rows) != 2 {
		t.Fatalf("versions after replay = %+v (err %v), want exactly 2", rows, err)
	}

	// The error taxonomy the HTTP route reports survives the embedded call.
	if _, err := srv.RollbackWorkflow(ctx, id, "v9"); !errors.Is(err, backend.ErrWorkflowVersionNotFound) {
		t.Fatalf("RollbackWorkflow(v9) error = %v, want backend.ErrWorkflowVersionNotFound", err)
	}
	if _, err := srv.GetWorkflowVersion(ctx, id, "v9"); !errors.Is(err, backend.ErrWorkflowVersionNotFound) {
		t.Fatalf("GetWorkflowVersion(v9) error = %v, want backend.ErrWorkflowVersionNotFound", err)
	}
	if _, err := srv.ListWorkflowVersions(ctx, "wf-srv-missing"); !errors.Is(err, backend.ErrWorkflowNotFound) {
		t.Fatalf("ListWorkflowVersions(unknown id) error = %v, want backend.ErrWorkflowNotFound", err)
	}
}

// TestServerWorkflowVersionsFollowTheContextNamespace pins the namespace rule
// the SDK shares with Engine.Invoke: a workflow registered in another namespace
// is addressable only with that namespace on the context. Resolving it from the
// id alone would let a by-id call read or rewrite state its caller never named,
// which is the cross-namespace disclosure the HTTP routes defend against with
// the authenticated namespace.
func TestServerWorkflowVersionsFollowTheContextNamespace(t *testing.T) {
	base, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	srv, err := NewServer(ServerConfig{}, WithServerInsecureNoRunnerAuth())
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	defer func() { _ = srv.Shutdown(context.Background()) }()

	nsCtx := namespace.WithNamespace(base, "team-a")
	id, err := srv.AddWorkflow(nsCtx, pinVersionWorkflow("srv-ns-versions", "v1", "only-v1").Namespace("team-a"))
	if err != nil {
		t.Fatalf("AddWorkflow: %v", err)
	}
	publishWorkflowVersion(t, nsCtx, srv.Engine().workflowRegistry, id,
		pinVersionWorkflow("srv-ns-versions", "v2", "only-v2").Namespace("team-a"))

	if _, err := srv.ListWorkflowVersions(nsCtx, id); err != nil {
		t.Fatalf("ListWorkflowVersions in team-a: %v", err)
	}
	if _, err := srv.ListWorkflowVersions(base, id); !errors.Is(err, backend.ErrWorkflowNotFound) {
		t.Fatalf("ListWorkflowVersions without the namespace error = %v, want backend.ErrWorkflowNotFound", err)
	}

	res, err := srv.RollbackWorkflow(nsCtx, id, "v1")
	if err != nil {
		t.Fatalf("RollbackWorkflow in team-a: %v", err)
	}
	if res.Unchanged || res.Version != "v1" {
		t.Fatalf("rollback result = %+v, want v1 installed", res)
	}
	if _, err := srv.RollbackWorkflow(base, id, "v2"); !errors.Is(err, backend.ErrWorkflowNotFound) {
		t.Fatalf("RollbackWorkflow without the namespace error = %v, want backend.ErrWorkflowNotFound", err)
	}
}
