package apiserver

import (
	"io"
	"net/http"
	"testing"

	"github.com/xbcio/xflow/types"
)

// This file pins the three distinct 409 codes the workflow registry can answer
// with, because collapsing them would send the caller to the wrong fix:
//
//   - workflow_name_conflict — another LIVE record holds this (namespace,
//     name) at a different version. The fix is to address the existing record
//     (PUT its id) or delete it, never to change the definition. This is the
//     check that closes the silent version split: before it, registering the
//     same name again under a new version produced two live records sharing an
//     activation key space.
//   - workflow_version_immutable — this version label was already published
//     with a different definition. A version label is a promise about
//     semantics; re-registering it with a different payload must not silently
//     rewrite it.
//   - workflow_conflict — the destination (namespace, name, version) key is
//     held by a different record; the fix is to change the definition's
//     identity or replace the holder.

// mutatedTestWorkflow is listTestWorkflow with a structurally different node
// set: same name and version, different runtime semantics.
func mutatedTestWorkflow(name, version string) *types.WorkflowDef {
	def := listTestWorkflow(name, version)
	def.Nodes[1].Name = "work-changed"
	def.Connections = types.Connections{
		"start": {"main": types.PortConnections{Targets: []types.Connection{{Node: "work-changed", Input: "main"}}}},
	}
	return def
}

// TestRegisterWorkflowSameNameDifferentVersionIsANameConflict pins the
// second-live-record refusal on the register path.
func TestRegisterWorkflowSameNameDifferentVersionIsANameConflict(t *testing.T) {
	srv, _ := newWorkflowReplaceDependencyTestServer(t, nil, nil, listTestPrincipals())
	id := registerListWorkflow(t, srv.URL, "tok-a", "conflict-name", "v1")

	resp := postRegister(t, srv.URL, "tok-a", listTestWorkflow("conflict-name", "v2"))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusConflict {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 409 (body=%s)", resp.StatusCode, body)
	}
	env := decodeEnvelope(t, resp, nil)
	if env.Success || env.Code != "workflow_name_conflict" {
		t.Fatalf("envelope = %+v, want workflow_name_conflict", env)
	}
	// The refusal must not have created the second record: the original id
	// still resolves and holds the name.
	if status, body := getWorkflowVersions(t, srv.URL, "tok-a", id, ""); status != http.StatusOK {
		t.Fatalf("original workflow versions status = %d, want 200 (body=%s)", status, body)
	}
}

// TestRegisterWorkflowSameVersionDifferentDefinitionIsImmutable pins the
// version-label freeze on the register path: the key already holds a version
// published with different semantics, and re-registering that label must be
// refused rather than silently rewriting what the label means.
func TestRegisterWorkflowSameVersionDifferentDefinitionIsImmutable(t *testing.T) {
	srv, _ := newWorkflowReplaceDependencyTestServer(t, nil, nil, listTestPrincipals())
	registerListWorkflow(t, srv.URL, "tok-a", "conflict-version", "v1")

	resp := postRegister(t, srv.URL, "tok-a", mutatedTestWorkflow("conflict-version", "v1"))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusConflict {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 409 (body=%s)", resp.StatusCode, body)
	}
	env := decodeEnvelope(t, resp, nil)
	if env.Success || env.Code != "workflow_version_immutable" {
		t.Fatalf("envelope = %+v, want workflow_version_immutable", env)
	}
}

// TestPutWorkflowByIDRenameOntoAnotherWorkflowsNameIsANameConflict pins the
// same invariant on the replace path, in the shape only the name index can
// catch: the destination KEY is free (the holder uses a different version), so
// a key-only registry would accept the rename and leave two live records
// sharing a name.
func TestPutWorkflowByIDRenameOntoAnotherWorkflowsNameIsANameConflict(t *testing.T) {
	srv, cp := newWorkflowReplaceDependencyTestServer(t, nil, nil, listTestPrincipals())
	holderID := registerListWorkflow(t, srv.URL, "tok-a", "conflict-holder", "v2")
	targetID := registerListWorkflow(t, srv.URL, "tok-a", "conflict-target", "v1")

	resp := putWorkflow(t, srv.URL, "tok-a", targetID, listTestWorkflow("conflict-holder", "v1"))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusConflict {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 409 (body=%s)", resp.StatusCode, body)
	}
	env := decodeEnvelope(t, resp, nil)
	if env.Success || env.Code != "workflow_name_conflict" {
		t.Fatalf("envelope = %+v, want workflow_name_conflict", env)
	}

	// Neither workflow moved: the holder keeps the name, the target keeps its
	// own.
	if rec := liveWorkflowRecord(t, cp, holderID); rec.Name != "conflict-holder" || rec.Version != "v2" {
		t.Fatalf("holder = %s@%s, want conflict-holder@v2 untouched", rec.Name, rec.Version)
	}
	if rec := liveWorkflowRecord(t, cp, targetID); rec.Name != "conflict-target" || rec.Version != "v1" {
		t.Fatalf("target = %s@%s, want conflict-target@v1 untouched", rec.Name, rec.Version)
	}
}

// TestPutWorkflowByIDInstallingATamperedRetiredLabelIsImmutable covers the
// replace-side shape of version immutability: v1 is in history, the record is
// live at v2, and a replace asks to install the v1 label with a DIFFERENT
// definition. Reinstalling the label with the payload it was retired with is
// the rollback path and is allowed; rewriting what the label means is 409
// workflow_version_immutable, and the live record is left untouched.
func TestPutWorkflowByIDInstallingATamperedRetiredLabelIsImmutable(t *testing.T) {
	srv, cp := newWorkflowReplaceDependencyTestServer(t, nil, nil, listTestPrincipals())
	id := registerListWorkflow(t, srv.URL, "tok-a", "conflict-retired", "v1")
	publishRun(t, srv.URL, "tok-a", id, "conflict-retired", "v2")

	before := liveWorkflowRecord(t, cp, id)

	resp := putWorkflow(t, srv.URL, "tok-a", id, mutatedTestWorkflow("conflict-retired", "v1"))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusConflict {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 409 (body=%s)", resp.StatusCode, body)
	}
	env := decodeEnvelope(t, resp, nil)
	if env.Success || env.Code != "workflow_version_immutable" {
		t.Fatalf("envelope = %+v, want workflow_version_immutable", env)
	}

	after := liveWorkflowRecord(t, cp, id)
	if after.Version != "v2" || after.DefinitionHash != before.DefinitionHash || after.RegistryRevision != before.RegistryRevision {
		t.Fatalf("live record after refused replace = version %q hash %q revision %d, want v2 hash %q revision %d untouched",
			after.Version, after.DefinitionHash, after.RegistryRevision, before.DefinitionHash, before.RegistryRevision)
	}
}
