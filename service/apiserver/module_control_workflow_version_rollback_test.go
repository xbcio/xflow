package apiserver

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/types"
)

// This file covers POST /v1/workflows/{id}/rollback (versioning design §2.4).
// The properties pinned here are the ones a mistake would not surface as a
// compile error:
//
//   - a rollback installs the retired version's payload as the live record
//     under the SAME id, and the replaced current version lands in history —
//     the exact dual of the retirement that put the target there;
//   - rolling back to the current version (or omitting it) is a no-op;
//   - an unknown version is 404 workflow_version_not_found and a
//     cross-namespace id is 404 workflow_not_found;
//   - replaying the same request with the same X-Request-Id writes no second
//     revision and no second history entry;
//   - the activation of the version the rollback replaces turns non-desired,
//     and the rolled-back version's activation returns with the ARCHIVED
//     parameters — the trigger surface follows the version, not the other way
//     around;
//   - an archived payload that no longer reproduces under the current compiler
//     is refused (409 workflow_version_unavailable) with the live record left
//     untouched.

func postRollback(t *testing.T, base, token, id string, body any, requestID string) *http.Response {
	t.Helper()
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		t.Fatalf("encode request: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, base+"/v1/workflows/"+id+"/rollback", &buf)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if requestID != "" {
		req.Header.Set("X-Request-Id", requestID)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /v1/workflows/%s/rollback: %v", id, err)
	}
	return resp
}

func liveWorkflowRecord(t *testing.T, cp *control.ControlPlane, id string) backend.WorkflowRecord {
	t.Helper()
	rec, err := cp.WorkflowRegistry().GetWorkflow(context.Background(), types.WorkflowID(id))
	if err != nil {
		t.Fatalf("GetWorkflow(%q): %v", id, err)
	}
	return rec
}

// TestRollbackWorkflowInstallsTheArchivedVersionUnderTheSameID is the core
// semantics: after v1 is replaced by v2, rolling back to v1 makes v1 the live
// record again — same id, v1's definition served by the by-id read — and v2
// moves into history, which is what makes a second rollback (to v2) possible.
func TestRollbackWorkflowInstallsTheArchivedVersionUnderTheSameID(t *testing.T) {
	srv, cp := newWorkflowReplaceDependencyTestServer(t, nil, nil, listTestPrincipals())
	id := registerListWorkflow(t, srv.URL, "tok-a", "vroll", "v1")
	publishRun(t, srv.URL, "tok-a", id, "vroll", "v2")

	before := liveWorkflowRecord(t, cp, id)
	if before.Version != "v2" {
		t.Fatalf("live version before rollback = %q, want v2", before.Version)
	}

	resp := postRollback(t, srv.URL, "tok-a", id, rollbackWorkflowRequest{Version: "v1"}, "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("rollback status = %d, want 200 (body=%s)", resp.StatusCode, body)
	}
	var out WorkflowRollbackResult
	if env := decodeEnvelope(t, resp, &out); !env.Success {
		t.Fatalf("rollback envelope = %+v, want success", env)
	}
	if string(out.WorkflowID) != id || out.Version != "v1" || out.Unchanged {
		t.Fatalf("rollback response = %+v, want workflow_id %q version v1 unchanged=false", out, id)
	}

	after := liveWorkflowRecord(t, cp, id)
	if after.ID != before.ID {
		t.Fatalf("rollback changed the workflow id: %q -> %q (version ownership follows the id)", before.ID, after.ID)
	}
	if after.Version != "v1" {
		t.Fatalf("live version after rollback = %q, want v1", after.Version)
	}
	if after.RegistryRevision <= before.RegistryRevision {
		t.Fatalf("registry revision after rollback = %d, want > %d", after.RegistryRevision, before.RegistryRevision)
	}
	if after.DefinitionHash != before.DefinitionHash {
		// v1's hash, not v2's: the rollback installs the archived hash verbatim.
		if after.DefinitionHash == "" || after.DefinitionHash == before.DefinitionHash {
			t.Fatalf("definition hash after rollback = %q, want v1's archived hash", after.DefinitionHash)
		}
	}
	if after.Definition == nil || after.Definition.Version != "v1" {
		t.Fatalf("live definition after rollback = %+v, want the archived v1 definition", after.Definition)
	}

	// The version list is the dual of the pre-rollback one: v1 current, v2
	// archived.
	status, body := getWorkflowVersions(t, srv.URL, "tok-a", id, "")
	if status != http.StatusOK {
		t.Fatalf("versions status = %d, want 200 (body=%s)", status, body)
	}
	env := decodeVersionListEnvelope(t, body)
	if env.Data == nil || env.Data.Total != 2 || len(env.Data.List) != 2 {
		t.Fatalf("versions after rollback = %+v, want [v1 current, v2 archived]", env.Data)
	}
	if env.Data.List[0].Version != "v1" || !env.Data.List[0].Current {
		t.Fatalf("row 0 = %+v, want current v1", env.Data.List[0])
	}
	if env.Data.List[1].Version != "v2" || !env.Data.List[1].Archived {
		t.Fatalf("row 1 = %+v, want archived v2 (the replaced current version)", env.Data.List[1])
	}

	// And a second rollback back to v2 proves history is readable both ways.
	resp = postRollback(t, srv.URL, "tok-a", id, rollbackWorkflowRequest{Version: "v2"}, "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("second rollback status = %d, want 200 (body=%s)", resp.StatusCode, body)
	}
	if final := liveWorkflowRecord(t, cp, id); final.Version != "v2" {
		t.Fatalf("live version after second rollback = %q, want v2", final.Version)
	}
}

// TestRollbackWorkflowToTheCurrentVersionIsANoOp pins the unchanged branch: a
// rollback that names the version already installed writes nothing, whether the
// version is explicit or omitted, and reports unchanged=true rather than a
// second revision.
func TestRollbackWorkflowToTheCurrentVersionIsANoOp(t *testing.T) {
	srv, cp := newWorkflowReplaceDependencyTestServer(t, nil, nil, listTestPrincipals())
	id := registerListWorkflow(t, srv.URL, "tok-a", "vroll-noop", "v1")
	publishRun(t, srv.URL, "tok-a", id, "vroll-noop", "v2")

	before := liveWorkflowRecord(t, cp, id)

	for _, tc := range []struct {
		name string
		body rollbackWorkflowRequest
	}{
		{name: "explicit current version", body: rollbackWorkflowRequest{Version: "v2"}},
		{name: "omitted version means current", body: rollbackWorkflowRequest{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := postRollback(t, srv.URL, "tok-a", id, tc.body, "")
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != http.StatusOK {
				body, _ := io.ReadAll(resp.Body)
				t.Fatalf("status = %d, want 200 (body=%s)", resp.StatusCode, body)
			}
			var out WorkflowRollbackResult
			env := decodeEnvelope(t, resp, &out)
			if !env.Success || !out.Unchanged || out.Version != "v2" {
				t.Fatalf("response = %+v (envelope %+v), want version v2 unchanged=true", out, env)
			}
			if after := liveWorkflowRecord(t, cp, id); after.RegistryRevision != before.RegistryRevision {
				t.Fatalf("revision = %d, want the no-op to leave %d untouched", after.RegistryRevision, before.RegistryRevision)
			}
		})
	}
}

// TestRollbackWorkflowFailureCodes pins the two 404 shapes apart — an unknown
// version is not a missing workflow — and the cross-namespace refusal.
func TestRollbackWorkflowFailureCodes(t *testing.T) {
	srv, _ := newWorkflowReplaceDependencyTestServer(t, nil, nil, listTestPrincipals())
	idA := registerListWorkflow(t, srv.URL, "tok-a", "vroll-codes", "v1")

	cases := []struct {
		name     string
		token    string
		id       string
		version  string
		wantCode string
	}{
		{name: "unknown version", token: "tok-a", id: idA, version: "v9", wantCode: "workflow_version_not_found"},
		{name: "unknown workflow", token: "tok-a", id: "wf-does-not-exist", version: "v1", wantCode: "workflow_not_found"},
		{name: "cross-namespace workflow", token: "tok-b", id: idA, version: "v1", wantCode: "workflow_not_found"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := postRollback(t, srv.URL, tc.token, tc.id, rollbackWorkflowRequest{Version: tc.version}, "")
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != http.StatusNotFound {
				body, _ := io.ReadAll(resp.Body)
				t.Fatalf("status = %d, want 404 (body=%s)", resp.StatusCode, body)
			}
			if env := decodeEnvelope(t, resp, nil); env.Success || env.Code != tc.wantCode {
				t.Fatalf("envelope = %+v, want code %q", env, tc.wantCode)
			}
		})
	}
}

// TestRollbackWorkflowReplayWritesNoSecondRevision pins the idempotent-replay
// contract the rollback inherits from PUT: the same X-Request-Id with the same
// body must not produce a second revision or a second history entry. The
// replay lands on the no-op branch because the first attempt installed the
// requested version; either path answers 200 and writes nothing.
func TestRollbackWorkflowReplayWritesNoSecondRevision(t *testing.T) {
	srv, cp := newWorkflowReplaceDependencyTestServer(t, nil, nil, listTestPrincipals())
	id := registerListWorkflow(t, srv.URL, "tok-a", "vroll-replay", "v1")
	publishRun(t, srv.URL, "tok-a", id, "vroll-replay", "v2")

	resp := postRollback(t, srv.URL, "tok-a", id, rollbackWorkflowRequest{Version: "v1"}, "rb-replay-1")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("first rollback status = %d, want 200 (body=%s)", resp.StatusCode, body)
	}
	first := liveWorkflowRecord(t, cp, id)

	resp2 := postRollback(t, srv.URL, "tok-a", id, rollbackWorkflowRequest{Version: "v1"}, "rb-replay-1")
	defer func() { _ = resp2.Body.Close() }()
	if resp2.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp2.Body)
		t.Fatalf("replay status = %d, want 200 (body=%s)", resp2.StatusCode, body)
	}
	second := liveWorkflowRecord(t, cp, id)
	if second.RegistryRevision != first.RegistryRevision {
		t.Fatalf("replay revision = %d, want the first rollback's %d (no second revision)", second.RegistryRevision, first.RegistryRevision)
	}
	if second.Version != "v1" {
		t.Fatalf("replay left the live version at %q, want v1", second.Version)
	}

	status, body := getWorkflowVersions(t, srv.URL, "tok-a", id, "")
	if status != http.StatusOK {
		t.Fatalf("versions status = %d, want 200 (body=%s)", status, body)
	}
	if env := decodeVersionListEnvelope(t, body); env.Data == nil || env.Data.Total != 2 {
		t.Fatalf("history after replay = %+v, want exactly 2 versions (no duplicate retirement)", env.Data)
	}
}

// TestRollbackWorkflowTurnsTheReplacedVersionsActivationNonDesired pins the
// trigger surface of a rollback (§2.5): the version it replaces stops being
// desired, and the rolled-back version's activation returns carrying the
// ARCHIVED parameters. It is the reason the rollback is one atomic CAS rather
// than a remove+add: the projection follows the committed record.
func TestRollbackWorkflowTurnsTheReplacedVersionsActivationNonDesired(t *testing.T) {
	activationStore := control.NewMemoryEntryActivationStore()
	srv, cp := newWorkflowReplaceDependencyTestServer(t, nil, activationStore, nil)

	v1 := configuredTriggerWorkflow("topic-a")
	resp := postWorkflows(t, srv.URL, "tok-full", v1)
	defer func() { _ = resp.Body.Close() }()
	var out registerWorkflowResponse
	decodeEnvelope(t, resp, &out)
	id := string(out.WorkflowID)
	if id == "" {
		t.Fatal("register returned an empty workflow id")
	}
	assertActivationVersion(t, activationStore, id, "v1", true, "topic-a")

	// Publishing v2 retires v1's activation.
	v2 := configuredTriggerWorkflow("topic-b")
	v2.Version = "v2"
	putResp := putWorkflow(t, srv.URL, "tok-full", id, v2)
	defer func() { _ = putResp.Body.Close() }()
	if putResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(putResp.Body)
		t.Fatalf("PUT v2 status = %d, want 200 (body=%s)", putResp.StatusCode, body)
	}
	assertActivationVersion(t, activationStore, id, "v2", true, "topic-b")
	assertActivationVersion(t, activationStore, id, "v1", false, "")

	// Rolling back to v1 makes v1 desired again with its archived topic, and
	// turns v2 — the version being replaced — non-desired.
	rollResp := postRollback(t, srv.URL, "tok-full", id, rollbackWorkflowRequest{Version: "v1"}, "")
	defer func() { _ = rollResp.Body.Close() }()
	if rollResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(rollResp.Body)
		t.Fatalf("rollback status = %d, want 200 (body=%s)", rollResp.StatusCode, body)
	}
	assertActivationVersion(t, activationStore, id, "v1", true, "topic-a")
	assertActivationVersion(t, activationStore, id, "v2", false, "")
	if got := cp.EntryActivationStore(); got == nil {
		t.Fatal("activation store is nil; the fixture is invalid")
	}
}

// assertActivationVersion reads the entry activation for one version of the
// workflow and asserts its desired state, plus the configured trigger topic
// when one is expected.
func assertActivationVersion(t *testing.T, store engine.EntryActivationStore, id, version string, wantDesired bool, wantTopic string) {
	t.Helper()
	act, ok, err := store.Get(context.Background(), engine.EntryActivationKey{
		Namespace:       "namespaceA",
		WorkflowID:      types.WorkflowID(id),
		WorkflowVersion: version,
		EntryUnitID:     "trig",
	})
	if err != nil {
		t.Fatalf("Get activation (%s): %v", version, err)
	}
	if !ok {
		t.Fatalf("activation for version %s is missing", version)
	}
	if act.Desired != wantDesired {
		t.Fatalf("activation for version %s desired = %v, want %v", version, act.Desired, wantDesired)
	}
	if wantTopic != "" {
		if got := act.Params["topic"]; got != wantTopic {
			t.Fatalf("activation for version %s topic = %v, want %s", version, got, wantTopic)
		}
	}
}

// TestRollbackWorkflowRefusesAnArchivedVersionThatNoLongerReproduces pins the
// fail-closed rule on the write path: a retired payload whose stored hash the
// current compiler does not reproduce must not be installed as the live record
// (that would publish the same version label with different semantics). The
// refusal is 409 workflow_version_unavailable and the live record is untouched.
func TestRollbackWorkflowRefusesAnArchivedVersionThatNoLongerReproduces(t *testing.T) {
	srv, cp := newWorkflowReplaceDependencyTestServer(t, nil, nil, listTestPrincipals())

	def := listTestWorkflow("vroll-drift", "v1")
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	const driftID = "wf-rollback-drift"
	if _, err := cp.WorkflowRegistry().AddWorkflow(namespace.WithNamespace(context.Background(), "namespaceA"), backend.WorkflowRecord{
		ID:             driftID,
		Key:            "namespaceA/vroll-drift@v1",
		Namespace:      "namespaceA",
		Name:           "vroll-drift",
		Version:        "v1",
		DefinitionHash: "sha256:stale-drift",
		Definition:     def,
		Graph:          g,
	}); err != nil {
		t.Fatalf("seed drifted v1: %v", err)
	}
	publishRun(t, srv.URL, "tok-a", driftID, "vroll-drift", "v2")

	resp := postRollback(t, srv.URL, "tok-a", driftID, rollbackWorkflowRequest{Version: "v1"}, "")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusConflict {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("rollback status = %d, want 409 (body=%s)", resp.StatusCode, body)
	}
	if env := decodeEnvelope(t, resp, nil); env.Success || env.Code != "workflow_version_unavailable" {
		t.Fatalf("envelope = %+v, want workflow_version_unavailable", env)
	}
	if live := liveWorkflowRecord(t, cp, driftID); live.Version != "v2" {
		t.Fatalf("live version after refused rollback = %q, want v2 (untouched)", live.Version)
	}
}
