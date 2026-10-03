package apiserver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/backend/workflowhash"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/types"
)

// waitWorkflow is start -> wait. withMode spells out wait.mode, the builtin
// Default the SDK builder writes; without it the definition is what an editor
// or YAML author sends.
func waitWorkflow(name string, withMode bool) *types.WorkflowDef {
	params := map[string]any{"signal_name": "go"}
	if withMode {
		params["mode"] = "signal"
	}
	return &types.WorkflowDef{
		Name: name,
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "wait", Type: "xflow.wait", Parameters: params},
		},
		Connections: types.Connections{
			"start": {"main": types.PortConnections{Targets: []types.Connection{{Node: "wait", Input: "main"}}}},
		},
	}
}

func postRegisterID(t *testing.T, base string, def *types.WorkflowDef, wantStatus int) types.WorkflowID {
	t.Helper()
	resp := postRegister(t, base, "tok-full", def)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != wantStatus {
		t.Fatalf("POST status = %d, want %d", resp.StatusCode, wantStatus)
	}
	if wantStatus != http.StatusCreated {
		return ""
	}
	var out registerWorkflowResponse
	decodeRegisterData(t, resp, &out)
	return out.WorkflowID
}

// TestRegisterWorkflowStoresCanonicalRuntimeHash pins that POST stores the
// runtime hash the SDK computes, over the definition with builtin Defaults
// filled in memory only: the stored definition stays as sent.
func TestRegisterWorkflowStoresCanonicalRuntimeHash(t *testing.T) {
	srv, cp := newRegisterTestServer(t)

	id := postRegisterID(t, srv.URL, waitWorkflow("hash", false), http.StatusCreated)
	rec, err := cp.WorkflowRegistry().GetWorkflow(context.Background(), id)
	if err != nil {
		t.Fatalf("GetWorkflow: %v", err)
	}
	if !strings.HasPrefix(rec.DefinitionHash, workflowhash.RuntimePrefixV1) {
		t.Fatalf("DefinitionHash = %q, want %s prefix", rec.DefinitionHash, workflowhash.RuntimePrefixV1)
	}
	filled := waitWorkflow("hash", true)
	filled.Namespace = "namespaceA"
	want, err := workflowhash.Runtime(filled, nil)
	if err != nil {
		t.Fatalf("Runtime: %v", err)
	}
	if rec.DefinitionHash != want {
		t.Fatalf("DefinitionHash = %q, want the hash of the Default-filled definition %q", rec.DefinitionHash, want)
	}
	if _, ok := rec.Definition.Nodes[1].Parameters["mode"]; ok {
		t.Fatal("stored definition gained wait.mode; the HTTP path must not write Defaults")
	}
}

// TestRegisterWorkflowRecordsAuditFingerprint pins that POST records the
// full-definition audit fingerprint the SDK records: Audit's fingerprint
// over the stored definition and its (possibly absent) editor metadata
// sibling, under the audit prefix.
//
// Audit's payload shape changed from "bare json.Marshal(def)" to
// "{definition, editor_metadata}" when editor_metadata became a wrapper
// field rather than inline NodeDef fields (ADR-D4 D1/D6): the fingerprint is
// no longer byte-identical to a bare marshal of the definition, by design,
// since it must also cover editor_metadata. There is no backward-compat
// requirement to preserve the old byte-for-byte form (D6).
func TestRegisterWorkflowRecordsAuditFingerprint(t *testing.T) {
	srv, cp := newRegisterTestServer(t)

	def := waitWorkflow("audit", false)
	def.Description = "editor note"
	id := postRegisterID(t, srv.URL, def, http.StatusCreated)
	rec, err := cp.WorkflowRegistry().GetWorkflow(context.Background(), id)
	if err != nil {
		t.Fatalf("GetWorkflow: %v", err)
	}
	want, err := workflowhash.Audit(rec.Definition, rec.EditorMetadata)
	if err != nil {
		t.Fatalf("Audit: %v", err)
	}
	if rec.AuditFingerprint != want {
		t.Fatalf("AuditFingerprint = %q, want %q", rec.AuditFingerprint, want)
	}
}

// TestRegisterWorkflowOmittedDefaultIsIdempotent pins that an omitted builtin
// param and its spelled-out Default are one registration identity.
func TestRegisterWorkflowOmittedDefaultIsIdempotent(t *testing.T) {
	srv, _ := newRegisterTestServer(t)

	first := postRegisterID(t, srv.URL, waitWorkflow("idem", false), http.StatusCreated)
	second := postRegisterID(t, srv.URL, waitWorkflow("idem", true), http.StatusCreated)
	if first != second {
		t.Fatalf("second POST id = %q, want the existing %q", second, first)
	}

	changed := waitWorkflow("idem", false)
	changed.Nodes[1].Parameters["signal_name"] = "other"
	postRegisterID(t, srv.URL, changed, http.StatusConflict)
}

// legacySHA256 is the "sha256:" hash the HTTP path stored before it hashed
// with workflowhash.Runtime: SHA-256 over json.Marshal of the full definition.
func legacySHA256(t *testing.T, def *types.WorkflowDef) string {
	t.Helper()
	data, err := json.Marshal(def)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// seedLegacyRecord stores def under a pre-runtime-hash "sha256:" record, as an
// older server's POST wrote it.
func seedLegacyRecord(t *testing.T, reg backend.WorkflowRegistry, def *types.WorkflowDef) backend.WorkflowRecord {
	t.Helper()
	def.Namespace = "namespaceA"
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	ctx := namespace.WithNamespace(context.Background(), "namespaceA")
	rec, err := reg.AddWorkflow(ctx, backend.WorkflowRecord{
		Key:            workflowRegistryKey("namespaceA", def.Name, def.Version),
		Namespace:      "namespaceA",
		Name:           def.Name,
		Version:        def.Version,
		DefinitionHash: legacySHA256(t, def),
		Definition:     def,
		Graph:          g,
	})
	if err != nil {
		t.Fatalf("seed AddWorkflow: %v", err)
	}
	return rec
}

// TestRegisterWorkflowReconcilesLegacyHash pins that re-POSTing the definition
// behind a legacy "sha256:" record is idempotent -- 201 with the existing id --
// and upgrades the stored hash in place, even when the re-POST spells out a
// builtin Default the stored definition omits. A real change still conflicts.
func TestRegisterWorkflowReconcilesLegacyHash(t *testing.T) {
	for _, withMode := range []bool{false, true} {
		srv, cp := newRegisterTestServer(t)
		reg := cp.WorkflowRegistry()
		legacy := seedLegacyRecord(t, reg, waitWorkflow("legacy", false))

		id := postRegisterID(t, srv.URL, waitWorkflow("legacy", withMode), http.StatusCreated)
		if id != legacy.ID {
			t.Fatalf("withMode=%v: POST id = %q, want the legacy record %q", withMode, id, legacy.ID)
		}
		got, err := reg.GetWorkflow(context.Background(), id)
		if err != nil {
			t.Fatalf("GetWorkflow: %v", err)
		}
		want, err := workflowhash.Runtime(got.Definition, hashParamSpecs)
		if err != nil {
			t.Fatalf("Runtime: %v", err)
		}
		if got.DefinitionHash != want {
			t.Fatalf("withMode=%v: stored hash = %q, want it upgraded to %q", withMode, got.DefinitionHash, want)
		}
		if _, ok := got.Definition.Nodes[1].Parameters["mode"]; ok {
			t.Fatalf("withMode=%v: reconcile rewrote the stored definition", withMode)
		}

		changed := waitWorkflow("legacy", withMode)
		changed.Nodes[1].Parameters["signal_name"] = "other"
		postRegisterID(t, srv.URL, changed, http.StatusConflict)
	}
}

// lostUpgradeRegistry loses the CAS hash upgrade to a concurrent registrar: it
// applies the upgrade itself, then reports the caller's CAS as failed.
type lostUpgradeRegistry struct {
	backend.WorkflowRegistry
	lost int
}

func (r *lostUpgradeRegistry) UpdateDefinitionHash(ctx context.Context, id types.WorkflowID, expectedOldHash, newHash string) error {
	if err := r.WorkflowRegistry.UpdateDefinitionHash(ctx, id, expectedOldHash, newHash); err != nil {
		return err
	}
	r.lost++
	return errors.New("definition hash changed concurrently")
}

// TestRegisterWorkflowLostUpgradeRaceRefetches pins the lost-CAS path: the
// re-fetched record already carries the new hash, so the POST is still an
// idempotent 201 with the existing id.
func TestRegisterWorkflowLostUpgradeRaceRefetches(t *testing.T) {
	reg := &lostUpgradeRegistry{WorkflowRegistry: local.New().WorkflowRegistry()}
	srv, _ := newWorkflowReplaceDependencyTestServer(t, reg, nil, nil)
	legacy := seedLegacyRecord(t, reg.WorkflowRegistry, waitWorkflow("race", false))

	id := postRegisterID(t, srv.URL, waitWorkflow("race", true), http.StatusCreated)
	if id != legacy.ID {
		t.Fatalf("POST id = %q, want the legacy record %q", id, legacy.ID)
	}
	if reg.lost != 1 {
		t.Fatalf("lost upgrades = %d, want 1 (the reconcile must have hit the CAS path)", reg.lost)
	}
}

// TestPutWorkflowMetadataOnlyChangeIsAReplace pins that PUT keeps
// full-definition no-op semantics: a change the runtime hash ignores (here the
// description) is still written as a new revision, while the identical PUT
// after it is a no-op.
func TestPutWorkflowMetadataOnlyChangeIsAReplace(t *testing.T) {
	srv, cp := newRegisterTestServer(t)
	reg := cp.WorkflowRegistry()

	id := postRegisterID(t, srv.URL, waitWorkflow("meta", false), http.StatusCreated)
	before, err := reg.GetWorkflow(context.Background(), id)
	if err != nil {
		t.Fatalf("GetWorkflow: %v", err)
	}

	edited := waitWorkflow("meta", false)
	edited.ID = string(id)
	edited.Description = "edited in the editor"
	putStatus := func(def *types.WorkflowDef) {
		t.Helper()
		resp := putWorkflow(t, srv.URL, "tok-full", string(id), def)
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("PUT status = %d, want 200", resp.StatusCode)
		}
	}
	putStatus(edited)
	after, err := reg.GetWorkflow(context.Background(), id)
	if err != nil {
		t.Fatalf("GetWorkflow after PUT: %v", err)
	}
	if after.RegistryRevision == before.RegistryRevision || after.Definition.Description != "edited in the editor" {
		t.Fatalf("metadata-only PUT was dropped: revision %d -> %d, description %q",
			before.RegistryRevision, after.RegistryRevision, after.Definition.Description)
	}
	if after.DefinitionHash != before.DefinitionHash {
		t.Fatalf("runtime hash moved on a metadata-only change: %q -> %q", before.DefinitionHash, after.DefinitionHash)
	}
	if after.AuditFingerprint == before.AuditFingerprint {
		t.Fatal("audit fingerprint did not move on a metadata-only change")
	}

	putStatus(edited)
	again, err := reg.GetWorkflow(context.Background(), id)
	if err != nil {
		t.Fatalf("GetWorkflow after identical PUT: %v", err)
	}
	if again.RegistryRevision != after.RegistryRevision {
		t.Fatalf("identical PUT wrote a revision: %d -> %d", after.RegistryRevision, again.RegistryRevision)
	}
}

// TestPutWorkflowUnchangedLegacyRecordIsNoOp pins the audit compare against a
// record written before registrations carried a fingerprint: its fingerprint
// is recomputed from the stored definition.
func TestPutWorkflowUnchangedLegacyRecordIsNoOp(t *testing.T) {
	srv, cp := newRegisterTestServer(t)
	reg := cp.WorkflowRegistry()
	legacy := seedLegacyRecord(t, reg, waitWorkflow("legacy-put", false))

	same := waitWorkflow("legacy-put", false)
	same.ID = string(legacy.ID)
	resp := putWorkflow(t, srv.URL, "tok-full", string(legacy.ID), same)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status = %d, want 200", resp.StatusCode)
	}
	after, err := reg.GetWorkflow(context.Background(), legacy.ID)
	if err != nil {
		t.Fatalf("GetWorkflow: %v", err)
	}
	if after.RegistryRevision != legacy.RegistryRevision || after.DefinitionHash != legacy.DefinitionHash {
		t.Fatalf("unchanged PUT over a legacy record wrote it: rev %d -> %d, hash %q -> %q",
			legacy.RegistryRevision, after.RegistryRevision, legacy.DefinitionHash, after.DefinitionHash)
	}
}

// TestPutWorkflowRepeatedIdenticalPutIsNoOp pins the no-op replace over a
// record whose stored definition already carries its id: one last written by
// PUT, or a legacy "sha256:" record written that way. An identical PUT without
// a client mutation id returns the same id, keeps the revision and hash, and
// never reaches the CAS (so it neither upgrades a legacy hash nor records a
// mutation). Retries that carry a client mutation id still replay through the
// operation ledger; module_control_atomic_replace_test.go pins that.
func TestPutWorkflowRepeatedIdenticalPutIsNoOp(t *testing.T) {
	tests := []struct {
		name string
		// seed stores the record and returns its id plus the PUT body that is
		// identical to the stored definition.
		seed func(t *testing.T, base backend.WorkflowRegistry, srvURL string) (types.WorkflowID, *types.WorkflowDef)
	}{
		{
			name: "last written by PUT",
			seed: func(t *testing.T, _ backend.WorkflowRegistry, srvURL string) (types.WorkflowID, *types.WorkflowDef) {
				t.Helper()
				id := postRegisterID(t, srvURL, waitWorkflow("repeat-put", false), http.StatusCreated)
				edited := waitWorkflow("repeat-put", false)
				edited.ID = string(id)
				edited.Description = "edited in the editor"
				resp := putWorkflow(t, srvURL, "tok-full", string(id), edited)
				defer func() { _ = resp.Body.Close() }()
				if resp.StatusCode != http.StatusOK {
					t.Fatalf("changing PUT status = %d, want 200", resp.StatusCode)
				}
				same := waitWorkflow("repeat-put", false)
				same.ID = string(id)
				same.Description = "edited in the editor"
				return id, same
			},
		},
		{
			name: "legacy record carrying its id",
			seed: func(t *testing.T, base backend.WorkflowRegistry, _ string) (types.WorkflowID, *types.WorkflowDef) {
				t.Helper()
				const id = types.WorkflowID("legacy-put-written")
				def := waitWorkflow("repeat-put", false)
				def.ID = string(id)
				def.Namespace = "namespaceA"
				g, err := graph.Compile(def)
				if err != nil {
					t.Fatalf("Compile: %v", err)
				}
				ctx := namespace.WithNamespace(context.Background(), "namespaceA")
				if _, err := base.AddWorkflow(ctx, backend.WorkflowRecord{
					ID:             id,
					Key:            workflowRegistryKey("namespaceA", def.Name, def.Version),
					Namespace:      "namespaceA",
					Name:           def.Name,
					Version:        def.Version,
					DefinitionHash: legacySHA256(t, def),
					Definition:     def,
					Graph:          g,
				}); err != nil {
					t.Fatalf("seed AddWorkflow: %v", err)
				}
				same := waitWorkflow("repeat-put", false)
				same.ID = string(id)
				return id, same
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := local.New().WorkflowRegistry()
			hooked := newHookedAtomicWorkflowRegistry(base)
			srv, _ := newWorkflowReplaceDependencyTestServer(t, hooked, control.NewMemoryEntryActivationStore(), nil)
			id, same := tt.seed(t, base, srv.URL)
			before, err := base.GetWorkflow(context.Background(), id)
			if err != nil {
				t.Fatalf("GetWorkflow(%q) before identical PUT: %v", id, err)
			}
			if before.Definition == nil || before.Definition.ID != string(id) {
				t.Fatalf("stored definition id = %v, want it to carry %q", before.Definition, id)
			}
			callsBefore, _, _, _ := hooked.observations()

			resp := putWorkflow(t, srv.URL, "tok-full", string(id), same)
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("identical PUT status = %d, want 200", resp.StatusCode)
			}
			var out registerWorkflowResponse
			decodeEnvelope(t, resp, &out)
			if out.WorkflowID != id {
				t.Fatalf("identical PUT workflow_id = %q, want %q", out.WorkflowID, id)
			}
			after, err := base.GetWorkflow(context.Background(), id)
			if err != nil {
				t.Fatalf("GetWorkflow(%q) after identical PUT: %v", id, err)
			}
			if after.RegistryRevision != before.RegistryRevision || after.DefinitionHash != before.DefinitionHash {
				t.Fatalf("identical PUT wrote the record: rev %d -> %d, hash %q -> %q",
					before.RegistryRevision, after.RegistryRevision, before.DefinitionHash, after.DefinitionHash)
			}
			if callsAfter, _, _, _ := hooked.observations(); callsAfter != callsBefore {
				t.Fatalf("CompareAndReplace calls = %d -> %d, want the identical PUT to skip the CAS", callsBefore, callsAfter)
			}
		})
	}
}

// TestReplaceWorkflowComparesAuditFingerprints pins the embedded replace no-op:
// an identical definition keeps the record and its id, even over a record whose
// stored hash string differs (a legacy "sha256:" one), while a metadata-only
// change is a real replace.
func TestReplaceWorkflowComparesAuditFingerprints(t *testing.T) {
	srv, cp := newReplaceTestServer(t)
	reg := cp.WorkflowRegistry()
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)

	def := waitWorkflow("embedded", false)
	def.Namespace = string(namespace.Default)
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	legacy, err := reg.AddWorkflow(ctx, backend.WorkflowRecord{
		Key:            workflowRegistryKey(string(namespace.Default), def.Name, def.Version),
		Namespace:      string(namespace.Default),
		Name:           def.Name,
		DefinitionHash: legacySHA256(t, def),
		Definition:     def,
		Graph:          g,
	})
	if err != nil {
		t.Fatalf("seed AddWorkflow: %v", err)
	}

	id, _, err := srv.ReplaceWorkflow(ctx, namespace.Default, waitWorkflow("embedded", false))
	if err != nil {
		t.Fatalf("identical ReplaceWorkflow: %v", err)
	}
	if id != legacy.ID {
		t.Fatalf("identical ReplaceWorkflow id = %q, want the existing %q", id, legacy.ID)
	}

	edited := waitWorkflow("embedded", false)
	edited.Description = "new description"
	newID, _, err := srv.ReplaceWorkflow(ctx, namespace.Default, edited)
	if err != nil {
		t.Fatalf("metadata-only ReplaceWorkflow: %v", err)
	}
	if newID == legacy.ID {
		t.Fatal("metadata-only ReplaceWorkflow kept the old record; the change was dropped")
	}
	rec, err := reg.GetWorkflow(ctx, newID)
	if err != nil {
		t.Fatalf("GetWorkflow(new): %v", err)
	}
	if rec.Definition.Description != "new description" {
		t.Fatalf("stored description = %q, want the replacement's", rec.Definition.Description)
	}
}
