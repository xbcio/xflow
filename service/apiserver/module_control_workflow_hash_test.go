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
// full-definition audit fingerprint the SDK records: the sha256 of the stored
// definition as sent, under the audit prefix.
func TestRegisterWorkflowRecordsAuditFingerprint(t *testing.T) {
	srv, cp := newRegisterTestServer(t)

	def := waitWorkflow("audit", false)
	def.Description = "editor note"
	id := postRegisterID(t, srv.URL, def, http.StatusCreated)
	rec, err := cp.WorkflowRegistry().GetWorkflow(context.Background(), id)
	if err != nil {
		t.Fatalf("GetWorkflow: %v", err)
	}
	want, err := workflowhash.Audit(rec.Definition)
	if err != nil {
		t.Fatalf("Audit: %v", err)
	}
	if rec.AuditFingerprint != want {
		t.Fatalf("AuditFingerprint = %q, want %q", rec.AuditFingerprint, want)
	}
	if got := strings.TrimPrefix(rec.AuditFingerprint, workflowhash.AuditPrefix); "sha256:"+got != legacySHA256(t, rec.Definition) {
		t.Fatalf("audit hex %q differs from the legacy full-definition sha256", got)
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
