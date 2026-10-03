//go:build integration

package integration

import (
	"encoding/json"
	"net/http"
	"testing"
)

// TestRedisEditorMetadataPersistsAcrossRegistryReconnect proves ADR-D4's
// server-side editor metadata storage (D4) end-to-end against a real Redis:
// registering with editor_metadata through the HTTP API, reading it back
// through a SEPARATE registry client connected to the same Redis (simulating
// a reconnect/restart), and confirming a metadata-only PUT still advances
// registry_revision without moving the runtime conflict hash (D3).
func TestRedisEditorMetadataPersistsAcrossRegistryReconnect(t *testing.T) {
	e := newHashReconcileEnv(t)
	name := hashReconcileName(t)

	body := e.body(hashReconcileWorkflow(name, "go"), true)
	body["editor_metadata"] = map[string]any{
		"positions": map[string]any{"start": map[string]any{"x": 1, "y": 2}},
		"viewport":  map[string]any{"x": 10, "y": 20, "zoom": 1.5},
		"notes":     map[string]any{"wait": "confirm before signaling"},
	}
	id := e.post(body, http.StatusCreated)

	// e.reg is a second distributed backend's WorkflowRegistry, opened on the
	// same Redis independently of the apiserver's own registry client -- this
	// is the "reconnect" the test name promises: a brand new client reads
	// back what a different client wrote.
	rec := e.record(id)
	if rec.EditorMetadata == nil {
		t.Fatal("stored record has no EditorMetadata after POST with editor_metadata")
	}
	if rec.EditorMetadata.Positions["start"].X != 1 || rec.EditorMetadata.Positions["start"].Y != 2 {
		t.Errorf("positions[start] = %+v", rec.EditorMetadata.Positions["start"])
	}
	if rec.EditorMetadata.Viewport == nil || rec.EditorMetadata.Viewport.Zoom != 1.5 {
		t.Errorf("viewport = %+v", rec.EditorMetadata.Viewport)
	}
	if rec.EditorMetadata.Notes["wait"] != "confirm before signaling" {
		t.Errorf("notes[wait] = %q", rec.EditorMetadata.Notes["wait"])
	}
	runtimeHash := rec.DefinitionHash
	beforeRevision := rec.RegistryRevision

	// A metadata-only PUT: same definition, new position. registry_revision
	// must advance; DefinitionHash (runtime conflict hash) must not move.
	metadataOnly := e.body(hashReconcileWorkflow(name, "go"), true)
	metadataOnly["editor_metadata"] = map[string]any{
		"positions": map[string]any{"start": map[string]any{"x": 99, "y": 99}},
	}
	e.put(id, metadataOnly)

	after := e.record(id)
	if after.RegistryRevision <= beforeRevision {
		t.Fatalf("registry_revision = %d, want > %d after a metadata-only PUT (D3)", after.RegistryRevision, beforeRevision)
	}
	if after.DefinitionHash != runtimeHash {
		t.Fatalf("DefinitionHash changed after a metadata-only PUT: %q != %q", after.DefinitionHash, runtimeHash)
	}
	if after.EditorMetadata == nil || after.EditorMetadata.Positions["start"].X != 99 {
		t.Fatalf("stored EditorMetadata = %+v, want the updated position", after.EditorMetadata)
	}
	// The PUT's editor_metadata omitted "notes" entirely; confirm the stored
	// metadata reflects exactly what was sent (replace semantics, not a merge
	// with the previous editor_metadata object).
	if len(after.EditorMetadata.Notes) != 0 {
		t.Fatalf("Notes = %+v, want empty: this PUT's editor_metadata carried no notes", after.EditorMetadata.Notes)
	}
}

// TestRedisEditorMetadataGetReturnsWhatWasStored proves GET /v1/workflows/{id}
// echoes the exact editor_metadata a prior POST stored, read back from Redis
// through the HTTP API rather than the direct registry client.
func TestRedisEditorMetadataGetReturnsWhatWasStored(t *testing.T) {
	e := newHashReconcileEnv(t)
	name := hashReconcileName(t)

	body := e.body(hashReconcileWorkflow(name, "go"), true)
	body["editor_metadata"] = map[string]any{
		"positions": map[string]any{"start": map[string]any{"x": 5, "y": 6}},
	}
	id := e.post(body, http.StatusCreated)

	req, err := http.NewRequest(http.MethodGet, e.ts.URL+"/v1/workflows/"+string(id), nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := e.ts.Client().Do(req)
	if err != nil {
		t.Fatalf("GET /v1/workflows/%s: %v", id, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/workflows/%s = %d, want 200", id, resp.StatusCode)
	}
	var env struct {
		Data struct {
			EditorMetadata struct {
				Positions map[string]struct {
					X float64 `json:"x"`
					Y float64 `json:"y"`
				} `json:"positions"`
			} `json:"editor_metadata"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		t.Fatalf("decode GET response: %v", err)
	}
	if got := env.Data.EditorMetadata.Positions["start"]; got.X != 5 || got.Y != 6 {
		t.Fatalf("GET editor_metadata.positions.start = %+v, want {5,6}", got)
	}
}
