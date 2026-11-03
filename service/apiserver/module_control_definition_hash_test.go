package apiserver

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/types"
)

// getExecutionDetailData issues GET /v1/executions/{id} and returns the raw
// detail object, so a test can assert both a field's value and its absence.
func getExecutionDetailData(t *testing.T, base, token string, id types.ExecutionID) map[string]any {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, base+"/v1/executions/"+string(id), nil)
	if err != nil {
		t.Fatalf("new inspect request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /v1/executions/%s: %v", id, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("inspect status = %d, want 200 (body=%s)", resp.StatusCode, body)
	}
	var env struct {
		Success bool           `json:"success"`
		Data    map[string]any `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		t.Fatalf("decode inspect envelope: %v", err)
	}
	if !env.Success || env.Data == nil {
		t.Fatalf("inspect envelope = %+v, want success with data", env)
	}
	return env.Data
}

func currentDefinitionHash(t *testing.T, provider *local.Backend, id string) string {
	t.Helper()
	rec, err := provider.WorkflowRegistry().GetWorkflow(context.Background(), types.WorkflowID(id))
	if err != nil {
		t.Fatalf("GetWorkflow(%q): %v", id, err)
	}
	return rec.DefinitionHash
}

func archivedDefinitionHash(t *testing.T, provider *local.Backend, id, version string) string {
	t.Helper()
	versions, ok := provider.WorkflowRegistry().(backend.WorkflowVersionCapability)
	if !ok {
		t.Fatal("local workflow registry does not serve version history")
	}
	rec, err := versions.GetWorkflowVersion(context.Background(), types.WorkflowID(id), version)
	if err != nil {
		t.Fatalf("GetWorkflowVersion(%q, %q): %v", id, version, err)
	}
	return rec.DefinitionHash
}

// TestExecuteByIDRecordsTheRegistryDefinitionHash pins that both execute
// resolution branches record the hash of the registry record whose graph they
// submitted: the live record, and the archived payload a pinned version
// substituted. The value is the record's, copied verbatim — not a hash of the
// submitted graph.
func TestExecuteByIDRecordsTheRegistryDefinitionHash(t *testing.T) {
	srv, provider := newVersionExecuteTestServer(t)
	id := registerListWorkflow(t, srv.URL, "tok-a", "dh-by-id", "v1")
	publishRun(t, srv.URL, "tok-a", id, "dh-by-id", "v2")

	liveHash := currentDefinitionHash(t, provider, id)
	archivedHash := archivedDefinitionHash(t, provider, id, "v1")
	if liveHash == "" || archivedHash == "" || liveHash == archivedHash {
		t.Fatalf("fixture hashes live=%q archived=%q, want two distinct non-empty hashes", liveHash, archivedHash)
	}

	cases := []struct {
		name    string
		version string
		want    string
	}{
		{name: "omitted runs the live record", version: "", want: liveHash},
		{name: "explicit current version", version: "v2", want: liveHash},
		{name: "retired version runs its archived record", version: "v1", want: archivedHash},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := readExecuteByIDOutcome(t, provider, postExecuteByID(t, srv.URL, "tok-a", id, executeRegisteredRequest{Version: tc.version}))
			data := getExecutionDetailData(t, srv.URL, "tok-exec-only", out.executionID)
			got, _ := data["definition_hash"].(string)
			if got != tc.want {
				t.Fatalf("execution detail definition_hash = %q, want the registry hash %q", got, tc.want)
			}
		})
	}
}

// TestExecuteInlineHasNoDefinitionHash pins the attribution rule: an inline
// definition has no server id and no registry record, so it has no definition
// identity to report and the field must be absent rather than guessed.
func TestExecuteInlineHasNoDefinitionHash(t *testing.T) {
	srv, _ := newVersionExecuteTestServer(t)

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(executeWorkflowRequest{Workflow: listTestWorkflow("dh-inline", "v1")}); err != nil {
		t.Fatalf("encode request: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/workflows/execute", &buf)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer tok-a")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /v1/workflows/execute: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("inline execute status = %d, want 200 (body=%s)", resp.StatusCode, body)
	}
	var out executeWorkflowResponse
	if env := decodeEnvelope(t, resp, &out); !env.Success {
		t.Fatalf("inline execute envelope = %+v, want success", env)
	}

	data := getExecutionDetailData(t, srv.URL, "tok-exec-only", out.ExecutionID)
	if hash, ok := data["definition_hash"]; ok {
		t.Fatalf("inline execution detail carries definition_hash = %v; an unattributed run has no definition identity", hash)
	}
}
