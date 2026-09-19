package apiserver

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/protocol"
)

// TestRunnerDetailCarriesTheDrainProjection pins the composition behind GET
// PathManagementRunnerByID. The registration snapshot deliberately carries only
// the scalar control state, because the full drain projection aggregates the
// fleet-wide handoff and deactivation ledgers; this endpoint is the management
// view, so it has to resolve that projection itself. Without the second read
// the response silently loses `control.drain`, which is the field an operator
// draining a runner actually reads.
func TestRunnerDetailCarriesTheDrainProjection(t *testing.T) {
	ctx := context.Background()
	m := newManagementModule(fakeControlPlaneForAuthz(t))
	dir := m.cp.RunnerDirectory()
	if dir == nil {
		t.Fatal("control plane has no runner directory")
	}
	if _, err := dir.Register(ctx, control.RegisterRunnerRequest{
		RunnerID:     "runner-detail",
		Capacity:     2,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
		Policy:       control.RunnerPolicy{AllowedNodeTypes: []string{"xflow.function"}},
		Now:          time.Unix(10, 0),
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	controlDir, ok := dir.(control.RunnerControlDirectory)
	if !ok {
		t.Fatal("directory does not expose the control surface this endpoint depends on")
	}
	if _, err := controlDir.SetRunnerControl(ctx, control.RunnerControlRequest{
		RunnerID:     "runner-detail",
		DesiredState: control.RunnerDesiredStateDraining,
		Actor:        "operator-a",
		Action:       "drain",
		Reason:       "maintenance",
		RequestID:    "req-detail-drain",
		RequestHash:  "req-detail-drain-hash",
		Now:          time.Unix(11, 0),
	}); err != nil {
		t.Fatalf("SetRunnerControl: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, PathManagementRunnerByID, nil)
	req.SetPathValue("id", "runner-detail")
	rec := httptest.NewRecorder()
	m.handleRunner(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", rec.Code, rec.Body.String())
	}

	var body struct {
		Data struct {
			RunnerID string `json:"runner_id"`
			Control  *struct {
				DesiredState string `json:"desired_state"`
				Drain        *struct {
					Phase string `json:"phase"`
				} `json:"drain"`
			} `json:"control"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v; body = %s", err, rec.Body.String())
	}
	if body.Data.RunnerID != "runner-detail" {
		t.Fatalf("runner_id = %q, want runner-detail; body = %s", body.Data.RunnerID, rec.Body.String())
	}
	if body.Data.Control == nil || body.Data.Control.DesiredState != string(control.RunnerDesiredStateDraining) {
		t.Fatalf("control = %+v, want draining", body.Data.Control)
	}
	if body.Data.Control.Drain == nil {
		t.Fatal("control.drain is absent; the detail endpoint did not resolve the debt projection")
	}
}
