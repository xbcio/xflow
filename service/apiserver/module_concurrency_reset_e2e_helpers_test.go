package apiserver

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"

	dto "github.com/prometheus/client_model/go"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/observability/metrics"
	"github.com/xbcio/xflow/types"
)

// resetE2EGraph is a definition that queues one execution behind another, so
// the fixture holds one running slot holder and one queued member — the two
// statuses the reset judgement has to see.
func resetE2EGraph(t *testing.T, name string) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:    name,
		Version: "1",
		Nodes:   []types.NodeDef{{Name: "start", Type: "test.echo"}},
		Settings: &types.WorkflowSettings{Concurrency: &types.ConcurrencySettings{
			Limit: 1,
			Mode:  types.ConcurrencyModeQueue,
		}},
	})
	if err != nil {
		t.Fatalf("Compile(%s) error = %v", name, err)
	}
	return g
}

// resetE2EState is the authoritative state store behind the server under test.
func resetE2EState(t *testing.T, srv *APIServer) engine.StateStore {
	t.Helper()
	return srv.Backend().State()
}

func resetE2EDo(t *testing.T, baseURL, token, path string, body any) *http.Response {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatal(err)
		}
	}
	req, err := http.NewRequest(http.MethodPost, baseURL+path, &buf)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	return resp
}

func resetE2EDecode(t *testing.T, resp *http.Response) concurrencyResetResponse {
	t.Helper()
	var body concurrencyResetResponse
	if resp.StatusCode != http.StatusOK {
		raw := readAll(t, resp)
		t.Fatalf("status = %d, body = %s", resp.StatusCode, raw)
	}
	env := decodeEnvelope(t, resp, &body)
	if !env.Success {
		t.Fatalf("envelope code = %q, want success", env.Code)
	}
	return body
}

func readAll(t *testing.T, resp *http.Response) []byte {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(resp.Body); err != nil {
		t.Fatalf("read body: %v", err)
	}
	return buf.Bytes()
}

func resetE2EStatus(t *testing.T, ctx context.Context, state engine.StateStore, id types.ExecutionID) types.ExecutionStatus {
	t.Helper()
	snap, err := state.GetExecution(ctx, id)
	if err != nil || snap == nil {
		t.Fatalf("GetExecution(%q) = %+v (err %v)", id, snap, err)
	}
	return snap.Status
}

func previewCandidateIDs(body concurrencyResetResponse) map[types.ExecutionID]types.ExecutionStatus {
	out := make(map[types.ExecutionID]types.ExecutionStatus, len(body.Candidates))
	for _, candidate := range body.Candidates {
		out[candidate.ExecutionID] = candidate.Status
	}
	return out
}

func previewCandidateStatus(t *testing.T, body concurrencyResetResponse, id types.ExecutionID) types.ExecutionStatus {
	t.Helper()
	status, ok := previewCandidateIDs(body)[id]
	if !ok {
		t.Fatalf("candidate %q missing from preview %+v", id, body.Candidates)
	}
	return status
}

// orphanReclaimedTotal sums every kind series of
// xflow_orphan_reclaimed_total carrying reason, so the assertion covers the
// full wiring story (handler → engine → store observer → Prometheus) without
// depending on which status a promoted member was judged from.
func orphanReclaimedTotal(t *testing.T, m *metrics.Metrics, reason string) float64 {
	t.Helper()
	families, err := m.Gatherer().Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	var total float64
	for _, family := range families {
		if family.GetName() != "xflow_orphan_reclaimed_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			if metricLabel(metric, "reason") == reason {
				total += metric.GetCounter().GetValue()
			}
		}
	}
	return total
}

func metricLabel(metric *dto.Metric, name string) string {
	for _, pair := range metric.GetLabel() {
		if pair.GetName() == name {
			return pair.GetValue()
		}
	}
	return ""
}
