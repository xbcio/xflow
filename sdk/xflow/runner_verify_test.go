package xflow

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xbcio/xflow/service/protocol"
)

// newVerifyControlPlane starts a control plane that answers exactly the two
// requests VerifyRunner sends — register and heartbeat — and nothing else.
// It is not newFlakyControlPlane (runner_reconnect_test.go): that fake carries
// an 80ms heartbeat delay, a poll-failure model, and a RunnerID hardcoded to
// "reconnect-probe", none of which this test needs or wants.
func newVerifyControlPlane(t *testing.T, supplyKey string) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc(protocol.RegisterRunnerPath, func(w http.ResponseWriter, r *http.Request) {
		writeJSONBody(w, protocol.RegisterRunnerResponse{
			RunnerID:  "verify-probe",
			SessionID: "session-1",
			SupplyKey: supplyKey,
		})
	})
	mux.HandleFunc(protocol.HeartbeatPath, func(w http.ResponseWriter, r *http.Request) {
		writeJSONBody(w, protocol.HeartbeatResponse{})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// TestVerifyRunnerReportsWhetherASupplyKeyWasIssued is the negative half of
// the contract: when the control plane issues no key, VerifyRunner must
// report that truthfully rather than defaulting to true.
func TestVerifyRunnerReportsWhetherASupplyKeyWasIssued(t *testing.T) {
	srv := newVerifyControlPlane(t, "")

	cfg := RunnerConfig{
		ServerURL:    srv.URL,
		Transport:    RunnerTransportHTTP,
		RunnerID:     "verify-probe",
		Concurrency:  1,
		Capabilities: []string{"xflow.function"},
	}

	res, err := VerifyRunner(context.Background(), cfg)
	if err != nil {
		t.Fatalf("VerifyRunner: %v", err)
	}
	if res.SupplyKeyIssued {
		t.Fatal("SupplyKeyIssued is true although the server returned no key")
	}
	if res.RunnerID != cfg.RunnerID {
		t.Fatalf("RunnerID = %q, want %q", res.RunnerID, cfg.RunnerID)
	}
}

// TestVerifyRunnerReportsASupplyKeyWasIssued is the positive half. Without
// it, a VerifyRunner that hardcodes SupplyKeyIssued to false would pass the
// test above and go undetected — this repository has shipped that exact gap
// six times.
func TestVerifyRunnerReportsASupplyKeyWasIssued(t *testing.T) {
	srv := newVerifyControlPlane(t, "c3VwcGx5LWtleS1tYXRlcmlhbA==")

	cfg := RunnerConfig{
		ServerURL:    srv.URL,
		Transport:    RunnerTransportHTTP,
		RunnerID:     "verify-probe",
		Concurrency:  1,
		Capabilities: []string{"xflow.function"},
	}

	res, err := VerifyRunner(context.Background(), cfg)
	if err != nil {
		t.Fatalf("VerifyRunner: %v", err)
	}
	if !res.SupplyKeyIssued {
		t.Fatal("SupplyKeyIssued is false although the server returned a non-empty key")
	}
	if res.RunnerID != cfg.RunnerID {
		t.Fatalf("RunnerID = %q, want %q", res.RunnerID, cfg.RunnerID)
	}
}
