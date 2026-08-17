package xflow

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/store"
)

// Every HTTP client the runner points at the control plane must trust
// TLSServerCA. The negative half is the regression that matters: a client built
// with http.DefaultTransport is rejected by the same server, and the supply
// fetch failing that way makes SupplyGate.Admit fail forever — so the runner
// declines every activation and never hosts its triggers.
func TestRunnerHTTPClientHonoursPrivateCA(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	caPath := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caPath, encodeCertPEM(t, srv.Certificate()), 0o600); err != nil {
		t.Fatalf("write CA bundle: %v", err)
	}

	client, err := newRunnerHTTPClient(RunnerConfig{ServerURL: srv.URL, TLSServerCA: caPath}, 10*time.Second)
	if err != nil {
		t.Fatalf("newRunnerHTTPClient: %v", err)
	}
	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("client built from TLSServerCA could not reach the TLS server: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}
	if client.Timeout != 10*time.Second {
		t.Errorf("client timeout = %v, want 10s", client.Timeout)
	}

	// Control: a DefaultTransport client cannot verify this server's chain. If
	// this ever starts succeeding, the test server stopped using a private CA
	// and the positive assertion above became vacuous.
	plain := &http.Client{Timeout: 10 * time.Second}
	if _, err := plain.Get(srv.URL); err == nil {
		t.Fatal("a DefaultTransport client reached the private-CA server; the positive assertion above proves nothing")
	}
}

// With no TLS fields the client carries no custom Transport, preserving the
// plaintext path.
func TestRunnerHTTPClientNoTLSFieldsLeavesDefaultTransport(t *testing.T) {
	client, err := newRunnerHTTPClient(RunnerConfig{}, 5*time.Second)
	if err != nil {
		t.Fatalf("newRunnerHTTPClient: %v", err)
	}
	if client.Transport != nil {
		t.Errorf("Transport = %#v, want nil (http.DefaultTransport) when no TLS fields are set", client.Transport)
	}
	if client.Timeout != 5*time.Second {
		t.Errorf("client timeout = %v, want 5s", client.Timeout)
	}
}

// A bad CA path must surface instead of silently returning a plaintext client —
// a swallowed error here downgrades the transport exactly when TLS was asked for.
func TestRunnerHTTPClientPropagatesTLSError(t *testing.T) {
	cfg := RunnerConfig{TLSServerCA: filepath.Join(t.TempDir(), "missing.pem")}
	if _, err := newRunnerHTTPClient(cfg, time.Second); err == nil {
		t.Fatal("newRunnerHTTPClient succeeded with an unreadable TLSServerCA; want an error")
	}
}

func encodeCertPEM(t *testing.T, cert *x509.Certificate) []byte {
	t.Helper()
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw})
}

// The end-to-end probe for the actual defect: the SupplyGate the assembly builds
// must reach a control plane behind a private CA. It fails against a supply
// client constructed as a bare &http.Client{Timeout: 30s}, which ignores
// TLSServerCA — and that failure is not cosmetic. Admit errors on every
// requirement, the runner declines every activation, and its triggers never start.
func TestRunnerServiceConfigSupplyGateHonoursPrivateCA(t *testing.T) {
	const supplyName = "tagger-rules"
	content := []byte(`{"rules":[]}`)

	var fetched atomic.Int64
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/supplies/"+supplyName {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		fetched.Add(1)
		w.Header().Set("ETag", store.ContentHash(content))
		w.Header().Set("X-Supply-Revision", "7")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(content)
	}))
	defer srv.Close()

	caPath := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caPath, encodeCertPEM(t, srv.Certificate()), 0o600); err != nil {
		t.Fatalf("write CA bundle: %v", err)
	}

	// A trigger capability is required for the gate to be built at all (see
	// runnerHostsTriggers); xflow.trigger.kafka is the SAS-shaped one.
	svcCfg, err := buildRunnerServiceConfig(RunnerConfig{
		RunnerID:     "tls-probe",
		ServerURL:    srv.URL,
		TLSServerCA:  caPath,
		Capabilities: []string{"xflow.trigger.kafka"},
	})
	if err != nil {
		t.Fatalf("buildRunnerServiceConfig: %v", err)
	}
	if svcCfg.SupplyGate == nil {
		t.Fatal("SupplyGate is nil; the trigger-hosting branch did not run, so this test proves nothing")
	}

	err = svcCfg.SupplyGate.Admit(context.Background(), "wf-tls-probe", []engine.SupplyRequirement{
		{Node: supplyName, Resource: supplyName, RequireReady: true},
	})
	if err != nil {
		t.Fatalf("SupplyGate.Admit failed against a private-CA control plane: %v\n"+
			"this is the pre-fix behaviour: the supply fetch client ignored TLSServerCA", err)
	}
	if got := fetched.Load(); got != 1 {
		t.Errorf("supply endpoint hit %d times, want 1", got)
	}
}
