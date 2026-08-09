package main

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

// TestRunnerHTTPClientHonoursPrivateCA proves newRunnerHTTPClient produces a
// client that trusts --tls-server-ca, and that a DefaultTransport client — what
// the artifact and seed/supply clients used before they were routed through this
// helper — is rejected by the same server.
//
// The negative half is the regression that matters: the supply fetch client
// ignoring the CA makes SupplyGate.Admit fail forever, so the runner declines
// every activation and never hosts its triggers.
func TestRunnerHTTPClientHonoursPrivateCA(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	caPath := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caPath, encodeCertPEM(t, srv.Certificate()), 0o600); err != nil {
		t.Fatalf("write CA bundle: %v", err)
	}

	cfg := runnerConfig{serverURL: srv.URL, tlsServerCA: caPath}
	client, err := newRunnerHTTPClient(cfg, 10*time.Second)
	if err != nil {
		t.Fatalf("newRunnerHTTPClient: %v", err)
	}
	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("client built from --tls-server-ca could not reach the TLS server: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}
	if client.Timeout != 10*time.Second {
		t.Errorf("client timeout = %v, want 10s", client.Timeout)
	}

	// Control: the pre-fix construction (&http.Client{Timeout: ...}, no
	// Transport) cannot verify this server's chain. If this ever starts
	// succeeding, the test server stopped using a private CA and the positive
	// assertion above became vacuous.
	plain := &http.Client{Timeout: 10 * time.Second}
	if _, err := plain.Get(srv.URL); err == nil {
		t.Fatal("a DefaultTransport client reached the private-CA server; the positive assertion above proves nothing")
	}
}

// TestRunnerHTTPClientNoTLSFlagsLeavesDefaultTransport pins the dev default:
// with no --tls-* flags the client carries no custom Transport, preserving the
// plaintext path.
func TestRunnerHTTPClientNoTLSFlagsLeavesDefaultTransport(t *testing.T) {
	client, err := newRunnerHTTPClient(runnerConfig{}, 5*time.Second)
	if err != nil {
		t.Fatalf("newRunnerHTTPClient: %v", err)
	}
	if client.Transport != nil {
		t.Errorf("Transport = %#v, want nil (http.DefaultTransport) when no TLS flags are set", client.Transport)
	}
	if client.Timeout != 5*time.Second {
		t.Errorf("client timeout = %v, want 5s", client.Timeout)
	}
}

// TestRunnerHTTPClientPropagatesTLSError checks the helper surfaces a bad CA
// path instead of silently returning a plaintext client — a swallowed error here
// would downgrade the transport exactly when the operator asked for TLS.
func TestRunnerHTTPClientPropagatesTLSError(t *testing.T) {
	cfg := runnerConfig{tlsServerCA: filepath.Join(t.TempDir(), "missing.pem")}
	if _, err := newRunnerHTTPClient(cfg, time.Second); err == nil {
		t.Fatal("newRunnerHTTPClient succeeded with an unreadable --tls-server-ca; want an error")
	}
}

func encodeCertPEM(t *testing.T, cert *x509.Certificate) []byte {
	t.Helper()
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw})
}

// TestRunnerServiceConfigSupplyGateHonoursPrivateCA is the end-to-end probe for
// the actual defect: the SupplyGate that runnerServiceConfig builds must be able
// to fetch supply content from a control plane behind a private CA.
//
// It fails against the pre-fix code, where the seed/supply client was
// constructed as &http.Client{Timeout: 30s} with no Transport and therefore
// ignored --tls-server-ca. That failure mode is not cosmetic: Admit returns an
// error for every requirement, the runner declines every activation, and its
// triggers never start.
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

	// A trigger capability is required for runnerServiceConfig to build the gate
	// at all (see hostsTriggers); xflow.trigger.kafka is the SAS-shaped one.
	svcCfg, err := runnerServiceConfig(runnerConfig{
		runnerID:          "tls-probe",
		serverURL:         srv.URL,
		tlsServerCA:       caPath,
		heartbeatInterval: "5s",
		pollWait:          "1s",
		capabilities:      parseCapabilities("xflow.trigger.kafka"),
	}, nil)
	if err != nil {
		t.Fatalf("runnerServiceConfig: %v", err)
	}
	if svcCfg.SupplyGate == nil {
		t.Fatal("SupplyGate is nil; the trigger-hosting branch did not run, so this test proves nothing")
	}

	// Use a fresh registry-free requirement: Admit fetches, then applies into
	// supply.Default. require_ready is what makes a failed fetch an error.
	err = svcCfg.SupplyGate.Admit(context.Background(), "wf-tls-probe", []engine.SupplyRequirement{
		{Node: supplyName, Resource: supplyName, RequireReady: true},
	})
	if err != nil {
		t.Fatalf("SupplyGate.Admit failed against a private-CA control plane: %v\n"+
			"this is the pre-fix behaviour: the supply fetch client ignored --tls-server-ca", err)
	}
	if got := fetched.Load(); got != 1 {
		t.Errorf("supply endpoint hit %d times, want 1", got)
	}
}
