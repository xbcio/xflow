package xflow

import (
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// mustTestCredentialReloader builds a *CredentialReloader from cfg's
// token/TLS fields, failing the test on any construction error. Every call
// site that needs a reloader just to satisfy newRunnerProtocolClient's
// signature, without exercising reload behavior itself, uses this instead of
// repeating the newCredentialReloader + error-check boilerplate.
func mustTestCredentialReloader(t *testing.T, cfg RunnerConfig) *CredentialReloader {
	t.Helper()
	r, err := newCredentialReloader(cfg)
	if err != nil {
		t.Fatalf("newCredentialReloader: %v", err)
	}
	return r
}

// writeTestCAFile writes cert's PEM encoding to a 0600 file in a fresh temp
// dir and returns its path, matching the shape --tls-server-ca and the mTLS
// client-CA tests already expect.
func writeTestCAFile(t *testing.T, cert *x509.Certificate) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(path, encodeCertPEM(t, cert), 0o600); err != nil {
		t.Fatalf("write CA bundle: %v", err)
	}
	return path
}

// clientCNRecorder captures, per request, the CommonName of whichever client
// certificate was presented and the Authorization header value — the two
// facts a credential-reload test needs to tell "the old material was still
// used" from "the new material was presented".
type clientCNRecorder struct {
	mu    sync.Mutex
	calls []clientCNCall
}

type clientCNCall struct {
	cn   string
	auth string
}

func (r *clientCNRecorder) record(cn, auth string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, clientCNCall{cn: cn, auth: auth})
}

func (r *clientCNRecorder) last() (clientCNCall, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.calls) == 0 {
		return clientCNCall{}, false
	}
	return r.calls[len(r.calls)-1], true
}

// newMTLSTestServer starts an httptest TLS server that requires a client
// certificate signed by caCert, and records the subject CommonName of
// whichever client certificate was actually presented on each request,
// alongside the Authorization header it carried. That pair is what proves a
// Reload took effect: a request made with the OLD certificate/token records
// the old CN/token, and one made after Reload records the new ones.
func newMTLSTestServer(t *testing.T, caCert *x509.Certificate) (*httptest.Server, *clientCNRecorder) {
	t.Helper()
	rec := &clientCNRecorder{}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cn := ""
		if r.TLS != nil && len(r.TLS.PeerCertificates) > 0 {
			cn = r.TLS.PeerCertificates[0].Subject.CommonName
		}
		rec.record(cn, r.Header.Get("Authorization"))
		w.WriteHeader(http.StatusNoContent)
	}))
	pool := x509.NewCertPool()
	pool.AddCert(caCert)
	srv.TLS = &tls.Config{
		ClientAuth: tls.RequireAndVerifyClientCert,
		ClientCAs:  pool,
	}
	srv.StartTLS()
	return srv, rec
}
