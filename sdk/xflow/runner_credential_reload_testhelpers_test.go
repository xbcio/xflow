package xflow

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
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

// newServerCATestServer starts an httptest TLS server whose leaf certificate
// is signed by caCert/caKey (instead of httptest's own self-signed leaf),
// and requires no client certificate. It exists for the server-CA reload
// test: a client's RootCAs pool can only be meaningfully exercised against a
// server whose leaf is actually issued by the CA under test, which
// httptest.NewTLSServer's own self-signed certificate is not.
func newServerCATestServer(t *testing.T, caCert *x509.Certificate, caKey *ecdsa.PrivateKey) *httptest.Server {
	t.Helper()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate server leaf key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		DNSNames:     []string{"127.0.0.1"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create server leaf certificate: %v", err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse server leaf certificate: %v", err)
	}
	srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{{
			Certificate: [][]byte{der},
			PrivateKey:  leafKey,
			Leaf:        leaf,
		}},
	}
	srv.StartTLS()
	return srv
}
