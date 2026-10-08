package xflow

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/protocol"
	runnersvc "github.com/xbcio/xflow/service/runner"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// The certificate-generation helpers below are deliberately independent of
// the ones in runner_tls_test.go and service/apiserver/tls_reload_test.go:
// each suite generates its own shapes, and this one specifically needs a
// client-auth leaf plus the matching CA to run two credential generations
// against the same CA (so a reload can be proven to swap the leaf while the
// server-side CA pool stays fixed) and, separately, two independent CAs (so
// a server-CA reload can be proven to swap the pool itself).

func generateCredReloadTestCA(t *testing.T, cn string) (*x509.Certificate, []byte, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA certificate: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA certificate: %v", err)
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	return cert, pemBytes, key
}

// writeCredReloadClientLeaf issues a client-auth leaf signed by ca/caKey and
// writes its cert+key as PEM files at certPath/keyPath.
func writeCredReloadClientLeaf(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, cn, certPath, keyPath string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate leaf key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create client certificate: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal leaf key: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
		t.Fatalf("write client cert file: %v", err)
	}
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatalf("write client key file: %v", err)
	}
}

// --- CredentialReloader unit tests ---

// TestCredentialReloaderReloadSwapsTokenAndCertificate proves the core swap
// contract: after Reload, both Token() and GetClientCertificate() report the
// NEW material, not the old.
func TestCredentialReloaderReloadSwapsTokenAndCertificate(t *testing.T) {
	dir := t.TempDir()
	ca, caPEM, caKey := generateCredReloadTestCA(t, "cred-reload CA")
	caPath := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(caPath, caPEM, 0o600); err != nil {
		t.Fatalf("write CA file: %v", err)
	}
	certPath := filepath.Join(dir, "client-a.crt")
	keyPath := filepath.Join(dir, "client-a.key")
	writeCredReloadClientLeaf(t, ca, caKey, "client-a", certPath, keyPath)

	r, err := newCredentialReloader(RunnerConfig{
		Token:         "token-a",
		TLSServerCA:   caPath,
		TLSClientCert: certPath,
		TLSClientKey:  keyPath,
	})
	if err != nil {
		t.Fatalf("newCredentialReloader: %v", err)
	}
	if got := r.Token(); got != "token-a" {
		t.Fatalf("Token() = %q, want token-a", got)
	}
	cert, err := r.GetClientCertificate(nil)
	if err != nil {
		t.Fatalf("GetClientCertificate: %v", err)
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatalf("parse leaf: %v", err)
	}
	if leaf.Subject.CommonName != "client-a" {
		t.Fatalf("initial leaf CN = %q, want client-a", leaf.Subject.CommonName)
	}

	certPathB := filepath.Join(dir, "client-b.crt")
	keyPathB := filepath.Join(dir, "client-b.key")
	writeCredReloadClientLeaf(t, ca, caKey, "client-b", certPathB, keyPathB)

	if err := r.Reload(CredentialReloaderSource{
		Token:         "token-b",
		TLSServerCA:   caPath,
		TLSClientCert: certPathB,
		TLSClientKey:  keyPathB,
	}); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	if got := r.Token(); got != "token-b" {
		t.Fatalf("Token() after Reload = %q, want token-b", got)
	}
	cert, err = r.GetClientCertificate(nil)
	if err != nil {
		t.Fatalf("GetClientCertificate after Reload: %v", err)
	}
	leaf, err = x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatalf("parse leaf after Reload: %v", err)
	}
	if leaf.Subject.CommonName != "client-b" {
		t.Fatalf("leaf CN after Reload = %q, want client-b", leaf.Subject.CommonName)
	}
}

// TestCredentialReloaderReloadFailsClosedOnBadCertificate proves the
// fail-closed contract for TLS material: a Reload whose new certificate
// cannot be loaded returns an error AND leaves the previous material fully
// intact (both the token and the certificate), matching
// apiserver.TLSReloader's "callers must not partially apply a reload" rule.
func TestCredentialReloaderReloadFailsClosedOnBadCertificate(t *testing.T) {
	dir := t.TempDir()
	ca, caPEM, caKey := generateCredReloadTestCA(t, "cred-reload CA")
	caPath := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(caPath, caPEM, 0o600); err != nil {
		t.Fatalf("write CA file: %v", err)
	}
	certPath := filepath.Join(dir, "client-a.crt")
	keyPath := filepath.Join(dir, "client-a.key")
	writeCredReloadClientLeaf(t, ca, caKey, "client-a", certPath, keyPath)

	r, err := newCredentialReloader(RunnerConfig{
		Token:         "token-a",
		TLSServerCA:   caPath,
		TLSClientCert: certPath,
		TLSClientKey:  keyPath,
	})
	if err != nil {
		t.Fatalf("newCredentialReloader: %v", err)
	}

	// Corrupt only the key file, so LoadX509KeyPair fails cleanly.
	if err := os.WriteFile(keyPath, []byte("not a valid key"), 0o600); err != nil {
		t.Fatalf("corrupt key file: %v", err)
	}

	err = r.Reload(CredentialReloaderSource{
		Token:         "token-b",
		TLSServerCA:   caPath,
		TLSClientCert: certPath,
		TLSClientKey:  keyPath,
	})
	if err == nil {
		t.Fatal("Reload with a corrupt key file: want error, got nil")
	}

	// The token must NOT have been swapped either: Reload is one atomic
	// operation over the whole snapshot, not two independent ones.
	if got := r.Token(); got != "token-a" {
		t.Fatalf("Token() after a failed Reload = %q, want token-a (unchanged)", got)
	}
	cert, certErr := r.GetClientCertificate(nil)
	if certErr != nil {
		t.Fatalf("GetClientCertificate after failed Reload: %v", certErr)
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatalf("parse leaf after failed Reload: %v", err)
	}
	if leaf.Subject.CommonName != "client-a" {
		t.Fatalf("leaf CN after failed Reload = %q, want client-a (unchanged)", leaf.Subject.CommonName)
	}
}

// TestCredentialReloaderReloadFailsClosedOnUnreadableToken proves the same
// fail-closed contract from the token-adjacent side: an unreadable CA file
// (the token has no file source of its own in RunnerConfig today — see
// CredentialReloaderSource's doc — so this exercises the other required-read
// path, the server CA, standing in for "a credential source that stopped
// being readable") must not touch the previously-stored token either.
func TestCredentialReloaderReloadFailsClosedOnUnreadableToken(t *testing.T) {
	dir := t.TempDir()
	r, err := newCredentialReloader(RunnerConfig{Token: "token-a"})
	if err != nil {
		t.Fatalf("newCredentialReloader: %v", err)
	}

	missingCA := filepath.Join(dir, "does-not-exist.pem")
	err = r.Reload(CredentialReloaderSource{Token: "token-b", TLSServerCA: missingCA})
	if err == nil {
		t.Fatal("Reload with a missing CA file: want error, got nil")
	}
	if !errors.Is(err, os.ErrNotExist) && !strings.Contains(err.Error(), "no such file") {
		// Not a hard requirement on wrapping shape, just confirms this is
		// genuinely the missing-file error and not something else entirely.
		t.Logf("Reload error (informational): %v", err)
	}
	if got := r.Token(); got != "token-a" {
		t.Fatalf("Token() after a failed Reload = %q, want token-a (unchanged)", got)
	}
}

// TestCredentialReloaderReloadRejectsMismatchedCertKeyPair pins the
// "TLSClientCert and TLSClientKey must be provided together" validation on
// the Reload path, not just at construction.
func TestCredentialReloaderReloadRejectsMismatchedCertKeyPair(t *testing.T) {
	r, err := newCredentialReloader(RunnerConfig{Token: "token-a"})
	if err != nil {
		t.Fatalf("newCredentialReloader: %v", err)
	}
	err = r.Reload(CredentialReloaderSource{TLSClientCert: "/some/path.crt"})
	if err == nil {
		t.Fatal("Reload with TLSClientCert but no TLSClientKey: want error, got nil")
	}
	if got := r.Token(); got != "token-a" {
		t.Fatalf("Token() after a rejected Reload = %q, want token-a (unchanged)", got)
	}
}

// TestCredentialReloaderErrorsNeverContainTheSecretValues is the no-secrets-
// in-logs requirement, checked the only way it can be from outside the
// package: every error Reload or newCredentialReloader can return is
// inspected for the literal token and private key material, across every
// failure case this file exercises. A future error path that interpolates a
// secret into its message would be caught here before it ever reaches a log
// line, since Runner.Reload's callers log exactly these error values.
func TestCredentialReloaderErrorsNeverContainTheSecretValues(t *testing.T) {
	const secretToken = "super-secret-token-value"
	dir := t.TempDir()

	// A key file whose content must never appear in an error even though the
	// load fails on it.
	badKeyPath := filepath.Join(dir, "bad.key")
	secretLookingKeyContents := "-----BEGIN PRIVATE KEY-----\nnot-actually-valid-but-secret-shaped\n-----END PRIVATE KEY-----\n"
	if err := os.WriteFile(badKeyPath, []byte(secretLookingKeyContents), 0o600); err != nil {
		t.Fatalf("write bad key file: %v", err)
	}
	certPath := filepath.Join(dir, "any.crt")
	_, caPEM, _ := generateCredReloadTestCA(t, "secret-check CA")
	if err := os.WriteFile(certPath, caPEM, 0o600); err != nil {
		t.Fatalf("write cert file: %v", err)
	}

	cases := []CredentialReloaderSource{
		{Token: secretToken, TLSClientCert: certPath, TLSClientKey: badKeyPath},
		{Token: secretToken, TLSClientCert: certPath},
		{Token: secretToken, TLSServerCA: filepath.Join(dir, "missing-ca.pem")},
	}
	for i, src := range cases {
		_, err := loadRunnerCredentialMaterial(src)
		if err == nil {
			t.Fatalf("case %d: want error, got nil", i)
		}
		msg := err.Error()
		if strings.Contains(msg, secretToken) {
			t.Fatalf("case %d: error %q contains the bearer token", i, msg)
		}
		if strings.Contains(msg, secretLookingKeyContents) || strings.Contains(msg, "not-actually-valid-but-secret-shaped") {
			t.Fatalf("case %d: error %q contains key file contents", i, msg)
		}
	}
}

// TestCredentialReloaderConcurrentReloadAndReadIsRaceFree exercises the
// atomic-swap contract under concurrent Reload calls racing concurrent
// Token()/GetClientCertificate() reads — the shape Runner.Reload (writer)
// and every in-flight HTTP/gRPC request (reader) actually produce. Run with
// -race; a data race here would be the regression this test exists to catch,
// not a flaky assertion on the values observed (readers may observe either
// generation, which is correct — only -race failing would mean something is
// wrong).
func TestCredentialReloaderConcurrentReloadAndReadIsRaceFree(t *testing.T) {
	dir := t.TempDir()
	ca, _, caKey := generateCredReloadTestCA(t, "concurrent CA")
	var paths [2]struct{ cert, key string }
	for i := range paths {
		paths[i].cert = filepath.Join(dir, fmt.Sprintf("client-%d.crt", i))
		paths[i].key = filepath.Join(dir, fmt.Sprintf("client-%d.key", i))
		writeCredReloadClientLeaf(t, ca, caKey, fmt.Sprintf("client-%d", i), paths[i].cert, paths[i].key)
	}

	r, err := newCredentialReloader(RunnerConfig{
		Token:         "token-0",
		TLSClientCert: paths[0].cert,
		TLSClientKey:  paths[0].key,
	})
	if err != nil {
		t.Fatalf("newCredentialReloader: %v", err)
	}

	const iterations = 200
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			idx := i % 2
			_ = r.Reload(CredentialReloaderSource{
				Token:         fmt.Sprintf("token-%d", i),
				TLSClientCert: paths[idx].cert,
				TLSClientKey:  paths[idx].key,
			})
		}
	}()

	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				_ = r.Token()
				if _, err := r.GetClientCertificate(nil); err != nil {
					t.Errorf("GetClientCertificate: %v", err)
				}
				_ = r.rootCAs()
				_ = r.tlsConfigured()
			}
		}()
	}
	wg.Wait()
}

// --- HTTP-level proof: a Reload actually changes what is presented on the wire ---

// TestReloadableHTTPTransportPresentsReloadedClientCertificate is the
// httptest-TLS-server-requiring-client-certs proof the task calls for: it
// drives real HTTP requests through newReloadableRunnerHTTPClient against a
// server that demands mTLS, and shows the server observes the OLD client
// certificate's CommonName before Reload and the NEW one strictly after it —
// on the SAME *http.Client, with no client rebuild in between.
func TestReloadableHTTPTransportPresentsReloadedClientCertificate(t *testing.T) {
	dir := t.TempDir()
	ca, caPEM, caKey := generateCredReloadTestCA(t, "mtls-reload CA")
	caPath := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(caPath, caPEM, 0o600); err != nil {
		t.Fatalf("write CA file: %v", err)
	}

	certA := filepath.Join(dir, "client-a.crt")
	keyA := filepath.Join(dir, "client-a.key")
	writeCredReloadClientLeaf(t, ca, caKey, "client-a", certA, keyA)
	certB := filepath.Join(dir, "client-b.crt")
	keyB := filepath.Join(dir, "client-b.key")
	writeCredReloadClientLeaf(t, ca, caKey, "client-b", certB, keyB)

	srv, rec := newMTLSTestServer(t, ca)
	defer srv.Close()

	reloader, err := newCredentialReloader(RunnerConfig{
		Token:         "token-a",
		TLSClientCert: certA,
		TLSClientKey:  keyA,
	})
	if err != nil {
		t.Fatalf("newCredentialReloader: %v", err)
	}
	// InsecureSkipVerify on the base config: this test is about the CLIENT
	// certificate reload (GetClientCertificate), not server-identity
	// verification, which the sibling root-CA reload test below covers. The
	// alternative — trusting srv.Certificate() via a RootCAs pool set after
	// construction — does not work here: newReloadableHTTPTransport's
	// DialTLSContext clones the *base* config captured at construction time
	// on every dial and then overwrites RootCAs from the reloader (nil,
	// since no TLSServerCA was configured above), so a pool assigned to
	// transport.TLSClientConfig afterward is never consulted.
	transport := newReloadableHTTPTransport(reloader, &tls.Config{ //nolint:gosec
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: true,
	})
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}

	doRequest := func(token string) {
		req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		_ = resp.Body.Close()
	}

	doRequest("token-a")
	call, ok := rec.last()
	if !ok {
		t.Fatal("server recorded no request")
	}
	if call.cn != "client-a" {
		t.Fatalf("initial request presented CN = %q, want client-a", call.cn)
	}
	if call.auth != "Bearer token-a" {
		t.Fatalf("initial request Authorization = %q, want %q", call.auth, "Bearer token-a")
	}

	// Reload to the new certificate, then close idle connections — exactly
	// what Runner.Reload does — so the next request cannot reuse the
	// keep-alive connection established under client-a's certificate.
	if err := reloader.Reload(CredentialReloaderSource{
		Token:         "token-b",
		TLSClientCert: certB,
		TLSClientKey:  keyB,
	}); err != nil {
		t.Fatalf("Reload: %v", err)
	}
	client.CloseIdleConnections()

	doRequest("token-b")
	call, ok = rec.last()
	if !ok {
		t.Fatal("server recorded no request after reload")
	}
	if call.cn != "client-b" {
		t.Fatalf("request after Reload presented CN = %q, want client-b (old certificate was reused)", call.cn)
	}
	if call.auth != "Bearer token-b" {
		t.Fatalf("request after Reload Authorization = %q, want %q (old token was still sent)", call.auth, "Bearer token-b")
	}
}

// TestReloadableHTTPTransportPresentsReloadedRootCAs proves the server-CA
// half of the same contract: a Reload that swaps the trusted CA pool takes
// effect on the client's next dial, with no client rebuild. The test server's
// leaf is signed by caNew; a client reloaded from caOld (which does not
// verify that leaf) to caNew must go from failing to succeeding on the exact
// same *http.Client, proving DialTLSContext re-reads reloader.rootCAs() on
// every dial rather than caching the pool captured at transport-construction
// time.
func TestReloadableHTTPTransportPresentsReloadedRootCAs(t *testing.T) {
	dir := t.TempDir()
	caOld, caOldPEM, _ := generateCredReloadTestCA(t, "root-reload CA old")
	caNew, caNewPEM, caNewKey := generateCredReloadTestCA(t, "root-reload CA new")

	caOldPath := filepath.Join(dir, "ca-old.pem")
	if err := os.WriteFile(caOldPath, caOldPEM, 0o600); err != nil {
		t.Fatalf("write old CA file: %v", err)
	}
	caNewPath := filepath.Join(dir, "ca-new.pem")
	if err := os.WriteFile(caNewPath, caNewPEM, 0o600); err != nil {
		t.Fatalf("write new CA file: %v", err)
	}

	srv := newServerCATestServer(t, caNew, caNewKey)
	defer srv.Close()
	_ = caOld

	reloader, err := newCredentialReloader(RunnerConfig{TLSServerCA: caOldPath})
	if err != nil {
		t.Fatalf("newCredentialReloader: %v", err)
	}
	transport := newReloadableHTTPTransport(reloader, &tls.Config{MinVersion: tls.VersionTLS12})
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}

	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if _, err := client.Do(req); err == nil {
		t.Fatal("request trusting only caOld unexpectedly succeeded against a caNew-signed leaf")
	}

	if err := reloader.Reload(CredentialReloaderSource{TLSServerCA: caNewPath}); err != nil {
		t.Fatalf("Reload: %v", err)
	}
	client.CloseIdleConnections()

	req2, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatalf("new request after reload: %v", err)
	}
	resp2, err := client.Do(req2)
	if err != nil {
		t.Fatalf("request after reloading TLSServerCA to caNew: %v", err)
	}
	_ = resp2.Body.Close()
}

// The reloadable transport replaces http.DefaultTransport for the Runner
// Protocol client, so it must keep its dial, handshake and idle limits, and it
// must not route through an environment proxy, whose HTTPS tunnel would verify
// against TLSClientConfig instead of the live CA pool.
func TestReloadableHTTPTransportKeepsDefaultLimitsWithoutProxy(t *testing.T) {
	reloader, err := newCredentialReloader(RunnerConfig{})
	if err != nil {
		t.Fatalf("newCredentialReloader: %v", err)
	}
	transport := newReloadableHTTPTransport(reloader, &tls.Config{MinVersion: tls.VersionTLS12})
	def := http.DefaultTransport.(*http.Transport)
	if transport.Proxy != nil {
		t.Fatal("Proxy is set, want a direct dial to the control plane")
	}
	if transport.TLSHandshakeTimeout != def.TLSHandshakeTimeout || transport.TLSHandshakeTimeout == 0 {
		t.Fatalf("TLSHandshakeTimeout = %v, want the default %v", transport.TLSHandshakeTimeout, def.TLSHandshakeTimeout)
	}
	if transport.IdleConnTimeout != def.IdleConnTimeout || transport.MaxIdleConns != def.MaxIdleConns {
		t.Fatalf("idle limits = %v/%d, want %v/%d", transport.IdleConnTimeout, transport.MaxIdleConns, def.IdleConnTimeout, def.MaxIdleConns)
	}
	if transport.DialTLSContext == nil || transport.TLSClientConfig.GetClientCertificate == nil {
		t.Fatal("live TLS hooks are missing")
	}
	if transport.TLSClientConfig.MinVersion != tls.VersionTLS12 {
		t.Fatalf("MinVersion = %x, want TLS 1.2", transport.TLSClientConfig.MinVersion)
	}
}

// --- Every runner client, not just the protocol client, follows Reload ---

// issueCredReloadServerLeaf issues a server-auth leaf for 127.0.0.1 signed by
// ca/caKey, ready to serve from a tls.Config.
func issueCredReloadServerLeaf(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate server leaf key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create server leaf: %v", err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse server leaf: %v", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}
}

// originCall is one request the control-plane stand-in observed.
type originCall struct {
	path string
	cn   string
	auth string
}

// newCredReloadOriginServer stands in for the control plane's HTTP API: its
// leaf is signed by serverCA, it requires a client certificate signed by
// clientCA, and it answers the three runner-side endpoints — supply fetch,
// entry seed, artifact fetch — recording the client CN and Authorization
// header of each request.
func newCredReloadOriginServer(t *testing.T, serverLeaf tls.Certificate, clientCA *x509.Certificate, artifact []byte) (*httptest.Server, func() []originCall) {
	t.Helper()
	var mu sync.Mutex
	var calls []originCall
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cn := ""
		if r.TLS != nil && len(r.TLS.PeerCertificates) > 0 {
			cn = r.TLS.PeerCertificates[0].Subject.CommonName
		}
		mu.Lock()
		calls = append(calls, originCall{path: r.URL.Path, cn: cn, auth: r.Header.Get("Authorization")})
		mu.Unlock()
		switch {
		case strings.HasPrefix(r.URL.Path, "/v1/supplies/"):
			content := []byte(`{"v":1}`)
			w.Header().Set("ETag", store.ContentHash(content))
			w.Header().Set("X-Supply-Revision", "1")
			_, _ = w.Write(content)
		case r.URL.Path == "/v1/executions":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"state":"accepted","execution_id":"exec-1"}`))
		case r.URL.Path == protocol.HeartbeatPath:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{}`))
		case r.URL.Path == "/v1/artifacts/"+store.ContentHash(artifact):
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write(artifact)
		default:
			http.NotFound(w, r)
		}
	}))
	pool := x509.NewCertPool()
	pool.AddCert(clientCA)
	srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{serverLeaf},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pool,
		MinVersion:   tls.VersionTLS12,
	}
	srv.StartTLS()
	return srv, func() []originCall {
		mu.Lock()
		defer mu.Unlock()
		return append([]originCall(nil), calls...)
	}
}

func writeCredReloadPEM(t *testing.T, path string, pemBytes []byte) string {
	t.Helper()
	if err := os.WriteFile(path, pemBytes, 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

// TestRunnerOriginClientsFollowReload proves the artifact-fetch client and the
// entry-seed/supply-fetch client — not only the Runner Protocol client — pick
// up a Reload's token, client certificate and server CA on their next request,
// with no rebuild. Before the fix both were built from RunnerConfig once and
// kept the original token and TLS material for the life of the process.
//
// The clients are taken from the assembly buildRunnerServiceConfig produced,
// given the reloader the way NewRunner gives it. The entry-seed runtime is
// built per activation inside runnersvc, so it is reconstructed here from the
// fetcher's Client and Token — the same instance and value the activation
// handler is handed (wireRunnerTriggerHosting passes seedClient and seedToken
// to both).
func TestRunnerOriginClientsFollowReload(t *testing.T) {
	dir := t.TempDir()
	serverCA, serverCAPEM, serverCAKey := generateCredReloadTestCA(t, "origin server CA")
	_, otherCAPEM, _ := generateCredReloadTestCA(t, "unrelated CA")
	clientCA, _, clientCAKey := generateCredReloadTestCA(t, "origin client CA")
	serverCAPath := writeCredReloadPEM(t, filepath.Join(dir, "server-ca.pem"), serverCAPEM)
	otherCAPath := writeCredReloadPEM(t, filepath.Join(dir, "other-ca.pem"), otherCAPEM)
	certA, keyA := filepath.Join(dir, "a.crt"), filepath.Join(dir, "a.key")
	writeCredReloadClientLeaf(t, clientCA, clientCAKey, "client-a", certA, keyA)
	certB, keyB := filepath.Join(dir, "b.crt"), filepath.Join(dir, "b.key")
	writeCredReloadClientLeaf(t, clientCA, clientCAKey, "client-b", certB, keyB)

	artifact := []byte("credential reload artifact probe")
	srv, calls := newCredReloadOriginServer(t, issueCredReloadServerLeaf(t, serverCA, serverCAKey), clientCA, artifact)
	defer srv.Close()

	cfg := RunnerConfig{
		ServerURL:        srv.URL,
		RunnerID:         "reload-probe",
		Token:            "token-a",
		TLSServerCA:      serverCAPath,
		TLSClientCert:    certA,
		TLSClientKey:     keyA,
		Capabilities:     []string{"xflow.trigger.kafka"},
		ArtifactCacheDir: t.TempDir(),
	}
	reloader := mustTestCredentialReloader(t, cfg)
	svcCfg, err := buildRunnerServiceConfig(cfg, func(o *runnerOptions) { o.credReloader = reloader })
	if err != nil {
		t.Fatalf("buildRunnerServiceConfig: %v", err)
	}
	if svcCfg.SupplyGate == nil {
		t.Fatal("SupplyGate is nil; the trigger-hosting branch did not run")
	}
	fetcher, ok := svcCfg.SupplyGate.Fetcher().(*runnersvc.HTTPSupplyFetcher)
	if !ok {
		t.Fatalf("supply fetcher = %T, want *runnersvc.HTTPSupplyFetcher", svcCfg.SupplyGate.Fetcher())
	}
	if fetcher.Token != "" {
		t.Fatal("supply fetcher carries a copied static token; it would keep sending it after Reload")
	}
	seed := &protocol.HTTPEntrySeedRuntime{BaseURL: srv.URL, Client: fetcher.Client, Token: fetcher.Token, RunnerID: cfg.RunnerID}

	// exercise drives one request through each client and returns the
	// requests the server saw for them, or the first client error.
	exercise := func() ([]originCall, error) {
		before := len(calls())
		if _, _, _, err := fetcher.Fetch(context.Background(), "probe-supply"); err != nil {
			return nil, fmt.Errorf("supply fetch: %w", err)
		}
		if _, err := seed.SeedExecutionFromEntry(context.Background(), types.EntrySeedRequest{
			AdmissionKey: "k", WorkflowID: "wf", EntryUnitID: "entry", Outcome: "success",
		}); err != nil {
			return nil, fmt.Errorf("entry seed: %w", err)
		}
		// A fresh cache directory per call is not possible here, so each
		// round fetches under its own namespace: the namespace-partitioned
		// cache misses and the request reaches the origin every time.
		ctx := namespace.WithNamespace(context.Background(), namespace.Namespace(fmt.Sprintf("round-%d", before)))
		if _, err := svcCfg.ArtifactCodeResolver(ctx, store.ContentHash(artifact)); err != nil {
			return nil, fmt.Errorf("artifact fetch: %w", err)
		}
		return calls()[before:], nil
	}
	assertAll := func(round string, got []originCall, wantCN, wantToken string) {
		t.Helper()
		if len(got) != 3 {
			t.Fatalf("%s: server saw %d requests, want 3 (supply, seed, artifact): %+v", round, len(got), got)
		}
		for _, c := range got {
			if c.cn != wantCN {
				t.Errorf("%s: %s presented CN %q, want %q", round, c.path, c.cn, wantCN)
			}
			if c.auth != "Bearer "+wantToken {
				t.Errorf("%s: %s sent Authorization %q, want %q", round, c.path, c.auth, "Bearer "+wantToken)
			}
		}
	}

	got, err := exercise()
	if err != nil {
		t.Fatalf("before Reload: %v", err)
	}
	assertAll("before Reload", got, "client-a", "token-a")

	// Rotate the token and the client certificate.
	if err := reloader.Reload(CredentialReloaderSource{
		Token: "token-b", TLSServerCA: serverCAPath, TLSClientCert: certB, TLSClientKey: keyB,
	}); err != nil {
		t.Fatalf("Reload: %v", err)
	}
	reloader.closeIdleConnections()
	got, err = exercise()
	if err != nil {
		t.Fatalf("after Reload: %v", err)
	}
	assertAll("after Reload", got, "client-b", "token-b")

	// Rotate the trusted CA to one that did not sign the server's leaf: every
	// client must now refuse the server, proving the pool is read per dial.
	if err := reloader.Reload(CredentialReloaderSource{
		Token: "token-b", TLSServerCA: otherCAPath, TLSClientCert: certB, TLSClientKey: keyB,
	}); err != nil {
		t.Fatalf("Reload to the unrelated CA: %v", err)
	}
	reloader.closeIdleConnections()
	before := len(calls())
	if _, _, _, err := fetcher.Fetch(context.Background(), "probe-supply"); err == nil {
		t.Error("supply fetch trusted the server after the CA pool was rotated away from its issuer")
	}
	if _, err := seed.SeedExecutionFromEntry(context.Background(), types.EntrySeedRequest{AdmissionKey: "k2", WorkflowID: "wf", EntryUnitID: "entry", Outcome: "success"}); err == nil {
		t.Error("entry seed trusted the server after the CA pool was rotated away from its issuer")
	}
	if _, err := svcCfg.ArtifactCodeResolver(namespace.WithNamespace(context.Background(), "round-ca"), store.ContentHash(artifact)); err == nil {
		t.Error("artifact fetch trusted the server after the CA pool was rotated away from its issuer")
	}
	if n := len(calls()) - before; n != 0 {
		t.Fatalf("server served %d requests under an untrusted CA pool, want 0", n)
	}

	// And back: the same clients recover with no rebuild.
	if err := reloader.Reload(CredentialReloaderSource{
		Token: "token-c", TLSServerCA: serverCAPath, TLSClientCert: certA, TLSClientKey: keyA,
	}); err != nil {
		t.Fatalf("Reload back to the server CA: %v", err)
	}
	reloader.closeIdleConnections()
	got, err = exercise()
	if err != nil {
		t.Fatalf("after restoring the CA: %v", err)
	}
	assertAll("after restoring the CA", got, "client-a", "token-c")
}

// TestReloadedBearerTransportKeepsTheTokenOnItsOrigin pins the redirect
// guarantee reloadedBearerTransport documents: the live token is attached only
// to requests for the configured origin.
func TestReloadedBearerTransportKeepsTheTokenOnItsOrigin(t *testing.T) {
	var foreignAuth string
	foreign := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		foreignAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer foreign.Close()
	var originAuth string
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originAuth = r.Header.Get("Authorization")
		http.Redirect(w, r, foreign.URL+"/elsewhere", http.StatusFound)
	}))
	defer origin.Close()

	cfg := RunnerConfig{ServerURL: origin.URL, Token: "origin-token"}
	client, token, err := newRunnerOriginHTTPClient(cfg, mustTestCredentialReloader(t, cfg), 5*time.Second)
	if err != nil {
		t.Fatalf("newRunnerOriginHTTPClient: %v", err)
	}
	if token != "" {
		t.Fatalf("returned token = %q, want empty: the transport supplies it", token)
	}
	resp, err := client.Get(origin.URL + "/v1/artifacts/x")
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	_ = resp.Body.Close()
	if originAuth != "Bearer origin-token" {
		t.Fatalf("origin saw Authorization %q, want the live token", originAuth)
	}
	if foreignAuth != "" {
		t.Fatalf("redirect target on another host received Authorization %q", foreignAuth)
	}
}

// TestControlPlaneHTTPClientSendsTheTokenOnlyToTheRunnersOrigin pins that the
// host-facing client takes its origin from the runner, not from its caller:
// a request through it to any other host carries no Authorization header,
// and only the ServerURL NewRunner was built with receives the live token.
func TestControlPlaneHTTPClientSendsTheTokenOnlyToTheRunnersOrigin(t *testing.T) {
	var foreignAuth atomic.Value
	foreignAuth.Store("")
	foreign := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		foreignAuth.Store(r.Header.Get("Authorization"))
		w.WriteHeader(http.StatusNoContent)
	}))
	defer foreign.Close()
	var originAuth atomic.Value
	originAuth.Store("")
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originAuth.Store(r.Header.Get("Authorization"))
		w.WriteHeader(http.StatusNoContent)
	}))
	defer origin.Close()

	r, err := NewRunner(RunnerConfig{
		ServerURL:        origin.URL,
		RunnerID:         "origin-pin",
		Token:            "runner-token",
		ArtifactCacheDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	defer r.Close()
	client, err := r.ControlPlaneHTTPClient(5 * time.Second)
	if err != nil {
		t.Fatalf("ControlPlaneHTTPClient: %v", err)
	}

	for _, target := range []string{foreign.URL + "/v1/anything", origin.URL + "/v1/anything"} {
		resp, err := client.Get(target)
		if err != nil {
			t.Fatalf("GET %s: %v", target, err)
		}
		_ = resp.Body.Close()
	}
	if got := foreignAuth.Load().(string); got != "" {
		t.Fatalf("a host other than the runner's ServerURL received Authorization %q", got)
	}
	if got := originAuth.Load().(string); got != "Bearer runner-token" {
		t.Fatalf("runner origin saw Authorization %q, want the live token", got)
	}
}

// TestReloadableGRPCCredentialsOverrideServerNameLeavesTheBaseAlone pins that
// the deprecated OverrideServerName does not write the base config every
// ClientHandshake clones concurrently.
func TestReloadableGRPCCredentialsOverrideServerNameLeavesTheBaseAlone(t *testing.T) {
	creds := newReloadableGRPCCredentials(mustTestCredentialReloader(t, RunnerConfig{ServerURL: "https://x.invalid"}), &tls.Config{MinVersion: tls.VersionTLS12})
	if err := creds.OverrideServerName("other.invalid"); err == nil {
		t.Fatal("OverrideServerName returned nil, want an error")
	}
	if creds.base.ServerName != "" {
		t.Fatalf("base ServerName = %q, want it untouched", creds.base.ServerName)
	}
}

// TestRunnerGRPCTransportReadsReloadedCAPoolOnHandshake is the gRPC CA
// rotation regression. The server's leaf is signed by caNew; the runner starts
// out trusting only caOld, so its connection attempts fail. After a Reload to
// caNew — with the same *grpc.ClientConn, no restart and no redial by the
// caller — grpc-go's own reconnect must succeed. With credentials.NewTLS, as
// before the fix, the pool captured at construction is used for every
// handshake and the RPC below never succeeds.
func TestRunnerGRPCTransportReadsReloadedCAPoolOnHandshake(t *testing.T) {
	dir := t.TempDir()
	_, caOldPEM, _ := generateCredReloadTestCA(t, "grpc CA old")
	caNew, caNewPEM, caNewKey := generateCredReloadTestCA(t, "grpc CA new")
	caOldPath := writeCredReloadPEM(t, filepath.Join(dir, "ca-old.pem"), caOldPEM)
	caNewPath := writeCredReloadPEM(t, filepath.Join(dir, "ca-new.pem"), caNewPEM)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{issueCredReloadServerLeaf(t, caNew, caNewKey)},
		MinVersion:   tls.VersionTLS12,
	})))
	healthpb.RegisterHealthServer(srv, health.NewServer())
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	cfg := RunnerConfig{Transport: RunnerTransportGRPC, GRPCTarget: lis.Addr().String(), TLSServerCA: caOldPath}
	reloader := mustTestCredentialReloader(t, cfg)
	conn, err := dialRunnerGRPC(cfg, reloader)
	if err != nil {
		t.Fatalf("dialRunnerGRPC: %v", err)
	}
	defer conn.Close()
	healthClient := healthpb.NewHealthClient(conn)

	// Fails fast: without WaitForReady an RPC returns as soon as the channel
	// reports the handshake failure.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	_, err = healthClient.Check(ctx, &healthpb.HealthCheckRequest{})
	cancel()
	if err == nil {
		t.Fatal("RPC succeeded while trusting only caOld; the server leaf is signed by caNew")
	}

	if err := reloader.Reload(CredentialReloaderSource{TLSServerCA: caNewPath}); err != nil {
		t.Fatalf("Reload: %v", err)
	}
	// Skip the reconnect backoff so the test does not wait it out; the
	// reconnect itself is grpc-go's, not a redial by this test.
	conn.ResetConnectBackoff()
	ctx, cancel = context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := healthClient.Check(ctx, &healthpb.HealthCheckRequest{}, grpc.WaitForReady(true)); err != nil {
		t.Fatalf("RPC after reloading the CA pool to caNew: %v (the pool was not re-read on reconnect)", err)
	}
}

// TestNewRunnerHandsItsReloaderToTheOriginClients pins the NewRunner wiring:
// the reloader the runner's Reload swaps is the one the assembly's origin
// clients were built over. Asserted through the options NewRunner hands to
// buildRunnerServiceConfig, since the clients themselves are not reachable
// from a *Runner.
func TestNewRunnerHandsItsReloaderToTheOriginClients(t *testing.T) {
	r, err := NewRunner(RunnerConfig{ServerURL: "http://127.0.0.1:1", RunnerID: "wiring", Token: "t"})
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	defer r.Close()
	if r.credReloader == nil {
		t.Fatal("NewRunner built no CredentialReloader for the HTTP transport")
	}
	cfg := RunnerConfig{ServerURL: "http://127.0.0.1:1", Token: "static"}
	client, token, err := newRunnerOriginHTTPClient(cfg, r.credReloader, time.Second)
	if err != nil {
		t.Fatalf("newRunnerOriginHTTPClient: %v", err)
	}
	if token != "" {
		t.Fatalf("token = %q, want empty under a reloader", token)
	}
	if _, ok := client.Transport.(*reloadedBearerTransport); !ok {
		t.Fatalf("transport = %T, want *reloadedBearerTransport", client.Transport)
	}
	r.credReloader.transportsMu.Lock()
	tracked := len(r.credReloader.transports)
	r.credReloader.transportsMu.Unlock()
	// The protocol client's transport, plus the artifact client's, plus the
	// one just built. (No trigger capability, so no seed client.)
	if tracked != 3 {
		t.Fatalf("reloader tracks %d transports, want 3: the artifact client was not built over it", tracked)
	}
}

// TestRunnerReloadRotatesTheLiveRunnerEndToEnd drives the rotation through
// NewRunner and Runner.Reload rather than through the reloader directly, so
// it pins Reload's own wiring: handing the new token to the protocol client,
// and closing the idle keep-alive connections of every HTTP client built over
// the reloader. Removing either from Reload turns this red — the protocol
// client would keep sending token-a, or reuse the connection established
// under client-a's certificate.
func TestRunnerReloadRotatesTheLiveRunnerEndToEnd(t *testing.T) {
	dir := t.TempDir()
	serverCA, serverCAPEM, serverCAKey := generateCredReloadTestCA(t, "e2e server CA")
	clientCA, _, clientCAKey := generateCredReloadTestCA(t, "e2e client CA")
	serverCAPath := writeCredReloadPEM(t, filepath.Join(dir, "server-ca.pem"), serverCAPEM)
	certA, keyA := filepath.Join(dir, "a.crt"), filepath.Join(dir, "a.key")
	writeCredReloadClientLeaf(t, clientCA, clientCAKey, "client-a", certA, keyA)
	certB, keyB := filepath.Join(dir, "b.crt"), filepath.Join(dir, "b.key")
	writeCredReloadClientLeaf(t, clientCA, clientCAKey, "client-b", certB, keyB)

	srv, calls := newCredReloadOriginServer(t, issueCredReloadServerLeaf(t, serverCA, serverCAKey), clientCA, nil)
	defer srv.Close()

	cfg := RunnerConfig{
		ServerURL:        srv.URL,
		RunnerID:         "e2e",
		Token:            "token-a",
		TLSServerCA:      serverCAPath,
		TLSClientCert:    certA,
		TLSClientKey:     keyA,
		ArtifactCacheDir: t.TempDir(),
	}
	r, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	defer r.Close()
	protoClient, ok := r.protocolClient.(*protocol.Client)
	if !ok {
		t.Fatalf("protocol client = %T, want *protocol.Client", r.protocolClient)
	}
	hostClient, err := r.ControlPlaneHTTPClient(5 * time.Second)
	if err != nil {
		t.Fatalf("ControlPlaneHTTPClient: %v", err)
	}

	// exercise sends one heartbeat through the protocol client and one
	// request through the host client, and returns what the server saw.
	exercise := func() []originCall {
		t.Helper()
		before := len(calls())
		if _, err := protoClient.Heartbeat(context.Background(), protocol.HeartbeatRequest{RunnerID: "e2e"}); err != nil {
			t.Fatalf("heartbeat: %v", err)
		}
		resp, err := hostClient.Post(srv.URL+protocol.HeartbeatPath, "application/json", strings.NewReader(`{}`))
		if err != nil {
			t.Fatalf("host client request: %v", err)
		}
		_ = resp.Body.Close()
		return calls()[before:]
	}
	check := func(round string, got []originCall, wantCN, wantToken string) {
		t.Helper()
		if len(got) != 2 {
			t.Fatalf("%s: server saw %d requests, want 2", round, len(got))
		}
		for i, c := range got {
			if c.cn != wantCN || c.auth != "Bearer "+wantToken {
				t.Errorf("%s: request %d presented CN %q with %q, want CN %q with %q",
					round, i, c.cn, c.auth, wantCN, "Bearer "+wantToken)
			}
		}
	}

	check("before Reload", exercise(), "client-a", "token-a")
	if err := r.Reload(CredentialReloaderSource{
		Token: "token-b", TLSServerCA: serverCAPath, TLSClientCert: certB, TLSClientKey: keyB,
	}); err != nil {
		t.Fatalf("Runner.Reload: %v", err)
	}
	check("after Runner.Reload", exercise(), "client-b", "token-b")
}

// TestRunnerReloadRefusesToWeakenServerVerification pins the fail-closed rule
// for trust: a reload that empties every TLS setting, or drops the private
// server CA, is rejected and the previous material keeps serving.
func TestRunnerReloadRefusesToWeakenServerVerification(t *testing.T) {
	dir := t.TempDir()
	ca, caPEM, caKey := generateCredReloadTestCA(t, "weaken CA")
	caPath := writeCredReloadPEM(t, filepath.Join(dir, "ca.pem"), caPEM)
	cert, key := filepath.Join(dir, "c.crt"), filepath.Join(dir, "c.key")
	writeCredReloadClientLeaf(t, ca, caKey, "client", cert, key)

	cases := []struct {
		name string
		src  CredentialReloaderSource
	}{
		{"every TLS setting emptied", CredentialReloaderSource{Token: "token-b"}},
		{"server CA dropped, client cert kept", CredentialReloaderSource{Token: "token-b", TLSClientCert: cert, TLSClientKey: key}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, err := NewRunner(RunnerConfig{
				ServerURL: "https://127.0.0.1:1", RunnerID: "weaken", Token: "token-a",
				TLSServerCA: caPath, TLSClientCert: cert, TLSClientKey: key,
				ArtifactCacheDir: t.TempDir(),
			})
			if err != nil {
				t.Fatalf("NewRunner: %v", err)
			}
			defer r.Close()
			if err := r.Reload(tc.src); err == nil {
				t.Fatal("Reload weakening server verification: want error, got nil")
			}
			if got := r.credReloader.Token(); got != "token-a" {
				t.Fatalf("token after a refused Reload = %q, want token-a", got)
			}
			if r.credReloader.rootCAs() == nil || !r.credReloader.tlsConfigured() {
				t.Fatal("a refused Reload still replaced the TLS material")
			}
		})
	}

	// A runner that never had a private CA may still reload without one.
	plain, err := newCredentialReloader(RunnerConfig{Token: "token-a"})
	if err != nil {
		t.Fatalf("newCredentialReloader: %v", err)
	}
	if err := plain.Reload(CredentialReloaderSource{Token: "token-b"}); err != nil {
		t.Fatalf("plaintext-to-plaintext Reload: %v", err)
	}
	// Adding a CA is a strengthening, so it is allowed.
	if err := plain.Reload(CredentialReloaderSource{Token: "token-c", TLSServerCA: caPath}); err != nil {
		t.Fatalf("Reload adding a server CA: %v", err)
	}
}

// TestRunnerReloadKeepsTheProtocolTokenInStepWithTheReloader exercises
// concurrent Runner.Reload calls: once they settle, the token the protocol
// client sends must be the reloader's current one, not an earlier call's.
// Run with -race.
func TestRunnerReloadKeepsTheProtocolTokenInStepWithTheReloader(t *testing.T) {
	var lastAuth atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lastAuth.Store(r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	r, err := NewRunner(RunnerConfig{ServerURL: srv.URL, RunnerID: "concurrent", Token: "token-0", ArtifactCacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	defer r.Close()

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				if err := r.Reload(CredentialReloaderSource{Token: fmt.Sprintf("token-%d-%d", g, i)}); err != nil {
					t.Errorf("Reload: %v", err)
				}
			}
		}(g)
	}
	wg.Wait()

	protoClient := r.protocolClient.(*protocol.Client)
	if _, err := protoClient.Heartbeat(context.Background(), protocol.HeartbeatRequest{RunnerID: "concurrent"}); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if got, want := lastAuth.Load(), "Bearer "+r.credReloader.Token(); got != want {
		t.Fatalf("protocol client sent %q, reloader holds %q", got, want)
	}
}
