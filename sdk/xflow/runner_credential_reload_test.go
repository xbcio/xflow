package xflow

import (
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
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
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
