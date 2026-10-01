package apiserver

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// The certificate/CA generation helpers in this file are deliberately
// independent of the ecdsa self-signed pair in
// service/control/mtls_identity_test.go: that package cannot be imported here
// without an import cycle (control does not depend on apiserver, but this
// package already depends on control), and the two suites generate distinct
// certificate shapes (a leaf identified by SAN/CN here vs. a client cert there).

// generateTLSReloadTestCA creates a self-signed CA certificate/key pair,
// returning both the parsed certificate and its PEM encoding (for writing to
// a CA bundle file) plus the signing key for issuing leaves.
func generateTLSReloadTestCA(t *testing.T, cn string) (*x509.Certificate, []byte, *ecdsa.PrivateKey) {
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

// writeServerLeaf issues a server (or client, via forClient) leaf certificate
// signed by ca/caKey and writes its cert+key as PEM files at certPath/keyPath.
// cn is embedded both as the Subject CommonName and, for a server leaf, as a
// DNS SAN so a "127.0.0.1"-dialing test client can still verify against cn via
// InsecureSkipVerify + VerifyConnection, or simply read back cn from the
// negotiated certificate without hostname verification getting in the way.
func writeServerLeaf(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, cn, certPath, keyPath string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate leaf key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		DNSNames:     []string{cn},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create leaf certificate: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal leaf key: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
		t.Fatalf("write cert file: %v", err)
	}
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatalf("write key file: %v", err)
	}
}

// writeClientLeaf issues a client-auth leaf signed by ca/caKey and returns it
// as a ready-to-dial tls.Certificate (never written to disk — tests wire it
// directly into a client's tls.Config.Certificates).
func writeClientLeaf(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, cn string) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate client key: %v", err)
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
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// writeCABundle writes one or more PEM-encoded CA certificates concatenated
// into a single bundle file, mirroring the dual-CA bundle trick documented in
// the credential-key-rotation-runbook.
func writeCABundle(t *testing.T, path string, caPEMs ...[]byte) {
	t.Helper()
	var all []byte
	for _, p := range caPEMs {
		all = append(all, p...)
	}
	if err := os.WriteFile(path, all, 0o600); err != nil {
		t.Fatalf("write CA bundle: %v", err)
	}
}

// startTLSReloadListener starts a bare TLS listener (no HTTP) over cfg,
// accepting connections in the background and, on a successful handshake,
// writing a single byte before closing so a dialing client can distinguish a
// genuine accept from a TLS 1.3 post-handshake rejection. It exists because
// TLSReloader is exercised at the crypto/tls handshake layer directly — this
// test does not need apiserver.Run's HTTP/gRPC/metrics wiring, only that a
// tls.Config built from a *TLSReloader actually serves the reloader's live
// material.
//
// The write-after-handshake step matters specifically for mTLS rejections: in
// TLS 1.3, a server that cannot verify the client's certificate signals that
// failure with a fatal alert sent only after tls.Conn.Handshake() considers
// itself complete on the SERVER side. The client's own Handshake/Dial call can
// return successfully before that alert arrives, so a client that only checks
// the error from Dial (rather than performing at least one Read) can observe
// a false accept for a connection the server is about to tear down. Reading
// after the handshake, on both sides, is what makes the rejection visible to
// the dialer.
func startTLSReloadListener(t *testing.T, cfg *tls.Config) (addr string, stop func()) {
	t.Helper()
	ln, err := tls.Listen("tcp", "127.0.0.1:0", cfg)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	done := make(chan struct{})
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				close(done)
				return
			}
			tconn, ok := conn.(*tls.Conn)
			if !ok {
				_ = conn.Close()
				continue
			}
			if err := tconn.Handshake(); err == nil {
				// Ignore the write error: a client that already detected the
				// (successful) handshake and closed first is not this test's
				// failure mode — only a rejected handshake matters here, and
				// that path never reaches this branch.
				_, _ = tconn.Write([]byte("ok"))
			}
			_ = conn.Close()
		}
	}()
	return ln.Addr().String(), func() {
		_ = ln.Close()
		<-done
	}
}

// dialAndGetPeerCN dials addr over TLS with the given client config, reads
// the one-byte marker startTLSReloadListener's accept loop writes after a
// successful handshake, and returns the CommonName of the leaf certificate
// the server presented. A handshake the server rejects — including a TLS 1.3
// mTLS rejection that only manifests as a post-handshake alert — surfaces as
// a non-nil error here because the Read (not just Dial) is what observes it;
// see startTLSReloadListener's doc comment.
func dialAndGetPeerCN(clientCfg *tls.Config, addr string) (string, error) {
	conn, err := tls.Dial("tcp", addr, clientCfg)
	if err != nil {
		return "", err
	}
	defer func() { _ = conn.Close() }()
	buf := make([]byte, 2)
	if _, err := conn.Read(buf); err != nil {
		return "", err
	}
	state := conn.ConnectionState()
	if len(state.PeerCertificates) == 0 {
		return "", nil
	}
	return state.PeerCertificates[0].Subject.CommonName, nil
}

// TestTLSReloaderServesReloadedCertificate is the core certificate-rotation
// scenario: a handshake against the reloader-backed listener observes cert A,
// then after writing cert B's files and calling Reload, a fresh handshake
// observes cert B — with no listener restart in between.
func TestTLSReloaderServesReloadedCertificate(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "server.crt")
	keyPath := filepath.Join(dir, "server.key")

	ca, _, caKey := generateTLSReloadTestCA(t, "xflow test CA")
	writeServerLeaf(t, ca, caKey, "cert-a.xflow.test", certPath, keyPath)

	reloader, err := newTLSReloader(certPath, keyPath, "")
	if err != nil {
		t.Fatalf("newTLSReloader: %v", err)
	}
	serverCfg := &tls.Config{MinVersion: tls.VersionTLS12, GetCertificate: reloader.GetCertificate}
	addr, stop := startTLSReloadListener(t, serverCfg)
	defer stop()

	clientCfg := &tls.Config{InsecureSkipVerify: true} //nolint:gosec // test dials its own throwaway listener
	cn, err := dialAndGetPeerCN(clientCfg, addr)
	if err != nil {
		t.Fatalf("dial before reload: %v", err)
	}
	if cn != "cert-a.xflow.test" {
		t.Fatalf("peer CN before reload = %q, want cert-a.xflow.test", cn)
	}

	writeServerLeaf(t, ca, caKey, "cert-b.xflow.test", certPath, keyPath)
	if err := reloader.Reload(); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	cn, err = dialAndGetPeerCN(clientCfg, addr)
	if err != nil {
		t.Fatalf("dial after reload: %v", err)
	}
	if cn != "cert-b.xflow.test" {
		t.Fatalf("peer CN after reload = %q, want cert-b.xflow.test", cn)
	}
}

// TestTLSReloaderBadFilesKeepsServingOldCertificate pins that Reload returns
// an error and leaves the previous certificate serving when the new files do
// not parse (mismatched key here; a missing file is covered by
// TestTLSReloaderMissingFileKeepsServingOldCertificate).
func TestTLSReloaderBadFilesKeepsServingOldCertificate(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "server.crt")
	keyPath := filepath.Join(dir, "server.key")

	ca, _, caKey := generateTLSReloadTestCA(t, "xflow test CA")
	writeServerLeaf(t, ca, caKey, "cert-a.xflow.test", certPath, keyPath)

	reloader, err := newTLSReloader(certPath, keyPath, "")
	if err != nil {
		t.Fatalf("newTLSReloader: %v", err)
	}
	serverCfg := &tls.Config{MinVersion: tls.VersionTLS12, GetCertificate: reloader.GetCertificate}
	addr, stop := startTLSReloadListener(t, serverCfg)
	defer stop()

	// Corrupt the key file only, so LoadX509KeyPair fails cleanly.
	if err := os.WriteFile(keyPath, []byte("not a valid key"), 0o600); err != nil {
		t.Fatalf("corrupt key file: %v", err)
	}
	if err := reloader.Reload(); err == nil {
		t.Fatal("Reload with corrupt key file: want error, got nil")
	}

	clientCfg := &tls.Config{InsecureSkipVerify: true} //nolint:gosec
	cn, err := dialAndGetPeerCN(clientCfg, addr)
	if err != nil {
		t.Fatalf("dial after failed reload: %v", err)
	}
	if cn != "cert-a.xflow.test" {
		t.Fatalf("peer CN after failed reload = %q, want cert-a.xflow.test (old cert still serving)", cn)
	}
}

// TestTLSReloaderMissingFileKeepsServingOldCertificate is the missing-file
// analogue of the bad-files case above, exercised after one successful reload
// (cert A -> cert B) so the assertion pins "still B", not merely "still
// whatever was first loaded".
func TestTLSReloaderMissingFileKeepsServingOldCertificate(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "server.crt")
	keyPath := filepath.Join(dir, "server.key")

	ca, _, caKey := generateTLSReloadTestCA(t, "xflow test CA")
	writeServerLeaf(t, ca, caKey, "cert-a.xflow.test", certPath, keyPath)

	reloader, err := newTLSReloader(certPath, keyPath, "")
	if err != nil {
		t.Fatalf("newTLSReloader: %v", err)
	}
	writeServerLeaf(t, ca, caKey, "cert-b.xflow.test", certPath, keyPath)
	if err := reloader.Reload(); err != nil {
		t.Fatalf("Reload to cert-b: %v", err)
	}

	if err := os.Remove(certPath); err != nil {
		t.Fatalf("remove cert file: %v", err)
	}
	if err := reloader.Reload(); err == nil {
		t.Fatal("Reload with missing cert file: want error, got nil")
	}

	serverCfg := &tls.Config{MinVersion: tls.VersionTLS12, GetCertificate: reloader.GetCertificate}
	addr, stop := startTLSReloadListener(t, serverCfg)
	defer stop()
	clientCfg := &tls.Config{InsecureSkipVerify: true} //nolint:gosec
	cn, err := dialAndGetPeerCN(clientCfg, addr)
	if err != nil {
		t.Fatalf("dial after failed reload: %v", err)
	}
	if cn != "cert-b.xflow.test" {
		t.Fatalf("peer CN after failed reload = %q, want cert-b.xflow.test (old cert still serving)", cn)
	}
}

// TestLoadTLSConfigForClientPreservesNextProtos pins a regression: once
// tls.Config.GetConfigForClient is non-nil, crypto/tls replaces the entire
// live connection config with whatever it returns (see
// tls.Conn.readClientHello) rather than merging it into the config the
// listener was constructed with. Both net/http's ServeTLS and grpc-go's
// credentials.NewTLS only inject "h2"/"http/1.1" into a *clone* of the config
// made at listener-construction time — never into the *APIServer.loadTLS
// pointer GetConfigForClient closes over. If that pointer's own NextProtos is
// left empty, every handshake negotiates no ALPN protocol at all, silently
// breaking HTTP/2 and gRPC (which requires "h2") even though the listener's
// own config looks correct. loadTLS must set NextProtos directly on the base
// config so GetConfigForClient's clone carries it through.
func TestLoadTLSConfigForClientPreservesNextProtos(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "server.crt")
	keyPath := filepath.Join(dir, "server.key")
	ca, _, caKey := generateTLSReloadTestCA(t, "xflow test CA")
	writeServerLeaf(t, ca, caKey, "server.xflow.test", certPath, keyPath)

	s := &APIServer{cfg: Config{TLS: &TLSConfig{Cert: certPath, Key: keyPath}}}
	tlsCfg, _, err := s.loadTLS()
	if err != nil {
		t.Fatalf("loadTLS: %v", err)
	}
	if tlsCfg.GetConfigForClient == nil {
		t.Fatal("loadTLS: GetConfigForClient is nil, cannot verify per-handshake config")
	}
	got, err := tlsCfg.GetConfigForClient(&tls.ClientHelloInfo{})
	if err != nil {
		t.Fatalf("GetConfigForClient: %v", err)
	}
	want := []string{"h2", "http/1.1"}
	if len(got.NextProtos) != len(want) {
		t.Fatalf("GetConfigForClient().NextProtos = %v, want %v", got.NextProtos, want)
	}
	for i, p := range want {
		if got.NextProtos[i] != p {
			t.Fatalf("GetConfigForClient().NextProtos = %v, want %v", got.NextProtos, want)
		}
	}
}

// TestTLSReloaderClientCASwap is the mTLS scenario: a client certificate
// issued by CA1 is accepted while CA1 is the configured client CA; after
// Reload swaps the client CA file to CA2 only, the same CA1 client is
// rejected and a CA2 client is accepted.
func TestTLSReloaderClientCASwap(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "server.crt")
	keyPath := filepath.Join(dir, "server.key")
	caBundlePath := filepath.Join(dir, "client-ca.pem")

	serverCA, _, serverCAKey := generateTLSReloadTestCA(t, "xflow server CA")
	writeServerLeaf(t, serverCA, serverCAKey, "server.xflow.test", certPath, keyPath)

	ca1, ca1PEM, ca1Key := generateTLSReloadTestCA(t, "xflow client CA 1")
	ca2, ca2PEM, ca2Key := generateTLSReloadTestCA(t, "xflow client CA 2")
	writeCABundle(t, caBundlePath, ca1PEM)

	reloader, err := newTLSReloader(certPath, keyPath, caBundlePath)
	if err != nil {
		t.Fatalf("newTLSReloader: %v", err)
	}
	serverCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	serverCfg.GetConfigForClient = reloader.GetConfigForClient(serverCfg)
	addr, stop := startTLSReloadListener(t, serverCfg)
	defer stop()

	ca1Client := writeClientLeaf(t, ca1, ca1Key, "runner-ca1")
	ca2Client := writeClientLeaf(t, ca2, ca2Key, "runner-ca2")
	rootPool := x509.NewCertPool()
	rootPool.AddCert(serverCA)

	// Before any reload: a CA1-issued client is accepted.
	ca1ClientCfg := &tls.Config{Certificates: []tls.Certificate{ca1Client}, RootCAs: rootPool, ServerName: "server.xflow.test"}
	if _, err := dialAndGetPeerCN(ca1ClientCfg, addr); err != nil {
		t.Fatalf("CA1 client before CA swap: want accepted, got %v", err)
	}

	// Reload to a CA2-only bundle. GetConfigForClient reads the reloader's live
	// snapshot on every handshake, so the already-running listener observes
	// the swap with no restart.
	writeCABundle(t, caBundlePath, ca2PEM)
	if err := reloader.Reload(); err != nil {
		t.Fatalf("Reload to CA2-only bundle: %v", err)
	}

	if _, err := dialAndGetPeerCN(ca1ClientCfg, addr); err == nil {
		t.Fatal("CA1 client after CA swap to CA2-only: want rejected, got accepted")
	}
	ca2ClientCfg := &tls.Config{Certificates: []tls.Certificate{ca2Client}, RootCAs: rootPool, ServerName: "server.xflow.test"}
	if _, err := dialAndGetPeerCN(ca2ClientCfg, addr); err != nil {
		t.Fatalf("CA2 client after CA swap to CA2-only: want accepted, got %v", err)
	}
}
