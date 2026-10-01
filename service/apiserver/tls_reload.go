package apiserver

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sync/atomic"
)

// tlsMaterial is one atomically-swapped snapshot of the server's TLS
// identity: the leaf certificate/key pair and, when mTLS is enabled, the
// client CA pool used to verify runner certificates. Both fields are set (or
// both left nil/empty) by the same Reload call, so a handshake in progress
// never observes a new certificate paired with an old CA pool or vice versa.
type tlsMaterial struct {
	cert     tls.Certificate
	clientCA *x509.CertPool
	// mtls records whether ClientCA was configured for this snapshot. It
	// exists because an empty *x509.CertPool is distinguishable from "no CA
	// configured" only by tracking the operator's original intent — Reload
	// must not silently drop mTLS enforcement if a reloaded CA file happened
	// to parse to an empty-looking pool.
	mtls bool
}

// TLSReloader holds the server's TLS material behind an atomic pointer so
// Reload can swap in newly read certificate/key/CA files without restarting
// the listener. tls.Config.GetCertificate and GetConfigForClient read through
// the holder on every handshake, so a swap takes effect on the very next
// connection with no listener restart.
//
// A TLSReloader that never had TLS configured (loadTLSReloader returned
// (nil, nil, nil)) is not constructed at all — callers must check for a nil
// *TLSReloader before wiring GetCertificate/GetConfigForClient.
type TLSReloader struct {
	certPath     string
	keyPath      string
	clientCAPath string
	current      atomic.Pointer[tlsMaterial]
}

// newTLSReloader reads certPath/keyPath (and clientCAPath, when set) once and
// returns a ready TLSReloader. It fails exactly like the one-shot loadTLS it
// replaces: cert and key must be provided together, and a configured client CA
// that contains no parseable certificate is an error.
func newTLSReloader(certPath, keyPath, clientCAPath string) (*TLSReloader, error) {
	r := &TLSReloader{certPath: certPath, keyPath: keyPath, clientCAPath: clientCAPath}
	mat, err := r.load()
	if err != nil {
		return nil, err
	}
	r.current.Store(mat)
	return r, nil
}

// load reads the configured files into a fresh tlsMaterial without mutating
// the reloader's current snapshot. Reload and newTLSReloader both go through
// this so "read" and "swap" stay two separate steps — the swap only happens
// once every read has already succeeded.
func (r *TLSReloader) load() (*tlsMaterial, error) {
	cert, err := tls.LoadX509KeyPair(r.certPath, r.keyPath)
	if err != nil {
		return nil, fmt.Errorf("load tls keypair: %w", err)
	}
	mat := &tlsMaterial{cert: cert}
	if r.clientCAPath != "" {
		caPEM, err := os.ReadFile(r.clientCAPath)
		if err != nil {
			return nil, fmt.Errorf("read tls client CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("tls client CA %q contains no valid certs", r.clientCAPath)
		}
		mat.clientCA = pool
		mat.mtls = true
	}
	return mat, nil
}

// Reload re-reads the certificate, key, and (when configured) client CA files
// from the same paths supplied at construction and swaps them in only if every
// file parses. On any error the previous material keeps serving unchanged —
// callers must not partially apply a reload.
func (r *TLSReloader) Reload() error {
	mat, err := r.load()
	if err != nil {
		return err
	}
	r.current.Store(mat)
	return nil
}

// snapshot returns the current material. Never nil after successful
// construction.
func (r *TLSReloader) snapshot() *tlsMaterial {
	return r.current.Load()
}

// GetCertificate implements the tls.Config.GetCertificate hook: every
// handshake reads through the holder, so a Reload takes effect on the next
// connection with no listener restart.
func (r *TLSReloader) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	mat := r.snapshot()
	return &mat.cert, nil
}

// clientAuthType reports whether mTLS should be enforced for the snapshot
// that was current at construction time — used only to decide the base
// tls.Config's ClientAuth field, which GetConfigForClient's returned config
// overrides per-handshake with the live value.
func (r *TLSReloader) clientAuthType() tls.ClientAuthType {
	if r.snapshot().mtls {
		return tls.RequireAndVerifyClientCert
	}
	return tls.NoClientCert
}

// GetConfigForClient implements the tls.Config.GetConfigForClient hook: it
// returns a fresh *tls.Config per handshake so ClientCAs and ClientAuth
// always reflect the material current at the moment a client connects,
// including after a Reload that adds, removes, or replaces the CA pool.
// MinVersion and every other static setting are carried over unchanged from
// base.
func (r *TLSReloader) GetConfigForClient(base *tls.Config) func(*tls.ClientHelloInfo) (*tls.Config, error) {
	return func(*tls.ClientHelloInfo) (*tls.Config, error) {
		mat := r.snapshot()
		cfg := base.Clone()
		cfg.GetConfigForClient = nil // avoid infinite recursion on the cloned config
		cfg.Certificates = nil
		cfg.GetCertificate = r.GetCertificate
		if mat.mtls {
			cfg.ClientCAs = mat.clientCA
			cfg.ClientAuth = tls.RequireAndVerifyClientCert
		} else {
			cfg.ClientCAs = nil
			cfg.ClientAuth = tls.NoClientCert
		}
		return cfg, nil
	}
}
