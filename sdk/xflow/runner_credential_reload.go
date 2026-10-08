package xflow

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/credentials"
)

// runnerCredentialMaterial is one atomically-swapped snapshot of the
// runner-side credentials used to reach the control plane: the bearer token
// and, when mTLS is configured, the client leaf certificate plus the pool
// used to verify the server's certificate. All of it is set (or left at zero
// value together) by the same Reload call, mirroring apiserver.tlsMaterial —
// a handshake or request in flight never observes a new certificate paired
// with an old CA pool, or a new token paired with material it was never
// validated against.
type runnerCredentialMaterial struct {
	token    string
	cert     *tls.Certificate // nil: no client certificate configured
	rootCAs  *x509.CertPool   // nil: no private CA configured (system roots)
	tlsPlain bool             // true: no TLS material was configured at all
}

// CredentialReloader holds the runner's control-plane credentials — the
// bearer token and the mTLS client certificate / server CA bundle — behind an
// atomic pointer, and is the seam that lets them change without restarting
// the process. It is built once by NewRunner (whenever RunnerConfig carried a
// token or TLS material) and is what Runner.Reload re-reads.
//
// Every client NewRunner points at the control plane reads through the same
// CredentialReloader, so one Reload call updates all of them:
//   - The Runner Protocol HTTP client: client certificate and CA pool per dial
//     (newReloadableHTTPTransport); the token through protocol.Client.SetToken,
//     which Runner.Reload calls.
//   - The artifact-fetch client and the entry-seed/supply-fetch client (one
//     instance shared by both): client certificate and CA pool per dial, and
//     the bearer token per request (reloadedBearerTransport).
//   - The gRPC Runner Protocol transport: client certificate and CA pool per
//     TLS handshake (reloadableGRPCCredentials); the token through
//     protocol.GRPCClient.SetToken.
//   - Runner.ControlPlaneHTTPClient, which sdk/runner's identity-renewal loop
//     calls through: the same per-dial TLS material and per-request token as
//     the artifact and entry-seed/supply clients.
//
// Not covered: NewRunnerHTTPClient reads cfg once and stays static — it is
// for calls made before a Runner exists (sdk/runner's enrollment); and
// RunnerTransportInProc builds no reloader at all.
//
// Reload is fail-closed: on any read or parse error, the previous snapshot
// keeps serving and the error is returned without ever logging a token value
// or key material.
type CredentialReloader struct {
	current atomic.Pointer[runnerCredentialMaterial]

	// transports are the HTTP transports built over this reloader. Runner.Reload
	// closes their idle connections after a successful swap, so a keep-alive
	// connection established under the OLD material is not reused.
	transportsMu sync.Mutex
	transports   []*http.Transport
}

// CredentialReloaderSource supplies the live values a Reload call re-reads.
// A host passes the same values it would pass to RunnerConfig; the standalone
// command re-resolves them from the config file/env/flags first (see
// sdk/runner's SIGHUP wiring), so a rotated secret becomes visible to this
// source before Reload is called.
type CredentialReloaderSource struct {
	// Token is the runner's live bearer token. Empty means no auth.
	Token string
	// TLSServerCA, TLSClientCert, TLSClientKey mirror RunnerConfig's fields of
	// the same name: all empty means plaintext, TLSClientCert and
	// TLSClientKey must be supplied together, and TLSServerCA alone is TLS
	// with no client certificate.
	TLSServerCA   string
	TLSClientCert string
	TLSClientKey  string
}

// newCredentialReloader builds a reloader already holding cfg's credentials,
// read and validated the same way buildRunnerTLSConfig validates them at
// startup. An error here means NewRunner itself fails to construct — the same
// failure mode a bad --tls-client-cert has always had.
func newCredentialReloader(cfg RunnerConfig) (*CredentialReloader, error) {
	r := &CredentialReloader{}
	mat, err := loadRunnerCredentialMaterial(CredentialReloaderSource{
		Token:         cfg.Token,
		TLSServerCA:   cfg.TLSServerCA,
		TLSClientCert: cfg.TLSClientCert,
		TLSClientKey:  cfg.TLSClientKey,
	})
	if err != nil {
		return nil, err
	}
	r.current.Store(mat)
	return r, nil
}

// loadRunnerCredentialMaterial reads and fully validates src into a fresh
// snapshot without mutating any reloader. Reload and newCredentialReloader
// both go through this so "read" and "swap" are two separate steps: the swap
// only happens once every read has already succeeded, which is what makes
// Reload fail-closed.
func loadRunnerCredentialMaterial(src CredentialReloaderSource) (*runnerCredentialMaterial, error) {
	mat := &runnerCredentialMaterial{token: src.Token}
	if src.TLSServerCA == "" && src.TLSClientCert == "" && src.TLSClientKey == "" {
		mat.tlsPlain = true
		return mat, nil
	}
	if src.TLSServerCA != "" {
		caPEM, err := os.ReadFile(src.TLSServerCA)
		if err != nil {
			return nil, fmt.Errorf("read server CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("server CA %q contains no valid certs", src.TLSServerCA)
		}
		mat.rootCAs = pool
	}
	switch {
	case src.TLSClientCert == "" && src.TLSClientKey == "":
		// TLS only, no client auth.
	case src.TLSClientCert != "" && src.TLSClientKey != "":
		cert, err := tls.LoadX509KeyPair(src.TLSClientCert, src.TLSClientKey)
		if err != nil {
			return nil, fmt.Errorf("load client keypair: %w", err)
		}
		mat.cert = &cert
	default:
		return nil, fmt.Errorf("TLSClientCert and TLSClientKey must be provided together")
	}
	return mat, nil
}

// Reload re-reads src and swaps it in only if every piece of material parses.
// On any error the previous snapshot keeps serving, unchanged, and the error
// is returned so the caller can log it — never with a token value or key
// material inside it, since this function's own errors only ever name a path
// or a parse failure, never a secret.
//
// A reload that would weaken server verification is refused the same way:
// dropping every TLS setting while TLS is configured, or dropping the server
// CA while a private CA is trusted. Either would otherwise apply silently —
// verification would fall back to the system roots, and the gRPC transport,
// which decides TLS-or-plaintext once at dial time, would keep the TLS
// handshake with no private CA to check it against. A config edit that
// empties these settings is far more often a mistake than an intent; a real
// move off a private CA is made with a restart.
func (r *CredentialReloader) Reload(src CredentialReloaderSource) error {
	mat, err := loadRunnerCredentialMaterial(src)
	if err != nil {
		return err
	}
	if err := checkRunnerTrustNotWeakened(r.snapshot(), mat); err != nil {
		return err
	}
	r.current.Store(mat)
	return nil
}

// checkRunnerTrustNotWeakened rejects the two reload transitions that would
// silently loosen how the runner verifies the control plane.
func checkRunnerTrustNotWeakened(prev, next *runnerCredentialMaterial) error {
	if prev == nil {
		return nil
	}
	if !prev.tlsPlain && next.tlsPlain {
		return errors.New("refusing to reload: the new configuration drops all TLS settings " +
			"(server CA, client certificate and key) while TLS is configured; restart the runner to stop using TLS")
	}
	if prev.rootCAs != nil && next.rootCAs == nil {
		return errors.New("refusing to reload: the new configuration drops the server CA, " +
			"which would fall back to the system trust store; restart the runner to stop trusting a private CA")
	}
	return nil
}

// snapshot returns the current material. Never nil after successful
// construction.
func (r *CredentialReloader) snapshot() *runnerCredentialMaterial {
	return r.current.Load()
}

// Token returns the live bearer token.
func (r *CredentialReloader) Token() string {
	return r.snapshot().token
}

// GetClientCertificate implements the tls.Config.GetClientCertificate hook:
// every handshake reads through the holder, so a Reload's new leaf
// certificate is presented on the very next connection. Returning a
// zero-value *tls.Certificate (no error) is the documented way to present no
// certificate, which is correct when no client cert was ever configured — the
// dial still proceeds as server-authenticated-only TLS.
func (r *CredentialReloader) GetClientCertificate(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
	mat := r.snapshot()
	if mat.cert == nil {
		return &tls.Certificate{}, nil
	}
	return mat.cert, nil
}

// rootCAs returns the live server-CA pool, or nil for the system pool.
func (r *CredentialReloader) rootCAs() *x509.CertPool {
	return r.snapshot().rootCAs
}

// closeIdleConnections closes the idle keep-alive connections of every HTTP
// transport built over r, so the next request on each of them redials and
// handshakes with the current material.
func (r *CredentialReloader) closeIdleConnections() {
	r.transportsMu.Lock()
	transports := append([]*http.Transport(nil), r.transports...)
	r.transportsMu.Unlock()
	for _, t := range transports {
		t.CloseIdleConnections()
	}
}

func (r *CredentialReloader) trackTransport(t *http.Transport) {
	r.transportsMu.Lock()
	r.transports = append(r.transports, t)
	r.transportsMu.Unlock()
}

// tlsConfigured reports whether any TLS material is live right now, mirroring
// buildRunnerTLSConfig's "all empty means plaintext" rule.
func (r *CredentialReloader) tlsConfigured() bool {
	return !r.snapshot().tlsPlain
}

// newReloadableHTTPTransport builds an *http.Transport whose TLS material is
// read live from r on every dial and every handshake, so a Reload takes
// effect on the next request with no client rebuild. baseTLSCfg carries the
// static settings (MinVersion today); its Certificates/RootCAs are stripped —
// GetClientCertificate and DialTLSContext below are what actually supply
// them, each from the reloader's current snapshot.
//
// DialTLSContext (not a static RootCAs field) is what makes the CA pool
// reloadable: crypto/tls has no client-side analogue of the server's
// GetConfigForClient hook, so the only way to hand a dial the live pool is to
// build a fresh *tls.Config per connection attempt — the same shape
// apiserver.TLSReloader.GetConfigForClient uses on the server side, just
// invoked from the dial path instead of from a handshake callback.
func newReloadableHTTPTransport(r *CredentialReloader, baseTLSCfg *tls.Config) *http.Transport {
	base := baseTLSCfg.Clone()
	base.Certificates = nil
	base.RootCAs = nil
	base.GetClientCertificate = r.GetClientCertificate
	// Start from http.DefaultTransport's timeouts and idle-pool limits, but
	// dial the control plane directly (no environment proxy): net/http
	// tunnels an HTTPS request through a proxy itself and verifies it against
	// TLSClientConfig, bypassing DialTLSContext and therefore the live CA pool.
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.TLSClientConfig = base
	dialer := &net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}
	transport.DialTLSContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		cfg := base.Clone()
		cfg.RootCAs = r.rootCAs()
		// http.Transport's own TLS dial path derives ServerName from the
		// dial address when the config leaves it empty; DialTLSContext
		// bypasses that derivation entirely (per net/http's docs: "it is
		// the caller's responsibility to set up ServerName"), so this
		// must do it explicitly or every handshake fails verification
		// with "either ServerName or InsecureSkipVerify must be
		// specified" regardless of how correct the RootCAs pool is.
		if cfg.ServerName == "" {
			if host, _, splitErr := net.SplitHostPort(addr); splitErr == nil {
				cfg.ServerName = host
			} else {
				cfg.ServerName = addr
			}
		}
		rawConn, err := dialer.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		tlsConn := tls.Client(rawConn, cfg)
		handshakeCtx := ctx
		if timeout := transport.TLSHandshakeTimeout; timeout > 0 {
			var cancel context.CancelFunc
			handshakeCtx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}
		if err := tlsConn.HandshakeContext(handshakeCtx); err != nil {
			_ = rawConn.Close()
			return nil, err
		}
		return tlsConn, nil
	}
	r.trackTransport(transport)
	return transport
}

// reloadedBearerTransport attaches the reloader's live bearer token to every
// request bound for the control-plane origin. It serves the clients whose
// callers (objectstore.HTTPStore, runnersvc.HTTPSupplyFetcher, the entry-seed
// runtime) would otherwise copy a token into a struct field once at
// construction: those are handed an empty Token, so they set no Authorization
// header of their own, and this transport supplies the current one instead.
//
// The token is only attached when the request's scheme and host match the
// configured origin. A redirect to any other host therefore leaves without
// it, which is the same guarantee net/http gives a header set on the original
// request.
type reloadedBearerTransport struct {
	base     *http.Transport
	reloader *CredentialReloader
	scheme   string
	host     string
}

func (t *reloadedBearerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if token := t.reloader.Token(); token != "" && t.host != "" &&
		req.URL.Scheme == t.scheme && req.URL.Host == t.host {
		// RoundTrip must not modify the caller's request.
		req = req.Clone(req.Context())
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return t.base.RoundTrip(req)
}

// CloseIdleConnections lets http.Client.CloseIdleConnections reach the
// wrapped transport.
func (t *reloadedBearerTransport) CloseIdleConnections() {
	t.base.CloseIdleConnections()
}

// reloadableGRPCCredentials is the gRPC transport's counterpart of
// newReloadableHTTPTransport. credentials.NewTLS takes its *tls.Config once,
// and that config's RootCAs pool is used for every later handshake; this type
// instead builds a fresh config for each ClientHandshake, with the reloader's
// current CA pool and its GetClientCertificate hook, and hands it to
// credentials.NewTLS. Every connection grpc-go establishes after a Reload,
// including its own reconnects, therefore verifies the server against the
// new pool and presents the new leaf.
//
// A connection already established keeps the material it handshook with:
// grpc-go offers no per-connection hook to re-handshake, and the runner does
// not tear down a live connection on Reload.
type reloadableGRPCCredentials struct {
	reloader *CredentialReloader
	base     *tls.Config
}

func newReloadableGRPCCredentials(r *CredentialReloader, baseTLSCfg *tls.Config) *reloadableGRPCCredentials {
	base := baseTLSCfg.Clone()
	base.Certificates = nil
	base.RootCAs = nil
	base.GetClientCertificate = nil
	return &reloadableGRPCCredentials{reloader: r, base: base}
}

func (c *reloadableGRPCCredentials) ClientHandshake(ctx context.Context, authority string, rawConn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	cfg := c.base.Clone()
	cfg.RootCAs = c.reloader.rootCAs()
	cfg.GetClientCertificate = c.reloader.GetClientCertificate
	return credentials.NewTLS(cfg).ClientHandshake(ctx, authority, rawConn)
}

// ServerHandshake is never called: these credentials only dial.
func (c *reloadableGRPCCredentials) ServerHandshake(net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return nil, nil, errors.New("xflow: runner gRPC credentials are client-only")
}

func (c *reloadableGRPCCredentials) Info() credentials.ProtocolInfo {
	return credentials.NewTLS(c.base).Info()
}

func (c *reloadableGRPCCredentials) Clone() credentials.TransportCredentials {
	return &reloadableGRPCCredentials{reloader: c.reloader, base: c.base.Clone()}
}

// OverrideServerName is deprecated in grpc-go; it is kept for the interface,
// and like credentials.NewTLS's implementation only records the name.
func (c *reloadableGRPCCredentials) OverrideServerName(name string) error {
	c.base.ServerName = name
	return nil
}
