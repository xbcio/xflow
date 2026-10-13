package xflow

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/http/httpproxy"
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
	// Start from http.DefaultTransport's timeouts and idle-pool limits. The
	// environment proxy is honoured through DialTLSContext, not through
	// http.Transport.Proxy: net/http's own proxy path tunnels an HTTPS request
	// itself and verifies it against TLSClientConfig, which would bypass this
	// dial and therefore the live CA pool this transport exists to provide.
	// dialRunnerTLSConn performs the CONNECT handshake by hand and then runs
	// the same per-dial TLS setup (ServerName, handshake deadline, live
	// RootCAs), so "reloadable CA" and "dials through $HTTPS_PROXY" hold at
	// the same time instead of trading one away for the other. Proxy = nil is
	// what keeps net/http from double-tunnelling.
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.TLSClientConfig = base
	dialer := newRunnerDialer()
	transport.DialTLSContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		return dialRunnerTLSConn(ctx, dialer, network, addr, base, r.rootCAs, transport.TLSHandshakeTimeout)
	}
	r.trackTransport(transport)
	return transport
}

// newRunnerDialer is the shared TCP dialer for every runner-side transport:
// the same connect and keep-alive timeouts http.DefaultTransport uses.
func newRunnerDialer() *net.Dialer {
	return &net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}
}

// dialRunnerTLSConn dials addr — directly, or through the environment's HTTP
// proxy when one is configured — and completes the TLS handshake against
// liveRootCAs() (nil means the system pool). It is the one place every runner
// HTTP client gets its TLS dial from, so the reloadable and static clients
// cannot disagree about proxy handling.
//
// liveRootCAs is a function rather than a pool because the reloadable client's
// pool is swapped by CredentialReloader.Reload: reading it at dial time is what
// lets a connection made after a rotation verify against the new CA. A static
// caller simply passes a closure over its fixed pool.
//
// The proxy is resolved from the process environment per dial
// (httpproxy.FromEnvironment, so HTTP_PROXY/HTTPS_PROXY/NO_PROXY semantics are
// exactly net/http's, including case-insensitivity and CIDR entries), never
// cached: a long-lived process must notice a proxy configuration change the
// same way it notices a credential rotation. An HTTPS proxy is dialled with
// the system roots and no client certificate — the runner's mTLS material is
// for the control plane, not for its proxy.
//
// Error messages name host:port only. A proxy URL may carry userinfo, and a
// credential must never reach a log or an error string.
func dialRunnerTLSConn(ctx context.Context, dialer *net.Dialer, network, addr string, baseTLSCfg *tls.Config, liveRootCAs func() *x509.CertPool, handshakeTimeout time.Duration) (net.Conn, error) {
	rawConn, err := dialRunnerConn(ctx, dialer, network, addr, baseTLSCfg)
	if err != nil {
		return nil, err
	}
	cfg := baseTLSCfg.Clone()
	if liveRootCAs != nil {
		cfg.RootCAs = liveRootCAs()
	}
	// http.Transport's own TLS dial path derives ServerName from the dial
	// address when the config leaves it empty; DialTLSContext bypasses that
	// derivation entirely (per net/http's docs: "it is the caller's
	// responsibility to set up ServerName"), so this must do it explicitly or
	// every handshake fails verification with "either ServerName or
	// InsecureSkipVerify must be specified" regardless of how correct the
	// RootCAs pool is.
	if cfg.ServerName == "" {
		if host, _, splitErr := net.SplitHostPort(addr); splitErr == nil {
			cfg.ServerName = host
		} else {
			cfg.ServerName = addr
		}
	}
	tlsConn := tls.Client(rawConn, cfg)
	handshakeCtx := ctx
	if handshakeTimeout > 0 {
		var cancel context.CancelFunc
		handshakeCtx, cancel = context.WithTimeout(ctx, handshakeTimeout)
		defer cancel()
	}
	if err := tlsConn.HandshakeContext(handshakeCtx); err != nil {
		_ = rawConn.Close()
		return nil, err
	}
	return tlsConn, nil
}

// dialRunnerConn dials addr directly or opens a CONNECT tunnel through the
// environment's proxy first, returning the raw (untunnelled-TLS) connection.
//
// A plaintext configuration passes baseTLSCfg == nil. The proxy is then not
// consulted, exactly as before this change: a plain http:// origin through a
// proxy is net/http's own business (it rewrites the request line to absolute
// form and never calls DialTLSContext), and stepping in here would only
// produce a tunnel for a request that asks for none.
func dialRunnerConn(ctx context.Context, dialer *net.Dialer, network, addr string, baseTLSCfg *tls.Config) (net.Conn, error) {
	if baseTLSCfg == nil {
		return dialer.DialContext(ctx, network, addr)
	}
	proxyURL, err := runnerProxyFor(addr)
	if err != nil {
		return nil, err
	}
	if proxyURL == nil {
		return dialer.DialContext(ctx, network, addr)
	}
	return dialConnectTunnel(ctx, dialer, network, addr, proxyURL)
}

// runnerProxyFor resolves the environment proxy for one HTTPS destination,
// with NO_PROXY honoured, and refuses schemes this client cannot speak.
//
// socks5 is rejected explicitly rather than silently ignored: grpc-go's
// environment proxy support does not carry socks5 either, so the two runner
// transports stay consistent — and a deployment that set ALL_PROXY expecting
// it to work gets an error instead of a runner that quietly bypasses its
// egress control.
func runnerProxyFor(addr string) (*url.URL, error) {
	reqURL := &url.URL{Scheme: "https", Host: addr}
	proxyURL, err := httpproxy.FromEnvironment().ProxyFunc()(reqURL)
	if err != nil {
		return nil, fmt.Errorf("resolve environment proxy for %s: %s", addr, runnerProxyErrorText(err))
	}
	if proxyURL == nil {
		return nil, nil
	}
	if proxyURL.Scheme != "http" && proxyURL.Scheme != "https" {
		return nil, fmt.Errorf("environment proxy for %s has unsupported scheme %q (only http and https are supported)", addr, proxyURL.Scheme)
	}
	return proxyURL, nil
}

// runnerProxyErrorText strips credentials from an error produced while
// resolving a proxy from the environment. httpproxy's parse failure quotes the
// raw value — "invalid proxy address %q" — and a proxy URL may carry userinfo,
// so everything from the scheme separator on is replaced. A credential must
// never travel through an error string to a log.
//
// Defensive rather than currently reachable: the httpproxy in use reports no
// error for a value it cannot parse (init drops the proxy and the result is a
// direct dial, matching http.ProxyFromEnvironment), but ProxyFunc's error
// return is the documented channel for a malformed value, and a version that
// starts using it must not leak userinfo through a caller that logs this error.
func runnerProxyErrorText(err error) string {
	text := err.Error()
	if idx := strings.Index(text, "://"); idx >= 0 {
		return text[:idx+3] + "<redacted>"
	}
	return text
}

// dialConnectTunnel dials the proxy, issues CONNECT for target, and asserts a
// 2xx reply before returning the tunnel. The caller layers TLS on the returned
// connection; this function never sees the tunnel's plaintext.
func dialConnectTunnel(ctx context.Context, dialer *net.Dialer, network, target string, proxyURL *url.URL) (net.Conn, error) {
	proxyAddr := proxyURL.Host
	if !strings.Contains(proxyAddr, ":") {
		if proxyURL.Scheme == "https" {
			proxyAddr = net.JoinHostPort(proxyAddr, "443")
		} else {
			proxyAddr = net.JoinHostPort(proxyAddr, "80")
		}
	}
	conn, err := dialer.DialContext(ctx, network, proxyAddr)
	if err != nil {
		// Only the proxy's host:port, never the URL: it may carry userinfo.
		return nil, fmt.Errorf("dial proxy %s: %w", proxyAddr, err)
	}
	if proxyURL.Scheme == "https" {
		// The proxy leg uses the system roots and presents no client
		// certificate: the runner's private CA and leaf are the control
		// plane's, and sending them to a proxy would leak them.
		proxyTLS := tls.Client(conn, &tls.Config{
			ServerName: proxyURL.Hostname(),
			MinVersion: tls.VersionTLS12,
		})
		handshakeCtx := ctx
		if dialer.Timeout > 0 {
			var cancel context.CancelFunc
			handshakeCtx, cancel = context.WithTimeout(ctx, dialer.Timeout)
			defer cancel()
		}
		if err := proxyTLS.HandshakeContext(handshakeCtx); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("tls handshake with proxy %s: %w", proxyAddr, err)
		}
		conn = proxyTLS
	}

	connectReq := &http.Request{
		Method: "CONNECT",
		URL:    &url.URL{Opaque: target},
		Host:   target,
		Header: make(http.Header),
	}
	if proxyURL.User != nil {
		password, _ := proxyURL.User.Password()
		connectReq.Header.Set("Proxy-Authorization",
			"Basic "+base64.StdEncoding.EncodeToString([]byte(proxyURL.User.Username()+":"+password)))
	}
	if err := connectReq.Write(conn); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("write CONNECT %s to proxy %s: %w", target, proxyAddr, err)
	}
	// The response is read from a buffered reader because the proxy may have
	// pipelined bytes after the CONNECT reply — the tunnel's first TLS records
	// can already be in that buffer, and a bare conn would lose them.
	//
	// The read carries its own deadline: a proxy that accepts the connection
	// and then says nothing would otherwise hold this dial open until the
	// caller's request context dies, pinning a runner connection per attempt.
	// dialer.Timeout is the budget the dial itself already used — a proxy that
	// cannot answer CONNECT within it is not usable — with newRunnerDialer's
	// 30s as the floor when the caller configured none. The bound is cleared
	// before the tunnel is handed back: the caller keeps this connection for
	// the whole request lifetime, so the CONNECT read's ceiling must not
	// become the tunnel's.
	readTimeout := dialer.Timeout
	if readTimeout <= 0 {
		readTimeout = 30 * time.Second
	}
	_ = conn.SetReadDeadline(time.Now().Add(readTimeout))
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, connectReq)
	_ = conn.SetReadDeadline(time.Time{})
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("read CONNECT response from proxy %s: %w", proxyAddr, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_ = conn.Close()
		return nil, fmt.Errorf("proxy %s refused CONNECT %s: %s", proxyAddr, target, resp.Status)
	}
	if br.Buffered() == 0 {
		return conn, nil
	}
	// Leftover bytes exist, so hand the caller a connection that returns them
	// first. Only the buffered count is wrapped: after it drains the underlying
	// reader is already position-correct.
	return &bufferedConn{Conn: conn, reader: br}, nil
}

// bufferedConn is a net.Conn whose Read drains a bufio.Reader before touching
// the socket, so bytes the CONNECT response reader consumed are not lost.
type bufferedConn struct {
	net.Conn
	reader *bufio.Reader
}

func (c *bufferedConn) Read(b []byte) (int, error) { return c.reader.Read(b) }

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

// OverrideServerName exists only to satisfy credentials.TransportCredentials;
// grpc-go has deprecated it and never calls it (the channel authority, set by
// grpc.WithAuthority, is what ClientHandshake verifies against). It refuses
// rather than writing c.base, which ClientHandshake clones concurrently on
// every handshake: the write would be a data race for no effect.
func (c *reloadableGRPCCredentials) OverrideServerName(string) error {
	return errors.New("xflow: runner gRPC credentials do not support OverrideServerName; use grpc.WithAuthority")
}
