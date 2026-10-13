package xflow

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// The #27 fixture: every runner-side HTTPS client must dial through the
// environment's HTTP proxy when one is configured, tunnelling CONNECT itself
// and then running its own handshake — against the live, reloadable CA pool —
// inside the tunnel.
//
// Two properties of net/http's proxy resolution shape this file:
//
//   - A destination that is "localhost" or a loopback address is NEVER proxied
//     (httpproxy's documented exemption, pinned by
//     TestRunnerProxyForReadsTheEnvironment). The tunnelling tests therefore
//     request a non-loopback NAME and let the fake proxy route that name to a
//     loopback test server it knows; no DNS is involved on the client side.
//   - A malformed proxy value is silently ignored (the proxy is treated as
//     unset), matching http.ProxyFromEnvironment.
//
// The proxy is a fake — an httptest.Server that speaks CONNECT by hijacking
// the connection and relaying bytes — so the tests stay hermetic and can
// inspect exactly what the client sent.

// setRunnerProxyEnv points the proxy environment at httpsProxy and noProxy for
// the duration of the test. Every variable is set explicitly, including the
// empty ones, so a developer's own HTTP(S)_PROXY cannot decide a test's
// outcome; both cases of each name are set because httpproxy reads either.
func setRunnerProxyEnv(t *testing.T, httpsProxy, noProxy string) {
	t.Helper()
	for name, value := range map[string]string{
		"HTTPS_PROXY": httpsProxy, "https_proxy": httpsProxy,
		"HTTP_PROXY": httpsProxy, "http_proxy": httpsProxy,
		"NO_PROXY": noProxy, "no_proxy": noProxy,
	} {
		t.Setenv(name, value)
	}
}

// proxyConnect is one CONNECT request the fake proxy received.
type proxyConnect struct {
	target string
	auth   string
}

// fakeProxyTunnelProxy is a CONNECT-speaking HTTP proxy. It hijacks each
// connection, dials the requested target (or, when backend is set, the fixed
// backend the test wants the request to arrive at) and relays bytes both ways.
// What it recorded is how a test tells "the client tunnelled through the
// proxy" from "the client dialled the origin directly".
type fakeProxyTunnelProxy struct {
	srv *httptest.Server
	// backend, when non-empty, overrides the CONNECT target the proxy dials.
	backend string

	mu       sync.Mutex
	connects []proxyConnect
	plain    []string
	refuse   bool
	tunnels  []net.Conn
}

func newFakeProxyTunnelProxy(t *testing.T, backend string) *fakeProxyTunnelProxy {
	t.Helper()
	p := &fakeProxyTunnelProxy{backend: backend}
	p.srv = httptest.NewServer(http.HandlerFunc(p.handle))
	t.Cleanup(func() {
		p.closeTunnels()
		p.srv.Close()
	})
	return p
}

// url is the proxy's own address, for HTTPS_PROXY.
func (p *fakeProxyTunnelProxy) url() string { return p.srv.URL }

func (p *fakeProxyTunnelProxy) handle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodConnect {
		p.mu.Lock()
		p.plain = append(p.plain, r.Method+" "+r.URL.String())
		p.mu.Unlock()
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	p.mu.Lock()
	p.connects = append(p.connects, proxyConnect{target: r.Host, auth: r.Header.Get("Proxy-Authorization")})
	refuse := p.refuse
	p.mu.Unlock()
	if refuse {
		// A proxy that wants different credentials: the shape a runner in a
		// proxied network meets when its Proxy-Authorization is wrong or absent.
		w.Header().Set("Proxy-Authenticate", `Basic realm="xflow-proxy-test"`)
		w.WriteHeader(http.StatusProxyAuthRequired)
		return
	}
	target := r.Host
	if p.backend != "" {
		target = p.backend
	}
	upstream, err := net.DialTimeout("tcp", target, 5*time.Second)
	if err != nil {
		http.Error(w, "upstream unreachable", http.StatusBadGateway)
		return
	}
	hj, ok := w.(http.Hijacker)
	if !ok {
		_ = upstream.Close()
		http.Error(w, "hijacking unsupported", http.StatusInternalServerError)
		return
	}
	clientConn, rw, err := hj.Hijack()
	if err != nil {
		_ = upstream.Close()
		return
	}
	p.track(clientConn, upstream)
	if _, err := clientConn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n")); err != nil {
		_ = clientConn.Close()
		_ = upstream.Close()
		return
	}
	// Anything the server's reader already took off the socket (a client may
	// pipeline its first TLS record with the CONNECT) goes upstream before the
	// relay takes over the raw connection.
	if n := rw.Reader.Buffered(); n > 0 {
		if _, err := io.CopyN(upstream, rw.Reader, int64(n)); err != nil {
			_ = clientConn.Close()
			_ = upstream.Close()
			return
		}
	}
	closeBoth := func() {
		_ = clientConn.Close()
		_ = upstream.Close()
	}
	go func() {
		_, _ = io.Copy(upstream, clientConn)
		closeBoth()
	}()
	go func() {
		_, _ = io.Copy(clientConn, upstream)
		closeBoth()
	}()
}

func (p *fakeProxyTunnelProxy) track(conns ...net.Conn) {
	p.mu.Lock()
	p.tunnels = append(p.tunnels, conns...)
	p.mu.Unlock()
}

func (p *fakeProxyTunnelProxy) closeTunnels() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.tunnels {
		_ = c.Close()
	}
}

func (p *fakeProxyTunnelProxy) setRefuse(refuse bool) {
	p.mu.Lock()
	p.refuse = refuse
	p.mu.Unlock()
}

func (p *fakeProxyTunnelProxy) connectLog() []proxyConnect {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]proxyConnect(nil), p.connects...)
}

func (p *fakeProxyTunnelProxy) plainLog() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.plain...)
}

// newProxyTunnelTestOrigin starts an HTTPS server on a loopback listener whose
// leaf certificate is issued for hostname by ca/caKey, and returns it with the
// https://hostname:port URL a tunnelling test must request. The destination is
// the NAME, not the loopback address, because a loopback destination is never
// proxied; the fake proxy routes the name to this listener, so the client
// never resolves it and no DNS is involved.
func newProxyTunnelTestOrigin(t *testing.T, hostname string, ca *x509.Certificate, caKey *ecdsa.PrivateKey) (*httptest.Server, string) {
	t.Helper()
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate origin leaf key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: hostname},
		DNSNames:     []string{hostname},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create origin leaf certificate: %v", err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse origin leaf certificate: %v", err)
	}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	// The CA-reload test's first handshake is expected to fail, and the server
	// would log it; keep that failure out of the test's output.
	srv.Config.ErrorLog = log.New(io.Discard, "", 0)
	srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: leafKey, Leaf: leaf}},
		MinVersion:   tls.VersionTLS12,
	}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	_, port, err := net.SplitHostPort(srv.Listener.Addr().String())
	if err != nil {
		t.Fatalf("origin listener address %q: %v", srv.Listener.Addr(), err)
	}
	return srv, "https://" + net.JoinHostPort(hostname, port)
}

// fakeProxyURLWithUserinfo returns the proxy's URL with credentials embedded,
// the shape an authenticated proxy deployment puts in HTTPS_PROXY.
func fakeProxyURLWithUserinfo(t *testing.T, raw, user, password string) string {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse proxy URL %q: %v", raw, err)
	}
	u.User = url.UserPassword(user, password)
	return u.String()
}

// TestRunnerProxyForReadsTheEnvironment pins the resolution rules the tunnel
// dial is built on: HTTPS_PROXY applies to an HTTPS destination (userinfo
// preserved for Proxy-Authorization), NO_PROXY wins, a proxy-less environment
// yields nil, a socks5 proxy is refused outright rather than silently ignored,
// and a loopback destination is never proxied.
func TestRunnerProxyForReadsTheEnvironment(t *testing.T) {
	const dest = "control-plane.example:8443"

	t.Run("no proxy configured", func(t *testing.T) {
		setRunnerProxyEnv(t, "", "")
		proxy, err := runnerProxyFor(dest)
		if err != nil || proxy != nil {
			t.Fatalf("runnerProxyFor = (%v, %v), want (nil, nil)", proxy, err)
		}
	})

	t.Run("http proxy with credentials", func(t *testing.T) {
		setRunnerProxyEnv(t, "http://proxy-user:proxy-pass@proxy.example:3128", "")
		proxy, err := runnerProxyFor(dest)
		if err != nil {
			t.Fatalf("runnerProxyFor: %v", err)
		}
		if proxy == nil || proxy.Host != "proxy.example:3128" {
			t.Fatalf("proxy = %v, want proxy.example:3128", proxy)
		}
		user := proxy.User.Username()
		password, _ := proxy.User.Password()
		if user != "proxy-user" || password != "proxy-pass" {
			t.Fatalf("proxy userinfo = %q/%q, want it preserved for Proxy-Authorization", user, password)
		}
	})

	t.Run("host and port without a scheme", func(t *testing.T) {
		setRunnerProxyEnv(t, "proxy.example:3128", "")
		proxy, err := runnerProxyFor(dest)
		if err != nil || proxy == nil || proxy.Scheme != "http" {
			t.Fatalf("runnerProxyFor = (%v, %v), want the http scheme assumed", proxy, err)
		}
	})

	t.Run("no_proxy wins", func(t *testing.T) {
		setRunnerProxyEnv(t, "http://proxy.example:3128", "control-plane.example, .internal")
		proxy, err := runnerProxyFor(dest)
		if err != nil || proxy != nil {
			t.Fatalf("runnerProxyFor with a NO_PROXY match = (%v, %v), want (nil, nil)", proxy, err)
		}
	})

	t.Run("no_proxy wildcard", func(t *testing.T) {
		setRunnerProxyEnv(t, "http://proxy.example:3128", "*")
		proxy, err := runnerProxyFor(dest)
		if err != nil || proxy != nil {
			t.Fatalf("runnerProxyFor with NO_PROXY=* = (%v, %v), want (nil, nil)", proxy, err)
		}
	})

	t.Run("loopback destinations are never proxied", func(t *testing.T) {
		setRunnerProxyEnv(t, "http://proxy.example:3128", "")
		for _, addr := range []string{"127.0.0.1:8080", "localhost:8080", "[::1]:8080"} {
			proxy, err := runnerProxyFor(addr)
			if err != nil || proxy != nil {
				t.Fatalf("runnerProxyFor(%q) = (%v, %v), want (nil, nil): net/http never proxies a loopback destination", addr, proxy, err)
			}
		}
	})

	t.Run("socks5 is refused", func(t *testing.T) {
		setRunnerProxyEnv(t, "socks5://proxy.example:1080", "")
		_, err := runnerProxyFor(dest)
		if err == nil {
			t.Fatal("runnerProxyFor accepted a socks5 proxy; neither runner transport can speak socks5")
		}
		if !strings.Contains(err.Error(), "socks5") {
			t.Fatalf("error %q does not name the unsupported scheme", err)
		}
	})

	t.Run("malformed value is ignored", func(t *testing.T) {
		// httpproxy drops a value it cannot parse and reports no proxy, so the
		// runner dials direct — the same thing http.ProxyFromEnvironment does
		// with a typo'd HTTPS_PROXY.
		setRunnerProxyEnv(t, "http://proxy-user:proxy-pass@bad\x01host:3128", "")
		proxy, err := runnerProxyFor(dest)
		if err != nil || proxy != nil {
			t.Fatalf("runnerProxyFor with a malformed value = (%v, %v), want (nil, nil)", proxy, err)
		}
	})
}

// TestRunnerProxyErrorTextRedactsCredentials pins the no-credentials-in-logs
// rule for the error channel httpproxy documents for a malformed proxy value:
// its message quotes the raw value, which may carry userinfo.
func TestRunnerProxyErrorTextRedactsCredentials(t *testing.T) {
	raw := "http://proxy-user:proxy-pass@bad\x01host:3128"
	// The shape httpproxy's parseProxy produces for a value it cannot parse.
	err := fmt.Errorf("invalid proxy address %q: parse %q: net/url: invalid control character in URL", raw, raw)
	got := runnerProxyErrorText(err)
	for _, secret := range []string{"proxy-user", "proxy-pass"} {
		if strings.Contains(got, secret) {
			t.Fatalf("redacted error %q still contains %q", got, secret)
		}
	}
	if !strings.Contains(got, "<redacted>") {
		t.Fatalf("redacted error %q does not mark where the value was cut", got)
	}
}

// TestRunnerProxyLoopbackOriginIsNeverProxied is the loopback half of the
// resolution rules, driven through the real client: with HTTPS_PROXY set and
// NO_PROXY empty, a request to a loopback control plane still succeeds, and
// the proxy saw nothing at all.
func TestRunnerProxyLoopbackOriginIsNeverProxied(t *testing.T) {
	ca, caPEM, caKey := generateCredReloadTestCA(t, "loopback bypass CA")
	caPath := writeCredReloadPEM(t, filepath.Join(t.TempDir(), "ca.pem"), caPEM)
	srv := newServerCATestServer(t, ca, caKey)
	defer srv.Close()

	proxy := newFakeProxyTunnelProxy(t, "")
	setRunnerProxyEnv(t, proxy.url(), "")

	client, err := newReloadableRunnerHTTPClient(mustTestCredentialReloader(t, RunnerConfig{TLSServerCA: caPath}), 10*time.Second)
	if err != nil {
		t.Fatalf("newReloadableRunnerHTTPClient: %v", err)
	}
	defer client.CloseIdleConnections()

	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("request to a loopback control plane with a proxy configured: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}
	if got := proxy.connectLog(); len(got) != 0 {
		t.Fatalf("proxy saw %+v, want nothing: a loopback destination is never proxied", got)
	}
}

// TestRunnerProxyTunnelsTheReloadableClient is the core #27 contract, driven
// through the client the runner actually uses for the control plane: with
// HTTPS_PROXY set, the request reaches a private-CA origin through the proxy's
// CONNECT tunnel, with the handshake verified against the configured CA pool
// inside the tunnel — the combination that used to be impossible, since
// net/http's own proxy path would verify against TLSClientConfig and bypass
// the reloadable dial.
func TestRunnerProxyTunnelsTheReloadableClient(t *testing.T) {
	ca, caPEM, caKey := generateCredReloadTestCA(t, "proxy tunnel CA")
	caPath := writeCredReloadPEM(t, filepath.Join(t.TempDir(), "ca.pem"), caPEM)
	const hostname = "xflow-proxy-test.invalid"
	srv, urlStr := newProxyTunnelTestOrigin(t, hostname, ca, caKey)

	proxy := newFakeProxyTunnelProxy(t, srv.Listener.Addr().String())
	setRunnerProxyEnv(t, proxy.url(), "")

	client, err := newReloadableRunnerHTTPClient(mustTestCredentialReloader(t, RunnerConfig{TLSServerCA: caPath}), 10*time.Second)
	if err != nil {
		t.Fatalf("newReloadableRunnerHTTPClient: %v", err)
	}
	defer client.CloseIdleConnections()

	resp, err := client.Get(urlStr)
	if err != nil {
		t.Fatalf("request through the environment proxy: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}

	log := proxy.connectLog()
	if len(log) != 1 {
		t.Fatalf("proxy saw %d CONNECT requests, want 1: %+v", len(log), log)
	}
	if want := strings.TrimPrefix(urlStr, "https://"); log[0].target != want {
		t.Fatalf("CONNECT target = %q, want %q", log[0].target, want)
	}
	if plain := proxy.plainLog(); len(plain) != 0 {
		t.Fatalf("proxy saw non-CONNECT traffic %v; an HTTPS origin must arrive as a tunnel", plain)
	}
}

// TestRunnerProxyReloadedCAPoolAppliesInsideTheTunnel composes the two
// guarantees the tunnel exists to preserve: the request must go through the
// proxy, AND the CA pool it verifies against must be the one the reloader
// holds at dial time. The origin's leaf is signed by caNew; a client trusting
// only caOld fails, and after a Reload — same *http.Client, no rebuild — the
// next tunnelled connection succeeds.
func TestRunnerProxyReloadedCAPoolAppliesInsideTheTunnel(t *testing.T) {
	dir := t.TempDir()
	_, caOldPEM, _ := generateCredReloadTestCA(t, "proxy reload CA old")
	caNew, caNewPEM, caNewKey := generateCredReloadTestCA(t, "proxy reload CA new")
	caOldPath := writeCredReloadPEM(t, filepath.Join(dir, "ca-old.pem"), caOldPEM)
	caNewPath := writeCredReloadPEM(t, filepath.Join(dir, "ca-new.pem"), caNewPEM)

	const hostname = "xflow-proxy-test.invalid"
	srv, urlStr := newProxyTunnelTestOrigin(t, hostname, caNew, caNewKey)
	proxy := newFakeProxyTunnelProxy(t, srv.Listener.Addr().String())
	setRunnerProxyEnv(t, proxy.url(), "")

	reloader := mustTestCredentialReloader(t, RunnerConfig{TLSServerCA: caOldPath})
	client, err := newReloadableRunnerHTTPClient(reloader, 10*time.Second)
	if err != nil {
		t.Fatalf("newReloadableRunnerHTTPClient: %v", err)
	}
	defer client.CloseIdleConnections()

	if resp, err := client.Get(urlStr); err == nil {
		_ = resp.Body.Close()
		t.Fatal("request trusting only caOld succeeded; the origin's leaf is signed by caNew")
	}
	if got := len(proxy.connectLog()); got != 1 {
		t.Fatalf("proxy saw %d CONNECT requests before the reload, want 1 (the tunnel was opened, then the handshake inside it failed)", got)
	}

	if err := reloader.Reload(CredentialReloaderSource{TLSServerCA: caNewPath}); err != nil {
		t.Fatalf("Reload: %v", err)
	}
	client.CloseIdleConnections()

	resp, err := client.Get(urlStr)
	if err != nil {
		t.Fatalf("request through the tunnel after reloading TLSServerCA to caNew: %v (the tunnelled dial did not read the live CA pool)", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}
	if got := len(proxy.connectLog()); got != 2 {
		t.Fatalf("proxy saw %d CONNECT requests, want 2: one per dial", got)
	}
}

// TestRunnerProxyRefusedConnectFailsWithoutLeakingCredentials covers a proxy
// that turns the tunnel down (407): the request must fail with the proxy's
// refusal named, the Proxy-Authorization header must actually have been sent,
// and the proxy's credential must not appear anywhere in the error.
func TestRunnerProxyRefusedConnectFailsWithoutLeakingCredentials(t *testing.T) {
	ca, caPEM, caKey := generateCredReloadTestCA(t, "proxy refusal CA")
	caPath := writeCredReloadPEM(t, filepath.Join(t.TempDir(), "ca.pem"), caPEM)
	const hostname = "xflow-proxy-test.invalid"
	srv, urlStr := newProxyTunnelTestOrigin(t, hostname, ca, caKey)

	proxy := newFakeProxyTunnelProxy(t, srv.Listener.Addr().String())
	proxy.setRefuse(true)
	const user, password = "proxy-user", "proxy-pass"
	setRunnerProxyEnv(t, fakeProxyURLWithUserinfo(t, proxy.url(), user, password), "")

	client, err := newReloadableRunnerHTTPClient(mustTestCredentialReloader(t, RunnerConfig{TLSServerCA: caPath}), 10*time.Second)
	if err != nil {
		t.Fatalf("newReloadableRunnerHTTPClient: %v", err)
	}
	defer client.CloseIdleConnections()

	_, err = client.Get(urlStr)
	if err == nil {
		t.Fatal("request succeeded through a proxy that refused CONNECT")
	}
	msg := err.Error()
	if !strings.Contains(msg, "refused CONNECT") || !strings.Contains(msg, "407") {
		t.Fatalf("error %q does not name the proxy's refusal and status", msg)
	}
	auth := "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+password))
	for _, secret := range []string{user, password, user + ":" + password, auth} {
		if strings.Contains(msg, secret) {
			t.Fatalf("error %q contains proxy credential material %q", msg, secret)
		}
	}

	log := proxy.connectLog()
	if len(log) != 1 {
		t.Fatalf("proxy saw %d CONNECT requests, want 1: %+v", len(log), log)
	}
	if want := strings.TrimPrefix(urlStr, "https://"); log[0].target != want {
		t.Fatalf("CONNECT target = %q, want %q", log[0].target, want)
	}
	if log[0].auth != auth {
		t.Fatalf("proxy saw Proxy-Authorization %q, want the HTTPS_PROXY userinfo as %q", log[0].auth, auth)
	}
}

// TestRunnerProxyTunnelsTheStaticClient pins the same behaviour for the
// static client newRunnerHTTPClient builds (and NewRunnerHTTPClient exports
// for sdk/runner's enrollment bootstrap): a one-shot caller gets the
// environment proxy too, so enrollment cannot fail on exactly the deployments
// that need the proxy.
func TestRunnerProxyTunnelsTheStaticClient(t *testing.T) {
	ca, caPEM, caKey := generateCredReloadTestCA(t, "static proxy CA")
	caPath := writeCredReloadPEM(t, filepath.Join(t.TempDir(), "ca.pem"), caPEM)
	const hostname = "xflow-proxy-test.invalid"
	srv, urlStr := newProxyTunnelTestOrigin(t, hostname, ca, caKey)

	proxy := newFakeProxyTunnelProxy(t, srv.Listener.Addr().String())
	setRunnerProxyEnv(t, proxy.url(), "")

	client, err := newRunnerHTTPClient(RunnerConfig{ServerURL: urlStr, TLSServerCA: caPath}, 10*time.Second)
	if err != nil {
		t.Fatalf("newRunnerHTTPClient: %v", err)
	}
	defer client.CloseIdleConnections()

	resp, err := client.Get(urlStr)
	if err != nil {
		t.Fatalf("request through the environment proxy: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}
	log := proxy.connectLog()
	if len(log) != 1 {
		t.Fatalf("proxy saw %d CONNECT requests, want 1: %+v", len(log), log)
	}
	if want := strings.TrimPrefix(urlStr, "https://"); log[0].target != want {
		t.Fatalf("CONNECT target = %q, want %q", log[0].target, want)
	}
}

// TestRunnerProxyPlaintextDialDoesNotOpenATunnel pins the plaintext branch of
// dialRunnerConn: with no TLS configuration the connection is dialled
// directly, exactly as before this change. A plaintext client's proxying is
// net/http's own business (and only applies to an http:// origin), so this
// layer must not open a tunnel for a request that asks for none.
func TestRunnerProxyPlaintextDialDoesNotOpenATunnel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	proxy := newFakeProxyTunnelProxy(t, "")
	setRunnerProxyEnv(t, proxy.url(), "")

	addr := strings.TrimPrefix(srv.URL, "http://")
	conn, err := dialRunnerConn(context.Background(), newRunnerDialer(), "tcp", addr, nil)
	if err != nil {
		t.Fatalf("dialRunnerConn with no TLS configuration: %v", err)
	}
	defer conn.Close()

	if _, err := fmt.Fprintf(conn, "GET / HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n", addr); err != nil {
		t.Fatalf("write request over the direct connection: %v", err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("read response over the direct connection: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}
	if got := proxy.connectLog(); len(got) != 0 {
		t.Fatalf("plaintext dial opened %d proxy tunnels, want 0: %+v", len(got), got)
	}
}
