package xflow

import (
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// This file pins the two scheme rules the tunnel work left uncovered: the
// https:// proxy leg (its own TLS handshake against the system roots, no client
// certificate) and the deliberate bypass for plaintext requests.

// newFakeTLSProxyTunnelProxy is the https:// proxy twin of
// newFakeProxyTunnelProxy: the same CONNECT relay behind a TLS listener whose
// certificate is signed by httptest's in-memory CA — trusted by nothing.
func newFakeTLSProxyTunnelProxy(t *testing.T) *fakeProxyTunnelProxy {
	t.Helper()
	p := &fakeProxyTunnelProxy{}
	p.srv = httptest.NewTLSServer(http.HandlerFunc(p.handle))
	// A client that (correctly) rejects the certificate aborts the handshake,
	// which the server would otherwise log to the test's output.
	p.srv.Config.ErrorLog = log.New(io.Discard, "", 0)
	t.Cleanup(func() {
		p.closeTunnels()
		p.srv.Close()
	})
	return p
}

// TestRunnerProxyTLSProxyIsVerifiedAgainstSystemRoots pins the https:// arm of
// dialConnectTunnel: the proxy leg runs its own TLS handshake — against the
// system roots, presenting no client certificate — so a proxy whose certificate
// no system pool trusts must be rejected before a single byte of CONNECT is
// written. Asserting the failure SHAPE, not just "an error", is what proves the
// handshake ran and was checked: the dial reached the proxy (not a refusal, not
// an unreachable address), and verification is what turned it away — the
// hermetic-testable half of the branch, since no test can mint a certificate the
// host system trusts.
func TestRunnerProxyTLSProxyIsVerifiedAgainstSystemRoots(t *testing.T) {
	ca, caPEM, caKey := generateCredReloadTestCA(t, "tls proxy CA")
	caPath := writeCredReloadPEM(t, filepath.Join(t.TempDir(), "ca.pem"), caPEM)
	const hostname = "xflow-proxy-test.invalid"
	_, urlStr := newProxyTunnelTestOrigin(t, hostname, ca, caKey)

	proxy := newFakeTLSProxyTunnelProxy(t)
	const user, password = "proxy-user", "proxy-pass"
	setRunnerProxyEnv(t, fakeProxyURLWithUserinfo(t, proxy.url(), user, password), "")

	client, err := newReloadableRunnerHTTPClient(mustTestCredentialReloader(t, RunnerConfig{TLSServerCA: caPath}), 10*time.Second)
	if err != nil {
		t.Fatalf("newReloadableRunnerHTTPClient: %v", err)
	}
	defer client.CloseIdleConnections()

	resp, err := client.Get(urlStr)
	if err == nil {
		_ = resp.Body.Close()
		t.Fatal("request through an https:// proxy with an untrusted certificate succeeded; " +
			"the proxy leg must verify the handshake against the system roots")
	}
	msg := err.Error()
	if !strings.Contains(msg, "tls handshake with proxy") {
		t.Fatalf("error %q does not name the proxy TLS handshake", msg)
	}
	if !strings.Contains(msg, "certificate") {
		t.Fatalf("error %q does not read as a certificate-verification failure", msg)
	}
	for _, secret := range []string{user, password} {
		if strings.Contains(msg, secret) {
			t.Fatalf("error %q contains proxy credential material %q", msg, secret)
		}
	}
	if got := proxy.connectLog(); len(got) != 0 {
		t.Fatalf("proxy saw %d CONNECT requests despite its untrusted certificate: %+v", len(got), got)
	}
	if got := proxy.plainLog(); len(got) != 0 {
		t.Fatalf("proxy saw plain traffic %v; nothing may be written before the proxy handshake verifies", got)
	}
}

// TestRunnerProxyPlaintextRequestBypassesTheProxy pins the deliberate asymmetry
// for a plain http:// origin: the runner's reloadable transport sets Proxy =
// nil, so net/http never consults HTTP_PROXY for a plaintext request — the dial
// layer refuses to open a tunnel for one, and a request that asks for no tunnel
// must not be handed to a proxy by another layer either. The Proxy = nil
// assertion is what makes the observation decisive rather than suggestive: a
// loopback origin is exempt from proxying even with a Proxy function installed,
// so "the fake proxy saw nothing" alone could not tell a bypass from the
// exemption.
func TestRunnerProxyPlaintextRequestBypassesTheProxy(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	proxy := newFakeProxyTunnelProxy(t, "")
	setRunnerProxyEnv(t, proxy.url(), "")

	client, err := newReloadableRunnerHTTPClient(mustTestCredentialReloader(t, RunnerConfig{}), 10*time.Second)
	if err != nil {
		t.Fatalf("newReloadableRunnerHTTPClient: %v", err)
	}
	defer client.CloseIdleConnections()

	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("client transport = %T, want *http.Transport", client.Transport)
	}
	if transport.Proxy != nil {
		t.Fatal("the runner transport installs a Proxy function; a plaintext request would be " +
			"handed to the environment proxy even though the dial layer refuses to tunnel for one")
	}

	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("plaintext request with a proxy configured: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}
	if got := proxy.connectLog(); len(got) != 0 {
		t.Fatalf("proxy saw CONNECT requests %+v for a plaintext request", got)
	}
	if got := proxy.plainLog(); len(got) != 0 {
		t.Fatalf("proxy saw plain requests %v; the runner's plaintext traffic bypasses it", got)
	}
}
