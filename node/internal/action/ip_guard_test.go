package action

import (
	"context"
	"crypto/tls"
	"database/sql"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/structpb"
)

// withAddressGuard installs the dial-time guard for one test and removes it
// afterwards. Tests in this package run sequentially, so the process-wide
// install cannot leak into a concurrent test.
func installAddressGuard(t *testing.T, allow []string) {
	t.Helper()
	SetHTTPPrivateIPBlocking(true, allow)
	t.Cleanup(func() { SetHTTPPrivateIPBlocking(false, nil) })
}

// withLookupHostIP replaces the DNS seam for one test.
func withLookupHostIP(t *testing.T, fn func(context.Context, string) ([]netip.Addr, error)) {
	t.Helper()
	previous := lookupHostIP
	lookupHostIP = fn
	t.Cleanup(func() { lookupHostIP = previous })
}

func serverPort(t *testing.T, srv *httptest.Server) string {
	t.Helper()
	_, port, err := net.SplitHostPort(srv.Listener.Addr().String())
	if err != nil {
		t.Fatalf("split httptest listener address: %v", err)
	}
	return port
}

func executeHTTP(t *testing.T, ctx context.Context, method, rawURL string) (*types.Output, error) {
	t.Helper()
	n := &HTTPNode{Method: method, URL: rawURL}
	return n.Execute(ctx, &types.Input{Params: n.RawParams().(map[string]any)})
}

func TestBlockedAddressClassification(t *testing.T) {
	for _, tt := range []struct {
		addr    string
		blocked bool
	}{
		{addr: "127.0.0.1", blocked: true},
		{addr: "127.255.255.254", blocked: true},
		{addr: "10.0.0.1", blocked: true},
		{addr: "172.16.0.1", blocked: true},
		{addr: "172.31.255.255", blocked: true},
		{addr: "192.168.1.1", blocked: true},
		{addr: "169.254.169.254", blocked: true},
		{addr: "::1", blocked: true},
		{addr: "fc00::1", blocked: true},
		{addr: "fd12:3456::1", blocked: true},
		{addr: "fe80::1", blocked: true},
		{addr: "0.0.0.0", blocked: true},
		{addr: "::", blocked: true},
		{addr: "224.0.0.1", blocked: true},
		{addr: "ff02::1", blocked: true},
		{addr: "100.64.0.1", blocked: true},
		{addr: "100.100.100.200", blocked: true},
		{addr: "198.18.0.1", blocked: true},
		{addr: "192.0.0.192", blocked: true},
		{addr: "240.0.0.1", blocked: true},
		{addr: "255.255.255.255", blocked: true},
		{addr: "0.1.2.3", blocked: true},
		{addr: "0.255.255.255", blocked: true},
		{addr: "64:ff9b::1", blocked: true},
		{addr: "64:ff9b::a00:1", blocked: true},
		{addr: "2002::1", blocked: true},
		{addr: "2002:a00:1::", blocked: true},
		{addr: "2001::1", blocked: true},
		{addr: "::ffff:127.0.0.1", blocked: true},
		{addr: "::ffff:10.0.0.1", blocked: true},
		{addr: "8.8.8.8", blocked: false},
		{addr: "1.1.1.1", blocked: false},
		{addr: "203.0.113.10", blocked: false},
		{addr: "2001:4860:4860::8888", blocked: false},
		{addr: "::ffff:8.8.8.8", blocked: false},
	} {
		t.Run(tt.addr, func(t *testing.T) {
			addr := netip.MustParseAddr(tt.addr)
			if got := blockedAddress(addr); got != tt.blocked {
				t.Fatalf("blockedAddress(%s) = %t, want %t", tt.addr, got, tt.blocked)
			}
		})
	}
}

func TestSetHTTPPrivateIPBlockingInstallsAndRemoves(t *testing.T) {
	SetHTTPPrivateIPBlocking(false, nil)
	if currentAddressGuard() != nil {
		t.Fatal("guard is installed while disabled")
	}
	SetHTTPPrivateIPBlocking(true, []string{".internal.test"})
	t.Cleanup(func() { SetHTTPPrivateIPBlocking(false, nil) })
	guard := currentAddressGuard()
	if guard == nil {
		t.Fatal("guard was not installed")
	}
	if !guard.hostExempt("api.internal.test") {
		t.Fatal("allowlisted host is not exempt")
	}
	if !guard.hostExempt("internal.test") {
		t.Fatal("suffix allowlist does not include its apex")
	}
	if guard.hostExempt("other.test") {
		t.Fatal("host outside the allowlist is exempt")
	}
}

func TestSetHTTPPrivateIPBlockingInvalidAllowlistFailsClosed(t *testing.T) {
	// "*.127.0.0.1" is rejected by the host-pattern compiler. The guard must
	// still block non-public addresses, with no exemptions, rather than
	// dropping the policy or treating the malformed entry as a match.
	installAddressGuard(t, []string{"*.127.0.0.1"})
	guard := currentAddressGuard()
	if guard == nil {
		t.Fatal("guard not installed for a malformed allowlist")
	}
	if err := guard.checkResolved([]netip.Addr{netip.MustParseAddr("127.0.0.1")}, "127.0.0.1"); err == nil {
		t.Fatal("malformed allowlist exempted a loopback address")
	}
}

// TestHTTPAddressGuardRejectsPrivateResolutionBeforeRequest pins the core
// HTTP behavior: the destination is resolved after the request is built, the
// non-public answer is refused, and no request is sent to the server.
func TestHTTPAddressGuardRejectsPrivateResolutionBeforeRequest(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	installAddressGuard(t, nil)
	var lookups atomic.Int32
	withLookupHostIP(t, func(_ context.Context, host string) ([]netip.Addr, error) {
		lookups.Add(1)
		if host != "blocked.test" {
			return nil, errors.New("unexpected host")
		}
		return []netip.Addr{netip.MustParseAddr("127.0.0.1")}, nil
	})

	out, err := executeHTTP(t, context.Background(), "GET", "http://blocked.test:"+serverPort(t, srv))
	if err == nil {
		t.Fatal("expected a permanent error for a loopback destination, got nil")
	}
	if out != nil {
		t.Fatalf("expected nil output, got %+v", out)
	}
	if !types.IsPermanent(err) {
		t.Fatalf("address rejection must be permanent, got %v", err)
	}
	var ce *types.ClassifiedError
	if !errors.As(err, &ce) || ce.Code != "http.address_not_allowed" {
		t.Fatalf("expected code=http.address_not_allowed, got %T %v", err, err)
	}
	if strings.Contains(err.Error(), "127.0.0.1") {
		t.Fatalf("error leaks the resolved address: %v", err)
	}
	if hits.Load() != 0 {
		t.Fatalf("request reached the server %d times, want 0", hits.Load())
	}
	if lookups.Load() != 1 {
		t.Fatalf("resolved %d times, want 1", lookups.Load())
	}
}

// TestHTTPAddressGuardAllowlistedHostMayResolvePrivate pins the allowlist
// exemption: a host the operator named is dialed wherever it resolves.
func TestHTTPAddressGuardAllowlistedHostMayResolvePrivate(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	installAddressGuard(t, []string{"internal.test"})
	withLookupHostIP(t, func(_ context.Context, host string) ([]netip.Addr, error) {
		if host != "internal.test" {
			return nil, errors.New("unexpected host")
		}
		return []netip.Addr{netip.MustParseAddr("127.0.0.1")}, nil
	})

	out, err := executeHTTP(t, context.Background(), "GET", "http://internal.test:"+serverPort(t, srv))
	if err != nil {
		t.Fatalf("allowlisted host must be dialed, got %v", err)
	}
	if out.Data["status"] != http.StatusOK {
		t.Fatalf("status = %v, want 200", out.Data["status"])
	}
	if hits.Load() != 1 {
		t.Fatalf("server saw %d requests, want 1", hits.Load())
	}
}

// TestHTTPAddressGuardRejectsRedirectToPrivate pins redirect coverage: the
// first hop is allowlisted, the redirect target is not, and the second hop is
// refused before it reaches the target server.
func TestHTTPAddressGuardRejectsRedirectToPrivate(t *testing.T) {
	var targetHits atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetHits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	var firstHits atomic.Int32
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		firstHits.Add(1)
		http.Redirect(w, r, "http://redirected.test:"+serverPort(t, target), http.StatusFound)
	}))
	defer first.Close()

	installAddressGuard(t, []string{"internal.test"})
	withLookupHostIP(t, func(_ context.Context, host string) ([]netip.Addr, error) {
		switch host {
		case "internal.test", "redirected.test":
			return []netip.Addr{netip.MustParseAddr("127.0.0.1")}, nil
		default:
			return nil, errors.New("unexpected host")
		}
	})

	_, err := executeHTTP(t, context.Background(), "GET", "http://internal.test:"+serverPort(t, first))
	if err == nil {
		t.Fatal("expected the redirect to a private destination to be refused")
	}
	if !types.IsPermanent(err) {
		t.Fatalf("redirect rejection must be permanent, got %v", err)
	}
	var ce *types.ClassifiedError
	if !errors.As(err, &ce) || ce.Code != "http.address_not_allowed" {
		t.Fatalf("expected code=http.address_not_allowed, got %T %v", err, err)
	}
	if firstHits.Load() != 1 {
		t.Fatalf("first hop saw %d requests, want 1", firstHits.Load())
	}
	if targetHits.Load() != 0 {
		t.Fatalf("redirect target saw %d requests, want 0", targetHits.Load())
	}
}

// TestHTTPAddressGuardRejectsWhenAnyRecordIsPrivate pins the all-records rule:
// one non-public answer rejects the host before any of the addresses is
// dialed, so a multi-record answer cannot be used to smuggle a private
// destination through a public record.
func TestHTTPAddressGuardRejectsWhenAnyRecordIsPrivate(t *testing.T) {
	installAddressGuard(t, nil)
	var lookups atomic.Int32
	withLookupHostIP(t, func(_ context.Context, host string) ([]netip.Addr, error) {
		lookups.Add(1)
		if host != "mixed.test" {
			return nil, errors.New("unexpected host")
		}
		return []netip.Addr{
			netip.MustParseAddr("203.0.113.10"),
			netip.MustParseAddr("127.0.0.1"),
		}, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := executeHTTP(t, ctx, "GET", "http://mixed.test:443")
	if err == nil {
		t.Fatal("expected a mixed public/private answer to be refused")
	}
	if !types.IsPermanent(err) {
		t.Fatalf("mixed-answer rejection must be permanent, got %v", err)
	}
	if lookups.Load() != 1 {
		t.Fatalf("resolved %d times, want 1", lookups.Load())
	}
}

// TestHTTPAddressGuardDisabledByDefault pins backward compatibility for
// embedded use: with no guard installed a loopback destination is reachable.
func TestHTTPAddressGuardDisabledByDefault(t *testing.T) {
	if currentAddressGuard() != nil {
		t.Fatal("guard must be disabled unless installed")
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	out, err := executeHTTP(t, context.Background(), "GET", srv.URL)
	if err != nil {
		t.Fatalf("loopback request without a guard must succeed, got %v", err)
	}
	if out.Data["status"] != http.StatusOK {
		t.Fatalf("status = %v, want 200", out.Data["status"])
	}
}

// TestHTTPAddressGuardDialsCheckedIPWithOriginalServerName pins the TLS half
// of the dial contract: the connection goes to the checked address while SNI
// (and therefore certificate verification in production) still names the
// original host from the URL.
func TestHTTPAddressGuardDialsCheckedIPWithOriginalServerName(t *testing.T) {
	var serverName atomic.Value
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	srv.StartTLS()
	defer srv.Close()
	srv.TLS.GetConfigForClient = func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
		serverName.Store(hello.ServerName)
		return nil, nil
	}

	originalClient := DefaultHTTPClient
	DefaultHTTPClient = &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // test-only: the assertion is SNI, not trust
	}}
	t.Cleanup(func() { DefaultHTTPClient = originalClient })

	// The test server is necessarily on loopback, so the hostname is
	// allowlisted to reach it; the assertion here is that the guarded dialer
	// still uses the URL host for TLS while connecting to the resolved IP.
	installAddressGuard(t, []string{"secure.internal.test"})
	withLookupHostIP(t, func(_ context.Context, host string) ([]netip.Addr, error) {
		if host != "secure.internal.test" {
			return nil, errors.New("unexpected host")
		}
		return []netip.Addr{netip.MustParseAddr("127.0.0.1")}, nil
	})

	out, err := executeHTTP(t, context.Background(), "GET", "https://secure.internal.test:"+serverPort(t, srv))
	if err != nil {
		t.Fatalf("guarded TLS request failed: %v", err)
	}
	if out.Data["status"] != http.StatusOK {
		t.Fatalf("status = %v, want 200", out.Data["status"])
	}
	if got, _ := serverName.Load().(string); got != "secure.internal.test" {
		t.Fatalf("TLS ServerName = %q, want the original URL host", got)
	}
}

// TestGRPCAddressGuardDialerRejectsPrivate pins the gRPC dial hook: an address
// handed over by the resolver is refused when it is not public, and the error
// names the requested target rather than leaking the resolved address.
func TestGRPCAddressGuardDialerRejectsPrivate(t *testing.T) {
	installAddressGuard(t, nil)
	guard := currentAddressGuard()
	if guard == nil {
		t.Fatal("guard not installed")
	}
	_, err := guard.grpcDialer("grpc-private.test")(context.Background(), "10.0.0.1:443")
	if err == nil {
		t.Fatal("expected a private address to be refused")
	}
	if !errors.Is(err, errBlockedAddress) {
		t.Fatalf("error %v does not match errBlockedAddress", err)
	}
	if strings.Contains(err.Error(), "10.0.0.1") {
		t.Fatalf("error leaks the resolved address: %v", err)
	}
}

// TestGRPCAddressGuardDialerAllowsAllowlistedTarget pins the gRPC allowlist
// exemption. gRPC's resolver hands the dialer an IP, so the exemption must be
// decided on the original target host; a real listener proves the connection
// is actually made.
func TestGRPCAddressGuardDialerAllowsAllowlistedTarget(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = listener.Close() }()
	accepted := make(chan struct{})
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			_ = conn.Close()
			close(accepted)
		}
	}()

	installAddressGuard(t, []string{"grpc-allowed.test"})
	guard := currentAddressGuard()
	if guard == nil {
		t.Fatal("guard not installed")
	}
	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("split listener address: %v", err)
	}
	conn, err := guard.grpcDialer("grpc-allowed.test")(context.Background(), net.JoinHostPort("127.0.0.1", port))
	if err != nil {
		t.Fatalf("allowlisted target was refused: %v", err)
	}
	_ = conn.Close()
	select {
	case <-accepted:
	case <-time.After(2 * time.Second):
		t.Fatal("listener never accepted the guarded connection")
	}
}

// TestWithAddressGuardRefusesCustomTLSDialers pins both TLS dialer hooks. A
// transport with only the deprecated DialTLS set used to slip through the
// DialTLSContext-only check, and net/http falls back to it for https, so the
// request bypassed the guarded DialContext entirely.
func TestWithAddressGuardRefusesCustomTLSDialers(t *testing.T) {
	installAddressGuard(t, nil)
	guard := currentAddressGuard()
	if guard == nil {
		t.Fatal("guard not installed")
	}
	for _, tt := range []struct {
		name string
		base *http.Transport
	}{
		{
			name: "DialTLSContext",
			base: &http.Transport{DialTLSContext: func(context.Context, string, string) (net.Conn, error) {
				return nil, errors.New("unused")
			}},
		},
		{
			name: "DialTLS",
			base: &http.Transport{DialTLS: func(string, string) (net.Conn, error) {
				return nil, errors.New("unused")
			}},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := withAddressGuard(tt.base, guard)
			if err == nil {
				t.Fatalf("withAddressGuard accepted a transport with %s set", tt.name)
			}
			if !types.IsPermanent(err) {
				t.Fatalf("guard-unavailable error must be permanent, got %v", err)
			}
			var ce *types.ClassifiedError
			if !errors.As(err, &ce) || ce.Code != "http.address_guard_unavailable" {
				t.Fatalf("expected code=http.address_guard_unavailable, got %T %v", err, err)
			}
		})
	}
}

// withDialOne replaces the per-attempt dial seam for one test.
func withDialOne(t *testing.T, fn func(context.Context, string, string) (net.Conn, error)) {
	t.Helper()
	previous := dialOne
	dialOne = fn
	t.Cleanup(func() { dialOne = previous })
}

func mustAddr(t *testing.T, raw string) netip.Addr {
	t.Helper()
	addr, err := netip.ParseAddr(raw)
	if err != nil {
		t.Fatalf("parse address %q: %v", raw, err)
	}
	return addr
}

// TestInterleaveAddressesPrefersIPv6AndAlternates pins the RFC 8305 order:
// IPv6 first, then one address from each family in turn, preserving order
// within a family. IPv4-mapped IPv6 records count as IPv4.
func TestInterleaveAddressesPrefersIPv6AndAlternates(t *testing.T) {
	got := interleaveAddresses([]netip.Addr{
		mustAddr(t, "203.0.113.1"),
		mustAddr(t, "::ffff:203.0.113.9"),
		mustAddr(t, "2001:db8::1"),
		mustAddr(t, "203.0.113.2"),
		mustAddr(t, "2001:db8::2"),
	})
	want := []netip.Addr{
		mustAddr(t, "2001:db8::1"),
		mustAddr(t, "203.0.113.1"),
		mustAddr(t, "2001:db8::2"),
		mustAddr(t, "::ffff:203.0.113.9"),
		mustAddr(t, "203.0.113.2"),
	}
	if len(got) != len(want) {
		t.Fatalf("interleaveAddresses returned %d addresses, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("interleaveAddresses[%d] = %s, want %s", i, got[i], want[i])
		}
	}
}

// TestDialResolvedStaggersAttemptsAndFirstSuccessWins pins the Happy Eyeballs
// behavior: the first address is dialed immediately, the second starts one
// stagger delay later even though the first has not failed, and the first
// success is returned while the loser is canceled.
func TestDialResolvedStaggersAttemptsAndFirstSuccessWins(t *testing.T) {
	var mu sync.Mutex
	var starts []time.Time

	withDialOne(t, func(ctx context.Context, _, address string) (net.Conn, error) {
		mu.Lock()
		starts = append(starts, time.Now())
		mu.Unlock()
		if strings.HasPrefix(address, "[2001:db8::1]") {
			// A blackholed first family: the connection attempt hangs until the
			// winner cancels the shared dial context.
			<-ctx.Done()
			return nil, ctx.Err()
		}
		client, server := net.Pipe()
		_ = server.Close()
		return client, nil
	})

	started := time.Now()
	conn, err := dialResolved(context.Background(), "tcp", "443", []netip.Addr{
		mustAddr(t, "2001:db8::1"),
		mustAddr(t, "203.0.113.10"),
	})
	elapsed := time.Since(started)
	if err != nil {
		t.Fatalf("dialResolved: %v", err)
	}
	_ = conn.Close()

	if elapsed < dialAttemptStagger {
		t.Fatalf("dialResolved returned after %s, before the stagger delay: the second address was dialed immediately", elapsed)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("dialResolved took %s; the second address did not win promptly", elapsed)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(starts) != 2 {
		t.Fatalf("dial attempts = %d, want 2", len(starts))
	}
	if gap := starts[1].Sub(starts[0]); gap < 250*time.Millisecond || gap > 2*time.Second {
		t.Fatalf("second attempt started %s after the first, want about %s", gap, dialAttemptStagger)
	}
}

// TestDialResolvedReturnsPromptlyOnContextCancel pins that the caller's ctx
// bounds the whole race, not one attempt.
func TestDialResolvedReturnsPromptlyOnContextCancel(t *testing.T) {
	withDialOne(t, func(ctx context.Context, _, _ string) (net.Conn, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	started := time.Now()
	_, err := dialResolved(ctx, "tcp", "443", []netip.Addr{
		mustAddr(t, "2001:db8::1"),
		mustAddr(t, "203.0.113.10"),
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("dialResolved error = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("dialResolved returned after %s, want a prompt cancellation", elapsed)
	}
}

// TestDialResolvedJoinsEveryAddressError pins the all-failed aggregate: the
// caller gets one error that still matches every attempt's cause.
func TestDialResolvedJoinsEveryAddressError(t *testing.T) {
	errV6 := errors.New("v6 unreachable")
	errV4 := errors.New("v4 unreachable")
	withDialOne(t, func(_ context.Context, _, address string) (net.Conn, error) {
		if strings.HasPrefix(address, "[2001:db8::1]") {
			return nil, errV6
		}
		return nil, errV4
	})

	_, err := dialResolved(context.Background(), "tcp", "443", []netip.Addr{
		mustAddr(t, "2001:db8::1"),
		mustAddr(t, "203.0.113.10"),
	})
	if err == nil {
		t.Fatal("dialResolved returned nil with every address failing")
	}
	if !errors.Is(err, errV6) || !errors.Is(err, errV4) {
		t.Fatalf("aggregate error %v does not carry both address errors", err)
	}
}

// stubResourcePool records gRPC pool acquisitions and always fails the dial.
// Only GRPC is exercised; the other methods exist to satisfy the interface.
type stubResourcePool struct {
	grpcCalls int
	// conn, when set, models a pool hit: it is returned without dialing.
	conn  *grpc.ClientConn
	dials int
}

func (p *stubResourcePool) SQL(context.Context, string, string) (*sql.DB, error) {
	return nil, errors.New("stub pool: SQL is not used")
}

func (p *stubResourcePool) GRPC(context.Context, string, bool, ...grpc.DialOption) (*grpc.ClientConn, error) {
	p.grpcCalls++
	if p.conn != nil {
		return p.conn, nil
	}
	p.dials++
	return nil, errors.New("stub pool: dial failure")
}

func (p *stubResourcePool) Close(context.Context) error { return nil }

// TestAcquireGRPCRejectsBlockedTargetPermanently pins the classification: a
// policy rejection is permanent and happens before the pool is asked to dial.
func TestAcquireGRPCRejectsBlockedTargetPermanently(t *testing.T) {
	installAddressGuard(t, nil)
	withLookupHostIP(t, func(_ context.Context, host string) ([]netip.Addr, error) {
		if host != "grpc-blocked.test" {
			return nil, errors.New("unexpected host")
		}
		return []netip.Addr{netip.MustParseAddr("10.1.2.3")}, nil
	})

	pool := &stubResourcePool{}
	ctx := types.WithResourcePool(context.Background(), pool)
	_, _, err := acquireGRPC(ctx, "grpc-blocked.test:443", false)
	if err == nil {
		t.Fatal("expected the target to be refused")
	}
	if !types.IsPermanent(err) {
		t.Fatalf("address rejection must be permanent, got %v", err)
	}
	var ce *types.ClassifiedError
	if !errors.As(err, &ce) || ce.Code != "grpc.address_not_allowed" {
		t.Fatalf("expected code=grpc.address_not_allowed, got %T %v", err, err)
	}
	if pool.grpcCalls != 0 {
		t.Fatalf("pool was asked to dial %d times, want 0", pool.grpcCalls)
	}
	if strings.Contains(err.Error(), "10.1.2.3") {
		t.Fatalf("error leaks the resolved address: %v", err)
	}
}

// TestAcquireGRPCRejectsBlockedResolverTarget pins that a resolver-style target
// is checked on the hostname it names rather than on the URL text.
func TestAcquireGRPCRejectsBlockedResolverTarget(t *testing.T) {
	installAddressGuard(t, nil)
	withLookupHostIP(t, func(_ context.Context, host string) ([]netip.Addr, error) {
		if host != "grpc-blocked.test" {
			return nil, errors.New("unexpected host")
		}
		return []netip.Addr{netip.MustParseAddr("10.1.2.3")}, nil
	})

	pool := &stubResourcePool{}
	ctx := types.WithResourcePool(context.Background(), pool)
	_, _, err := acquireGRPC(ctx, "dns:///grpc-blocked.test:443", false)
	if err == nil {
		t.Fatal("expected the resolver target to be refused")
	}
	var ce *types.ClassifiedError
	if !errors.As(err, &ce) || ce.Code != "grpc.address_not_allowed" {
		t.Fatalf("expected code=grpc.address_not_allowed, got %T %v", err, err)
	}
	if pool.grpcCalls != 0 {
		t.Fatalf("pool was asked to dial %d times, want 0", pool.grpcCalls)
	}
}

// TestAcquireGRPCAllowlistedTargetReachesThePool pins that an allowlisted host
// passes the address check and the pool is then consulted.
func TestAcquireGRPCAllowlistedTargetReachesThePool(t *testing.T) {
	installAddressGuard(t, []string{"grpc-allowed.test"})
	withLookupHostIP(t, func(_ context.Context, host string) ([]netip.Addr, error) {
		if host != "grpc-allowed.test" {
			return nil, errors.New("unexpected host")
		}
		return []netip.Addr{netip.MustParseAddr("10.1.2.3")}, nil
	})

	pool := &stubResourcePool{}
	ctx := types.WithResourcePool(context.Background(), pool)
	_, release, err := acquireGRPC(ctx, "grpc-allowed.test:443", false)
	if err == nil {
		t.Fatal("expected the stub pool's dial failure")
	}
	if types.IsPermanent(err) {
		t.Fatalf("stub dial failure should classify transient, got %v", err)
	}
	if pool.grpcCalls != 1 {
		t.Fatalf("pool was asked to dial %d times, want 1", pool.grpcCalls)
	}
	release()
}

// TestAcquireGRPCPoolHitSurvivesResolverFailure pins the DNS-blip tolerance: a
// preflight resolution failure is not a policy rejection, so a cached pool
// connection is still returned and used without a dial. The guard dialer is
// still installed, which is what keeps the security decision at dial time on a
// pool miss.
func TestAcquireGRPCPoolHitSurvivesResolverFailure(t *testing.T) {
	installAddressGuard(t, nil)
	var lookups atomic.Int32
	withLookupHostIP(t, func(context.Context, string) ([]netip.Addr, error) {
		lookups.Add(1)
		return nil, errors.New("dns is down")
	})

	cached, err := grpc.NewClient("passthrough:///127.0.0.1:1",
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("build cached ClientConn: %v", err)
	}
	t.Cleanup(func() { _ = cached.Close() })

	pool := &stubResourcePool{conn: cached}
	ctx := types.WithResourcePool(context.Background(), pool)
	conn, release, err := acquireGRPC(ctx, "grpc-cached.test:443", false)
	if err != nil {
		t.Fatalf("acquireGRPC with a pool hit and a DNS failure: %v", err)
	}
	defer release()
	if conn != cached {
		t.Fatalf("acquireGRPC returned %p, want the cached connection %p", conn, cached)
	}
	if pool.grpcCalls != 1 || pool.dials != 0 {
		t.Fatalf("pool calls/dials = %d/%d, want 1/0 (the cached connection must not be re-dialed)",
			pool.grpcCalls, pool.dials)
	}
	if lookups.Load() != 1 {
		t.Fatalf("DNS lookups = %d, want 1 (the failed preflight only; a pool hit must not dial)", lookups.Load())
	}
}

// TestAcquireGRPCPoolMissStillFailsOnResolverFailure pins that the tolerance
// above does not turn a broken target into a success: with no cached
// connection the pool dial fails and the error stays transient.
func TestAcquireGRPCPoolMissStillFailsOnResolverFailure(t *testing.T) {
	installAddressGuard(t, nil)
	withLookupHostIP(t, func(context.Context, string) ([]netip.Addr, error) {
		return nil, errors.New("dns is down")
	})

	pool := &stubResourcePool{}
	ctx := types.WithResourcePool(context.Background(), pool)
	_, release, err := acquireGRPC(ctx, "grpc-unresolvable.test:443", false)
	if err == nil {
		t.Fatal("acquireGRPC succeeded with no cached connection and a failing resolver")
	}
	if types.IsPermanent(err) {
		t.Fatalf("pool dial failure should stay transient, got %v", err)
	}
	if pool.grpcCalls != 1 || pool.dials != 1 {
		t.Fatalf("pool calls/dials = %d/%d, want 1/1", pool.grpcCalls, pool.dials)
	}
	release()
}

// dialingGRPCPool models a pool miss that really dials: it builds the client
// from the options acquireGRPC produced, so the test exercises the actual
// dialer wiring.
type dialingGRPCPool struct{ target string }

func (p *dialingGRPCPool) SQL(context.Context, string, string) (*sql.DB, error) {
	return nil, errors.New("dialing pool: SQL is not used")
}

func (p *dialingGRPCPool) GRPC(_ context.Context, _ string, _ bool, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	return grpc.NewClient(p.target, opts...)
}

func (p *dialingGRPCPool) Close(context.Context) error { return nil }

// TestAcquireGRPCUnixTargetKeepsDefaultDialer pins the non-TCP rule: a unix
// socket target gets no address-guard dialer (whose host:port split would
// reject the socket path) and no DNS preflight, so a working socket target
// keeps working with the guard enabled.
func TestAcquireGRPCUnixTargetKeepsDefaultDialer(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "grpc.sock")
	listener, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen on unix socket: %v", err)
	}
	srv := grpc.NewServer(grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
		in := &structpb.Struct{}
		if err := stream.RecvMsg(in); err != nil {
			return err
		}
		return stream.SendMsg(&structpb.Struct{})
	}))
	go func() { _ = srv.Serve(listener) }()
	t.Cleanup(srv.Stop)

	installAddressGuard(t, nil)
	var lookups atomic.Int32
	withLookupHostIP(t, func(context.Context, string) ([]netip.Addr, error) {
		lookups.Add(1)
		return nil, errors.New("unix targets must not be resolved")
	})

	for _, target := range []string{"unix://" + sock, "unix:" + sock} {
		t.Run(target, func(t *testing.T) {
			pool := &dialingGRPCPool{target: target}
			ctx := types.WithResourcePool(context.Background(), pool)
			conn, release, err := acquireGRPC(ctx, target, false,
				grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatalf("acquireGRPC(%q): %v", target, err)
			}
			defer release()

			invokeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := conn.Invoke(invokeCtx, "/probe.Svc/Method", &structpb.Struct{}, &structpb.Struct{}); err != nil {
				t.Fatalf("invoke over %q: %v (the guarded host:port dialer must not be installed)", target, err)
			}
		})
	}
	if lookups.Load() != 0 {
		t.Fatalf("DNS lookups = %d, want 0 for unix targets", lookups.Load())
	}
}
