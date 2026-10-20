package action

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xbcio/xflow/types"
)

// outboundDialTimeout bounds one TCP connect attempt made by the address
// guard. It matches the standard library's default dialer timeout.
const outboundDialTimeout = 30 * time.Second

// dialAttemptStagger is the RFC 8305 "Connection Attempt Delay" default: the
// guard starts the next resolved address of a host after this long, even if
// the previous attempt has not failed yet. Without it a dual-stack host whose
// first family silently drops SYNs costs one full outboundDialTimeout before
// the other family is tried at all, turning a sub-second fallback into a
// 30-second failure.
const dialAttemptStagger = 300 * time.Millisecond

// errBlockedAddress marks a dial refused by the outbound address policy. It
// exists so callers can tell a policy rejection from a transport failure with
// errors.Is, without matching on a message.
var errBlockedAddress = errors.New("outbound address is not public")

// blockedAddressError reports a host whose resolved address the policy
// refuses. The message names the requested host but never a resolved address:
// the resolution result is an internal detail, and the operator only needs to
// know which host to fix or allowlist.
type blockedAddressError struct{ host string }

func (e *blockedAddressError) Error() string {
	return fmt.Sprintf("host %q resolves to a non-public address; refusing to connect (an allowlisted host is exempt)", e.host)
}

func (e *blockedAddressError) Is(target error) bool { return target == errBlockedAddress }

// addressGuard refuses outbound connections whose destination is not a
// publicly routable address. It backs both the xflow.http transport dialer and
// the xflow.grpc context dialer.
//
// The allow patterns are compiled from the same host-policy grammar (and the
// same compiler) as NewHostPolicy. A host matching them is an explicit operator
// decision, so its addresses are permitted whatever they resolve to — the
// exemption that lets a runner reach an internal service the operator named in
// http_host_policy.block_private_ips_allow. It is a separate list from
// http_host_policy.allow on purpose: the name-level allowlist doubles as
// "allowlist-only mode", so reusing it here would make one private-host
// exemption reject every public destination not listed. Matching in the dialer
// uses the original request host, not a resolved address, so name-level and
// address-level rules cannot disagree about what was allowlisted.
type addressGuard struct {
	allow []hostPattern
}

// installedAddressGuard is nil when the guard is disabled, which is the
// default for embedded SDK use. The standalone runner installs it at startup.
var installedAddressGuard atomic.Pointer[addressGuard]

// SetHTTPPrivateIPBlocking installs (enabled=true) or removes the dial-time
// outbound address guard shared by xflow.http and xflow.grpc. With it enabled,
// a connection is refused when the destination resolves to a loopback,
// private, link-local, unspecified, multicast, or otherwise non-public address.
// The connection is made to the exact address that was checked, so a DNS
// answer that changes between check and dial (rebinding) cannot redirect the
// connection.
//
// allow is the dedicated exemption list for the address guard
// (http_host_policy.block_private_ips_allow), using the same
// exact/.suffix/*.wildcard grammar as NewHTTPHostPolicy. It is deliberately
// separate from the name-level http_host_policy.allow list: exempting one
// internal host from the address refusal must not switch the name policy into
// allowlist-only mode and break every other public destination. An unparsable
// exemption list fails closed: the guard still refuses every non-public
// address, with no exemptions, rather than widening access. Call once at
// startup, before any node executes. Embedded hosts that never call this keep
// the historical unfiltered dial behavior.
func SetHTTPPrivateIPBlocking(enabled bool, allow []string) {
	if !enabled {
		installedAddressGuard.Store(nil)
		return
	}
	patterns, err := compileHostPatterns(allow)
	if err != nil {
		patterns = nil
	}
	installedAddressGuard.Store(&addressGuard{allow: patterns})
}

// currentAddressGuard returns the installed guard, or nil when disabled.
func currentAddressGuard() *addressGuard { return installedAddressGuard.Load() }

// lookupHostIP is the DNS seam tests replace. Production uses the standard
// resolver and keeps every A/AAAA record so the policy can inspect all of
// them.
var lookupHostIP = func(ctx context.Context, host string) ([]netip.Addr, error) {
	return net.DefaultResolver.LookupNetIP(ctx, "ip", host)
}

func (g *addressGuard) hostExempt(host string) bool {
	return len(g.allow) > 0 && hostPatternsMatch(g.allow, host)
}

// resolve returns every address of host without applying the policy. An IP
// literal resolves to itself, so a caller that already holds an address never
// re-resolves a name (the second resolution is where a rebinding host would
// answer differently).
func (g *addressGuard) resolve(ctx context.Context, host string) ([]netip.Addr, error) {
	if addr, err := netip.ParseAddr(host); err == nil {
		return []netip.Addr{addr}, nil
	}
	addrs, err := lookupHostIP(ctx, host)
	if err != nil {
		return nil, err
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("host %q resolved to no address", host)
	}
	return addrs, nil
}

// checkResolved applies the policy to every address returned for reportHost.
// Any non-public record rejects the whole host: with a rotating A/AAAA set, a
// check that accepted the public half would let the next dial take the private
// half.
func (g *addressGuard) checkResolved(addrs []netip.Addr, reportHost string) error {
	if g.hostExempt(reportHost) {
		return nil
	}
	for _, addr := range addrs {
		if blockedAddress(addr) {
			return &blockedAddressError{host: reportHost}
		}
	}
	return nil
}

// dialContext is the net/http transport dial hook. http.Transport resolves
// through it, so addr's host is the URL hostname (or, with an explicit proxy,
// the proxy's host — the address that would actually be connected to).
func (g *addressGuard) dialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("outbound address %q is invalid", addr)
	}
	addrs, err := g.resolve(ctx, host)
	if err != nil {
		return nil, err
	}
	if err := g.checkResolved(addrs, host); err != nil {
		return nil, err
	}
	return dialResolved(ctx, network, port, addrs)
}

// grpcDialer returns the grpc.WithContextDialer hook. gRPC's default resolver
// resolves the target before dialing, so addr usually carries an already
// resolved IP; targetHost is the operator-supplied host, used for the allowlist
// decision and for the error message. Whatever address the resolver hands over,
// a non-public one is refused unless the target host is allowlisted.
func (g *addressGuard) grpcDialer(targetHost string) func(context.Context, string) (net.Conn, error) {
	return func(ctx context.Context, addr string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, fmt.Errorf("outbound address %q is invalid", addr)
		}
		addrs, err := g.resolve(ctx, host)
		if err != nil {
			return nil, err
		}
		if err := g.checkResolved(addrs, targetHost); err != nil {
			return nil, err
		}
		return dialResolved(ctx, "tcp", port, addrs)
	}
}

// validateTarget applies the policy to a gRPC target (host:port, or a resolver
// URL naming one). It runs before the lazy gRPC dial so a rejection is reported
// as a policy error rather than as a generic connection status. A target that
// does not name a TCP address (for example a unix socket) is left to the gRPC
// transport, which rejects it on its own terms.
func (g *addressGuard) validateTarget(ctx context.Context, target string) error {
	host, ok := targetTCPHost(target)
	if !ok {
		return nil
	}
	addrs, err := g.resolve(ctx, host)
	if err != nil {
		return err
	}
	return g.checkResolved(addrs, host)
}

// nonTCPTargetSchemes are gRPC target schemes that do not name a TCP
// host:port. They must not be read as one: "unix:/tmp/s.sock" splits as host
// "unix", port "/tmp/s.sock" under net.SplitHostPort, which would make the
// guard resolve a bogus name and, worse, install a host:port dialer on a
// working socket target.
var nonTCPTargetSchemes = map[string]bool{
	"unix":          true,
	"unixpacket":    true,
	"unix-abstract": true,
}

// targetTCPHost extracts the host from a gRPC target or resolver URL. ok is
// false when the target does not name a TCP host, in which case there is no
// hostname to resolve and no address for this guard to judge.
func targetTCPHost(target string) (string, bool) {
	if schemeEnd := strings.Index(target, "://"); schemeEnd >= 0 {
		if nonTCPTargetSchemes[strings.ToLower(target[:schemeEnd])] {
			return "", false
		}
		target = strings.TrimLeft(target[schemeEnd+len("://"):], "/")
	} else if scheme, _, ok := strings.Cut(target, ":"); ok && nonTCPTargetSchemes[strings.ToLower(scheme)] {
		return "", false
	}
	host, _, err := net.SplitHostPort(target)
	if err != nil || host == "" {
		return "", false
	}
	return host, true
}

// dialOne performs one connect attempt to an already-checked address. It is a
// package variable so tests can replace the network with a scripted dialer.
var dialOne = func(ctx context.Context, network, address string) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: outboundDialTimeout, KeepAlive: 30 * time.Second}
	return dialer.DialContext(ctx, network, address)
}

// interleaveAddresses orders resolved addresses the way RFC 8305 recommends:
// IPv6 first, then alternating between the families, so a host whose first
// record is in a blackholed family reaches the other family after one stagger
// delay rather than after every address of the first family. Order within a
// family is preserved, which keeps the behaviour deterministic for a fixed
// resolver answer.
func interleaveAddresses(addrs []netip.Addr) []netip.Addr {
	v6 := make([]netip.Addr, 0, len(addrs))
	v4 := make([]netip.Addr, 0, len(addrs))
	for _, addr := range addrs {
		if addr.Unmap().Is4() {
			v4 = append(v4, addr)
			continue
		}
		v6 = append(v6, addr)
	}
	out := make([]netip.Addr, 0, len(addrs))
	for i := 0; i < len(v6) || i < len(v4); i++ {
		if i < len(v6) {
			out = append(out, v6[i])
		}
		if i < len(v4) {
			out = append(out, v4[i])
		}
	}
	return out
}

// dialResolved connects to the checked addresses and never to a hostname:
// dialing the name would resolve it a second time, which is exactly what the
// check is meant to prevent.
//
// Attempts are staggered (RFC 8305): the first address starts immediately and
// the next starts dialAttemptStagger later, or as soon as an earlier attempt
// fails. The first success wins and cancels the rest, so a multi-address host
// is not serialized behind one unreachable address. The caller's ctx bounds the
// whole operation. When every attempt fails, the per-address errors are joined
// into one error; that error is internal — callers report the requested host,
// never a resolved address (see blockedAddressError and the gRPC preflight).
func dialResolved(ctx context.Context, network, port string, addrs []netip.Addr) (net.Conn, error) {
	if len(addrs) == 0 {
		return nil, errors.New("host resolved to no dialable address")
	}
	ordered := interleaveAddresses(addrs)

	dialCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	type attemptResult struct {
		conn net.Conn
		err  error
	}
	results := make(chan attemptResult, len(ordered))
	launched, completed := 0, 0
	launchNext := func() bool {
		if launched >= len(ordered) {
			return false
		}
		address := net.JoinHostPort(ordered[launched].String(), port)
		launched++
		go func() {
			conn, err := dialOne(dialCtx, network, address)
			results <- attemptResult{conn: conn, err: err}
		}()
		return true
	}

	// Drain results the winning/canceled path will not read, closing any
	// connection an attempt produced after the race was decided. The channel is
	// sized for every attempt, so the drainer never blocks a dialing goroutine.
	drain := func(outstanding int) {
		if outstanding <= 0 {
			return
		}
		go func() {
			for i := 0; i < outstanding; i++ {
				if res := <-results; res.conn != nil {
					_ = res.conn.Close()
				}
			}
		}()
	}

	launchNext()
	timer := time.NewTimer(dialAttemptStagger)
	defer timer.Stop()
	timerC := timer.C
	resetTimer := func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(dialAttemptStagger)
	}

	var errs []error
	for {
		select {
		case res := <-results:
			completed++
			if res.err == nil {
				cancel()
				drain(launched - completed)
				return res.conn, nil
			}
			errs = append(errs, res.err)
			if completed == launched && launched == len(ordered) {
				return nil, errors.Join(errs...)
			}
			if launchNext() {
				// A failure is a strong signal that this path is unusable; start
				// the next address now rather than waiting out the stagger delay.
				resetTimer()
			}
		case <-timerC:
			if launchNext() {
				resetTimer()
			} else {
				// Every address is already in flight; the timer has nothing left
				// to launch and must not fire again.
				timerC = nil
			}
		case <-ctx.Done():
			cancel()
			drain(launched - completed)
			return nil, ctx.Err()
		}
	}
}

// blockedPrefixes are ranges the stdlib predicates do not cover but that
// cannot be a public destination: 0.0.0.0/8 ("this network"; 0.0.0.0 itself is
// caught by IsUnspecified, the rest of the range is not), RFC 6598 shared
// address space (which holds cloud metadata endpoints such as
// 100.100.100.200), the benchmarking range, IETF protocol assignments (which
// hold further metadata addresses), and reserved space (which holds the
// broadcast address). The IPv6 entries cover the transition prefixes that
// embed an IPv4 destination: NAT64 (64:ff9b::/96), 6to4 (2002::/16) and Teredo
// (2001::/32), so a private IPv4 address cannot be smuggled through an IPv6
// literal. This mirrors the supply-pull policy in service/runner, which the
// runner process already uses for its own outbound fetches; that policy does
// not yet classify the transition prefixes, and is deliberately not changed
// here.
var blockedPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("64:ff9b::/96"),
	netip.MustParsePrefix("2001::/32"),
	netip.MustParsePrefix("2002::/16"),
}

// blockedAddress reports whether addr is unreachable-by-design as a public
// destination. IPv4-mapped IPv6 addresses are classified by the IPv4 they
// carry, so ::ffff:127.0.0.1 cannot slip past the loopback rule.
func blockedAddress(addr netip.Addr) bool {
	if !addr.IsValid() {
		return true
	}
	addr = addr.Unmap()
	if addr.IsLoopback() ||
		addr.IsPrivate() ||
		addr.IsLinkLocalUnicast() || // includes 169.254.169.254, the metadata service
		addr.IsLinkLocalMulticast() ||
		addr.IsInterfaceLocalMulticast() ||
		addr.IsUnspecified() ||
		addr.IsMulticast() {
		return true
	}
	for _, prefix := range blockedPrefixes {
		if prefix.Contains(addr) {
			return true
		}
	}
	return false
}

// guardedTransportCache reuses the guarded clone of a base transport so that
// enabling the guard does not disable connection reuse: cloning per request
// would open a fresh connection (and lose keep-alive) for every node
// execution. Entries are keyed by the base transport's identity and are
// discarded whenever a new guard generation is installed, so a removed or
// replaced guard can never keep serving traffic.
var guardedTransportCache struct {
	mu     sync.Mutex
	guard  *addressGuard
	byBase map[*http.Transport]*http.Transport
}

// maxGuardedTransports bounds the cache. Fresh base transports come from the
// per-request insecure_skip_verify clone, which by design cannot be reused;
// the cap keeps those from accumulating.
const maxGuardedTransports = 8

// withAddressGuard returns a transport that runs every dial through g. The base
// transport is cloned so the process-wide DefaultHTTPClient keeps its own pool
// untouched. Redirects are followed by the same transport, so every hop is
// dialed under the guard. A base that cannot be guarded is a permanent
// configuration error: silently continuing would send the request unfiltered.
func withAddressGuard(base http.RoundTripper, g *addressGuard) (*http.Transport, error) {
	var baseTransport *http.Transport
	switch t := base.(type) {
	case *http.Transport:
		baseTransport = t
	case nil:
		defaulted, ok := http.DefaultTransport.(*http.Transport)
		if !ok {
			return nil, types.NewPermanentError("http.address_guard_unavailable",
				"private-address blocking requires an http.Transport, and the default transport is not one")
		}
		baseTransport = defaulted
	default:
		return nil, types.NewPermanentError("http.address_guard_unavailable",
			fmt.Sprintf("private-address blocking requires an http.Transport, got %T", base))
	}
	if baseTransport.DialTLSContext != nil || baseTransport.DialTLS != nil { //nolint:staticcheck // DialTLS is deprecated but net/http still consults it; a configured value must keep the guard from overwriting it
		// A custom TLS dialer would bypass the guarded DialContext for https,
		// which is exactly the case the guard exists to cover. DialTLS alone
		// matters too: net/http's customDialTLS falls back to the deprecated
		// DialTLS when DialTLSContext is nil, so checking only the Context form
		// would leave an https request dialing through the unguarded hook.
		return nil, types.NewPermanentError("http.address_guard_unavailable",
			"private-address blocking cannot guard a transport with a custom TLS dialer")
	}

	cache := &guardedTransportCache
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.guard != g {
		cache.guard = g
		cache.byBase = make(map[*http.Transport]*http.Transport)
	}
	if transport := cache.byBase[baseTransport]; transport != nil {
		return transport, nil
	}

	transport := baseTransport.Clone()
	transport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := g.dialContext(ctx, network, addr)
		if err != nil && errors.Is(err, errBlockedAddress) {
			// A policy rejection is permanent: retrying cannot change the
			// decision and must not consume the node's retry budget. Returning a
			// ClassifiedError here lets Execute's existing errors.As path keep
			// that classification instead of reporting a transient connection
			// failure.
			return nil, types.NewPermanentError("http.address_not_allowed", err.Error())
		}
		return conn, err
	}
	if len(cache.byBase) >= maxGuardedTransports {
		cache.byBase = make(map[*http.Transport]*http.Transport)
	}
	cache.byBase[baseTransport] = transport
	return transport, nil
}
