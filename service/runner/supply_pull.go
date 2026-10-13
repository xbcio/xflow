package runner

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cast"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/node/supply"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// ErrSupplyPullSourceConflict reports that one supply key — a (namespace, node
// name) pair — is declared with two different content sources. Content is
// cached per key and shared by every consumer of that key, so serving the first
// source to the second declaration's workflow would silently feed one tenant's
// rules to another's nodes. The declaring activation is declined instead.
var ErrSupplyPullSourceConflict = errors.New("supply pull: conflicting declarations for one supply key")

// ErrSupplyPullUnavailable reports that an activation declared a pull source
// but this runner has no PullSupplyFetcher configured (it hosts no triggers, so
// no gate wiring ran). Refusing is the only safe answer: the alternative is to
// treat the requirement as store-backed and serve content from an unrelated
// record, or serve nothing while the author believes a source is configured.
var ErrSupplyPullUnavailable = errors.New("supply pull: this runner has no pull fetcher configured")

// Pull fetch bounds. Both mirror the control-plane supply client
// (supply_client.go): one timeout budget per fetch, one cap on the body. The
// cap also bounds the return-on-304 cache below, so the fetcher's memory is
// bounded by hosted-key count x 1 MiB.
const (
	pullFetchTimeout     = supplyFetchTimeout
	maxPullResponseBytes = maxSupplyResponseBytes
)

// PullSupplyFetcher fetches supply content from a source declared by an
// xflow.supply.http node (types.SupplyPullSource). It is the runner-side half
// of pull mode: the URL, the runner's network position, and the runner's
// credentials never leave this process, and the content goes into the local
// registry only — there is no store write.
//
// Transport policy, all fail-closed:
//
//   - https only, re-checked here even though the compiler already validated it;
//   - redirects are NOT followed (a 3xx is a fetch failure), so every request
//     goes to the configured URL and one SSRF check covers the whole fetch;
//   - the resolved IP is validated AT DIAL TIME, after resolution, against the
//     operator's allowlist (or the default reserved/private/loopback/link-local
//     block, see pullBlockedRanges), which is what makes DNS rebinding
//     harmless: the address that was checked is the address that is dialed;
//   - no proxy: an environment proxy would make the dial guard inspect the
//     proxy's address instead of the target's.
type PullSupplyFetcher struct {
	// AllowedHosts is the operator's runner-level allowlist (RunnerConfig.
	// SupplyPullAllowedHosts). Empty means "any public host": reserved,
	// loopback, private, link-local and unspecified addresses are then refused
	// at dial time. A non-empty list permits only matching hosts and is an
	// explicit operator decision to allow whatever address they resolve to,
	// private included.
	// Entries mirror the node host-policy forms: an exact host, a suffix
	// prefixed with '.', or a wildcard prefixed with '*.', matched
	// case-insensitively against the hostname only (no port, no scheme).
	AllowedHosts []string
	// Credentials resolves the `authentication` name of a pull node to the
	// credential map held in the runner's process configuration. A name that
	// does not resolve fails the fetch; it is never downgraded to an anonymous
	// request, because an unauthenticated fetch against an endpoint that
	// expected credentials is an authorization failure the operator must see.
	//
	// The resolver is namespace-agnostic here, matching the executor's
	// CredentialResolver: a known gap (a same-named credential in another
	// namespace resolves to the same map), documented in DEPLOYMENT-TOPOLOGIES.
	Credentials func(namespace.Namespace, string) map[string]any
	// TLSClientConfig, when non-nil, replaces the transport's TLS configuration.
	// Tests use it to trust an httptest server's certificate; production leaves
	// it nil. It cannot weaken the dial-time address guard.
	TLSClientConfig *tls.Config
	// Logger receives pull-level diagnostics. nil disables logging.
	Logger *slog.Logger
	// Observer, when non-nil, receives one observation per Fetch attempt:
	// result "ok" or "error". It feeds xflow_supply_pull_fetch_total, a series
	// separate from the gate's fetch counter — the gate counts who asked for a
	// fetch, this counts whether the EXTERNAL source answered it (see
	// observability/metrics.SupplyMetrics.OnSupplyPullFetch).
	Observer SupplyPullObserver

	policy pullHostPolicy

	buildOnce sync.Once
	built     *http.Client

	// cacheMu guards etagCache: the content last seen for a key together with
	// the validator that asks the source for a cheap 304 next time. Only keys
	// whose server sends an ETag are cached (P6: a conditional request is only
	// possible when the source offers a validator).
	cacheMu   sync.Mutex
	etagCache map[supply.Key]pullCachedContent
}

// pullCachedContent is what a 304 answers from: the content and hash of the
// last full response, keyed by the validator the source gave for it.
type pullCachedContent struct {
	etag    string
	content []byte
	hash    string
}

// NewPullSupplyFetcher builds the fetcher, compiling the allowlist up front so
// a malformed entry fails the runner's construction rather than every fetch.
func NewPullSupplyFetcher(allowedHosts []string, creds func(namespace.Namespace, string) map[string]any, logger *slog.Logger) (*PullSupplyFetcher, error) {
	policy, err := compilePullHostAllowlist(allowedHosts)
	if err != nil {
		return nil, err
	}
	return &PullSupplyFetcher{AllowedHosts: allowedHosts, Credentials: creds, Logger: logger, policy: policy}, nil
}

// ValidateSupplyPullAllowedHosts reports whether an allowlist is usable. It is
// the config-surface check (sdk/xflow.RunnerConfig): a malformed entry must be
// refused at startup, not silently turned into a fetcher that declines every
// source.
func ValidateSupplyPullAllowedHosts(hosts []string) error {
	_, err := compilePullHostAllowlist(hosts)
	return err
}

// SupplyPullObserver receives one observation per pull fetch attempt, with
// result "ok" or "error". It is satisfied by the metrics observer; a caller
// that wires none leaves the metric unrecorded rather than recording zero.
type SupplyPullObserver interface {
	OnSupplyPullFetch(ctx context.Context, result string)
}

// Fetch retrieves the current content of k from src. The returned hash is
// computed from the content (store.ContentHash), so it is identical whether the
// body arrived on a 200 or from the 304 cache. revision is always 0: a pull
// source has no cluster-wide version, and the field exists for the store-backed
// path's X-Supply-Revision.
//
// Fetch observes every attempt exactly once, on every return path, including
// the ones that fail before a request is sent (a blocked host, an unresolvable
// credential): to an operator watching xflow_supply_pull_fetch_total, a source
// this runner refuses to contact and a source that timed out are the same
// event — the pull did not produce content.
//
// A failure is also logged through Logger, with the reason. That line is the
// only place a refusal is explained: the gate's own log is deliberately
// cause-free, and a fetch this runner declined to make (a blocked address, a
// missing credential) has no request and therefore no access log anywhere
// else. See logFailure for exactly what it carries and what it refuses to.
func (f *PullSupplyFetcher) Fetch(ctx context.Context, k supply.Key, src types.SupplyPullSource) ([]byte, string, uint64, error) {
	content, hash, revision, err := f.fetch(ctx, k, src)
	if f != nil {
		if err != nil {
			f.logFailure(ctx, k, err)
		}
		if f.Observer != nil {
			result := "ok"
			if err != nil {
				result = "error"
			}
			f.Observer.OnSupplyPullFetch(ctx, result)
		}
	}
	return content, hash, revision, err
}

// logFailure records one failed or refused pull: the supply key, and the
// reason an operator needs. Never the URL (a query string may carry a token the
// operator did not intend as log content), never a credential value, never
// content, never a response body — the fetcher's errors are built to that rule
// (pullReason strips the URL out of the ones net/http echoes).
//
// The two errors the gate logs itself are skipped, so one event is one line
// where both loggers are installed: ErrSupplyNotFound is the ordinary cold
// start (the gate logs it at info), and ErrSupplyPullUnavailable is already
// reported by the gate with its own message.
func (f *PullSupplyFetcher) logFailure(ctx context.Context, k supply.Key, err error) {
	if f == nil || f.Logger == nil {
		return
	}
	if errors.Is(err, ErrSupplyNotFound) || errors.Is(err, ErrSupplyPullUnavailable) {
		return
	}
	f.Logger.WarnContext(ctx, "supply pull: fetch failed",
		slog.String("supply", k.Name),
		slog.String("namespace", string(k.Namespace)),
		slog.String("error", err.Error()))
}

// fetch is Fetch's body; the split exists only so every return path is observed
// in one place (see Fetch).
func (f *PullSupplyFetcher) fetch(ctx context.Context, k supply.Key, src types.SupplyPullSource) ([]byte, string, uint64, error) {
	if f == nil {
		return nil, "", 0, ErrSupplyPullUnavailable
	}
	u, err := url.Parse(src.URL)
	if err != nil {
		return nil, "", 0, fmt.Errorf("supply pull: source URL for %q is not a URL: %w", k.Name, pullReason(err))
	}
	if u.User != nil {
		// The compiler refuses userinfo, but a requirement can also arrive from a
		// stored activation written by another version. Both directions leak:
		// net/http echoes the URL in its errors, and this fetcher's errors reach
		// operators' logs. A source that needs a credential names one through the
		// node's `authentication` field, which sets a header and never the URL.
		return nil, "", 0, fmt.Errorf("supply pull: source for %q carries userinfo in its URL; name a credential in the node's authentication field instead", k.Name)
	}
	if !strings.EqualFold(u.Scheme, "https") {
		// Unreachable through the compiler, which rejects non-https at
		// registration. It stays as the fetcher's own guard: a requirement can
		// also arrive from a stored activation written by another version.
		return nil, "", 0, fmt.Errorf("supply pull: source for %q is not https", k.Name)
	}
	host := u.Hostname()
	if err := f.policy.checkHost(host); err != nil {
		return nil, "", 0, fmt.Errorf("supply pull: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, pullFetchTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, src.URL, nil)
	if err != nil {
		return nil, "", 0, fmt.Errorf("supply pull: build request for %q: %w", k.Name, pullReason(err))
	}
	if src.Authentication != "" {
		cred := f.resolveCredential(k.Namespace, src.Authentication)
		if cred == nil {
			return nil, "", 0, fmt.Errorf("supply pull: credential %q is not configured on this runner; refusing to fetch %q anonymously",
				src.Authentication, k.Name)
		}
		if err := applyPullAuth(req, cred); err != nil {
			return nil, "", 0, fmt.Errorf("supply pull: credential %q: %w", src.Authentication, err)
		}
	}
	cached, hasCache := f.cachedContent(k)
	if hasCache {
		req.Header.Set("If-None-Match", cached.etag)
	}

	resp, err := f.httpClient().Do(req)
	if err != nil {
		return nil, "", 0, fmt.Errorf("supply pull: fetch %s: %w", host, pullReason(err))
	}
	defer func() { _ = resp.Body.Close() }()

	switch resp.StatusCode {
	case http.StatusNotModified:
		if !hasCache {
			// A 304 with nothing cached means the source's validator does not
			// match any request we could have sent, or the cache was lost while
			// the validator survived. There is nothing to serve, and inventing
			// content would be worse.
			return nil, "", 0, fmt.Errorf("supply pull: source %s answered 304 with no cached content", host)
		}
		return cached.content, cached.hash, 0, nil
	case http.StatusOK:
	case http.StatusNotFound:
		// The source has no content yet. Distinct from a transport failure: it
		// is the ordinary cold-start state, and the gate logs it at info.
		return nil, "", 0, ErrSupplyNotFound
	default:
		// A redirect lands here too (CheckRedirect returns ErrUseLastResponse),
		// as does every other status. Do not echo the body: it is not ours.
		return nil, "", 0, fmt.Errorf("supply pull: unexpected status %d from %s", resp.StatusCode, host)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxPullResponseBytes+1))
	if err != nil {
		return nil, "", 0, fmt.Errorf("supply pull: read body: %w", err)
	}
	if len(body) > maxPullResponseBytes {
		return nil, "", 0, fmt.Errorf("supply pull: content exceeds %d bytes", maxPullResponseBytes)
	}
	hash := store.ContentHash(body)
	f.storeCache(k, resp.Header.Get("ETag"), body, hash)
	return body, hash, 0, nil
}

// resolveCredential looks the name up through the runner's resolver, treating a
// nil resolver as "this runner has no credentials configured".
func (f *PullSupplyFetcher) resolveCredential(ns namespace.Namespace, name string) map[string]any {
	if f.Credentials == nil {
		return nil
	}
	return f.Credentials(ns, name)
}

// httpClient builds the one client every fetch uses. Building it once keeps the
// connection pool warm between ticks; the fields set here are exactly the ones
// the policy needs, so they cannot be dropped by a later edit that constructs a
// client somewhere else.
func (f *PullSupplyFetcher) httpClient() *http.Client {
	f.buildOnce.Do(func() {
		f.built = &http.Client{
			Transport: &http.Transport{
				// Deliberately no Proxy: the dial-time address check must see the
				// address the connection actually goes to, and with a proxy that
				// is the proxy's address, not the source's.
				DialContext:         f.dialContext,
				MaxIdleConns:        8,
				MaxIdleConnsPerHost: 2,
				IdleConnTimeout:     30 * time.Second,
				TLSClientConfig:     f.TLSClientConfig,
				ForceAttemptHTTP2:   true,
			},
			CheckRedirect: func(*http.Request, []*http.Request) error {
				// ErrUseLastResponse makes the client return the 3xx response
				// itself, which Fetch then treats as a failure. Not following is
				// the policy: a redirect is a second destination that would need
				// its own SSRF check, and a configured source has a stable URL.
				return http.ErrUseLastResponse
			},
		}
	})
	return f.built
}

// dialContext resolves the host once and validates each resolved address
// against the policy before connecting to that exact address. Resolving here
// rather than letting the transport resolve again is the DNS-rebinding defense:
// the checked address and the dialed address are the same value.
func (f *PullSupplyFetcher) dialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("supply pull: invalid address %q: %w", addr, err)
	}
	addrs, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("supply pull: resolve %s: %w", host, err)
	}
	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	var firstErr error
	for _, a := range addrs {
		if err := f.policy.checkIP(host, a.IP); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		// Dial the checked address, never the hostname: a second resolution
		// could answer with a different address than the one just validated.
		conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(a.IP.String(), port))
		if err == nil {
			return conn, nil
		}
		if firstErr == nil {
			firstErr = err
		}
	}
	if firstErr != nil {
		return nil, firstErr
	}
	return nil, fmt.Errorf("supply pull: %s resolved to no usable address", host)
}

// cachedContent returns the stored content for k, if a full response with an
// ETag was seen before.
func (f *PullSupplyFetcher) cachedContent(k supply.Key) (pullCachedContent, bool) {
	f.cacheMu.Lock()
	defer f.cacheMu.Unlock()
	entry, ok := f.etagCache[k]
	return entry, ok
}

// storeCache records the response body as the content a later 304 answers
// with. No ETag means no conditional request is possible, so any earlier entry
// is dropped rather than left to be paired with a validator the source no
// longer uses.
func (f *PullSupplyFetcher) storeCache(k supply.Key, etag string, content []byte, hash string) {
	f.cacheMu.Lock()
	defer f.cacheMu.Unlock()
	if etag == "" {
		delete(f.etagCache, k)
		return
	}
	if f.etagCache == nil {
		f.etagCache = make(map[supply.Key]pullCachedContent)
	}
	f.etagCache[k] = pullCachedContent{etag: etag, content: content, hash: hash}
}

// pullReason strips the URL out of an error the net/http stack built from one.
// Every *url.Error's text starts `Get "<full url>":` or `parse "<full url>":` —
// URL, query string and any userinfo included — and these errors become
// caller-visible results and log lines (see logFailure). The underlying cause
// ("dial tcp 10.0.0.7:443: connect: connection refused") is what an operator
// needs; where the URL pointed is already in the declaration they wrote.
func pullReason(err error) error {
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return urlErr.Err
	}
	return err
}

// applyPullAuth sets the request's credential header from a credential map.
//
// The three shapes mirror node/internal/action/http.go's applyHTTPAuth
// (bearer/basic/api_key with token/username+password/header+value), which is
// unexported and lives under node/internal — out of reach for this package.
// Unlike the node helper, every malformed shape is an error rather than a
// silent no-op: a fetch that drops its credential would hit the source
// unauthenticated, and "which credential failed" must be answerable.
func applyPullAuth(req *http.Request, cred map[string]any) error {
	switch cast.ToString(cred["type"]) {
	case "bearer":
		token := cast.ToString(cred["token"])
		if token == "" {
			return errors.New(`a "bearer" credential has no token`)
		}
		req.Header.Set("Authorization", "Bearer "+token)
	case "basic":
		user := cast.ToString(cred["username"])
		if user == "" {
			return errors.New(`a "basic" credential has no username`)
		}
		req.SetBasicAuth(user, cast.ToString(cred["password"]))
	case "api_key":
		value := cast.ToString(cred["value"])
		if value == "" {
			return errors.New(`an "api_key" credential has no value`)
		}
		header := cast.ToString(cred["header"])
		if header == "" {
			header = "X-API-Key"
		}
		req.Header.Set(header, value)
	default:
		return fmt.Errorf("unsupported credential type %q (want bearer, basic, or api_key)", cast.ToString(cred["type"]))
	}
	return nil
}

// pullHostPolicy is the runner-level destination policy for pull sources:
// an optional operator allowlist plus the always-on private-address block.
//
// The matching semantics are deliberately the node host policy's
// (node/internal/action/host_policy.go: exact, '.suffix', '*.wildcard',
// case-insensitive, hostname only). That file cannot be imported from here —
// it is under an internal boundary — so the forms are re-expressed rather than
// silently reduced to a subset; an operator who wrote '*.corp.example' for a
// node policy must get the same meaning here.
type pullHostPolicy struct {
	patterns []pullHostPattern
}

type pullHostPatternKind uint8

const (
	pullHostExact pullHostPatternKind = iota
	pullHostSuffix
	pullHostWildcard
)

type pullHostPattern struct {
	kind pullHostPatternKind
	host string
}

func compilePullHostAllowlist(raw []string) (pullHostPolicy, error) {
	byValue := make(map[string]pullHostPattern, len(raw))
	for _, entry := range raw {
		p, err := parsePullHostPattern(entry)
		if err != nil {
			return pullHostPolicy{}, fmt.Errorf("supply pull allowed host %q: %w", entry, err)
		}
		byValue[p.String()] = p
	}
	patterns := make([]pullHostPattern, 0, len(byValue))
	for _, p := range byValue {
		patterns = append(patterns, p)
	}
	sort.Slice(patterns, func(i, j int) bool { return patterns[i].String() < patterns[j].String() })
	return pullHostPolicy{patterns: patterns}, nil
}

func parsePullHostPattern(raw string) (pullHostPattern, error) {
	p := pullHostPattern{kind: pullHostExact, host: raw}
	switch {
	case strings.HasPrefix(raw, "*."):
		p.kind = pullHostWildcard
		p.host = raw[2:]
	case strings.HasPrefix(raw, "."):
		p.kind = pullHostSuffix
		p.host = raw[1:]
	}
	host, err := normalizePullHost(p.host)
	if err != nil {
		return pullHostPattern{}, err
	}
	if p.kind != pullHostExact && net.ParseIP(host) != nil {
		return pullHostPattern{}, errors.New("host patterns cannot use IP literals")
	}
	p.host = host
	return p, nil
}

func (p pullHostPattern) String() string {
	switch p.kind {
	case pullHostSuffix:
		return "." + p.host
	case pullHostWildcard:
		return "*." + p.host
	default:
		return p.host
	}
}

func (p pullHostPattern) matches(normalizedHost string) bool {
	switch p.kind {
	case pullHostSuffix:
		// A suffix includes its apex: ".corp.example" matches corp.example too.
		return normalizedHost == p.host || strings.HasSuffix(normalizedHost, "."+p.host)
	case pullHostWildcard:
		// A wildcard requires at least one additional label.
		return normalizedHost != p.host && strings.HasSuffix(normalizedHost, "."+p.host)
	default:
		return normalizedHost == p.host
	}
}

// checkHost applies the allowlist half of the policy, on the hostname, before
// any request is built. With an empty allowlist every syntactically valid host
// passes here; the address-level decision happens at dial time.
func (p pullHostPolicy) checkHost(host string) error {
	normalized, err := normalizePullHost(host)
	if err != nil {
		return fmt.Errorf("source host %q is invalid", host)
	}
	if len(p.patterns) == 0 {
		return nil
	}
	for _, pattern := range p.patterns {
		if pattern.matches(normalized) {
			return nil
		}
	}
	return fmt.Errorf("host %q is not in the runner's pull allowlist", host)
}

// checkIP applies the address half of the policy, at dial time, against the
// address that is about to be dialed. A configured allowlist entry for the host
// is an explicit operator decision, so it also permits an address that the
// default policy blocks (a private source inside the operator's own network is
// the ordinary reason to configure an allowlist at all).
func (p pullHostPolicy) checkIP(host string, ip net.IP) error {
	if len(p.patterns) > 0 {
		return nil
	}
	if pullIPBlocked(ip) {
		return fmt.Errorf("address %s of host %q is blocked by the runner's default pull policy (a reserved, loopback, private, link-local, or unspecified address); add the host to SupplyPullAllowedHosts to permit it", ip, host)
	}
	return nil
}

// pullBlockedRanges are the ranges the stdlib predicates do not cover but that
// cannot be a public config source: 100.64.0.0/10 (RFC 6598 shared address
// space, which contains 100.100.100.200, the Alibaba Cloud metadata service),
// 198.18.0.0/15 and 192.0.0.0/24 (benchmarking / IETF protocol assignments,
// which include 192.0.0.192 and the metadata addresses some clouds use), and
// 240.0.0.0/4 (reserved, which contains the 255.255.255.255 broadcast).
var pullBlockedRanges = []*net.IPNet{
	{IP: net.IPv4(100, 64, 0, 0), Mask: net.CIDRMask(10, 32)},
	{IP: net.IPv4(198, 18, 0, 0), Mask: net.CIDRMask(15, 32)},
	{IP: net.IPv4(192, 0, 0, 0), Mask: net.CIDRMask(24, 32)},
	{IP: net.IPv4(240, 0, 0, 0), Mask: net.CIDRMask(4, 32)},
}

// pullIPBlocked reports whether an address is unreachable-by-design from a
// runner under the default policy. Multicast and the unspecified address are
// included as destinations that cannot be a config source at all.
func pullIPBlocked(ip net.IP) bool {
	if ip.IsLoopback() ||
		ip.IsPrivate() ||
		ip.IsLinkLocalUnicast() || // includes 169.254.169.254, the metadata service
		ip.IsLinkLocalMulticast() ||
		ip.IsInterfaceLocalMulticast() ||
		ip.IsUnspecified() ||
		ip.IsMulticast() {
		return true
	}
	for _, r := range pullBlockedRanges {
		if r.Contains(ip) {
			return true
		}
	}
	return false
}

// normalizePullHost canonicalizes a hostname for matching. It mirrors
// node/internal/action/host_policy.go's normalizeHostOnly: ports have already
// been stripped by the caller, IP literals are canonicalized, and anything
// carrying a scheme, path, credential, or whitespace is rejected rather than
// matched loosely.
func normalizePullHost(raw string) (string, error) {
	if raw == "" || raw != strings.TrimSpace(raw) || strings.ContainsAny(raw, "/\\?#@") || strings.ContainsAny(raw, " \t\r\n") {
		return "", errors.New("invalid host")
	}
	host := strings.TrimSuffix(strings.TrimPrefix(raw, "["), "]")
	if strings.ContainsAny(host, "[]") {
		return "", errors.New("invalid host")
	}
	if ip := net.ParseIP(host); ip != nil {
		return strings.ToLower(ip.String()), nil
	}
	if strings.Contains(host, ":") || len(host) > 253 {
		return "", errors.New("invalid host")
	}
	for _, label := range strings.Split(host, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return "", errors.New("invalid host")
		}
		for _, r := range label {
			if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_') {
				return "", errors.New("invalid host")
			}
		}
	}
	return strings.ToLower(host), nil
}
