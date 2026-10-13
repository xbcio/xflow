package runner

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/node/supply"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// pullTestSource is a TLS source the pull fetcher can talk to: httptest because
// the fetcher only speaks https, TLS because that is the only scheme accepted.
// It records what each request carried so the conditional-request and
// credential paths are observable from the server side.
type pullTestSource struct {
	srv *httptest.Server

	mu      sync.Mutex
	status  int // 0 means 200
	body    string
	etag    string
	headers []http.Header
}

func newPullTestSource(t *testing.T, body, etag string) *pullTestSource {
	t.Helper()
	s := &pullTestSource{body: body, etag: etag}
	s.srv = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.headers = append(s.headers, r.Header.Clone())
		status, body, etag := s.status, s.body, s.etag
		s.mu.Unlock()
		if status != 0 {
			w.WriteHeader(status)
			return
		}
		if etag != "" && r.Header.Get("If-None-Match") == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		if etag != "" {
			w.Header().Set("ETag", etag)
		}
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(s.srv.Close)
	return s
}

func (s *pullTestSource) url() string { return s.srv.URL }

func (s *pullTestSource) host() string {
	u, err := url.Parse(s.srv.URL)
	if err != nil {
		panic(err)
	}
	return u.Hostname()
}

func (s *pullTestSource) setStatus(status int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status = status
}

func (s *pullTestSource) setBody(body string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.body = body
}

func (s *pullTestSource) setETag(etag string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.etag = etag
}

func (s *pullTestSource) requests() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.headers)
}

func (s *pullTestSource) lastHeaders() http.Header {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.headers) == 0 {
		return nil
	}
	return s.headers[len(s.headers)-1]
}

// newPullFetcher builds a fetcher allowed to reach the test source. The
// allowlist entry is what makes the loopback address acceptable: without it the
// default policy blocks the address at dial time, which is its own test.
func newPullFetcher(t *testing.T, src *pullTestSource, hosts []string, creds func(namespace.Namespace, string) map[string]any) *PullSupplyFetcher {
	t.Helper()
	pf, err := NewPullSupplyFetcher(hosts, creds, quietLogger())
	if err != nil {
		t.Fatalf("NewPullSupplyFetcher: %v", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(src.srv.Certificate())
	pf.TLSClientConfig = &tls.Config{RootCAs: pool}
	return pf
}

func pullReq(node string, srcURL string) engine.SupplyRequirement {
	return engine.SupplyRequirement{
		Node: node, Resource: node, RequireReady: true,
		PullSource: &types.SupplyPullSource{URL: srcURL},
	}
}

// --- admission through the gate ---

// A pull requirement is fetched from the declared source into the local
// registry, under the supply node's key, without touching the store-backed
// fetcher at all: that is the whole point of P1/P2 — the content never exists
// on the control plane.
func TestPullAdmitFetchesFromTheDeclaredSource(t *testing.T) {
	body := `{"rules":[1,2]}`
	src := newPullTestSource(t, body, `"v1"`)
	registry := supply.NewRegistry()
	storeFetcher := &stubFetcher{}
	g := NewSupplyGate(storeFetcher, registry, quietLogger())
	g.SetPullFetcher(newPullFetcher(t, src, []string{src.host()}, nil))

	if err := g.Admit(context.Background(), "wf", []engine.SupplyRequirement{pullReq("rules", src.url())}); err != nil {
		t.Fatalf("Admit: %v", err)
	}
	snap, ok := registry.Get(defaultKey("rules"))
	if !ok {
		t.Fatal("fetched content must be cached under the supply node key")
	}
	if string(snap.Content) != body {
		t.Fatalf("cached content = %q, want %q", snap.Content, body)
	}
	if snap.Hash != store.ContentHash([]byte(body)) {
		t.Fatalf("cached hash = %q, want the content hash %q", snap.Hash, store.ContentHash([]byte(body)))
	}
	if snap.Revision != 0 {
		t.Fatalf("cached revision = %d; a pull source has no server-side revision", snap.Revision)
	}
	if n := storeFetcher.calls.Load(); n != 0 {
		t.Fatalf("store-backed fetcher called %d times; a pull requirement must never read the store", n)
	}
	if n := src.requests(); n != 1 {
		t.Fatalf("source requests = %d, want 1", n)
	}

	// A second admission for the same key is served from the cache: the fast
	// path must not re-fetch content the registry already holds and a consumer
	// has not declared anything about.
	if err := g.Admit(context.Background(), "wf", []engine.SupplyRequirement{pullReq("rules", src.url())}); err != nil {
		t.Fatalf("second Admit: %v", err)
	}
	if n := src.requests(); n != 1 {
		t.Fatalf("source requests = %d after a cached admission, want 1", n)
	}
}

// require_ready=true declines when the source is down; require_ready=false
// takes the activation over and reports the supply as serving-unavailable.
// Both are the existing gate semantics — a pull source changes the fetcher,
// not the decision table.
func TestPullAdmitFailureFollowsRequireReady(t *testing.T) {
	src := newPullTestSource(t, "x", "")
	src.setStatus(http.StatusInternalServerError)
	pf := newPullFetcher(t, src, []string{src.host()}, nil)

	t.Run("required supply declines", func(t *testing.T) {
		g := NewSupplyGate(&stubFetcher{}, supply.NewRegistry(), quietLogger())
		g.SetPullFetcher(pf)
		err := g.Admit(context.Background(), "wf", []engine.SupplyRequirement{pullReq("rules", src.url())})
		var nr *NotReadyError
		if !errors.As(err, &nr) {
			t.Fatalf("Admit = %v, want *NotReadyError", err)
		}
	})

	t.Run("optional supply serves unavailable", func(t *testing.T) {
		rec := &recordingGateObserver{}
		g := NewSupplyGate(&stubFetcher{}, supply.NewRegistry(), quietLogger())
		g.SetPullFetcher(pf)
		g.SetObserver(rec)
		req := pullReq("hints", src.url())
		req.RequireReady = false
		if err := g.Admit(context.Background(), "wf", []engine.SupplyRequirement{req}); err != nil {
			t.Fatalf("Admit with require_ready=false must pass: %v", err)
		}
		if len(rec.unavailable) != 1 || rec.unavailable[0] != (unavailableCall{"hints", true}) {
			t.Fatalf("unavailable = %#v, want one serving=true entry", rec.unavailable)
		}
	})
}

// A pull requirement on a runner with no pull fetcher must fail closed rather
// than fall back to the store-backed path, where consuming an unrelated record
// is a real possibility (the resource name is the node's, and a store record
// with that name may exist).
func TestPullAdmitWithoutPullFetcherFailsClosed(t *testing.T) {
	storeFetcher := &stubFetcher{content: map[string][]byte{"rules": []byte("store record")}}
	registry := supply.NewRegistry()
	g := NewSupplyGate(storeFetcher, registry, quietLogger())

	err := g.Admit(context.Background(), "wf", []engine.SupplyRequirement{pullReq("rules", "https://config.example.com/rules.json")})
	var nr *NotReadyError
	if !errors.As(err, &nr) {
		t.Fatalf("Admit = %v, want *NotReadyError (the activation must be declined)", err)
	}
	if n := storeFetcher.calls.Load(); n != 0 {
		t.Fatalf("store-backed fetcher called %d times; a pull requirement must not fall back", n)
	}
	if _, ok := registry.Get(defaultKey("rules")); ok {
		t.Fatal("no content may be published when pull cannot be fetched")
	}
}

// One key, two workflows, two different sources: the second declaration is
// refused. Content is cached per key and shared by every consumer of that key,
// so accepting it would feed the first workflow's nodes the second's content.
func TestPullDeclarationConflictFailsClosed(t *testing.T) {
	srcOne := newPullTestSource(t, "one", "")
	srcTwo := newPullTestSource(t, "two", "")
	g := NewSupplyGate(&stubFetcher{}, supply.NewRegistry(), quietLogger())
	pf, err := NewPullSupplyFetcher([]string{"127.0.0.1"}, nil, quietLogger())
	if err != nil {
		t.Fatalf("NewPullSupplyFetcher: %v", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(srcOne.srv.Certificate())
	pool.AddCert(srcTwo.srv.Certificate())
	pf.TLSClientConfig = &tls.Config{RootCAs: pool}
	g.SetPullFetcher(pf)

	if err := g.Admit(context.Background(), "wf-a", []engine.SupplyRequirement{pullReq("rules", srcOne.url())}); err != nil {
		t.Fatalf("first Admit: %v", err)
	}
	err = g.Admit(context.Background(), "wf-b", []engine.SupplyRequirement{pullReq("rules", srcTwo.url())})
	if !errors.Is(err, ErrSupplyPullSourceConflict) {
		t.Fatalf("second Admit = %v, want ErrSupplyPullSourceConflict", err)
	}
	// The message must name both origins so an operator can find the pair
	// without guessing which of two workflows is wrong.
	if !strings.Contains(err.Error(), srcOne.host()) || !strings.Contains(err.Error(), srcTwo.host()) {
		t.Fatalf("conflict error %q does not name both origins", err)
	}

	// The SAME workflow may move the URL: that is a version upgrade, and
	// refusing it would make a source immutable until this runner restarted.
	if err := g.Admit(context.Background(), "wf-a", []engine.SupplyRequirement{pullReq("rules", srcTwo.url())}); err != nil {
		t.Fatalf("same-workflow upgrade must be accepted: %v", err)
	}
	// Another workflow declaring the now-current origin agrees: one snapshot
	// serves both, which is the sharing the key model wants.
	if err := g.Admit(context.Background(), "wf-c", []engine.SupplyRequirement{pullReq("rules", srcTwo.url())}); err != nil {
		t.Fatalf("matching origin from a second workflow must be accepted: %v", err)
	}
	// A store-backed declaration of the same key from a third workflow is the
	// same hazard in the other direction: its consumers would read pulled
	// content (or the pull's consumers would read a store record).
	storeReq := engine.SupplyRequirement{Node: "rules", Resource: "rules", RequireReady: true}
	if err := g.Admit(context.Background(), "wf-d", []engine.SupplyRequirement{storeReq}); !errors.Is(err, ErrSupplyPullSourceConflict) {
		t.Fatalf("store-backed declaration for a pull key = %v, want ErrSupplyPullSourceConflict", err)
	}
}

// --- the fetcher's transport policy ---

// The default policy refuses loopback, private and link-local addresses at dial
// time, so a source on the runner's own host or network cannot be reached
// without the operator saying so.
func TestPullDefaultPolicyBlocksLoopback(t *testing.T) {
	src := newPullTestSource(t, "x", "")
	pf, err := NewPullSupplyFetcher(nil, nil, quietLogger())
	if err != nil {
		t.Fatalf("NewPullSupplyFetcher: %v", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(src.srv.Certificate())
	pf.TLSClientConfig = &tls.Config{RootCAs: pool}

	_, _, _, err = pf.Fetch(context.Background(), supply.KeyOf(namespace.Default, "rules"), types.SupplyPullSource{URL: src.url()})
	if err == nil {
		t.Fatal("the default policy fetched a loopback source")
	}
	if !strings.Contains(err.Error(), "default pull policy") {
		t.Fatalf("error %q does not name the default policy that refused the address", err)
	}
	if n := src.requests(); n != 0 {
		t.Fatalf("source saw %d requests; the refusal must happen before the request is sent", n)
	}
}

// The allowlist is the operator's explicit permission: the same loopback source
// becomes reachable, and a host that is not on the list is refused before any
// connection is attempted.
func TestPullAllowlistPermitsAndRefuses(t *testing.T) {
	src := newPullTestSource(t, "x", "")

	permitted := newPullFetcher(t, src, []string{src.host()}, nil)
	if _, _, _, err := permitted.Fetch(context.Background(), supply.KeyOf(namespace.Default, "rules"), types.SupplyPullSource{URL: src.url()}); err != nil {
		t.Fatalf("allowlisted loopback source refused: %v", err)
	}
	if n := src.requests(); n != 1 {
		t.Fatalf("source requests = %d, want 1", n)
	}

	refused := newPullFetcher(t, src, []string{"config.example.com"}, nil)
	_, _, _, err := refused.Fetch(context.Background(), supply.KeyOf(namespace.Default, "rules"), types.SupplyPullSource{URL: src.url()})
	if err == nil || !strings.Contains(err.Error(), "not in the runner's pull allowlist") {
		t.Fatalf("Fetch = %v, want a refusal naming the allowlist", err)
	}
	if n := src.requests(); n != 1 {
		t.Fatalf("source requests = %d; a refused host must not be dialed", n)
	}
}

// Redirects are not followed: the fetcher must return the 3xx as a failure and
// must not contact the redirect target, because that target never passed the
// address policy.
func TestPullRedirectIsAFailureAndIsNotFollowed(t *testing.T) {
	target := newPullTestSource(t, "secret", "")
	redirector := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.url(), http.StatusFound)
	}))
	t.Cleanup(redirector.Close)

	pf, err := NewPullSupplyFetcher([]string{"127.0.0.1"}, nil, quietLogger())
	if err != nil {
		t.Fatalf("NewPullSupplyFetcher: %v", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(redirector.Certificate())
	pf.TLSClientConfig = &tls.Config{RootCAs: pool}

	_, _, _, err = pf.Fetch(context.Background(), supply.KeyOf(namespace.Default, "rules"), types.SupplyPullSource{URL: redirector.URL})
	if err == nil || !strings.Contains(err.Error(), "302") {
		t.Fatalf("Fetch = %v, want a failure naming the 3xx status", err)
	}
	if n := target.requests(); n != 0 {
		t.Fatalf("redirect target saw %d requests; a redirect must not be followed", n)
	}
}

// The body cap is enforced while reading, so a source that answers with an
// oversized body is refused rather than buffered.
func TestPullOversizedBodyIsRefused(t *testing.T) {
	src := newPullTestSource(t, strings.Repeat("a", maxPullResponseBytes+1), "")
	pf := newPullFetcher(t, src, []string{src.host()}, nil)

	_, _, _, err := pf.Fetch(context.Background(), supply.KeyOf(namespace.Default, "rules"), types.SupplyPullSource{URL: src.url()})
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("Fetch = %v, want an error naming the size cap", err)
	}
}

// --- conditional requests (P6) ---

// A source that offers an ETag is asked with If-None-Match, and a 304 answers
// from the cached copy with the same content and hash — the hash must not move
// just because the source skipped the body.
func TestPullUsesETagAndServesTheCacheOn304(t *testing.T) {
	body := `{"rules":[1]}`
	src := newPullTestSource(t, body, `"v1"`)
	pf := newPullFetcher(t, src, []string{src.host()}, nil)
	k := supply.KeyOf(namespace.Default, "rules")

	first, firstHash, _, err := pf.Fetch(context.Background(), k, types.SupplyPullSource{URL: src.url()})
	if err != nil {
		t.Fatalf("first Fetch: %v", err)
	}
	second, secondHash, _, err := pf.Fetch(context.Background(), k, types.SupplyPullSource{URL: src.url()})
	if err != nil {
		t.Fatalf("second Fetch: %v", err)
	}
	if string(second) != string(first) || secondHash != firstHash {
		t.Fatalf("304 served %q/%q, want the cached %q/%q", second, secondHash, first, firstHash)
	}
	if got := src.lastHeaders().Get("If-None-Match"); got != `"v1"` {
		t.Fatalf("second request If-None-Match = %q, want the source's ETag", got)
	}
	if n := src.requests(); n != 2 {
		t.Fatalf("source requests = %d, want 2 (the second answered 304)", n)
	}

	// A new validator means new content: the fetch must return it and the hash
	// must move.
	src.setETag(`"v2"`)
	src.setBody(`{"rules":[2]}`)
	third, thirdHash, _, err := pf.Fetch(context.Background(), k, types.SupplyPullSource{URL: src.url()})
	if err != nil {
		t.Fatalf("third Fetch: %v", err)
	}
	if string(third) != `{"rules":[2]}` || thirdHash == firstHash {
		t.Fatalf("content after the ETag moved = %q/%q, want the new body and hash", third, thirdHash)
	}
}

// A 304 with nothing cached is a failure: there is no content to serve, and
// inventing an empty one would publish "the source says nothing" as if it were
// content.
func TestPull304WithoutCacheFails(t *testing.T) {
	src := newPullTestSource(t, "x", `"v1"`)
	src.setStatus(http.StatusNotModified)
	pf := newPullFetcher(t, src, []string{src.host()}, nil)

	_, _, _, err := pf.Fetch(context.Background(), supply.KeyOf(namespace.Default, "rules"), types.SupplyPullSource{URL: src.url()})
	if err == nil || !strings.Contains(err.Error(), "304") {
		t.Fatalf("Fetch = %v, want a failure naming the 304", err)
	}
}

// --- credentials ---

// An authentication name that does not resolve fails the fetch. Handing the
// source an anonymous request instead would turn a misconfigured credential
// into a silent, unauthenticated probe.
func TestPullUnresolvableCredentialFailsClosed(t *testing.T) {
	src := newPullTestSource(t, "x", "")
	pf := newPullFetcher(t, src, []string{src.host()}, nil)

	_, _, _, err := pf.Fetch(context.Background(), supply.KeyOf(namespace.Default, "rules"), types.SupplyPullSource{
		URL: src.url(), Authentication: "rules_token",
	})
	if err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("Fetch = %v, want a refusal naming the missing credential", err)
	}
	if n := src.requests(); n != 0 {
		t.Fatalf("source saw %d requests; a missing credential must fail before the fetch", n)
	}
}

// The credential map shapes must match the node HTTP node's, and every malformed
// one is an error here rather than the node helper's silent no-op.
func TestApplyPullAuthShapes(t *testing.T) {
	cases := []struct {
		name    string
		cred    map[string]any
		wantHdr map[string]string
		wantErr string
	}{
		{"bearer", map[string]any{"type": "bearer", "token": "t0ken"},
			map[string]string{"Authorization": "Bearer t0ken"}, ""},
		{"bearer without token", map[string]any{"type": "bearer"}, nil, "no token"},
		{"basic", map[string]any{"type": "basic", "username": "u", "password": "p"},
			map[string]string{"Authorization": "Basic dTpw"}, ""},
		{"basic without username", map[string]any{"type": "basic"}, nil, "no username"},
		{"api_key", map[string]any{"type": "api_key", "header": "X-Tenant-Key", "value": "v"},
			map[string]string{"X-Tenant-Key": "v"}, ""},
		{"api_key default header", map[string]any{"type": "api_key", "value": "v"},
			map[string]string{"X-API-Key": "v"}, ""},
		{"api_key without value", map[string]any{"type": "api_key"}, nil, "no value"},
		{"unknown type", map[string]any{"type": "hmac"}, nil, "unsupported credential type"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, "https://config.example.com/x", nil)
			if err != nil {
				t.Fatal(err)
			}
			err = applyPullAuth(req, tc.cred)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("applyPullAuth = %v, want an error containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("applyPullAuth: %v", err)
			}
			for h, want := range tc.wantHdr {
				if got := req.Header.Get(h); got != want {
					t.Fatalf("header %s = %q, want %q", h, got, want)
				}
			}
		})
	}
}

// A resolvable credential is applied, and the credential NAME (never the secret)
// is all the definition carries.
func TestPullAppliesResolvedCredential(t *testing.T) {
	src := newPullTestSource(t, "x", "")
	pf := newPullFetcher(t, src, []string{src.host()}, func(ns namespace.Namespace, name string) map[string]any {
		if name != "rules_token" {
			return nil
		}
		return map[string]any{"type": "bearer", "token": "s3cret"}
	})

	if _, _, _, err := pf.Fetch(context.Background(), supply.KeyOf(namespace.Default, "rules"), types.SupplyPullSource{
		URL: src.url(), Authentication: "rules_token",
	}); err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if got := src.lastHeaders().Get("Authorization"); got != "Bearer s3cret" {
		t.Fatalf("Authorization = %q", got)
	}
}

// --- host policy ---

func TestPullHostAllowlistForms(t *testing.T) {
	policy, err := compilePullHostAllowlist([]string{"config.example.com", ".corp.example", "*.wild.example"})
	if err != nil {
		t.Fatalf("compilePullHostAllowlist: %v", err)
	}
	for _, host := range []string{"config.example.com", "CONFIG.EXAMPLE.COM", "corp.example", "a.corp.example", "a.wild.example"} {
		if err := policy.checkHost(host); err != nil {
			t.Errorf("checkHost(%q) = %v, want nil", host, err)
		}
	}
	for _, host := range []string{"config.example.com.evil.test", "wild.example", "other.example"} {
		if err := policy.checkHost(host); err == nil {
			t.Errorf("checkHost(%q) = nil, want a refusal", host)
		}
	}
}

func TestValidateSupplyPullAllowedHosts(t *testing.T) {
	for _, hosts := range [][]string{nil, {"config.example.com"}, {".corp.example"}, {"*.corp.example"}, {"127.0.0.1"}, {"[::1]"}} {
		if err := ValidateSupplyPullAllowedHosts(hosts); err != nil {
			t.Errorf("ValidateSupplyPullAllowedHosts(%v) = %v, want nil", hosts, err)
		}
	}
	for _, hosts := range [][]string{{""}, {"http://config.example.com"}, {"config.example.com/path"}, {"*.10.0.0.1"}, {"*"}} {
		if err := ValidateSupplyPullAllowedHosts(hosts); err == nil {
			t.Errorf("ValidateSupplyPullAllowedHosts(%v) = nil, want an error", hosts)
		}
	}
}

// --- poll integration ---

// A declined activation's supply is polled like any other: the gate records the
// key before it decides, so once the source recovers one tick converges and the
// next delivery of the directive passes the gate.
func TestPullPollConvergesAfterTheSourceRecovers(t *testing.T) {
	src := newPullTestSource(t, `{"rules":[1]}`, "")
	src.setStatus(http.StatusInternalServerError)
	registry := supply.NewRegistry()
	g := NewSupplyGate(&stubFetcher{}, registry, quietLogger())
	g.SetPullFetcher(newPullFetcher(t, src, []string{src.host()}, nil))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := g.Admit(ctx, "wf", []engine.SupplyRequirement{pullReq("rules", src.url())})
	var nr *NotReadyError
	if !errors.As(err, &nr) {
		t.Fatalf("Admit = %v, want *NotReadyError while the source is down", err)
	}
	if _, ok := registry.Get(defaultKey("rules")); ok {
		t.Fatal("no content may be cached while the source is down")
	}

	src.setStatus(0)
	ticker := installFakePoller(g)
	done := make(chan struct{})
	go func() { defer close(done); g.RunPoller(ctx, time.Hour) }()
	ticker.tick()
	waitForSupplyContent(t, registry, defaultKey("rules"), `{"rules":[1]}`)

	// The next delivery of the directive now passes the gate.
	if err := g.Admit(context.Background(), "wf", []engine.SupplyRequirement{pullReq("rules", src.url())}); err != nil {
		t.Fatalf("Admit after convergence: %v", err)
	}
	cancel()
	<-done
}

// A declared poll_interval shortens the runner's poll period; it cannot lengthen
// it, and a value below the floor is clamped rather than honored.
func TestPollPeriodHonorsTheDeclaredPullInterval(t *testing.T) {
	g := NewSupplyGate(&stubFetcher{}, supply.NewRegistry(), quietLogger())
	declare := func(name string, interval time.Duration) {
		t.Helper()
		if err := g.recordDeclaration(supply.KeyOf(namespace.Default, name), "wf", engine.SupplyRequirement{
			Node:       name,
			Resource:   name,
			PullSource: &types.SupplyPullSource{URL: "https://config.example.com/" + name + ".json", PollInterval: interval},
		}); err != nil {
			t.Fatalf("recordDeclaration(%s): %v", name, err)
		}
	}
	declare("rules", 30*time.Second)
	if got := g.pollPeriod(time.Hour); got != 30*time.Second {
		t.Fatalf("pollPeriod(1h) = %v, want the declared 30s", got)
	}
	if got := g.pollPeriod(10 * time.Second); got != 10*time.Second {
		t.Fatalf("pollPeriod(10s) = %v, want the shorter runner interval to win", got)
	}

	// A stored requirement from another version can carry anything; the floor
	// still bounds it. recordDeclaration is the production write path, so this
	// is exactly the state a below-floor record would leave behind.
	declare("fast", time.Second)
	if got := g.pollPeriod(time.Hour); got != MinSupplyPollInterval {
		t.Fatalf("pollPeriod with a below-floor declaration = %v, want the %v floor", got, MinSupplyPollInterval)
	}
}

// The RunPoller loop derives its period from the declarations, so a fast pull
// supply speeds the whole loop up without any runtime reconfiguration.
func TestRunPollerAdoptsTheDeclaredPullInterval(t *testing.T) {
	g := NewSupplyGate(&stubFetcher{}, supply.NewRegistry(), quietLogger())
	if err := g.recordDeclaration(supply.KeyOf(namespace.Default, "rules"), "wf", engine.SupplyRequirement{
		Node: "rules", Resource: "rules",
		PullSource: &types.SupplyPullSource{URL: "https://config.example.com/rules.json", PollInterval: 20 * time.Second},
	}); err != nil {
		t.Fatalf("recordDeclaration: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ticker := installFakePoller(g)
	done := make(chan struct{})
	go func() { defer close(done); g.RunPoller(ctx, time.Hour) }()

	deadline := time.Now().Add(2 * time.Second)
	var period time.Duration
	for time.Now().Before(deadline) {
		if p, ok := ticker.recordedPeriod(); ok {
			period = p
			break
		}
		time.Sleep(time.Millisecond)
	}
	if period == 0 {
		t.Fatal("the poller never asked its tick source for a period")
	}
	// ±10% jitter is applied on top of the declared interval.
	if period < 18*time.Second || period > 22*time.Second {
		t.Fatalf("poll period = %v, want ~20s (the declared interval, jittered)", period)
	}
	cancel()
	<-done
}

// --- metrics ---

type recordingPullObserver struct {
	mu      sync.Mutex
	results []string
}

func (r *recordingPullObserver) OnSupplyPullFetch(_ context.Context, result string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.results = append(r.results, result)
}

func (r *recordingPullObserver) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.results...)
}

// Every attempt is observed exactly once, on the success and the failure paths
// alike. The failures matter most: an observation that only fired on success
// would leave xflow_supply_pull_fetch_total flat while every source is down,
// which is precisely the condition the metric exists to show.
func TestPullFetchObservesEveryAttempt(t *testing.T) {
	src := newPullTestSource(t, "v1", "")
	pf := newPullFetcher(t, src, []string{src.host()}, nil)
	rec := &recordingPullObserver{}
	pf.Observer = rec
	ctx := context.Background()

	// 200, then 200 with a validator, then 304: all three produced content, so
	// all three are "ok" — a 304 is the source answering, not a failure.
	if _, _, _, err := pf.Fetch(ctx, defaultKey("rules"), types.SupplyPullSource{URL: src.url()}); err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	src.setETag(`"e1"`)
	if _, _, _, err := pf.Fetch(ctx, defaultKey("rules"), types.SupplyPullSource{URL: src.url()}); err != nil {
		t.Fatalf("Fetch with validator: %v", err)
	}
	if _, _, _, err := pf.Fetch(ctx, defaultKey("rules"), types.SupplyPullSource{URL: src.url()}); err != nil {
		t.Fatalf("Fetch served from the 304 cache: %v", err)
	}
	if n := src.requests(); n != 3 {
		t.Fatalf("source requests = %d, want 3", n)
	}

	src.setStatus(http.StatusInternalServerError)
	if _, _, _, err := pf.Fetch(ctx, defaultKey("rules"), types.SupplyPullSource{URL: src.url()}); err == nil {
		t.Fatal("Fetch succeeded against a 500")
	}
	// Refused before any request is sent (host outside the allowlist), which is
	// still an attempt that produced no content and must be counted as one.
	if _, _, _, err := pf.Fetch(ctx, defaultKey("rules"), types.SupplyPullSource{URL: "https://blocked.invalid/rules.json"}); err == nil {
		t.Fatal("Fetch succeeded against a host outside the allowlist")
	}

	if got, want := rec.snapshot(), []string{"ok", "ok", "ok", "error", "error"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("observed results = %v, want %v", got, want)
	}
}
