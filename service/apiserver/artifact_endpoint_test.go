package apiserver

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/objectstore"
)

// memArtifactIndex is an in-memory store.ArtifactIndex for endpoint tests. It
// implements the same (namespace, filename, version) uniqueness and immutable
// binding the SQL repo does; the SQL repo itself is covered against real MySQL
// in test/integration/sqlstore_artifact_test.go.
type memArtifactIndex struct {
	rows []store.ArtifactIdentity
}

func (m *memArtifactIndex) Bind(_ context.Context, id store.ArtifactIdentity) error {
	for i := range m.rows {
		r := m.rows[i]
		if r.Namespace == id.Namespace && r.Filename == id.Filename && r.Version == id.Version {
			if r.Digest != id.Digest {
				return store.ErrVersionConflict
			}
			return nil
		}
	}
	m.rows = append(m.rows, id)
	return nil
}

func (m *memArtifactIndex) HasReference(_ context.Context, namespace, digest string) (bool, error) {
	for _, r := range m.rows {
		if r.Namespace == namespace && r.Digest == digest {
			return true, nil
		}
	}
	return false, nil
}

func (m *memArtifactIndex) CountReferences(_ context.Context, digest string) (int64, error) {
	var n int64
	for _, r := range m.rows {
		if r.Digest == digest {
			n++
		}
	}
	return n, nil
}

// ListLatestVersions is unused by these tests: they exercise retrieval
// authorization and endpoint behaviour, not the ops-page read path, which is
// covered against real MySQL in test/integration/sqlstore_artifact_test.go.
func (m *memArtifactIndex) ListLatestVersions(context.Context, string, store.ListOptions) ([]*store.ArtifactVersion, error) {
	return nil, nil
}

// ListVersions is unused by these tests; see ListLatestVersions above.
func (m *memArtifactIndex) ListVersions(context.Context, string, string, store.ListOptions) ([]*store.ArtifactVersion, error) {
	return nil, nil
}

// newArtifactTestServer wires an artifactModule over a filesystem object store
// and the in-memory index, with a stub principal carrying scopes and namespace.
func newArtifactTestServer(t *testing.T, scopes []string, ns string) (*http.ServeMux, *store.ArtifactStore, string) {
	t.Helper()
	dir := t.TempDir()
	as := store.NewArtifactStore(objectstore.NewFSStore(dir), &memArtifactIndex{})
	m := newArtifactModule(as)
	m.principalAuth = staticPrincipalAuth{principal: Principal{Subject: "test-runner", Namespace: ns, Scopes: scopes}}
	m.authorizer = ScopeAuthorizer{}
	m.audit = NewInMemoryAuditSink()
	mux := http.NewServeMux()
	m.RegisterHTTP(mux)
	return mux, as, dir
}

func TestArtifactEndpointGet(t *testing.T) {
	mux, as, _ := newArtifactTestServer(t, []string{"artifact.read"}, "ns1")
	content := []byte("\x00asm\x01\x00\x00\x00 fake wasm body")
	ref, err := as.Put(context.Background(), content, store.ArtifactMeta{
		Filename:  "tagger.wasm",
		Namespace: "ns1",
	})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/artifacts/"+ref.Digest, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET = %d, body=%s", rec.Code, rec.Body)
	}
	if got := rec.Body.Bytes(); string(got) != string(content) {
		t.Fatalf("body = %q, want %q", got, content)
	}
	if got := rec.Header().Get("ETag"); got != ref.Digest {
		t.Fatalf("ETag = %q, want %q", got, ref.Digest)
	}
	// Content-Length must be the authoritative size, not chunked: the client's
	// ReadThrough validates the byte count against it and a -1 would skip that
	// check entirely.
	if got := rec.Header().Get("Content-Length"); got != strconv.Itoa(len(content)) {
		t.Fatalf("Content-Length = %q, want %d", got, len(content))
	}
}

func TestArtifactEndpointHeadHasNoBody(t *testing.T) {
	mux, as, _ := newArtifactTestServer(t, []string{"artifact.read"}, "ns1")
	content := []byte("head must not carry these bytes")
	ref, err := as.Put(context.Background(), content, store.ArtifactMeta{
		Filename:  "mod.wasm",
		Namespace: "ns1",
	})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodHead, "/v1/artifacts/"+ref.Digest, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("HEAD = %d", rec.Code)
	}
	// httptest.ResponseRecorder does not strip a HEAD body the way a real
	// server does, so writing one here would show up — which is the point:
	// the handler must return before touching Open.
	if rec.Body.Len() != 0 {
		t.Fatalf("HEAD body = %q, want empty", rec.Body)
	}
	if got := rec.Header().Get("Content-Length"); got != strconv.Itoa(len(content)) {
		t.Fatalf("HEAD Content-Length = %q, want %d", got, len(content))
	}
}

// TestArtifactEndpointTenantIsolation is the core assertion of design §7.1 and
// the only directly testable benefit of the dual-table split: knowing a digest
// is not sufficient to read it. The response must be 404, not 403 — a 403 would
// confirm the digest exists.
func TestArtifactEndpointTenantIsolation(t *testing.T) {
	dir := t.TempDir()
	idx := &memArtifactIndex{}
	// One shared object store and index, two modules differing only in the
	// namespace their principal carries. This is the real deployment shape:
	// bytes are deduplicated globally, so tenant A's request reaches the very
	// same blob tenant B uploaded.
	as := store.NewArtifactStore(objectstore.NewFSStore(dir), idx)
	content := []byte("tenant B's private module bytes")
	ref, err := as.Put(context.Background(), content, store.ArtifactMeta{
		Filename:  "private.wasm",
		Namespace: "tenant-b",
	})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	serve := func(ns string) *httptest.ResponseRecorder {
		m := newArtifactModule(as)
		m.principalAuth = staticPrincipalAuth{principal: Principal{
			Subject: "runner-" + ns, Namespace: ns, Scopes: []string{"artifact.read"},
		}}
		m.authorizer = ScopeAuthorizer{}
		m.audit = NewInMemoryAuditSink()
		mux := http.NewServeMux()
		m.RegisterHTTP(mux)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/artifacts/"+ref.Digest, nil))
		return rec
	}

	// Tenant B owns a reference and reads its bytes.
	if rec := serve("tenant-b"); rec.Code != http.StatusOK {
		t.Fatalf("tenant-b GET = %d, want 200", rec.Code)
	}

	// Tenant A holds the same scope and a valid, existing digest. It must still
	// get 404 — and the body must not leak the bytes.
	rec := serve("tenant-a")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("tenant-a GET = %d, want 404 (403 would confirm existence)", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "private module") {
		t.Fatalf("tenant-a response leaked content: %q", rec.Body)
	}

	// A digest that exists nowhere must be indistinguishable from one owned by
	// another tenant. If these two differed, the difference itself would be the
	// existence oracle the 404 is there to close.
	absent := store.ContentHash([]byte("never uploaded by anyone"))
	m := newArtifactModule(as)
	m.principalAuth = staticPrincipalAuth{principal: Principal{
		Subject: "runner-a", Namespace: "tenant-a", Scopes: []string{"artifact.read"},
	}}
	m.authorizer = ScopeAuthorizer{}
	m.audit = NewInMemoryAuditSink()
	mux := http.NewServeMux()
	m.RegisterHTTP(mux)
	absentRec := httptest.NewRecorder()
	mux.ServeHTTP(absentRec, httptest.NewRequest(http.MethodGet, "/v1/artifacts/"+absent, nil))
	if absentRec.Code != rec.Code || absentRec.Body.String() != rec.Body.String() {
		t.Fatalf("absent digest answered %d %q but other-tenant digest answered %d %q; the two must be identical",
			absentRec.Code, absentRec.Body, rec.Code, rec.Body)
	}
}

func TestArtifactEndpointUnauthenticated(t *testing.T) {
	dir := t.TempDir()
	as := store.NewArtifactStore(objectstore.NewFSStore(dir), &memArtifactIndex{})
	ref, err := as.Put(context.Background(), []byte("x"), store.ArtifactMeta{
		Filename: "a.wasm", Namespace: "ns1",
	})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	m := newArtifactModule(as)
	m.principalAuth = staticPrincipalAuth{err: ErrWorkflowUnauthenticated}
	m.authorizer = ScopeAuthorizer{}
	m.audit = NewInMemoryAuditSink()
	mux := http.NewServeMux()
	m.RegisterHTTP(mux)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/artifacts/"+ref.Digest, nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated GET = %d, want 401", rec.Code)
	}
}

// TestArtifactEndpointMissingScope pins that the operation is actually wired
// into scopeForOperation. An operation missing from that map falls through to
// the "" scope, which BOTH authorizers deny — so a principal WITH the scope
// would also be refused and the route would be silently unreachable. Asserting
// only the deny case would pass either way; the allow case is what proves the
// mapping exists.
func TestArtifactEndpointMissingScope(t *testing.T) {
	mux, as, _ := newArtifactTestServer(t, []string{"supply.read", "workflow"}, "ns1")
	ref, err := as.Put(context.Background(), []byte("scoped"), store.ArtifactMeta{
		Filename: "s.wasm", Namespace: "ns1",
	})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/artifacts/"+ref.Digest, nil))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("GET without artifact.read = %d, want 403", rec.Code)
	}

	if got := scopeForOperation(OpArtifactRead); got != "artifact.read" {
		t.Fatalf("scopeForOperation(OpArtifactRead) = %q; an unmapped operation is denied even for a scoped principal", got)
	}
}

func TestArtifactEndpointRejectedPaths(t *testing.T) {
	mux, as, _ := newArtifactTestServer(t, []string{"artifact.read"}, "ns1")
	ref, err := as.Put(context.Background(), []byte("body"), store.ArtifactMeta{
		Filename: "p.wasm", Namespace: "ns1",
	})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	cases := []struct {
		name   string
		method string
		path   string
	}{
		{"no digest", http.MethodGet, "/v1/artifacts/"},
		{"nested path", http.MethodGet, "/v1/artifacts/sha256/" + strings.TrimPrefix(ref.Digest, "sha256:")},
		{"malformed digest", http.MethodGet, "/v1/artifacts/not-a-digest"},
		{"wrong algorithm", http.MethodGet, "/v1/artifacts/md5:" + strings.Repeat("a", 32)},
		{"uppercase hex", http.MethodGet, "/v1/artifacts/sha256:" + strings.Repeat("A", 64)},
		{"traversal", http.MethodGet, "/v1/artifacts/..%2f..%2fetc%2fpasswd"},
		{"write verb", http.MethodPut, "/v1/artifacts/" + ref.Digest},
		{"delete verb", http.MethodDelete, "/v1/artifacts/" + ref.Digest},
	}
	for _, tc := range cases {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(tc.method, tc.path, nil))
		if rec.Code != http.StatusNotFound {
			t.Errorf("%s: %s %s = %d, want 404", tc.name, tc.method, tc.path, rec.Code)
		}
	}
}

// TestArtifactEndpointReadPathPurity is design §2.3 premise 1: the retrieval
// route must not write. Under N server replicas serving the same hot digest, a
// write here (a last_used_at bump, say) turns every runner fetch into row-lock
// contention. The property is structural — there is no such column and no write
// call — and this test is what keeps it that way. It snapshots the whole
// filesystem backing store, mtimes included, because a "touch" would show up
// there and nowhere else.
func TestArtifactEndpointReadPathPurity(t *testing.T) {
	mux, as, dir := newArtifactTestServer(t, []string{"artifact.read"}, "ns1")
	ref, err := as.Put(context.Background(), []byte("purity content"), store.ArtifactMeta{
		Filename: "pure.wasm", Namespace: "ns1",
	})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	before := snapshotTree(t, dir)

	for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodGet} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(method, "/v1/artifacts/"+ref.Digest, nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("%s = %d", method, rec.Code)
		}
	}
	// A 404 path must be pure too — that is where a "record the miss" write
	// would most plausibly be added.
	missRec := httptest.NewRecorder()
	mux.ServeHTTP(missRec, httptest.NewRequest(http.MethodGet,
		"/v1/artifacts/"+store.ContentHash([]byte("absent")), nil))
	if missRec.Code != http.StatusNotFound {
		t.Fatalf("miss = %d, want 404", missRec.Code)
	}

	if after := snapshotTree(t, dir); before != after {
		t.Fatalf("object store modified by read requests:\nbefore=%s\nafter =%s", before, after)
	}
}

// snapshotTree renders every file under root as path|size|mtime|content, sorted
// for stability. Covering mtime and content both matters: a rewrite of
// identical bytes would leave size unchanged, and a bare touch would leave
// content unchanged.
func snapshotTree(t *testing.T, root string) string {
	t.Helper()
	var lines []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		data, rerr := os.ReadFile(path)
		if rerr != nil {
			return rerr
		}
		rel, rerr := filepath.Rel(root, path)
		if rerr != nil {
			return rerr
		}
		lines = append(lines, rel+"|"+strconv.FormatInt(info.Size(), 10)+"|"+
			strconv.FormatInt(info.ModTime().UnixNano(), 10)+"|"+string(data))
		return nil
	})
	if err != nil {
		t.Fatalf("snapshotTree: %v", err)
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

// TestArtifactEndpointNotRegisteredWithoutAuth pins the fail-closed
// registration: with no PrincipalAuthenticator there is no namespace, so the
// tenant check that IS this endpoint's authorization cannot run. The route must
// not exist at all rather than serve open.
func TestArtifactEndpointNotRegisteredWithoutAuth(t *testing.T) {
	dir := t.TempDir()
	as := store.NewArtifactStore(objectstore.NewFSStore(dir), &memArtifactIndex{})
	ref, err := as.Put(context.Background(), []byte("open?"), store.ArtifactMeta{
		Filename: "o.wasm", Namespace: "ns1",
	})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	for _, tc := range []struct {
		name string
		m    *artifactModule
	}{
		{"no principal auth", newArtifactModule(as)},
		{"no artifact store", func() *artifactModule {
			m := newArtifactModule(nil)
			m.principalAuth = staticPrincipalAuth{principal: Principal{Namespace: "ns1", Scopes: []string{"artifact.read"}}}
			return m
		}()},
	} {
		mux := http.NewServeMux()
		tc.m.RegisterHTTP(mux)
		_, pattern := mux.Handler(httptest.NewRequest(http.MethodGet, "/v1/artifacts/"+ref.Digest, nil))
		if pattern != "" {
			t.Errorf("%s: route registered as %q, want unregistered", tc.name, pattern)
		}
	}
}

// TestArtifactEndpointOrphanIdentity covers the branch where an identity row
// exists but the bytes do not. §6.1's ordering (bytes committed before the
// workflow is registered) rules this direction out, so reaching it means a blob
// was removed out of band — the handler must answer 404 rather than 200 with an
// empty body, which a client would cache as valid content.
func TestArtifactEndpointOrphanIdentity(t *testing.T) {
	dir := t.TempDir()
	idx := &memArtifactIndex{}
	as := store.NewArtifactStore(objectstore.NewFSStore(dir), idx)
	content := []byte("bytes that will be removed out of band")
	digest := store.ContentHash(content)

	// Bind the identity WITHOUT ever storing the bytes.
	if err := idx.Bind(context.Background(), store.ArtifactIdentity{
		Namespace: "ns1", Filename: "orphan.wasm", Version: store.DefaultVersion(digest), Digest: digest,
	}); err != nil {
		t.Fatalf("Bind: %v", err)
	}

	m := newArtifactModule(as)
	m.principalAuth = staticPrincipalAuth{principal: Principal{
		Subject: "runner", Namespace: "ns1", Scopes: []string{"artifact.read"},
	}}
	m.authorizer = ScopeAuthorizer{}
	m.audit = NewInMemoryAuditSink()
	mux := http.NewServeMux()
	m.RegisterHTTP(mux)

	for _, method := range []string{http.MethodGet, http.MethodHead} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(method, "/v1/artifacts/"+digest, nil))
		if rec.Code != http.StatusNotFound {
			t.Errorf("%s on orphan identity = %d, want 404", method, rec.Code)
		}
	}
}

// TestArtifactEndpointOverHTTPStore drives the real handler through the real
// runner-side client — objectstore.HTTPStore behind objectstore.ReadThrough —
// over a live HTTP listener.
//
// Both halves already have unit tests, and neither can catch what this does:
// HTTPStore was only ever exercised against a hand-written stub server, so
// nothing yet proved the two agree on the URL shape, on the digest surviving
// path escaping, or that the response actually carries the Content-Length that
// ReadThrough's size validation depends on (without it, that validation
// silently never runs and a truncated body would be cached as correct).
func TestArtifactEndpointOverHTTPStore(t *testing.T) {
	mux, as, _ := newArtifactTestServer(t, []string{"artifact.read"}, "ns1")
	content := make([]byte, 64<<10)
	for i := range content {
		content[i] = byte(i % 253)
	}
	ref, err := as.Put(context.Background(), content, store.ArtifactMeta{
		Filename: "e2e.wasm", Namespace: "ns1",
	})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	srv := httptest.NewServer(mux)
	defer srv.Close()

	origin := &objectstore.HTTPStore{BaseURL: srv.URL, Token: "unused-by-stub-auth", Client: srv.Client()}
	cacheDir := t.TempDir()
	rt := objectstore.NewReadThrough(objectstore.NewFSStore(cacheDir), origin)

	// The runner addresses by digest; the key derivation is the same function
	// the server-side store uses, which is what makes the two ends line up.
	key := store.ObjectKeyForDigest(ref.Digest)

	// A HEAD through the same path must report the real size — this is what a
	// cache uses to decide whether a local copy is still valid.
	obj, err := origin.HeadObject(context.Background(), key)
	if err != nil {
		t.Fatalf("HeadObject over HTTP: %v", err)
	}
	if obj.Size != int64(len(content)) {
		t.Fatalf("HEAD reported size %d, want %d (a -1 means no Content-Length, which disables size validation)", obj.Size, len(content))
	}

	for i := 1; i <= 2; i++ {
		rc, got, gerr := rt.GetObject(context.Background(), key)
		if gerr != nil {
			t.Fatalf("GetObject #%d: %v", i, gerr)
		}
		data, rerr := io.ReadAll(rc)
		_ = rc.Close()
		if rerr != nil {
			t.Fatalf("read #%d: %v", i, rerr)
		}
		if store.ContentHash(data) != ref.Digest {
			t.Fatalf("read #%d: served bytes do not hash to the requested digest", i)
		}
		if got.Size != int64(len(content)) {
			t.Fatalf("read #%d: Size = %d, want %d", i, got.Size, len(content))
		}
	}

	// The second read must have been served from the local cache: the runner's
	// whole reason to have one is that a cross-cloud fetch happens once.
	if n := snapshotTree(t, cacheDir); n == "" {
		t.Fatalf("cache directory empty after two reads; the read-through write never happened")
	}

	// A digest this namespace does not reference must reach the client as
	// ErrNotFound, not as an opaque error: §7.2 classifies it as a permanent
	// configuration error and must not retry it.
	absentKey := store.ObjectKeyForDigest(store.ContentHash([]byte("absent from this tenant")))
	if _, _, err := rt.GetObject(context.Background(), absentKey); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("GetObject(absent) = %v, want objectstore.ErrNotFound", err)
	}
}

// large enough to cross the io.Copy buffer boundary several times, since the
// small payloads above would pass even with a single-buffer read.
func TestArtifactEndpointServesFullBody(t *testing.T) {
	mux, as, _ := newArtifactTestServer(t, []string{"artifact.read"}, "ns1")
	content := make([]byte, 256<<10)
	for i := range content {
		content[i] = byte(i % 251)
	}
	ref, err := as.Put(context.Background(), content, store.ArtifactMeta{
		Filename: "big.wasm", Namespace: "ns1",
	})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/artifacts/"+ref.Digest, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET = %d", rec.Code)
	}
	got, _ := io.ReadAll(rec.Body)
	if len(got) != len(content) {
		t.Fatalf("body = %d bytes, want %d", len(got), len(content))
	}
	if store.ContentHash(got) != ref.Digest {
		t.Fatalf("served bytes do not hash to the requested digest")
	}
}
