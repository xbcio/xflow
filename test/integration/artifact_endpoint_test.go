//go:build integration

package integration

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/xbcio/xflow/service/apiserver"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/objectstore"
	"github.com/xbcio/xflow/store/sqlstore"
)

// artifactEndpointServer mounts the real apiserver artifact route over the real
// MySQL-backed artifact store, with a principal in ns.
//
// The apiserver package's own artifact_endpoint_test.go covers the handler over
// a filesystem object store. What only this file can prove is the pair: the
// route's tenant check running against the actual idx_content_hash query, and
// the route being pure-read at the level the design's premise is stated —
// database rows, not local files.
func artifactEndpointServer(t *testing.T, p *sqlstore.Provider, ns string, scopes []string) http.Handler {
	t.Helper()
	as := store.NewArtifactStore(p.ArtifactObjects(), p.ArtifactIndex())
	// An empty RedisAddr selects the in-memory backend: this test exercises the
	// HTTP route and MySQL, not the control plane.
	cfg := apiserver.Config{
		Artifacts: as,
		PrincipalAuth: apiserver.NewBearerPrincipalAuthMulti([]apiserver.TokenPrincipalMapping{{
			Token: "test-token-" + ns, Subject: "runner-" + ns, Namespace: ns, Scopes: scopes,
		}}),
		Authorizer: apiserver.NamespaceAwareAuthorizer{},
		AuditSink:  apiserver.NewInMemoryAuditSink(),
	}
	srv, err := apiserver.New(cfg)
	if err != nil {
		t.Fatalf("apiserver.New: %v", err)
	}
	return srv.Handler()
}

func artifactGET(t *testing.T, h http.Handler, ns, digest, method string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, "/v1/artifacts/"+digest, nil)
	req.Header.Set("Authorization", "Bearer test-token-"+ns)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// TestArtifactEndpointMySQLTenantIsolation is design §12 phase 1's core
// acceptance criterion against the authoritative backend: tenant A requesting a
// digest only tenant B references gets 404, not 403.
//
// Running it over MySQL rather than only over the in-memory index is the point.
// The check is a single indexed query (idx_content_hash, §4.2) whose WHERE
// clause carries both namespace and content_hash; dropping either predicate
// still passes a single-tenant test and still returns rows for the owner. Only
// two tenants against the real query catch it.
func TestArtifactEndpointMySQLTenantIsolation(t *testing.T) {
	_, p := newArtifactDB(t)
	ctx := context.Background()

	nsA := uniqueNS(t, "epA")
	nsB := uniqueNS(t, "epB")
	content := uniqueContent(t, "endpoint tenant isolation payload")

	as := store.NewArtifactStore(p.ArtifactObjects(), p.ArtifactIndex())
	ref, err := as.Put(ctx, content, store.ArtifactMeta{Filename: "iso.wasm", Namespace: nsB})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	hB := artifactEndpointServer(t, p, nsB, []string{"artifact.read"})
	if rec := artifactGET(t, hB, nsB, ref.Digest, http.MethodGet); rec.Code != http.StatusOK {
		t.Fatalf("owner GET = %d, body=%s", rec.Code, rec.Body)
	} else if rec.Body.String() != string(content) {
		t.Fatalf("owner GET returned wrong bytes")
	}

	hA := artifactEndpointServer(t, p, nsA, []string{"artifact.read"})
	rec := artifactGET(t, hA, nsA, ref.Digest, http.MethodGet)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("cross-tenant GET = %d, want 404 (403 would confirm the digest exists)", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "isolation payload") {
		t.Fatalf("cross-tenant response leaked content: %q", rec.Body)
	}

	// An unreferenced digest must be answered identically, or the difference is
	// itself the existence oracle the 404 exists to close.
	absent := store.ContentHash(uniqueContent(t, "referenced by nobody"))
	absentRec := artifactGET(t, hA, nsA, absent, http.MethodGet)
	if absentRec.Code != rec.Code || absentRec.Body.String() != rec.Body.String() {
		t.Fatalf("absent digest → %d %q but other-tenant digest → %d %q; must be indistinguishable",
			absentRec.Code, absentRec.Body, rec.Code, rec.Body)
	}
}

// TestArtifactEndpointMySQLReadPathPurity is design §2.3 premise 1 asserted
// where the premise actually lives: the database. Under N server replicas
// serving the same hot digest, a write on this path (a last_used_at bump) turns
// every runner fetch into row-lock contention on one row.
//
// It reuses readBlobRow / readIdentityRows from sqlstore_artifact_test.go, which
// snapshot EVERY column including created_at — the column a "touch" would move
// and the one a return-value-based snapshot would miss.
func TestArtifactEndpointMySQLReadPathPurity(t *testing.T) {
	db, p := newArtifactDB(t)
	ctx := context.Background()
	ns := uniqueNS(t, "eppure")

	as := store.NewArtifactStore(p.ArtifactObjects(), p.ArtifactIndex())
	content := uniqueContent(t, "endpoint read path purity")
	ref, err := as.Put(ctx, content, store.ArtifactMeta{Filename: "pure.wasm", Namespace: ns})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	blobBefore := readBlobRow(t, db, ref.Digest)
	identityBefore := readIdentityRows(t, db, ref.Digest)

	h := artifactEndpointServer(t, p, ns, []string{"artifact.read"})
	for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodGet} {
		if rec := artifactGET(t, h, ns, ref.Digest, method); rec.Code != http.StatusOK {
			t.Fatalf("%s = %d", method, rec.Code)
		}
	}
	// The 404 path too: "record the miss" is where such a write would most
	// plausibly be introduced later.
	absent := store.ContentHash(uniqueContent(t, "purity miss"))
	if rec := artifactGET(t, h, ns, absent, http.MethodGet); rec.Code != http.StatusNotFound {
		t.Fatalf("miss = %d, want 404", rec.Code)
	}

	if blobAfter := readBlobRow(t, db, ref.Digest); blobBefore != blobAfter {
		t.Fatalf("blob row modified by GET/HEAD:\nbefore=%s\nafter =%s", blobBefore, blobAfter)
	}
	if identityAfter := readIdentityRows(t, db, ref.Digest); identityBefore != identityAfter {
		t.Fatalf("identity rows modified by GET/HEAD:\nbefore=%s\nafter =%s", identityBefore, identityAfter)
	}
}

// TestArtifactEndpointMySQLAuthzMatrix covers the remaining three status codes
// of §12 phase 1: 200, 401, 403.
func TestArtifactEndpointMySQLAuthzMatrix(t *testing.T) {
	_, p := newArtifactDB(t)
	ctx := context.Background()
	ns := uniqueNS(t, "epauthz")

	as := store.NewArtifactStore(p.ArtifactObjects(), p.ArtifactIndex())
	content := uniqueContent(t, "authz matrix payload")
	ref, err := as.Put(ctx, content, store.ArtifactMeta{Filename: "authz.wasm", Namespace: ns})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	// 200 with the scope.
	h := artifactEndpointServer(t, p, ns, []string{"artifact.read"})
	if rec := artifactGET(t, h, ns, ref.Digest, http.MethodGet); rec.Code != http.StatusOK {
		t.Errorf("scoped GET = %d, want 200", rec.Code)
	}

	// 401 with no credential at all.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/artifacts/"+ref.Digest, nil))
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("unauthenticated GET = %d, want 401", rec.Code)
	}

	// 401 with a wrong token — same answer as none, no existence leak.
	wrong := httptest.NewRequest(http.MethodGet, "/v1/artifacts/"+ref.Digest, nil)
	wrong.Header.Set("Authorization", "Bearer not-a-real-token")
	wrongRec := httptest.NewRecorder()
	h.ServeHTTP(wrongRec, wrong)
	if wrongRec.Code != http.StatusUnauthorized {
		t.Errorf("wrong-token GET = %d, want 401", wrongRec.Code)
	}

	// 403 for an authenticated principal in the OWNING namespace that lacks the
	// scope. Using the owner is deliberate: if the scope check were missing, the
	// request would succeed, so a 403 here can only come from the scope gate and
	// not from the tenant check.
	noScope := artifactEndpointServer(t, p, ns, []string{"supply.read", "workflow"})
	if rec := artifactGET(t, noScope, ns, ref.Digest, http.MethodGet); rec.Code != http.StatusForbidden {
		t.Errorf("GET without artifact.read = %d, want 403", rec.Code)
	}
}

// TestArtifactEndpointMySQLOverHTTPStore is the full runner-side shape against
// the authoritative backend: objectstore.HTTPStore behind ReadThrough, over a
// live listener, fronting the real MySQL blob table. This is the composition
// §7 describes, assembled end to end for the first time here.
func TestArtifactEndpointMySQLOverHTTPStore(t *testing.T) {
	_, p := newArtifactDB(t)
	ctx := context.Background()
	ns := uniqueNS(t, "ephttp")

	as := store.NewArtifactStore(p.ArtifactObjects(), p.ArtifactIndex())
	// Large enough to cross io.Copy's buffer several times, so a truncating copy
	// on either side would show up.
	body := make([]byte, 300<<10)
	for i := range body {
		body[i] = byte(i % 251)
	}
	body = append(body, uniqueContent(t, "http store run marker")...)
	ref, err := as.Put(ctx, body, store.ArtifactMeta{Filename: "runner.wasm", Namespace: ns})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	srv := httptest.NewServer(artifactEndpointServer(t, p, ns, []string{"artifact.read"}))
	defer srv.Close()

	origin := &objectstore.HTTPStore{BaseURL: srv.URL, Token: "test-token-" + ns, Client: srv.Client()}
	rt := objectstore.NewReadThrough(objectstore.NewFSStore(t.TempDir()), origin)
	key := store.ObjectKeyForDigest(ref.Digest)

	// HEAD must report the real size: ReadThrough validates the received byte
	// count against it, and a missing Content-Length (-1) silently disables that
	// validation so a truncated body would be cached as correct.
	obj, err := origin.HeadObject(ctx, key)
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if obj.Size != int64(len(body)) {
		t.Fatalf("HEAD size = %d, want %d", obj.Size, len(body))
	}

	for i := 1; i <= 2; i++ {
		rc, got, gerr := rt.GetObject(ctx, key)
		if gerr != nil {
			t.Fatalf("GetObject #%d: %v", i, gerr)
		}
		data, rerr := io.ReadAll(rc)
		_ = rc.Close()
		if rerr != nil {
			t.Fatalf("read #%d: %v", i, rerr)
		}
		if store.ContentHash(data) != ref.Digest {
			t.Fatalf("read #%d: bytes do not hash to the requested digest (len=%d, want %d)", i, len(data), len(body))
		}
		if got.Size != int64(len(body)) {
			t.Fatalf("read #%d: Size = %d, want %d", i, got.Size, len(body))
		}
	}

	// A digest this tenant does not reference must arrive as ErrNotFound, since
	// §7.2 classifies it as a permanent configuration error and must not retry.
	absentKey := store.ObjectKeyForDigest(store.ContentHash(uniqueContent(t, "not referenced here")))
	if _, _, err := rt.GetObject(ctx, absentKey); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("GetObject(absent) = %v, want objectstore.ErrNotFound", err)
	}
}

// TestArtifactEndpointMySQLHeadSelectsNoContent pins that a HEAD through the
// HTTP route does not pull the multi-MiB BLOB out of MySQL. The repo-level test
// asserts the same about HeadObject's SQL; this one asserts the route reaches
// that method rather than falling back to a Get whose body it discards — which
// would be invisible in the response and cost a full column read per HEAD.
func TestArtifactEndpointMySQLHeadSelectsNoContent(t *testing.T) {
	dsn := requireMySQL(t)

	var captured []string
	db, p := newCapturingArtifactDB(t, dsn, &captured)
	_ = db
	ctx := context.Background()
	ns := uniqueNS(t, "ephead")

	as := store.NewArtifactStore(p.ArtifactObjects(), p.ArtifactIndex())
	content := uniqueContent(t, "endpoint head must not select content")
	ref, err := as.Put(ctx, content, store.ArtifactMeta{Filename: "h.wasm", Namespace: ns})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	h := artifactEndpointServer(t, p, ns, []string{"artifact.read"})
	captured = nil
	rec := artifactGET(t, h, ns, ref.Digest, http.MethodHead)
	if rec.Code != http.StatusOK {
		t.Fatalf("HEAD = %d", rec.Code)
	}
	if got := rec.Header().Get("Content-Length"); got != strconv.Itoa(len(content)) {
		t.Fatalf("HEAD Content-Length = %q, want %d", got, len(content))
	}

	found := false
	for _, sql := range captured {
		if strings.Contains(sql, "xflow_artifact_blobs") && strings.Contains(strings.ToUpper(sql), "SELECT") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("no SELECT against xflow_artifact_blobs captured during HEAD; captured=%v", captured)
	}
	assertNoBlobContentSelected(t, captured)
}
