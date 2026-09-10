//go:build integration

package integration

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/sdk/xflow"
	"github.com/xbcio/xflow/service/apiserver"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/objectstore"
)

// This file pins the runner-artifact-namespace-authorization design's §6
// acceptance set, against a real control-plane server backed by real Redis
// and real MySQL (mirrors embedded_server_artifact_e2e_test.go's harness).
// The three cases that need real infrastructure live here:
//
//   - test 1, over-privileged read: runner token bound to A, task declaring B,
//     digest referenced only by A -> must be 404
//     (TestArtifactDeclaredNamespaceDeniesOverPrivilegedRead).
//   - test 2, multi-namespace positive control: policy allows both A and B,
//     task declaring B, digest referenced by B -> must be 200
//     (TestArtifactDeclaredNamespaceAllowsMultiNamespaceRunner). Without it,
//     test 1 would also pass for an implementation that denies everything.
//   - test 3, cache does not cross tenants: the same runner fetches digest D
//     under A (succeeds), then under B -> must fail with
//     objectstore.ErrNotFound (HTTPStore's mapping of the server's 404) AND
//     must actually have reached origin again; without asserting that the
//     origin hit count went up, the test would be proving something about the
//     cache rather than about authorization
//     (TestArtifactRunnerCacheDoesNotCrossNamespaces).
//
// Test 4 (auth-disabled must not trust the declaration) and test 5
// (SetNamespace actually receives lease.Namespace) do not need real
// infrastructure and live elsewhere: test 5 in execution/runner_test.go
// (TestRunner_LeaseNamespaceInjectedIntoContext), test 4 as an in-memory
// apiserver unit test (service/apiserver's
// TestArtifactEndpointAuthDisabledIgnoresDeclaredNamespace).

// newArtifactNamespaceServer builds a real control-plane server whose
// principal (BearerPrincipalAuth) is bound to principalNS, and whose
// runner-protocol Authenticator grants the SAME token's runner id membership
// in every namespace in allowedNamespaces. This is the "multi-namespace
// runner, single-namespace principal binding" shape the design's gap
// concerns: a runner's principal is provisioned once, broadly, while any one
// task belongs to exactly one namespace.
func newArtifactNamespaceServer(t *testing.T, principalNS string, allowedNamespaces []string) (ts *httptest.Server, artifacts *store.ArtifactStore, token, runnerID string) {
	t.Helper()
	redisAddr := requireRedis(t)
	provider := newEmbeddedProvider(t)
	artifacts = store.NewArtifactStore(provider.ArtifactObjects(), provider.ArtifactIndex())

	token = uniqueNSBID("and-token")
	runnerID = uniqueNSBID("and-runner")

	runnerAuth, err := control.NewStaticTokenAuthenticator(runnerID, token, allowedNamespaces, []string{"*"})
	if err != nil {
		t.Fatalf("NewStaticTokenAuthenticator: %v", err)
	}

	principalAuth := apiserver.NewBearerPrincipalAuthMulti([]apiserver.TokenPrincipalMapping{
		{Token: token, Subject: runnerID, Namespace: principalNS, Scopes: []string{"artifact.read"}},
	})

	srv, err := xflow.NewServer(
		xflow.ServerConfig{RedisAddr: redisAddr, Store: provider},
		xflow.WithServerArtifacts(artifacts),
		xflow.WithServerPrincipalAuth(principalAuth, apiserver.NamespaceAwareAuthorizer{}, apiserver.NewSQLAuditSink(provider)),
		xflow.WithServerAuth(runnerAuth),
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("Server.Start: %v", err)
	}
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer shutdownCancel()
		_ = srv.Shutdown(shutdownCtx)
	})

	ts = httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts, artifacts, token, runnerID
}

// doArtifactRequest issues GET /v1/artifacts/{digest} with the bearer token,
// and (when non-empty) the X-Xflow-Runner-Id and X-Xflow-Namespace
// declaration headers a runner attaches per objectstore.HTTPStore.declareNamespace.
func doArtifactRequest(t *testing.T, client *http.Client, baseURL, digest, token, runnerID, declaredNS string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, baseURL+"/v1/artifacts/"+digest, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if runnerID != "" {
		req.Header.Set(protocol.RunnerIDHeader, runnerID)
	}
	if declaredNS != "" {
		req.Header.Set(objectstore.NamespaceHeader, declaredNS)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	return resp
}

// TestArtifactDeclaredNamespaceDeniesOverPrivilegedRead is design §6 test 1:
// a runner whose principal is bound to tenantA, but whose runner-protocol
// policy also grants tenantB (a genuinely multi-namespace runner), must NOT
// be able to read a digest that is referenced only by tenantA when it is
// currently executing a task for tenantB and declares that namespace. Before
// the fix, module_artifact.go always authorized against the principal's own
// namespace (tenantA) regardless of the declaration, so this read would
// succeed (200) -- exactly the over-privileged read the design closes.
func TestArtifactDeclaredNamespaceDeniesOverPrivilegedRead(t *testing.T) {
	tenantA := uniqueNSBID("and1-tenant-a")
	tenantB := uniqueNSBID("and1-tenant-b")
	ts, artifacts, token, runnerID := newArtifactNamespaceServer(t, tenantA, []string{tenantA, tenantB})

	content := []byte(uniqueNSBID("and1-secret-content"))
	ref, err := artifacts.Put(context.Background(), content, store.ArtifactMeta{
		Filename:  uniqueNSBID("and1-file") + ".bin",
		Namespace: tenantA,
	})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	resp := doArtifactRequest(t, ts.Client(), ts.URL, ref.Digest, token, runnerID, tenantB)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET declared namespace=%s = %d, want 404 (digest %s is only referenced by %s, not %s); body=%s",
			tenantB, resp.StatusCode, ref.Digest, tenantA, tenantB, body)
	}
}

// TestArtifactDeclaredNamespaceAllowsMultiNamespaceRunner is design §6 test 2,
// the positive control for test 1: the same multi-namespace runner, declaring
// the SAME namespace (tenantB) that actually references the digest, must
// succeed (200). Without this control, an implementation that always denies
// (e.g. resolveNamespace hard-coded to return "" or never trusting the
// declaration in a way that also breaks the legitimate case) would make test
// 1 pass for the wrong reason.
func TestArtifactDeclaredNamespaceAllowsMultiNamespaceRunner(t *testing.T) {
	tenantA := uniqueNSBID("and2-tenant-a")
	tenantB := uniqueNSBID("and2-tenant-b")
	ts, artifacts, token, runnerID := newArtifactNamespaceServer(t, tenantA, []string{tenantA, tenantB})

	content := []byte(uniqueNSBID("and2-content"))
	ref, err := artifacts.Put(context.Background(), content, store.ArtifactMeta{
		Filename:  uniqueNSBID("and2-file") + ".bin",
		Namespace: tenantB,
	})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	resp := doArtifactRequest(t, ts.Client(), ts.URL, ref.Digest, token, runnerID, tenantB)
	defer resp.Body.Close()
	got, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET declared namespace=%s = %d, want 200 (digest %s IS referenced by %s); body=%s",
			tenantB, resp.StatusCode, ref.Digest, tenantB, got)
	}
	if string(got) != string(content) {
		t.Fatalf("GET body = %q, want %q", got, content)
	}
}

// countingRoundTripper counts every request whose path is under
// /v1/artifacts/, so the cache-crossing test below can prove a fetch actually
// reached the origin server rather than being served from the runner's local
// disk cache.
type countingRoundTripper struct {
	base    http.RoundTripper
	counter int64
}

func (c *countingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if strings.HasPrefix(req.URL.Path, "/v1/artifacts/") {
		atomic.AddInt64(&c.counter, 1)
	}
	return c.base.RoundTrip(req)
}

func (c *countingRoundTripper) count() int64 { return atomic.LoadInt64(&c.counter) }

// TestArtifactRunnerCacheDoesNotCrossNamespaces is design §6 test 3: the SAME
// runner, using its real local read-through cache
// (objectstore.ReadThrough{FSStore, HTTPStore}), fetches the SAME digest
// first while declaring tenantA (succeeds, and would populate a
// namespace-unaware cache keyed only by digest), then again while declaring
// tenantB. The digest is referenced only by tenantA, so the second fetch
// must both (a) fail with ErrNotFound, and (b) actually reach the origin a
// second time -- proving the local cache did not serve tenantB the bytes it
// cached under tenantA's declaration without even asking the server.
//
// Mutation target: store/objectstore/fsstore.go's FSStore.PartitionByNamespace,
// set true only at this call site's cache construction below. Flipping it to
// false collapses both fetches onto the same on-disk path: the second fetch
// is served from cache, the origin counter stays at 1, and the read
// incorrectly succeeds.
func TestArtifactRunnerCacheDoesNotCrossNamespaces(t *testing.T) {
	tenantA := uniqueNSBID("and3-tenant-a")
	tenantB := uniqueNSBID("and3-tenant-b")
	ts, artifacts, token, runnerID := newArtifactNamespaceServer(t, tenantA, []string{tenantA, tenantB})

	content := []byte(uniqueNSBID("and3-content"))
	ref, err := artifacts.Put(context.Background(), content, store.ArtifactMeta{
		Filename:  uniqueNSBID("and3-file") + ".bin",
		Namespace: tenantA,
	})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	crt := &countingRoundTripper{base: http.DefaultTransport}
	httpClient := &http.Client{Transport: crt, Timeout: 30 * time.Second}

	origin := &objectstore.HTTPStore{BaseURL: ts.URL, Token: token, Client: httpClient, RunnerID: runnerID}
	cache := objectstore.NewFSStore(t.TempDir())
	cache.PartitionByNamespace = true
	readThrough := objectstore.NewReadThrough(cache, origin)
	runnerArtifacts := store.NewArtifactStore(readThrough, nil)

	ctxA := namespace.WithNamespace(context.Background(), namespace.Namespace(tenantA))
	rc, _, err := runnerArtifacts.Open(ctxA, ref.Digest)
	if err != nil {
		t.Fatalf("first fetch (declared namespace %s) = %v, want success", tenantA, err)
	}
	gotFirst, readErr := io.ReadAll(rc)
	_ = rc.Close()
	if readErr != nil {
		t.Fatalf("read first fetch body: %v", readErr)
	}
	if string(gotFirst) != string(content) {
		t.Fatalf("first fetch body = %q, want %q", gotFirst, content)
	}
	if got := crt.count(); got != 1 {
		t.Fatalf("origin hit count after first fetch = %d, want 1", got)
	}

	ctxB := namespace.WithNamespace(context.Background(), namespace.Namespace(tenantB))
	_, _, err = runnerArtifacts.Open(ctxB, ref.Digest)
	if err == nil {
		t.Fatalf("second fetch (declared namespace %s) succeeded, want ErrNotFound -- digest %s "+
			"is only referenced by %s", tenantB, ref.Digest, tenantA)
	}
	if !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("second fetch error = %v, want objectstore.ErrNotFound", err)
	}
	if got := crt.count(); got != 2 {
		t.Fatalf("origin hit count after second fetch = %d, want 2 -- the second fetch must "+
			"reach the origin, not be served bytes cached under namespace %s's cache entry", got, tenantA)
	}
}
