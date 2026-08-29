package xflow

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/service/apiserver"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/objectstore"
	"github.com/xbcio/xflow/types"
)

// serverTestToken is the bearer token the embedded server's principal
// authenticator accepts. 32 hex chars = 128 bits, the entropy floor
// BearerPrincipalAuth documents.
const serverTestToken = "e3b0c44298fc1c149afbf4c8996fb924"

// memServerArtifactIndex is an in-memory store.ArtifactIndex. HasReference is
// the artifact endpoint's ENTIRE authorization, so this must key on namespace,
// not just digest: an index that ignored namespace would let the endpoint pass
// a test that a cross-tenant caller also passes.
type memServerArtifactIndex struct {
	mu   sync.Mutex
	refs map[string]map[string]bool // namespace -> digest -> referenced
}

func newMemServerArtifactIndex() *memServerArtifactIndex {
	return &memServerArtifactIndex{refs: map[string]map[string]bool{}}
}

func (m *memServerArtifactIndex) Bind(_ context.Context, id store.ArtifactIdentity) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.refs[id.Namespace] == nil {
		m.refs[id.Namespace] = map[string]bool{}
	}
	m.refs[id.Namespace][id.Digest] = true
	return nil
}

func (m *memServerArtifactIndex) HasReference(_ context.Context, ns, digest string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.refs[ns][digest], nil
}

func (m *memServerArtifactIndex) CountReferences(_ context.Context, digest string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var n int64
	for _, byDigest := range m.refs {
		if byDigest[digest] {
			n++
		}
	}
	return n, nil
}

// ListLatestVersions is unused by these tests: they exercise retrieval
// authorization, not the ops-page read path, which is covered against real
// MySQL in test/integration/sqlstore_artifact_test.go.
func (m *memServerArtifactIndex) ListLatestVersions(context.Context, string, store.ListOptions) ([]*store.ArtifactVersion, error) {
	return nil, nil
}

// ListVersions is unused by these tests; see ListLatestVersions above.
func (m *memServerArtifactIndex) ListVersions(context.Context, string, string, store.ListOptions) ([]*store.ArtifactVersion, error) {
	return nil, nil
}

// newTestServer builds an embedded Server with the artifact endpoint fully
// wired -- artifact store plus the principal/authorizer/audit trio the endpoint
// requires -- and returns it with the index and an httptest server in front of
// Handler(). RedisAddr is empty: the in-memory backend is enough here, since
// what is under test is registration and artifact retrieval, not dispatch.
func newTestServer(t *testing.T) (*Server, *memServerArtifactIndex, *httptest.Server) {
	t.Helper()
	idx := newMemServerArtifactIndex()
	artifacts := store.NewArtifactStore(objectstore.NewFSStore(t.TempDir()), idx)

	srv, err := NewServer(ServerConfig{},
		WithServerArtifacts(artifacts),
		WithServerPrincipalAuth(
			apiserver.NewBearerPrincipalAuth(serverTestToken, "test-runner",
				[]string{"artifact.read", "workflow", "execution"}),
			apiserver.NamespaceAwareAuthorizer{},
			apiserver.NewInMemoryAuditSink(),
		),
		WithServerInsecureNoRunnerAuth())
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})

	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return srv, idx, ts
}

// mapBodyScriptWorkflow builds the topology under test: a map node whose body
// is a ScriptFile. The script lives inside the BODY, not at top level, because
// that is where a per-item wasm guest belongs and where the definition scan has
// to reach to rewrite the file path into a digest.
func mapBodyScriptWorkflow(t *testing.T, name string) *WorkflowBuilder {
	t.Helper()
	path := filepath.Join(t.TempDir(), "guest.js")
	if err := os.WriteFile(path, []byte(`({doubled: $item * 2})`), 0o600); err != nil {
		t.Fatalf("write script: %v", err)
	}

	body := Workflow(name + "-body")
	body.Node("s", node.ScriptFile(path).Language("js").Runtime("goja"))

	wf := Workflow(name)
	start := wf.Node("start", node.Start())
	m := wf.Node("m", node.Map("$input.ids", 1))
	m.Body(body)
	wf.Connect(start, m)
	return wf
}

// fetchArtifact performs the GET a runner performs: bearer token, digest in the
// path, no namespace anywhere in the request.
func fetchArtifact(t *testing.T, ts *httptest.Server, digest string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/artifacts/"+digest, nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+serverTestToken)
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("GET artifact: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

// TestServerAddWorkflowStoresFetchableArtifact is the end-to-end criterion for
// the embedded topology: a host registers a workflow in-process, and the runner
// that will execute it can then fetch the body script's bytes over HTTP.
//
// It spans three separate defects, which is why it is one test rather than
// three: without Server.AddWorkflow the host cannot register at all; without
// WithServerArtifacts the route is never mounted and every fetch 404s; and
// without a namespace on the stored artifact the identity row lands under ""
// and HasReference answers false for the caller's real namespace -- also a 404.
// Any one of them missing leaves the runner unable to get its code.
func TestServerAddWorkflowStoresFetchableArtifact(t *testing.T) {
	srv, idx, ts := newTestServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	id, err := srv.AddWorkflow(ctx, mapBodyScriptWorkflow(t, "embedded-map"))
	if err != nil {
		t.Fatalf("AddWorkflow: %v", err)
	}
	if id == "" {
		t.Fatal("AddWorkflow returned an empty workflow id")
	}

	// Exactly one artifact was stored, under the workflow's namespace. Reading
	// the digest from the index rather than recomputing it also pins that the
	// binding carries a namespace: a Put with no namespace would land under ""
	// and this lookup would come up empty.
	digests := idx.refs[string(namespace.Default)]
	if len(digests) != 1 {
		t.Fatalf("index holds %#v for namespace %q, want exactly one digest -- "+
			"the body's ScriptFile was not stored under the workflow's namespace",
			idx.refs, namespace.Default)
	}
	var digest string
	for d := range digests {
		digest = d
	}

	resp := fetchArtifact(t, ts, digest)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/artifacts/%s = %d, want 200 -- the runner cannot fetch "+
			"the code it was told to run", digest, resp.StatusCode)
	}
	buf := make([]byte, 64)
	n, _ := resp.Body.Read(buf)
	if got := string(buf[:n]); !strings.Contains(got, "doubled") {
		t.Fatalf("artifact body = %q, want the script source", got)
	}
}

// serverLocalHandler is a LocalNode handler: a Go instance bound to a node name
// in this process's registry. A Server has no such registry, which is the point
// of the rejection test below.
type serverLocalHandler struct{}

func (serverLocalHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.server_local"}
}

func (serverLocalHandler) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	return &types.Output{Data: map[string]any{"ok": true}}, nil
}

// TestServerAddWorkflowRejectsLocalHandlers pins the fail-closed behavior: a
// Server executes nothing itself, so a workflow carrying LocalNode handlers
// would register cleanly and then stall forever at the first such node. The
// rejection has to happen at registration, where the author can still see it.
func TestServerAddWorkflowRejectsLocalHandlers(t *testing.T) {
	srv, _, _ := newTestServer(t)

	wf := Workflow("with-local")
	start := wf.Node("start", node.Start())
	local := wf.LocalNode("work", &serverLocalHandler{})
	wf.Connect(start, local)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := srv.AddWorkflow(ctx, wf)
	if err == nil {
		t.Fatal("AddWorkflow accepted a workflow with local node handlers; a Server " +
			"has no executor for them, so it would register and then stall")
	}
	if !strings.Contains(err.Error(), "local node handlers") {
		t.Fatalf("error = %q, want it to name the local handlers as the reason", err)
	}
}

// TestServerArtifactRouteUnmountedWithoutStore pins that the artifact route is
// gated on configuration rather than always-present-but-empty: with no artifact
// store the route must not exist at all.
func TestServerArtifactRouteUnmountedWithoutStore(t *testing.T) {
	srv, err := NewServer(ServerConfig{},
		WithServerPrincipalAuth(
			apiserver.NewBearerPrincipalAuth(serverTestToken, "test-runner", []string{"artifact.read"}),
			apiserver.NamespaceAwareAuthorizer{},
			apiserver.NewInMemoryAuditSink(),
		),
		WithServerInsecureNoRunnerAuth())
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	resp := fetchArtifact(t, ts, "sha256:"+strings.Repeat("a", 64))
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET artifact with no store = %d, want 404", resp.StatusCode)
	}
}

// TestServerPrincipalAuthRequiresAuthorizerAndAudit pins the fail-closed
// contract that forces the three-in-one option shape: a principal
// authenticator without an authorizer or an audit sink must be rejected at
// construction, not accepted into a server that denies everything or audits
// nothing.
func TestServerPrincipalAuthRequiresAuthorizerAndAudit(t *testing.T) {
	auth := apiserver.NewBearerPrincipalAuth(serverTestToken, "test-runner", []string{"artifact.read"})

	if _, err := NewServer(ServerConfig{},
		WithServerPrincipalAuth(auth, nil, apiserver.NewInMemoryAuditSink()),
		WithServerInsecureNoRunnerAuth()); err == nil {
		t.Fatal("NewServer accepted a PrincipalAuth with no Authorizer; every " +
			"request would then be denied by default")
	}
	if _, err := NewServer(ServerConfig{},
		WithServerPrincipalAuth(auth, apiserver.NamespaceAwareAuthorizer{}, nil),
		WithServerInsecureNoRunnerAuth()); err == nil {
		t.Fatal("NewServer accepted a PrincipalAuth with no AuditSink; mutations " +
			"would execute unaudited")
	}
}
