//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/sdk/xflow"
	"github.com/xbcio/xflow/service/apiserver"
	"github.com/xbcio/xflow/service/protocol"
	runnersvc "github.com/xbcio/xflow/service/runner"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/objectstore"
	"github.com/xbcio/xflow/store/sqlstore"
	"github.com/xbcio/xflow/types"

	// Blank import so execution.Registry's nodereg fallback can resolve every
	// built-in handler (xflow.map, xflow.script). Without it the body's script
	// member has no handler and the batch fails with a lookup error rather than
	// exercising the artifact path under test.
	_ "github.com/xbcio/xflow/node"
)

// embeddedToken is the bearer token the embedded server's principal
// authenticator accepts. 32 hex chars = 128 bits, the entropy floor
// BearerPrincipalAuth documents. It travels ONLY in the Authorization header —
// never a query parameter, never a body field.
const embeddedToken = "9f2c41ba7de05c83a6114e7d29b8f350"

// embeddedRunnerID is the runner identity for this test's in-process runner.
// Unique per test binary run so a leftover heartbeat from another integration
// test cannot be mistaken for this one.
const embeddedRunnerID = "embedded-artifact-runner"

// newEmbeddedProvider opens the MySQL the SAS host would reuse and creates the
// xflow_* tables. This is the storage decision under test: one MySQL, one
// AutoMigrate, one Provider serving both the artifact blobs and the artifact
// identity index.
func newEmbeddedProvider(t *testing.T) *sqlstore.Provider {
	t.Helper()
	dsn := requireMySQL(t)
	db, err := gorm.Open(gormmysql.Open(dsn), &gorm.Config{TranslateError: true})
	if err != nil {
		t.Fatalf("gorm.Open: %v", err)
	}
	if err := sqlstore.AutoMigrate(db); err != nil {
		t.Fatalf("AutoMigrate: %v", err)
	}
	return sqlstore.New(db)
}

// embeddedArtifactResolver builds the runner-side artifact fetch chain exactly
// as cmd/runner does: an on-disk read-through cache in front of the control
// plane's HTTP artifact endpoint. Nothing here reaches the server's store
// directly — a byte the body executes has to come back over HTTP.
func embeddedArtifactResolver(baseURL string, client *http.Client, cacheDir string) func(context.Context, string) ([]byte, error) {
	origin := &objectstore.HTTPStore{BaseURL: baseURL, Token: embeddedToken, Client: client}
	readThrough := objectstore.NewReadThrough(objectstore.NewFSStore(cacheDir), origin)
	artifacts := store.NewArtifactStore(readThrough, nil)
	return func(ctx context.Context, digest string) ([]byte, error) {
		rc, _, err := artifacts.Open(ctx, digest)
		if err != nil {
			return nil, err
		}
		defer rc.Close()
		return io.ReadAll(rc)
	}
}

// embeddedMapWorkflow is the SAS topology in miniature: an entry node whose
// downstream is a map node, and a body that is a single artifact-backed script.
// The script lives in the BODY, which is the case no prior e2e covered — a
// top-level script resolves its artifact through the distributed dispatcher,
// while a body member resolves it through the subgraph runtime's inner backend.
//
// The doubling is what proves the guest actually ran: a resolver that returned
// empty bytes, or a body that echoed its item through, would satisfy a
// "no error" assertion just as well.
// embeddedRunTag makes every artifact this run stores content-unique.
//
// This is load-bearing, not hygiene. xflow_artifacts is content-addressed and
// survives the test process: a previous green run leaves a row for the same
// digest under namespace "default". With a fixed script body, a regression that
// stores the artifact under the WRONG namespace still passes, because the
// endpoint's HasReference check finds yesterday's correct row. Verified: the
// namespace-regression control passed until this tag existed.
var embeddedRunTag = fmt.Sprintf("run-%d", time.Now().UnixNano())

// embeddedScriptSource is the body guest: it doubles the item, and carries the
// run tag in a comment so its digest is unique to this run.
func embeddedScriptSource() string {
	return fmt.Sprintf("/* %s */ ({doubled: $item * 2})", embeddedRunTag)
}

func embeddedMapWorkflow(t *testing.T, name string) *xflow.WorkflowBuilder {
	t.Helper()
	path := filepath.Join(t.TempDir(), embeddedRunTag+".js")
	if err := os.WriteFile(path, []byte(embeddedScriptSource()), 0o600); err != nil {
		t.Fatalf("write script artifact: %v", err)
	}

	body := xflow.Workflow(name + "-body")
	body.Node("s", node.ScriptFile(path).Language("js").Runtime("goja"))

	wf := xflow.Workflow(name).Version("v1")
	start := wf.Node("start", node.Start())
	m := wf.Node("m", node.Map("$input.rows", 1))
	m.Body(body)
	wf.Connect(start, m)
	return wf
}

// registeredClient wraps the real protocol client and closes ready once the
// runner's registration call has actually succeeded over HTTP.
//
// This is an observation of the production path, not a substitute for it:
// every method delegates to the same protocol.Client cmd/runner uses, and the
// signal fires on the real Register response. The control plane exposes no
// runner-listing endpoint, so the alternative would be to seed blind and rely
// on the dispatcher requeueing the unroutable task -- which passes for the
// wrong reason if capabilities are misdeclared.
type registeredClient struct {
	runnersvc.ProtocolClient
	ready chan struct{}
	once  sync.Once
}

func (c *registeredClient) Register(ctx context.Context, req protocol.RegisterRunnerRequest) (protocol.RegisterRunnerResponse, error) {
	resp, err := c.ProtocolClient.Register(ctx, req)
	if err == nil {
		c.once.Do(func() { close(c.ready) })
	}
	return resp, err
}

// startEmbeddedRunner starts an in-process runner against the embedded server,
// wired the way cmd/runner wires the production binary: one artifact resolver
// shared by the top-level dispatcher, the group runtime and the subgraph
// runtime. It returns once the runner has registered.
//
// Capabilities matter and are easy to get silently wrong: batch routing
// advertises the MAP node's own type (engine.TaskRouting returns meta.Type for
// a non-group unit), so without xflow.map the batch is never offered to this
// runner and the execution simply hangs with no error anywhere.
func startEmbeddedRunner(t *testing.T, baseURL string, client *http.Client) {
	t.Helper()
	resolver := embeddedArtifactResolver(baseURL, client, t.TempDir())
	registry := execution.NewRegistry()

	groupRuntime := runnersvc.NewGroupRuntime(
		registry,
		runnersvc.NewPackageCache(runnersvc.PackageCacheConfig{MaxEntries: 32}),
		runnersvc.WithSuspendDisabled(),
		runnersvc.WithGroupArtifactCodeResolver(resolver))
	subgraphRuntime := runnersvc.NewSubgraphRuntime(
		registry,
		runnersvc.NewPackageCache(runnersvc.PackageCacheConfig{MaxEntries: 32}),
		runnersvc.WithSubgraphArtifactCodeResolver(resolver))

	pc := &registeredClient{
		ProtocolClient: protocol.NewClient(baseURL, client).WithToken(embeddedToken),
		ready:          make(chan struct{}),
	}
	r := runnersvc.New(
		pc,
		registry,
		runnersvc.Config{
			RunnerID:    embeddedRunnerID,
			Concurrency: 2,
			PollWait:    50 * time.Millisecond,
			Capabilities: []protocol.Capability{
				{NodeType: "xflow.map"},
				{NodeType: "xflow.script"},
				{NodeType: engine.GroupNodeType, Features: []string{engine.FeatureGroupExecV1}},
			},
			GroupRuntime:         groupRuntime,
			SubgraphRuntime:      subgraphRuntime,
			ArtifactCodeResolver: resolver,
		})

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- r.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-errCh:
		case <-time.After(10 * time.Second):
			t.Error("embedded runner did not stop within 10s of context cancellation")
		}
	})

	// Seeding before registration would race: the first batch would be
	// dispatched with no eligible runner and only picked up on a later sweep.
	select {
	case <-pc.ready:
	case err := <-errCh:
		t.Fatalf("embedded runner exited before registering: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("embedded runner did not register with the control plane within 30s")
	}
}

// seedEmbeddedExecution performs the admission a trigger performs: POST
// /v1/executions with the entry unit's boundary exits. The entry node's output
// travels in Exits[].Data, which the seed writes to the same output key
// buildInput reads for a single-in-edge downstream node — that is how the map
// node's "$input.rows" resolves without any node ever executing "start".
func seedEmbeddedExecution(t *testing.T, baseURL string, wfID types.WorkflowID, rows []any) types.ExecutionID {
	t.Helper()
	req := protocol.SeedExecutionRequest{
		ProtocolVersion: protocol.EntrySeedProtocolVersion,
		WorkflowID:      string(wfID),
		WorkflowVersion: "v1",
		EntryUnitID:     "start",
		AdmissionKey:    fmt.Sprintf("embedded-artifact-%d", time.Now().UnixNano()),
		Outcome:         string(engine.GroupOutcomeSuccess),
		Exits: []protocol.BoundaryExit{{
			NodeName: "start",
			Port:     "main",
			Data:     map[string]any{"rows": rows},
		}},
	}
	resp, raw := g1DoAuth(t, http.MethodPost, baseURL, "/v1/executions", embeddedToken, req)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /v1/executions = %d, body=%s", resp.StatusCode, raw)
	}
	var seeded protocol.SeedExecutionResponse
	if err := json.Unmarshal(raw, &seeded); err != nil {
		t.Fatalf("decode seed response: %v (raw=%s)", err, raw)
	}
	if seeded.State != "accepted" || seeded.ExecutionID == "" {
		t.Fatalf("seed response = %+v, want an accepted execution id", seeded)
	}
	return types.ExecutionID(seeded.ExecutionID)
}

// TestEmbeddedServerRunsMapBodyArtifact is the end-to-end criterion for the
// embedded (in-process control plane) topology the SAS host will run:
//
//	host process
//	├── xflow.NewServer            — mounted handler, MySQL-backed artifacts
//	└── runner (same process)      — fetches guest bytes over loopback HTTP
//
// It spans four separate defects that each independently break that topology:
// without Server.AddWorkflow the host cannot register its Go-value definition
// at all; without WithServerArtifacts the artifact route is never mounted and
// every fetch 404s; without a namespace on the stored artifact the identity row
// lands under "" and HasReference answers false for the caller's real namespace
// (also a 404); and without the artifact resolver reaching the subgraph
// runtime's inner backend the body's script fails with script.artifact_unavailable.
//
// Success of the execution is the load-bearing assertion. The control plane
// configures engine.WithRemoteBatchExecution unconditionally and holds no batch
// body executor of its own, so a batch it cannot route to a runner is never
// dispatched — reaching Success means the body ran on the runner, over the real
// artifact fetch, and not in the server.
func TestEmbeddedServerRunsMapBodyArtifact(t *testing.T) {
	redisAddr := requireRedis(t)
	provider := newEmbeddedProvider(t)

	artifacts := store.NewArtifactStore(provider.ArtifactObjects(), provider.ArtifactIndex())
	srv, err := xflow.NewServer(
		xflow.ServerConfig{RedisAddr: redisAddr, Store: provider},
		xflow.WithServerArtifacts(artifacts),
		xflow.WithServerPrincipalAuth(
			apiserver.NewBearerPrincipalAuth(embeddedToken, "embedded-host",
				[]string{"workflow", "execution", "artifact.read", "supply.read"}),
			apiserver.NamespaceAwareAuthorizer{},
			apiserver.NewSQLAuditSink(provider),
		))
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("Server.Start: %v", err)
	}
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer shutdownCancel()
		_ = srv.Shutdown(shutdownCtx)
	})

	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	wfID, err := srv.AddWorkflow(ctx, embeddedMapWorkflow(t, "embedded-map-artifact-"+embeddedRunTag))
	if err != nil {
		t.Fatalf("AddWorkflow: %v", err)
	}

	startEmbeddedRunner(t, ts.URL, ts.Client())

	auditMark := maxAuditID(t, ctx, provider)
	execID := seedEmbeddedExecution(t, ts.URL, wfID, []any{3, 4})
	detail := g1WaitForTerminal(t, ts.URL, embeddedToken, execID, 90*time.Second)
	if detail.Status != types.ExecutionStatusSuccess {
		t.Fatalf("execution status=%s, want %s (error=%q)", detail.Status, types.ExecutionStatusSuccess, detail.Error)
	}

	results := mapResults(t, detail)
	if len(results) != 2 {
		t.Fatalf("map produced %d results, want 2: %+v", len(results), results)
	}
	got := map[float64]bool{}
	for i := range results {
		row := itemRow(t, results, i)
		doubled, ok := row["doubled"].(float64)
		if !ok {
			t.Fatalf("results[%d] has no numeric doubled field: %#v", i, row)
		}
		got[doubled] = true
	}
	for _, want := range []float64{6, 8} {
		if !got[want] {
			t.Fatalf("results = %+v, want a doubled value of %v -- the body script "+
				"did not run over the fetched artifact", results, want)
		}
	}

	assertEmbeddedArtifactStored(t, ctx, provider, artifacts)
	assertEmbeddedSeedAudited(t, ctx, provider, auditMark)
}

// assertEmbeddedArtifactStored pins that the guest bytes really landed in MySQL
// under the workflow's namespace, and that a namespace-scoped reference check —
// the artifact endpoint's entire authorization — answers true for it. An
// artifact stored with an empty namespace would still be fetchable by digest
// from the object store, so asserting only "the body ran" cannot distinguish
// the two.
func assertEmbeddedArtifactStored(t *testing.T, ctx context.Context, provider *sqlstore.Provider, artifacts *store.ArtifactStore) {
	t.Helper()
	var rows []struct {
		Namespace   string
		Filename    string
		ContentHash string
	}
	if err := provider.DB().WithContext(ctx).
		Table("xflow_artifacts").
		Select("namespace, filename, content_hash").
		Where("filename = ?", embeddedRunTag+".js").
		Find(&rows).Error; err != nil {
		t.Fatalf("query xflow_artifacts: %v", err)
	}
	if len(rows) == 0 {
		t.Fatal("no xflow_artifacts row for the body script; AddWorkflow did not store the artifact")
	}
	var stored string
	for _, r := range rows {
		if r.Namespace == string(namespace.Default) {
			stored = r.ContentHash
		}
	}
	if stored == "" {
		t.Fatalf("xflow_artifacts rows = %+v, want one under namespace %q -- an artifact "+
			"bound to the wrong namespace makes the endpoint's reference check answer false",
			rows, namespace.Default)
	}
	ok, err := provider.ArtifactIndex().HasReference(ctx, string(namespace.Default), stored)
	if err != nil {
		t.Fatalf("HasReference: %v", err)
	}
	if !ok {
		t.Fatalf("HasReference(%q, %q) = false; the runner's fetch would 404", namespace.Default, stored)
	}
	rc, _, err := artifacts.Open(ctx, stored)
	if err != nil {
		t.Fatalf("open stored artifact: %v", err)
	}
	defer rc.Close()
	content, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read stored artifact: %v", err)
	}
	if string(content) != embeddedScriptSource() {
		t.Fatalf("stored artifact = %q, want the script source", content)
	}
}

// assertEmbeddedSeedAudited pins that the seed went through the authorizing
// path rather than an unauthenticated fallback: an admitted mutation writes an
// audit row before the handler runs, and fails closed if the sink is
// unavailable. Without WithServerPrincipalAuth the route would either 404 or
// run unaudited, and the execution assertions alone cannot tell those apart.
//
// Scoping note: the seed's audit row canNOT be found by execution_id. The
// /v1/executions route resolves its audit execution id with
// newExecutionIDResolver, which mints a FRESH random id per request, while a
// seeded execution's id is derived deterministically from the admission key
// inside the engine. The two never match, so the audit row for a seed points
// at an execution id that does not exist. That is a real correlation gap
// (tracked separately) — asserting on execution_id here would only hide it
// behind a red test of our own making. The rows are instead bounded by
// sinceID, the max audit id observed before the seed, so a row another test
// left behind cannot satisfy this.
func assertEmbeddedSeedAudited(t *testing.T, ctx context.Context, provider *sqlstore.Provider, sinceID int64) {
	t.Helper()
	var rows []struct {
		ID          int64
		Principal   string
		Operation   string
		Decision    string
		Namespace   string
		Phase       string
		ExecutionID string
	}
	if err := provider.DB().WithContext(ctx).
		Table("xflow_audit_events").
		Select("id, principal, operation, decision, namespace, phase, execution_id").
		Where("id > ? AND operation = ?", sinceID, apiserver.OpExecutionSeed).
		Order("id").
		Find(&rows).Error; err != nil {
		t.Fatalf("query xflow_audit_events: %v", err)
	}
	var admitted bool
	for _, r := range rows {
		if r.Phase == "admission" && r.Decision == string(apiserver.DecisionAllow) &&
			r.Principal == "embedded-host" && r.Namespace == string(namespace.Default) {
			admitted = true
		}
	}
	if !admitted {
		t.Fatalf("no admitted %s audit row after id %d (rows=%+v); the seed did not go "+
			"through the authenticated, audited path", apiserver.OpExecutionSeed, sinceID, rows)
	}
}

// maxAuditID reads the current high-water mark of the audit table so a later
// assertion can bound itself to rows this test produced.
func maxAuditID(t *testing.T, ctx context.Context, provider *sqlstore.Provider) int64 {
	t.Helper()
	var max struct{ ID int64 }
	if err := provider.DB().WithContext(ctx).
		Table("xflow_audit_events").
		Select("COALESCE(MAX(id), 0) AS id").
		Scan(&max).Error; err != nil {
		t.Fatalf("read max audit id: %v", err)
	}
	return max.ID
}
