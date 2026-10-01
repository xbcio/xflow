//go:build integration

package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/backend/providers/distributed"
	"github.com/xbcio/xflow/backend/workflowhash"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/sdk/xflow"
	"github.com/xbcio/xflow/types"
)

// The apiserver's registration reconcile (workflowhash.ReconcileAdd plus the
// UpdateDefinitionHash CAS) against the Redis workflow registry. The unit tests
// in service/apiserver and sdk/xflow run it on the local registry only; the
// Redis registry resolves a lost CAS differently (new-hash short-circuit plus a
// revision CAS in Lua), so it is exercised here against a real Redis.
//
// A legacy record is made by registering normally and then rewriting its hash
// to the bare "sha256:" form an older server's POST stored, through a second
// distributed backend on the same Redis. That reproduces the stored state
// without depending on the apiserver's registry key format.

type hashReconcileEnv struct {
	t   *testing.T
	srv *xflow.Server
	ts  *httptest.Server
	reg backend.WorkflowRegistry
	ctx context.Context
}

func newHashReconcileEnv(t *testing.T) *hashReconcileEnv {
	t.Helper()
	addr := requireRedis(t)

	srv, err := xflow.NewServer(xflow.ServerConfig{RedisAddr: addr}, xflow.WithServerInsecureNoRunnerAuth())
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := srv.Start(ctx); err != nil {
		cancel()
		t.Fatalf("Server.Start: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		shutCtx, shutCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer shutCancel()
		_ = srv.Shutdown(shutCtx)
	})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	direct, err := distributed.New(addr, nil, distributed.WithConsumer(false))
	if err != nil {
		t.Fatalf("distributed.New: %v", err)
	}
	// A non-consumer backend owns no workers; Bind only hands back the stop
	// that releases its transport and Redis client.
	t.Cleanup(direct.Bind(engine.New(direct.State(), direct.Queue())))

	return &hashReconcileEnv{
		t:   t,
		srv: srv,
		ts:  ts,
		reg: direct.WorkflowRegistry(),
		ctx: namespace.WithNamespace(context.Background(), namespace.Default),
	}
}

// hashReconcileWorkflow is start -> wait. The SDK builder writes wait.mode,
// the builtin Default; the stripped HTTP body omits it.
func hashReconcileWorkflow(name, signal string) *xflow.WorkflowBuilder {
	wf := xflow.Workflow(name)
	start := wf.Node("start", node.Start())
	wait := wf.Node("wait", node.Wait(signal))
	wf.Connect(start, wait)
	return wf
}

// body returns the SDK definition of wf as a JSON body, with wait.mode
// removed when stripped.
func (e *hashReconcileEnv) body(wf *xflow.WorkflowBuilder, stripped bool) map[string]any {
	e.t.Helper()
	def, err := wf.Definition()
	if err != nil {
		e.t.Fatalf("Definition: %v", err)
	}
	data, err := json.Marshal(def)
	if err != nil {
		e.t.Fatalf("marshal: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(data, &out); err != nil {
		e.t.Fatalf("unmarshal: %v", err)
	}
	if stripped {
		params := out["nodes"].([]any)[1].(map[string]any)["parameters"].(map[string]any)
		if _, ok := params["mode"]; !ok {
			e.t.Fatal("SDK definition has no wait.mode; the fixture no longer covers a written Default")
		}
		delete(params, "mode")
	}
	return out
}

func (e *hashReconcileEnv) send(method, path string, body map[string]any) (int, types.WorkflowID) {
	e.t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		e.t.Fatalf("marshal: %v", err)
	}
	req, err := http.NewRequest(method, e.ts.URL+path, bytes.NewReader(data))
	if err != nil {
		e.t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := e.ts.Client().Do(req)
	if err != nil {
		e.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	var env struct {
		Data struct {
			WorkflowID types.WorkflowID `json:"workflow_id"`
		} `json:"data"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&env)
	return resp.StatusCode, env.Data.WorkflowID
}

func (e *hashReconcileEnv) post(body map[string]any, want int) types.WorkflowID {
	e.t.Helper()
	status, id := e.send(http.MethodPost, "/v1/workflows", body)
	if status != want {
		e.t.Fatalf("POST /v1/workflows = %d, want %d", status, want)
	}
	return id
}

func (e *hashReconcileEnv) put(id types.WorkflowID, body map[string]any) {
	e.t.Helper()
	body["id"] = string(id)
	status, got := e.send(http.MethodPut, "/v1/workflows/"+string(id), body)
	if status != http.StatusOK || got != id {
		e.t.Fatalf("PUT /v1/workflows/%s = %d id %q, want 200 with the path id", id, status, got)
	}
}

func (e *hashReconcileEnv) record(id types.WorkflowID) backend.WorkflowRecord {
	e.t.Helper()
	rec, err := e.reg.GetWorkflow(e.ctx, id)
	if err != nil {
		e.t.Fatalf("GetWorkflow(%s): %v", id, err)
	}
	return rec
}

// makeLegacy rewrites id's stored hash to the bare "sha256:" hash an older
// server's POST stored (SHA-256 over the full definition JSON) and returns the
// record as left. The definition is not touched.
func (e *hashReconcileEnv) makeLegacy(id types.WorkflowID) backend.WorkflowRecord {
	e.t.Helper()
	rec := e.record(id)
	data, err := json.Marshal(rec.Definition)
	if err != nil {
		e.t.Fatalf("marshal stored definition: %v", err)
	}
	sum := sha256.Sum256(data)
	legacy := "sha256:" + hex.EncodeToString(sum[:])
	if err := e.reg.UpdateDefinitionHash(e.ctx, id, rec.DefinitionHash, legacy); err != nil {
		e.t.Fatalf("seed legacy hash: %v", err)
	}
	got := e.record(id)
	if got.DefinitionHash != legacy {
		e.t.Fatalf("seeded hash = %q, want %q", got.DefinitionHash, legacy)
	}
	return got
}

func hashReconcileName(t *testing.T) string {
	return "hash-reconcile-" + strings.ToLower(strings.ReplaceAll(t.Name(), "/", "-")) + "-" + time.Now().UTC().Format("150405.000000000")
}

// TestRedisRegistrationReconcilesLegacyHash: a legacy "sha256:" record behind
// a stripped HTTP registration is upgraded in place by an identical re-POST,
// by a re-POST that spells out the Default, and is then idempotent for the
// SDK. The stored definition is never rewritten; a real change still
// conflicts.
func TestRedisRegistrationReconcilesLegacyHash(t *testing.T) {
	e := newHashReconcileEnv(t)
	name := hashReconcileName(t)

	id := e.post(e.body(hashReconcileWorkflow(name, "go"), true), http.StatusCreated)
	current := e.record(id)
	if !strings.HasPrefix(current.DefinitionHash, workflowhash.RuntimePrefixV1) {
		t.Fatalf("POST stored hash %q, want the %s runtime hash", current.DefinitionHash, workflowhash.RuntimePrefixV1)
	}

	for _, stripped := range []bool{true, false} {
		legacy := e.makeLegacy(id)
		got := e.post(e.body(hashReconcileWorkflow(name, "go"), stripped), http.StatusCreated)
		if got != id {
			t.Fatalf("stripped=%v: re-POST id = %q, want the legacy record %q", stripped, got, id)
		}
		after := e.record(id)
		if after.DefinitionHash != current.DefinitionHash {
			t.Fatalf("stripped=%v: hash = %q, want it upgraded to %q", stripped, after.DefinitionHash, current.DefinitionHash)
		}
		if after.RegistryRevision <= legacy.RegistryRevision {
			t.Fatalf("stripped=%v: upgrade did not advance the revision: %d -> %d", stripped, legacy.RegistryRevision, after.RegistryRevision)
		}
		if _, ok := after.Definition.Nodes[1].Parameters["mode"]; ok {
			t.Fatalf("stripped=%v: reconcile rewrote the stored definition", stripped)
		}
	}

	sdkID, err := e.srv.Engine().AddWorkflow(context.Background(), hashReconcileWorkflow(name, "go"))
	if err != nil {
		t.Fatalf("Engine().AddWorkflow: %v", err)
	}
	if sdkID != id {
		t.Fatalf("SDK add id = %q, want %q", sdkID, id)
	}

	e.post(e.body(hashReconcileWorkflow(name, "other"), true), http.StatusConflict)
}

// TestRedisRegistrationConcurrentLegacyUpgrade races identical POSTs over one
// legacy record. Exactly one wins the hash CAS; every loser must resolve
// through the re-fetch to an idempotent 201 with the same id.
func TestRedisRegistrationConcurrentLegacyUpgrade(t *testing.T) {
	e := newHashReconcileEnv(t)
	name := hashReconcileName(t)

	id := e.post(e.body(hashReconcileWorkflow(name, "go"), true), http.StatusCreated)
	want := e.record(id).DefinitionHash
	e.makeLegacy(id)

	const racers = 8
	type result struct {
		status int
		id     types.WorkflowID
	}
	results := make([]result, racers)
	bodies := make([][]byte, racers)
	for i := range bodies {
		data, err := json.Marshal(e.body(hashReconcileWorkflow(name, "go"), i%2 == 0))
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		bodies[i] = data
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			resp, err := e.ts.Client().Post(e.ts.URL+"/v1/workflows", "application/json", bytes.NewReader(bodies[i]))
			if err != nil {
				return
			}
			defer func() { _ = resp.Body.Close() }()
			var env struct {
				Data struct {
					WorkflowID types.WorkflowID `json:"workflow_id"`
				} `json:"data"`
			}
			_ = json.NewDecoder(resp.Body).Decode(&env)
			results[i] = result{status: resp.StatusCode, id: env.Data.WorkflowID}
		}(i)
	}
	close(start)
	wg.Wait()

	for i, r := range results {
		if r.status != http.StatusCreated || r.id != id {
			t.Fatalf("racer %d: status %d id %q, want 201 with %q", i, r.status, r.id, id)
		}
	}
	if got := e.record(id).DefinitionHash; got != want {
		t.Fatalf("hash after race = %q, want %q", got, want)
	}
}

// TestRedisPutMetadataOnlyIsAReplace: on the Redis registry an identical PUT
// over a POST-registered legacy record writes nothing (its hash stays legacy:
// the no-op PUT does not upgrade it), a PUT that changes only the description
// is a real replace (audit compare) carrying the runtime hash, and the
// identical PUT after it writes nothing.
func TestRedisPutMetadataOnlyIsAReplace(t *testing.T) {
	e := newHashReconcileEnv(t)
	name := hashReconcileName(t)

	id := e.post(e.body(hashReconcileWorkflow(name, "go"), true), http.StatusCreated)
	runtimeHash := e.record(id).DefinitionHash
	legacy := e.makeLegacy(id)

	e.put(id, e.body(hashReconcileWorkflow(name, "go"), true))
	same := e.record(id)
	if same.RegistryRevision != legacy.RegistryRevision || same.DefinitionHash != legacy.DefinitionHash {
		t.Fatalf("identical PUT over a legacy record wrote it: rev %d -> %d, hash %q -> %q",
			legacy.RegistryRevision, same.RegistryRevision, legacy.DefinitionHash, same.DefinitionHash)
	}

	edited := e.body(hashReconcileWorkflow(name, "go"), true)
	edited["description"] = "edited in the editor"
	e.put(id, edited)
	after := e.record(id)
	if after.RegistryRevision == same.RegistryRevision || after.Definition.Description != "edited in the editor" {
		t.Fatalf("metadata-only PUT was dropped: revision %d -> %d, description %q",
			same.RegistryRevision, after.RegistryRevision, after.Definition.Description)
	}
	if after.DefinitionHash != runtimeHash {
		t.Fatalf("metadata-only PUT stored hash %q, want the runtime hash %q", after.DefinitionHash, runtimeHash)
	}

	edited = e.body(hashReconcileWorkflow(name, "go"), true)
	edited["description"] = "edited in the editor"
	e.put(id, edited)
	again := e.record(id)
	if again.RegistryRevision != after.RegistryRevision {
		t.Fatalf("identical PUT wrote a revision: %d -> %d", after.RegistryRevision, again.RegistryRevision)
	}
}
