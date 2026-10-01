package xflow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

// The cross-path matrix registers one workflow through the three paths that
// share a Server's workflow registry -- the SDK Engine facade (srv.Engine(),
// equivalent to NewCluster on the same Redis), the HTTP API (srv.Handler()),
// and the embedded Server.AddWorkflow/ReplaceWorkflow -- in every order that
// used to conflict.
//
// The SDK writes builtin ParamSpec Defaults into the definition; HTTP stores
// the body as sent. The "stripped" body is the SDK definition without the two
// Defaults the builder spelled out (wait.mode, call.method), which is what an
// editor or YAML author sends. Both are one registration identity.

type crossPath struct {
	t   *testing.T
	srv *Server
	ts  *httptest.Server
}

func newCrossPath(t *testing.T) *crossPath {
	t.Helper()
	srv, err := NewServer(ServerConfig{}, WithServerInsecureNoRunnerAuth())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := srv.Start(ctx); err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		shutCtx, shutCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutCancel()
		_ = srv.Shutdown(shutCtx)
	})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return &crossPath{t: t, srv: srv, ts: ts}
}

// crossPathWorkflow is start -> wait -> call. The http method is spelled out
// as its Default: an explicit "" is kept by both the builder and the
// canonical form, so it would not be the omitted-Default case.
func crossPathWorkflow(name string) *WorkflowBuilder {
	wf := Workflow(name)
	start := wf.Node("start", node.Start())
	wait := wf.Node("wait", node.Wait("go"))
	call := wf.Node("call", node.HTTP(http.MethodGet, "https://example.invalid/"))
	wf.Connect(start, wait)
	wf.Connect(wait, call)
	return wf
}

// sdkBody is the definition the SDK builds for wf, as a JSON body.
func (c *crossPath) sdkBody(wf *WorkflowBuilder) map[string]any {
	c.t.Helper()
	def, err := wf.build()
	if err != nil {
		c.t.Fatalf("build: %v", err)
	}
	data, err := json.Marshal(def)
	if err != nil {
		c.t.Fatalf("marshal: %v", err)
	}
	var body map[string]any
	if err := json.Unmarshal(data, &body); err != nil {
		c.t.Fatalf("unmarshal: %v", err)
	}
	return body
}

// strip removes param from the named node of body, which must carry it.
func (c *crossPath) strip(body map[string]any, nodeName, param string) map[string]any {
	c.t.Helper()
	for _, raw := range body["nodes"].([]any) {
		n := raw.(map[string]any)
		if n["name"] != nodeName {
			continue
		}
		params := n["parameters"].(map[string]any)
		if _, ok := params[param]; !ok {
			c.t.Fatalf("node %q has no %q to strip; the fixture no longer covers a written Default", nodeName, param)
		}
		delete(params, param)
		return body
	}
	c.t.Fatalf("node %q not found", nodeName)
	return nil
}

func (c *crossPath) strippedBody(wf *WorkflowBuilder) map[string]any {
	c.t.Helper()
	body := c.strip(c.sdkBody(wf), "wait", "mode")
	return c.strip(body, "call", "method")
}

func (c *crossPath) send(method, path string, body map[string]any) (int, types.WorkflowID) {
	c.t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		c.t.Fatalf("marshal: %v", err)
	}
	req, err := http.NewRequest(method, c.ts.URL+path, bytes.NewReader(data))
	if err != nil {
		c.t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatalf("%s %s: %v", method, path, err)
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

// post registers body over HTTP and requires wantStatus.
func (c *crossPath) post(body map[string]any, wantStatus int) types.WorkflowID {
	c.t.Helper()
	status, id := c.send(http.MethodPost, "/v1/workflows", body)
	if status != wantStatus {
		c.t.Fatalf("POST /v1/workflows = %d, want %d", status, wantStatus)
	}
	return id
}

func (c *crossPath) put(id types.WorkflowID, body map[string]any) {
	c.t.Helper()
	body["id"] = string(id)
	status, got := c.send(http.MethodPut, "/v1/workflows/"+string(id), body)
	if status != http.StatusOK || got != id {
		c.t.Fatalf("PUT /v1/workflows/%s = %d id %q, want 200 with the path id", id, status, got)
	}
}

func (c *crossPath) sdkAdd(wf *WorkflowBuilder) types.WorkflowID {
	c.t.Helper()
	id, err := c.srv.Engine().AddWorkflow(context.Background(), wf)
	if err != nil {
		c.t.Fatalf("Engine().AddWorkflow: %v", err)
	}
	return id
}

func (c *crossPath) record(id types.WorkflowID) backend.WorkflowRecord {
	c.t.Helper()
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)
	rec, err := c.srv.api.Backend().WorkflowRegistry().GetWorkflow(ctx, id)
	if err != nil {
		c.t.Fatalf("GetWorkflow(%s): %v", id, err)
	}
	return rec
}

func sameID(t *testing.T, step string, got, want types.WorkflowID) {
	t.Helper()
	if got == "" || got != want {
		t.Fatalf("%s: id = %q, want the existing %q", step, got, want)
	}
}

func TestCrossPathRegistrationMatrix(t *testing.T) {
	t.Run("1 HTTP stripped then SDK", func(t *testing.T) {
		c := newCrossPath(t)
		id := c.post(c.strippedBody(crossPathWorkflow("m1")), http.StatusCreated)
		sameID(t, "SDK add", c.sdkAdd(crossPathWorkflow("m1")), id)
	})

	t.Run("2 SDK then HTTP stripped", func(t *testing.T) {
		c := newCrossPath(t)
		id := c.sdkAdd(crossPathWorkflow("m2"))
		sameID(t, "POST stripped", c.post(c.strippedBody(crossPathWorkflow("m2")), http.StatusCreated), id)
	})

	t.Run("2b SDK then HTTP byte-identical", func(t *testing.T) {
		c := newCrossPath(t)
		id := c.sdkAdd(crossPathWorkflow("m2b"))
		sameID(t, "POST identical", c.post(c.sdkBody(crossPathWorkflow("m2b")), http.StatusCreated), id)
	})

	t.Run("2c SDK then HTTP PUT byte-identical is a no-op", func(t *testing.T) {
		c := newCrossPath(t)
		id := c.sdkAdd(crossPathWorkflow("m2c"))
		before := c.record(id)
		c.put(id, c.sdkBody(crossPathWorkflow("m2c")))
		after := c.record(id)
		if after.RegistryRevision != before.RegistryRevision || after.DefinitionHash != before.DefinitionHash {
			t.Fatalf("identical PUT wrote the record: rev %d -> %d, hash %q -> %q",
				before.RegistryRevision, after.RegistryRevision, before.DefinitionHash, after.DefinitionHash)
		}
	})

	t.Run("3 HTTP with defaults then SDK then HTTP again", func(t *testing.T) {
		c := newCrossPath(t)
		id := c.post(c.sdkBody(crossPathWorkflow("m3")), http.StatusCreated)
		sameID(t, "SDK add", c.sdkAdd(crossPathWorkflow("m3")), id)
		sameID(t, "POST again", c.post(c.sdkBody(crossPathWorkflow("m3")), http.StatusCreated), id)
	})

	t.Run("4 embedded Server then HTTP stripped then SDK then HTTP", func(t *testing.T) {
		c := newCrossPath(t)
		id, err := c.srv.AddWorkflow(context.Background(), crossPathWorkflow("m4"))
		if err != nil {
			t.Fatalf("Server.AddWorkflow: %v", err)
		}
		sameID(t, "POST stripped", c.post(c.strippedBody(crossPathWorkflow("m4")), http.StatusCreated), id)
		sameID(t, "SDK add", c.sdkAdd(crossPathWorkflow("m4")), id)
		sameID(t, "POST stripped again", c.post(c.strippedBody(crossPathWorkflow("m4")), http.StatusCreated), id)
		sameID(t, "POST with defaults", c.post(c.sdkBody(crossPathWorkflow("m4")), http.StatusCreated), id)
	})

	t.Run("5 SDK then embedded replace keeps the id", func(t *testing.T) {
		c := newCrossPath(t)
		id := c.sdkAdd(crossPathWorkflow("m5"))
		got, err := c.srv.ReplaceWorkflow(context.Background(), crossPathWorkflow("m5"))
		if err != nil {
			t.Fatalf("Server.ReplaceWorkflow: %v", err)
		}
		sameID(t, "ReplaceWorkflow", got, id)
	})

	// Replace no-op checks compare audit fingerprints over the full stored
	// definition, and the HTTP-stripped record omits the Defaults the builder
	// writes. So an embedded replace over it is a real replace with a new id,
	// although the runtime identity is equal (ADR-D4 §3.1). A following SDK
	// add is idempotent on the replaced record.
	t.Run("5b HTTP stripped then embedded replace mints a new id", func(t *testing.T) {
		c := newCrossPath(t)
		id := c.post(c.strippedBody(crossPathWorkflow("m5b")), http.StatusCreated)
		before := c.record(id)
		got, err := c.srv.ReplaceWorkflow(context.Background(), crossPathWorkflow("m5b"))
		if err != nil {
			t.Fatalf("Server.ReplaceWorkflow: %v", err)
		}
		if got == "" || got == id {
			t.Fatalf("ReplaceWorkflow id = %q, want a new id distinct from %q", got, id)
		}
		after := c.record(got)
		if after.DefinitionHash != before.DefinitionHash {
			t.Fatalf("replace changed the runtime hash: %q -> %q", before.DefinitionHash, after.DefinitionHash)
		}
		sameID(t, "SDK add", c.sdkAdd(crossPathWorkflow("m5b")), got)
	})

	t.Run("6 HTTP stripped then HTTP with defaults", func(t *testing.T) {
		c := newCrossPath(t)
		id := c.post(c.strippedBody(crossPathWorkflow("m6")), http.StatusCreated)
		sameID(t, "POST with defaults", c.post(c.sdkBody(crossPathWorkflow("m6")), http.StatusCreated), id)
	})

	t.Run("7 PUT stripped replaces then SDK and HTTP are idempotent", func(t *testing.T) {
		c := newCrossPath(t)
		id := c.sdkAdd(crossPathWorkflow("m7"))
		before := c.record(id)
		c.put(id, c.strippedBody(crossPathWorkflow("m7")))
		after := c.record(id)
		if after.RegistryRevision == before.RegistryRevision {
			t.Fatal("PUT of the stripped form did not replace the record")
		}
		if _, ok := after.Definition.Nodes[1].Parameters["mode"]; ok {
			t.Fatal("stored definition kept wait.mode after a PUT of the stripped form")
		}
		sameID(t, "SDK add", c.sdkAdd(crossPathWorkflow("m7")), id)
		sameID(t, "POST stripped", c.post(c.strippedBody(crossPathWorkflow("m7")), http.StatusCreated), id)
	})
}

// TestCrossPathRealParamChangeStillConflicts pins that canonicalization does
// not blur a real change: a different param value conflicts on every path.
func TestCrossPathRealParamChangeStillConflicts(t *testing.T) {
	changed := func(name string) *WorkflowBuilder {
		wf := Workflow(name)
		start := wf.Node("start", node.Start())
		wait := wf.Node("wait", node.Wait("other"))
		call := wf.Node("call", node.HTTP(http.MethodGet, "https://example.invalid/"))
		wf.Connect(start, wait)
		wf.Connect(wait, call)
		return wf
	}

	t.Run("SDK then HTTP", func(t *testing.T) {
		c := newCrossPath(t)
		c.sdkAdd(crossPathWorkflow("conflict-a"))
		c.post(c.strippedBody(changed("conflict-a")), http.StatusConflict)
	})
	t.Run("HTTP then SDK", func(t *testing.T) {
		c := newCrossPath(t)
		c.post(c.strippedBody(crossPathWorkflow("conflict-b")), http.StatusCreated)
		if _, err := c.srv.Engine().AddWorkflow(context.Background(), changed("conflict-b")); !errors.Is(err, backend.ErrWorkflowConflict) {
			t.Fatalf("Engine().AddWorkflow err = %v, want ErrWorkflowConflict", err)
		}
	})
}

// crossPathCustomNode is a non-builtin node type with a Default the SDK
// builder writes.
var crossPathCustomNode = node.Define("test.crosspath-custom", func(_ context.Context, input *types.Input) (*types.Output, error) {
	return &types.Output{Data: input.Data}, nil
}).Param(types.ParamSpec{Name: "level", Type: types.ParamString, Default: "info"})

// TestCrossPathCustomTypeOmittedDefaultStillConflicts pins the documented
// residual: canonicalization fills only builtin Defaults, so a custom node type
// whose Default the SDK wrote is a different identity from the same body with
// that param omitted. If this starts passing as idempotent, canonicalization
// has begun reading a descriptor source beyond the builtin table.
func TestCrossPathCustomTypeOmittedDefaultStillConflicts(t *testing.T) {
	custom := func() *WorkflowBuilder {
		wf := Workflow("custom-residual")
		start := wf.Node("start", node.Start())
		work := wf.Node("work", crossPathCustomNode.New(map[string]any{}))
		wf.Connect(start, work)
		return wf
	}

	c := newCrossPath(t)
	id := c.sdkAdd(custom())
	sameID(t, "POST with the Default", c.post(c.sdkBody(custom()), http.StatusCreated), id)
	c.post(c.strip(c.sdkBody(custom()), "work", "level"), http.StatusConflict)
}

// TestCrossPathExplicitEmptyBuiltinParamStillConflicts pins the second
// documented residual: an explicit "" is a value, not an omission, so neither
// the builder nor the canonical form fills it. node.HTTP("", url) therefore
// stores method "" and is a different identity from a body that omits method
// (canonicalized to its Default "GET"), although the handler runs both as GET.
func TestCrossPathExplicitEmptyBuiltinParamStillConflicts(t *testing.T) {
	emptyMethod := func(name string) *WorkflowBuilder {
		wf := Workflow(name)
		start := wf.Node("start", node.Start())
		call := wf.Node("call", node.HTTP("", "https://example.invalid/"))
		wf.Connect(start, call)
		return wf
	}

	t.Run("SDK then HTTP omitted", func(t *testing.T) {
		c := newCrossPath(t)
		body := c.sdkBody(emptyMethod("empty-a"))
		if got := body["nodes"].([]any)[1].(map[string]any)["parameters"].(map[string]any)["method"]; got != "" {
			t.Fatalf("SDK stored method = %#v, want the explicit \"\"", got)
		}
		id := c.sdkAdd(emptyMethod("empty-a"))
		sameID(t, "POST with the explicit \"\"", c.post(c.sdkBody(emptyMethod("empty-a")), http.StatusCreated), id)
		c.post(c.strip(c.sdkBody(emptyMethod("empty-a")), "call", "method"), http.StatusConflict)
	})
	t.Run("HTTP omitted then SDK", func(t *testing.T) {
		c := newCrossPath(t)
		c.post(c.strip(c.sdkBody(emptyMethod("empty-b")), "call", "method"), http.StatusCreated)
		if _, err := c.srv.Engine().AddWorkflow(context.Background(), emptyMethod("empty-b")); !errors.Is(err, backend.ErrWorkflowConflict) {
			t.Fatalf("Engine().AddWorkflow err = %v, want ErrWorkflowConflict", err)
		}
	})
}
