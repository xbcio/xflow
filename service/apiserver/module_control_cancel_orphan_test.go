package apiserver

// The HTTP verdict for cancellation of a concurrency orphan, end to end.
//
// The handler itself is trivial — it maps Engine.Cancel's error through
// writeExecEngineFail — so the load-bearing part is which engine outcome each
// operator-visible state produces:
//
//   - torn shape (status running, graph key gone, no lease): Cancel's self-heal
//     converges the execution and answers nil -> 200 {"accepted": true};
//   - terminal execution: ErrExecutionInactive -> 404 execution_not_found,
//     under the same code as an absent id (store.ErrNotFound and
//     ErrExecutionNotFound share the mapping);
//   - any other error: 500 internal_error, with no engine detail echoed.
//
// This test drives the first two through a real control plane and a real
// Redis-backed engine. The serving backend is deliberately a second backend
// over the same miniredis: its in-memory graph cache never saw the execution,
// which is the only way a single test process can reproduce the cross-process
// shape (a process whose cache is warm routes the graph and takes the ordinary
// cancel path — see the rstate engine-level test for that behaviour).

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/backend/providers/distributed"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/types"
)

func TestExecutionCancelConvergesGraphLostOrphan(t *testing.T) {
	redisServer, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(redisServer.Close)
	rdb := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	// Seeding happens in a separate backend so the serving backend's rstate
	// graph cache stays cold, modelling a process that restarted (or a control
	// plane that never executed this workflow).
	seedBackend, err := distributed.New(redisServer.Addr(), nil, distributed.WithConsumer(false))
	if err != nil {
		t.Fatalf("seed backend: %v", err)
	}
	seedEng := engine.New(seedBackend.State(), noQueue{})
	ctxA := namespace.WithNamespace(context.Background(), namespace.Namespace("namespaceA"))
	g, err := graph.Compile(&types.WorkflowDef{
		Name:  "cancel-orphan-wf",
		Nodes: []types.NodeDef{{Name: "start", Type: "test.echo"}},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	execID, err := seedEng.Submit(ctxA, g, nil)
	if err != nil {
		t.Fatalf("seed Submit: %v", err)
	}

	// Partial Redis loss: the graph key only. The status key survives, which is
	// what makes Cancel's self-heal (rather than the sweep) the converging path.
	graphKey := fmt.Sprintf("xflow:ns:namespaceA:exec:{%s}:graph", execID)
	if n, err := rdb.Del(context.Background(), graphKey).Result(); err != nil || n != 1 {
		t.Fatalf("delete graph key: n=%d err=%v, want 1/nil", n, err)
	}

	apiBackend, err := distributed.New(redisServer.Addr(), nil, distributed.WithConsumer(false))
	if err != nil {
		t.Fatalf("api backend: %v", err)
	}
	cp, err := control.NewControlPlane(control.Config{Backend: apiBackend})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	srv, err := New(Config{
		Concurrency: 1,
		PrincipalAuth: NewBearerPrincipalAuthMulti([]TokenPrincipalMapping{{
			Token: "tok-full", Subject: "op-full", Namespace: "namespaceA",
			Scopes: []string{"workflow", "execution", "execution.control", "execution.rerun"},
		}}),
		Authorizer: NamespaceAwareAuthorizer{},
		AuditSink:  NewInMemoryAuditSink(),
	}, WithControlPlane(cp))
	if err != nil {
		t.Fatalf("apiserver.New: %v", err)
	}
	httpSrv := httptest.NewServer(srv.Handler())
	t.Cleanup(httpSrv.Close)

	cancel := func(id string) *http.Response {
		t.Helper()
		req, err := http.NewRequest(http.MethodPost, httpSrv.URL+"/v1/executions/"+id+"/cancel", nil)
		if err != nil {
			t.Fatalf("build cancel request: %v", err)
		}
		req.Header.Set("Authorization", "Bearer tok-full")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("POST cancel %q: %v", id, err)
		}
		return resp
	}

	// Orphan cancel: the self-heal returns nil, so the route reports success.
	resp := cancel(string(execID))
	var accepted map[string]bool
	env := decodeEnvelope(t, resp, &accepted)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !env.Success || !accepted["accepted"] {
		t.Fatalf("orphan cancel = %d/%+v body=%v, want 200 success accepted=true", resp.StatusCode, env, accepted)
	}
	status, err := rdb.Get(context.Background(), fmt.Sprintf("xflow:ns:namespaceA:exec:{%s}:status", execID)).Result()
	if err != nil || status != string(types.ExecutionStatusCanceled) {
		t.Fatalf("durable status = %q err=%v, want canceled", status, err)
	}
	reason, err := rdb.Get(context.Background(), fmt.Sprintf("xflow:ns:namespaceA:exec:{%s}:error", execID)).Result()
	if err != nil || reason != engine.OrphanReclaimReason {
		t.Fatalf("durable reason = %q err=%v, want %q", reason, err, engine.OrphanReclaimReason)
	}

	// A second cancel finds a terminal execution: the pre-existing mapping is
	// 404 execution_not_found, not a success.
	resp = cancel(string(execID))
	env = decodeEnvelope(t, resp, nil)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound || env.Success || env.Code != "execution_not_found" {
		t.Fatalf("terminal cancel = %d/%+v, want 404 execution_not_found", resp.StatusCode, env)
	}

	// Absent execution: the same 404 shape.
	resp = cancel("exec-does-not-exist")
	env = decodeEnvelope(t, resp, nil)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound || env.Success || env.Code != "execution_not_found" {
		t.Fatalf("absent cancel = %d/%+v, want 404 execution_not_found", resp.StatusCode, env)
	}
}
