package apiserver

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/observability/metrics"
	"github.com/xbcio/xflow/types"
)

// TestResetWorkflowConcurrencyEndToEnd drives the route against a real server:
// real authz (namespace from the principal), a real in-memory concurrency store,
// the real engine judgement, and the real metric adapter. It proves the parts a
// handler-only test cannot: the namespace boundary, the 401/403 gates, and that
// in-process and HTTP previews agree on the same state.
func TestResetWorkflowConcurrencyEndToEnd(t *testing.T) {
	m := metrics.New()
	log := &recordingLogger{}
	principalAuth := NewBearerPrincipalAuthMulti([]TokenPrincipalMapping{
		{Token: "tok-a", Subject: "op-a", Namespace: "namespaceA", Scopes: []string{"execution"}},
		{Token: "tok-none", Subject: "op-none", Namespace: "namespaceA", Scopes: []string{"workflow"}},
		{Token: "tok-b", Subject: "op-b", Namespace: "namespaceB", Scopes: []string{"execution"}},
	})
	srv, err := New(Config{
		Concurrency:   1,
		Metrics:       m,
		Logger:        log,
		PrincipalAuth: principalAuth,
		Authorizer:    NamespaceAwareAuthorizer{},
		AuditSink:     NewInMemoryAuditSink(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	httpSrv := httptest.NewServer(srv.Handler())
	t.Cleanup(httpSrv.Close)
	t.Cleanup(func() { _ = srv.Shutdown(context.Background()) })

	ctxA := namespace.WithNamespace(context.Background(), namespace.Namespace("namespaceA"))
	g := resetE2EGraph(t, "reset-e2e")
	state := resetE2EState(t, srv)
	limited, ok := state.(engine.ConcurrencyLimitedStateStore)
	if !ok {
		t.Fatalf("state store %T does not implement ConcurrencyLimitedStateStore", state)
	}
	admission := engine.ConcurrencyAdmission{
		Namespace: "namespaceA",
		Workflow:  g.Name(),
		Limit:     1,
		Mode:      types.ConcurrencyModeQueue,
	}
	running := engine.NewExecutionID()
	queued := engine.NewExecutionID()
	for _, id := range []types.ExecutionID{running, queued} {
		if _, err := limited.CreateExecutionWithConcurrency(ctxA, &engine.ExecutionSnapshot{
			ID: id, Namespace: "namespaceA", Graph: g, Status: types.ExecutionStatusRunning,
		}, nil, admission); err != nil {
			t.Fatalf("admission %q error = %v", id, err)
		}
	}
	if snap, err := state.GetExecution(ctxA, queued); err != nil || snap == nil || snap.Status != types.ExecutionStatusQueued {
		t.Fatalf("queued fixture = %+v (err %v), want queued", snap, err)
	}

	path := "/v1/workflows/" + g.Name() + "/concurrency/reset"

	// Default judgement: both executions have a graph, so even an apply would
	// leave them alone. The preview must report them inspected but not candidates.
	resp := resetE2EDo(t, httpSrv.URL, "tok-a", path, nil)
	body := resetE2EDecode(t, resp)
	if body.Applied || body.IncludeLive || body.Reclaimed != 0 || len(body.Candidates) != 0 {
		t.Fatalf("default preview = %+v, want no candidates and no writes", body)
	}

	// include_live preview: lists both, writes nothing.
	resp = resetE2EDo(t, httpSrv.URL, "tok-a", path, map[string]any{"include_live": true})
	preview := resetE2EDecode(t, resp)
	if preview.Applied || preview.Reclaimed != 0 || len(preview.Candidates) != 2 {
		t.Fatalf("include_live preview = %+v, want two candidates and no writes", preview)
	}
	if got := previewCandidateIDs(preview); got[running] != types.ExecutionStatusRunning || got[queued] != types.ExecutionStatusQueued {
		t.Fatalf("preview candidate statuses = %v, want running/queued", got)
	}
	for _, id := range []types.ExecutionID{running, queued} {
		if got := resetE2EStatus(t, ctxA, state, id); got != previewCandidateStatus(t, preview, id) {
			t.Fatalf("preview changed %q status to %q", id, got)
		}
	}

	// In-process parity: the same state and options produce the same judgement.
	inProc, err := srv.Engine().ResetWorkflowConcurrency(ctxA, namespace.Namespace("namespaceA"), g.Name(), engine.ResetConcurrencyOptions{IncludeLive: true})
	if err != nil {
		t.Fatalf("in-process preview error = %v", err)
	}
	if inProc.Inspected != preview.Inspected || len(inProc.Candidates) != len(preview.Candidates) {
		t.Fatalf("in-process preview = %+v, HTTP preview = %+v: they disagree", inProc, preview)
	}
	httpIDs := previewCandidateIDs(preview)
	for _, candidate := range inProc.Candidates {
		if got, ok := httpIDs[candidate.ExecutionID]; !ok || got != candidate.Status {
			t.Fatalf("in-process candidate %+v absent or different over HTTP (%v)", candidate, httpIDs)
		}
	}

	// Cross-namespace: namespaceB's principal cannot see namespaceA's state.
	resp = resetE2EDo(t, httpSrv.URL, "tok-b", path, map[string]any{"include_live": true})
	if got := resetE2EDecode(t, resp); len(got.Candidates) != 0 || got.Inspected != 0 {
		t.Fatalf("cross-namespace preview = %+v, want an empty pass", got)
	}

	// Auth gates: no credential 401, wrong scope 403; neither reaches the store.
	resp = resetE2EDo(t, httpSrv.URL, "", path, map[string]any{"apply": true})
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status = %d, want 401", resp.StatusCode)
	}
	_ = readAll(t, resp)
	resp = resetE2EDo(t, httpSrv.URL, "tok-none", path, map[string]any{"apply": true})
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("wrong-scope status = %d, want 403", resp.StatusCode)
	}
	_ = readAll(t, resp)
	for _, id := range []types.ExecutionID{running, queued} {
		if got := resetE2EStatus(t, ctxA, state, id); got != types.ExecutionStatusRunning && got != types.ExecutionStatusQueued {
			t.Fatalf("denied request converged %q to %q", id, got)
		}
	}

	// A live apply without the workflow-name echo is refused before the store is
	// touched, so neither execution converges.
	resp = resetE2EDo(t, httpSrv.URL, "tok-a", path, map[string]any{"apply": true, "include_live": true})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("unconfirmed live apply status = %d, want 400", resp.StatusCode)
	}
	if raw := readAll(t, resp); !strings.Contains(string(raw), "confirm_workflow_required") {
		t.Fatalf("unconfirmed live apply body = %s, want confirm_workflow_required", raw)
	}
	for _, id := range []types.ExecutionID{running, queued} {
		if got := resetE2EStatus(t, ctxA, state, id); got != types.ExecutionStatusRunning && got != types.ExecutionStatusQueued {
			t.Fatalf("unconfirmed live apply converged %q to %q", id, got)
		}
	}

	// Apply include_live: both converge, and the metric records both under
	// reason operator_reset. The kind split is NOT pinned: the pass frees the
	// holder's slot, which promotes the queued member in the same pass, so the
	// queued execution may be judged before or after that promotion and its
	// kind label follows the status it was actually reclaimed from. The
	// deterministic kind coverage lives in the store observer tests.
	resp = resetE2EDo(t, httpSrv.URL, "tok-a", path, map[string]any{
		"apply": true, "include_live": true, "confirm_workflow": g.Name(),
	})
	applied := resetE2EDecode(t, resp)
	if !applied.Applied || !applied.IncludeLive || applied.Reclaimed != 2 || len(applied.Candidates) != 2 {
		t.Fatalf("applied response = %+v, want two reclaimed", applied)
	}
	for _, id := range []types.ExecutionID{running, queued} {
		if got := resetE2EStatus(t, ctxA, state, id); got != types.ExecutionStatusCanceled {
			t.Fatalf("status after apply for %q = %q, want canceled", id, got)
		}
	}
	if got := orphanReclaimedTotal(t, m, engine.OrphanTriggerReset); got != 2 {
		t.Fatalf("xflow_orphan_reclaimed_total{reason=%q} total = %v, want 2", engine.OrphanTriggerReset, got)
	}
	// The request-level audit line names the operation and the reason that ties
	// the request to the executions it terminated.
	if !log.mentions("workflow_concurrency_reset") {
		t.Fatal("applied reset wrote no workflow_concurrency_reset audit line")
	}
	if !log.mentions(engine.OrphanTriggerReset) {
		t.Fatal("applied reset's audit/log output does not carry reason operator_reset")
	}
	// The audit line names the verified caller and records the live apply as
	// confirmed. recordingLogger concatenates operands with fmt.Sprint.
	if !log.mentionsAll("workflow_concurrency_reset", "principal", "op-a", "confirmedtrue") {
		t.Fatalf("applied reset's audit line lacks principal op-a or confirmed=true: %v", log.all())
	}
}
