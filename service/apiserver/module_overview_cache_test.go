package apiserver

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/store/memstore"
	"github.com/xbcio/xflow/types"
)

// TestOverviewCacheIsolatesNamespaceAndClusterGrant pins the cache identity
// rule that keeps the overview payload from leaking between tenants or between
// callers with different grants: the key must include the namespace AND the
// cluster-permission bit. A cache keyed only by window would serve one
// namespace's aggregates (and possibly the cluster roster) to another caller.
func TestOverviewCacheIsolatesNamespaceAndClusterGrant(t *testing.T) {
	c := newOverviewCache()
	ctx := context.Background()

	builds := 0
	build := func(tag string) func() (*overviewResponse, error) {
		return func() (*overviewResponse, error) {
			builds++
			return &overviewResponse{Window: tag}, nil
		}
	}

	privateKey := overviewCacheKey{namespace: "ns-a", window: "24h"}
	clusterKey := overviewCacheKey{namespace: "ns-a", window: "24h", cluster: true}
	otherNamespaceKey := overviewCacheKey{namespace: "ns-b", window: "24h"}

	first, err := c.get(ctx, privateKey, build("private"))
	if err != nil {
		t.Fatalf("get(private) error = %v", err)
	}
	again, err := c.get(ctx, privateKey, build("private-rebuilt"))
	if err != nil {
		t.Fatalf("get(private, cached) error = %v", err)
	}
	if builds != 1 {
		t.Fatalf("builds after a repeat read = %d, want 1 (cache hit)", builds)
	}
	if again != first {
		t.Fatal("repeat read returned a different payload; the entry was rebuilt instead of hit")
	}

	cluster, err := c.get(ctx, clusterKey, build("cluster"))
	if err != nil {
		t.Fatalf("get(cluster) error = %v", err)
	}
	if builds != 2 {
		t.Fatalf("builds after the cluster-capable read = %d, want 2", builds)
	}
	if cluster == first {
		t.Fatal("cluster-capable caller shared the private entry; the permission bit is not part of the key")
	}

	other, err := c.get(ctx, otherNamespaceKey, build("other-ns"))
	if err != nil {
		t.Fatalf("get(other namespace) error = %v", err)
	}
	if builds != 3 {
		t.Fatalf("builds after the other-namespace read = %d, want 3", builds)
	}
	if other == first {
		t.Fatal("another namespace shared the first namespace's entry")
	}

	// An expired entry is rebuilt, not served stale forever.
	c.mu.Lock()
	entry := c.entries[privateKey]
	entry.expiresAt = time.Now().Add(-time.Second)
	c.entries[privateKey] = entry
	c.mu.Unlock()

	rebuilt, err := c.get(ctx, privateKey, build("private-expired"))
	if err != nil {
		t.Fatalf("get(private, expired) error = %v", err)
	}
	if builds != 4 {
		t.Fatalf("builds after expiry = %d, want 4", builds)
	}
	if rebuilt == first {
		t.Fatal("expired entry was served instead of rebuilt")
	}
}

// TestOverviewCacheDoesNotCacheBuildErrors verifies a failed aggregate read is
// not stored: caching an error payload would keep the dashboard broken for the
// whole TTL after a transient backend failure.
func TestOverviewCacheDoesNotCacheBuildErrors(t *testing.T) {
	c := newOverviewCache()
	ctx := context.Background()
	key := overviewCacheKey{namespace: "ns-a", window: "24h"}
	buildErr := errors.New("aggregate unavailable")

	if _, err := c.get(ctx, key, func() (*overviewResponse, error) {
		return nil, buildErr
	}); !errors.Is(err, buildErr) {
		t.Fatalf("get() error = %v, want %v", err, buildErr)
	}

	builds := 0
	got, err := c.get(ctx, key, func() (*overviewResponse, error) {
		builds++
		return &overviewResponse{Window: "24h"}, nil
	})
	if err != nil {
		t.Fatalf("get() after a failed build error = %v", err)
	}
	if builds != 1 || got == nil {
		t.Fatalf("builds after recovery = %d, payload = %v; want exactly one rebuild", builds, got)
	}
}

// TestOverviewCacheBuildPanicDoesNotStrandFlight pins the failure mode a
// straight-line cleanup after build() has: a panicking aggregate leaves the
// flight registered with a done channel nobody closes, so every later caller
// for the same key blocks on it until its own context expires. The panic must
// instead become an error for this caller, remove the flight, and leave the
// next request free to rebuild.
func TestOverviewCacheBuildPanicDoesNotStrandFlight(t *testing.T) {
	c := newOverviewCache()
	ctx := context.Background()
	key := overviewCacheKey{namespace: "ns-a", window: "24h"}

	_, err := c.get(ctx, key, func() (*overviewResponse, error) {
		panic("aggregate exploded")
	})
	if err == nil {
		t.Fatal("get() error = nil, want the recovered panic as an error")
	}
	if !strings.Contains(err.Error(), "panicked") {
		t.Fatalf("get() error = %v, want it to identify the panic", err)
	}

	c.mu.Lock()
	flights := len(c.flights)
	_, cached := c.entries[key]
	c.mu.Unlock()
	if flights != 0 {
		t.Fatalf("flights left after a panic = %d, want 0", flights)
	}
	if cached {
		t.Fatal("a panicked build was cached")
	}

	// The next request must complete on its own, not wait for a done channel
	// that will never close. A short deadline turns a stranded flight into a
	// test failure instead of a hang.
	nextCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	type result struct {
		resp *overviewResponse
		err  error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := c.get(nextCtx, key, func() (*overviewResponse, error) {
			return &overviewResponse{Window: "24h"}, nil
		})
		done <- result{resp: resp, err: err}
	}()
	select {
	case got := <-done:
		if got.err != nil || got.resp == nil || got.resp.Window != "24h" {
			t.Fatalf("get() after a panicked build = %+v, %v; want a fresh successful build", got.resp, got.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the request after a panicked build blocked; its flight was left registered")
	}
}

// TestOverviewHandlerIsNamespaceScopedAndCached drives GET /v1/overview through
// the real server for two tokens in different namespaces. The two assertions
// that matter are the ones a cache keyed only by window would break: the second
// namespace must see its own counts (not the first namespace's cached entry),
// and the first namespace must still see its own on the next read.
func TestOverviewHandlerIsNamespaceScopedAndCached(t *testing.T) {
	st := memstore.New()
	now := time.Now().UTC()
	for i, id := range []types.ExecutionID{"exec-a1", "exec-a2", "exec-a3"} {
		seedExecutionWorkflow(t, st, "namespaceA", "wf-a", "alpha", id,
			types.ExecutionStatusSuccess, now.Add(-time.Duration(10+i)*time.Minute))
	}
	seedExecutionWorkflow(t, st, "namespaceB", "wf-b", "beta", "exec-b1",
		types.ExecutionStatusSuccess, now.Add(-time.Minute))
	// Two rows for namespaceB in the PRECEDING window (25h/26h ago, outside
	// the 24h scan): one failed and one successful, so runs_previous and
	// failed_previous are both non-zero and cannot be satisfied by the same
	// number. They stay in namespaceB so namespaceA's 7d assertion below,
	// which counts them only if they leaked across the tenant boundary,
	// remains a 24h-only count.
	seedExecutionWorkflow(t, st, "namespaceB", "wf-b", "beta", "exec-b-prev-fail",
		types.ExecutionStatusFailed, now.Add(-25*time.Hour))
	seedExecutionWorkflow(t, st, "namespaceB", "wf-b", "beta", "exec-b-prev-ok",
		types.ExecutionStatusSuccess, now.Add(-26*time.Hour))

	srv, err := New(Config{
		Concurrency: 1,
		Store:       st,
		PrincipalAuth: NewBearerPrincipalAuthMulti([]TokenPrincipalMapping{
			{Token: "tok-a", Subject: "op-a", Namespace: "namespaceA", Scopes: []string{"execution"}},
			{Token: "tok-b", Subject: "op-b", Namespace: "namespaceB", Scopes: []string{"execution"}},
		}),
		Authorizer: NamespaceAwareAuthorizer{},
		AuditSink:  NewInMemoryAuditSink(),
	})
	if err != nil {
		t.Fatalf("apiserver.New: %v", err)
	}
	handler := srv.Handler()

	type overviewEnvelope struct {
		Success bool              `json:"success"`
		Code    string            `json:"code"`
		Data    *overviewResponse `json:"data"`
	}
	get := func(token, query string) (int, overviewEnvelope) {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, PathOverview+query, nil)
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		var env overviewEnvelope
		if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
			t.Fatalf("decode overview (token=%s query=%q): %v (body=%s)", token, query, err, rec.Body.String())
		}
		return rec.Code, env
	}

	status, env := get("tok-a", "")
	if status != http.StatusOK || env.Data == nil {
		t.Fatalf("overview(namespaceA) = %d %q, want 200 with data", status, env.Code)
	}
	if env.Data.Window != "24h" {
		t.Fatalf("window = %q, want the default 24h", env.Data.Window)
	}
	if env.Data.KPIs.Runs != 3 {
		t.Fatalf("namespaceA runs = %d, want 3", env.Data.KPIs.Runs)
	}
	if env.Data.KPIs.RunsPrevious != 0 || env.Data.KPIs.FailedPrevious != 0 {
		t.Fatalf("namespaceA previous-window KPIs = %d/%d, want 0/0 (namespaceB's rows leaked)",
			env.Data.KPIs.RunsPrevious, env.Data.KPIs.FailedPrevious)
	}
	if len(env.Data.Histogram) != overviewBucketCount || len(env.Data.KPIs.Sparkline.Runs) != overviewBucketCount {
		t.Fatalf("histogram/sparkline width = %d/%d, want %d each",
			len(env.Data.Histogram), len(env.Data.KPIs.Sparkline.Runs), overviewBucketCount)
	}
	if env.Data.Cluster != nil {
		t.Fatal("an execution-only caller received the cluster segment")
	}

	status, env = get("tok-b", "")
	if status != http.StatusOK || env.Data == nil {
		t.Fatalf("overview(namespaceB) = %d %q, want 200 with data", status, env.Code)
	}
	if env.Data.KPIs.Runs != 1 {
		t.Fatalf("namespaceB runs = %d, want 1; the cache served namespaceA's entry", env.Data.KPIs.Runs)
	}
	if env.Data.KPIs.RunsPrevious != 2 {
		t.Fatalf("namespaceB runs_previous = %d, want 2", env.Data.KPIs.RunsPrevious)
	}
	if env.Data.KPIs.FailedPrevious != 1 {
		t.Fatalf("namespaceB failed_previous = %d, want 1", env.Data.KPIs.FailedPrevious)
	}

	status, env = get("tok-a", "")
	if status != http.StatusOK || env.Data == nil || env.Data.KPIs.Runs != 3 {
		t.Fatalf("repeat overview(namespaceA) = %d %q runs=%v, want 200 with 3",
			status, env.Code, env.Data)
	}

	status, env = get("tok-a", "?window=7d")
	if status != http.StatusOK || env.Data == nil || env.Data.Window != "7d" || env.Data.KPIs.Runs != 3 {
		t.Fatalf("overview(window=7d) = %d %q window=%v runs=%v, want 200 with 7d and 3",
			status, env.Code, env.Data, env.Data)
	}

	status, env = get("tok-a", "?window=30d")
	if status != http.StatusBadRequest || env.Code != "overview_window_invalid" {
		t.Fatalf("overview(window=30d) = %d %q, want 400 overview_window_invalid", status, env.Code)
	}

	// A store without the optional reader must answer 501, never a false 200.
	mux := newUnitExecutionListMux(plainStatsStore{Executions: memstore.New()})
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, PathOverview, nil))
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("overview without the stats capability = %d, want 501", rec.Code)
	}
}
