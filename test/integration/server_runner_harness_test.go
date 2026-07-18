//go:build integration

package integration

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend/distributed"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/service/apiserver"
	"github.com/xbcio/xflow/service/control"
	"github.com/redis/go-redis/v9"
)

// serverRunnerHarness wires the production server-runner topology against a
// real Redis/Asynq backend: a ControlPlane (engine + dispatcher + runner
// protocol + workflow-control API) hosted by apiserver, with the dispatcher
// bound to the queue via cp.Start. The HTTP test server serves both the runner
// protocol (/v1/runners/*) and the workflow/control API (/v1/workflows), so a
// runner client and a workflow submitter can both talk to the same endpoint.
//
// Stage 3 of the control-plane refactor moved /v1/workflows out of
// control.Server and into the apiserver workflow-control module, so tests must
// use apiserver.New (not control.NewServer) for the workflow submit route to
// resolve — control.NewServer now serves only the runner protocol.
type serverRunnerHarness struct {
	srv     *apiserver.APIServer
	httpSrv *httptest.Server
	state   engine.StateStore
	runners control.RunnerDirectory
}

// newServerRunnerHarness brings up the server-runner topology against addr
// (Redis). It flushes stale asynq tasks from prior crashed runs (scoped to the
// asynq:* namespace so it does not disturb leader-election keys), starts the
// control plane, and registers cleanup.
func newServerRunnerHarness(t *testing.T, addr string, concurrency int) *serverRunnerHarness {
	t.Helper()
	b, err := distributed.New(addr, nil, distributed.WithConcurrency(concurrency), distributed.WithConsumer(true))
	if err != nil {
		t.Fatalf("distributed.New: %v", err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	flushAsynqKeys(context.Background(), t, rdb)
	_ = rdb.Close()

	cp, err := control.NewControlPlane(control.Config{Backend: b})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	srv, err := apiserver.New(apiserver.Config{}, apiserver.WithControlPlane(cp))
	if err != nil {
		t.Fatalf("apiserver.New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := srv.Start(ctx); err != nil {
		cancel()
		t.Fatalf("apiserver.Start: %v", err)
	}
	httpSrv := httptest.NewServer(srv.Handler())
	t.Cleanup(func() {
		cancel()
		shutdownCtx, sc := context.WithTimeout(context.Background(), 5*time.Second)
		defer sc()
		_ = srv.Shutdown(shutdownCtx)
		httpSrv.Close()
	})
	return &serverRunnerHarness{srv: srv, httpSrv: httpSrv, state: b.State(), runners: cp.RunnerDirectory()}
}
