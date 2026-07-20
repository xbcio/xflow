//go:build integration

package integration

import (
	"context"
	"net/http/httptest"
	"sync"
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
	cancel  context.CancelFunc

	stopOnce sync.Once
}

// stop tears the harness down: it stops the control plane (which stops the
// durable outbox dispatcher, lease monitor, and — critically for test isolation
// — the Asynq consumer bound by ControlPlane.Start) and closes the HTTP test
// server. It is idempotent via sync.Once so it is safe to call explicitly (to
// release the consumer before a later topology runs in the same subtest) and
// again from the t.Cleanup registered by newServerRunnerHarness.
//
// Why this matters: ControlPlane.Start binds the control-plane dispatcher to
// the Asynq queue and starts a consumer. That consumer's dispatcher looks up
// handlers in the control-plane backend registry, which a parity test does NOT
// populate with its custom node-type handlers (those live in the runner's own
// registry). If the consumer keeps running after this topology finishes, it
// races the next topology's consumer for the same Asynq queue and fails every
// task it grabs with "no handler registered", stalling the next execution. The
// parity matrix compares topologies that must each run in isolation, so we stop
// this harness's consumer as soon as its execution is observed.
func (h *serverRunnerHarness) stop() {
	h.stopOnce.Do(func() {
		if h.cancel != nil {
			h.cancel()
		}
		shutdownCtx, sc := context.WithTimeout(context.Background(), 5*time.Second)
		defer sc()
		_ = h.srv.Shutdown(shutdownCtx)
		if h.httpSrv != nil {
			h.httpSrv.Close()
		}
	})
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
	h := &serverRunnerHarness{
		srv:     srv,
		httpSrv: httpSrv,
		state:   b.State(),
		runners: cp.RunnerDirectory(),
		cancel:  cancel,
	}
	t.Cleanup(func() { h.stop() })
	return h
}
