//go:build integration

package integration

import (
	"context"
	"fmt"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend/providers/distributed"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/observability/metrics"
	"github.com/xbcio/xflow/observability/tracing"
	"github.com/xbcio/xflow/service/apiserver"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/sqlstore"
	"github.com/xbcio/xflow/store/sqlstore/mysqlstore"
	"github.com/redis/go-redis/v9"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// testLogger is a minimal engine.Logger that forwards to t.Logf. Used in the
// production harness to catch silent errors in flushInitialOutbox etc.
type testLogger struct {
	t *testing.T
}

func newTestLogger(t *testing.T) *testLogger { return &testLogger{t: t} }

func (l *testLogger) Debug(msg string, args ...any) {
	l.t.Logf("[engine] DEBUG %s %v", msg, args)
}
func (l *testLogger) Debugf(format string, args ...any) {
	l.t.Logf("[engine] DEBUG "+format, args...)
}
func (l *testLogger) Info(msg string, args ...any) {
	l.t.Logf("[engine] INFO %s %v", msg, args)
}
func (l *testLogger) Infof(format string, args ...any) {
	l.t.Logf("[engine] INFO "+format, args...)
}
func (l *testLogger) Warn(msg string, args ...any) {
	l.t.Logf("[engine] WARN %s %v", msg, args)
}
func (l *testLogger) Warnf(format string, args ...any) {
	l.t.Logf("[engine] WARN "+format, args...)
}
func (l *testLogger) Error(msg string, args ...any) {
	l.t.Logf("[engine] ERROR %s %v", msg, args)
}
func (l *testLogger) Errorf(format string, args ...any) {
	l.t.Logf("[engine] ERROR "+format, args...)
}

// Panic/Panicf log the engine panic message via t.Errorf (visible from any
// goroutine) and then re-panic so a recovering caller surfaces the failure.
// t.Fatalf is unsafe here: the engine may invoke Panic from a background
// goroutine (e.g. OutboxDispatcher), and testing.Fatalf terminates the
// test goroutine only — cross-goroutine Fatalf races the test runner and
// can mark the test passed before the failure is recorded. t.Errorf records
// the failure deterministically; the panic lets any deferred recover handle
// the unwind.
func (l *testLogger) Panic(msg string, args ...any) {
	l.t.Errorf("[engine] PANIC %s %v", msg, args)
	panic(fmt.Sprintf("[engine] PANIC %s %v", msg, args))
}
func (l *testLogger) Panicf(format string, args ...any) {
	l.t.Errorf("[engine] PANIC "+format, args...)
	panic(fmt.Sprintf("[engine] PANIC "+format, args...))
}

var _ engine.Logger = (*testLogger)(nil)

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
	srv      *apiserver.APIServer
	httpSrv  *httptest.Server
	state    engine.StateStore
	runners  control.RunnerDirectory
	cp       *control.ControlPlane
	cancel   context.CancelFunc
	evidence *engine.RuntimeEvidenceBuffer

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

// EvidenceBuffer returns the topology's runtime evidence buffer. Each harness
// owns a separate buffer; it must not be reused across topologies.
func (h *serverRunnerHarness) EvidenceBuffer() *engine.RuntimeEvidenceBuffer { return h.evidence }

// Sweeper returns the production LeaseSweeper so A0 request-loss tests can
// drive directory cleanup + engine reclaim synchronously with a short TTL.
func (h *serverRunnerHarness) Sweeper() *control.LeaseSweeper { return h.cp.Sweeper() }

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

	buf := engine.NewRuntimeEvidenceBuffer(64)
	cp, err := control.NewControlPlane(control.Config{Backend: b, RuntimeEvidenceBuffer: buf})
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
		srv:      srv,
		httpSrv:  httpSrv,
		state:    b.State(),
		runners:  cp.RunnerDirectory(),
		cp:       cp,
		cancel:   cancel,
		evidence: buf,
	}
	t.Cleanup(func() { h.stop() })
	return h
}

// newServerRunnerHarnessWithLeaseTTL is like newServerRunnerHarness but starts
// the control plane with a short engine lease TTL so A0 request-loss tests can
// deterministically reclaim the expired lease via the production sweeper.
func newServerRunnerHarnessWithLeaseTTL(t *testing.T, addr string, concurrency int, ttl time.Duration) *serverRunnerHarness {
	t.Helper()
	b, err := distributed.New(addr, nil, distributed.WithConcurrency(concurrency), distributed.WithConsumer(true))
	if err != nil {
		t.Fatalf("distributed.New: %v", err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	flushAsynqKeys(context.Background(), t, rdb)
	_ = rdb.Close()

	buf := engine.NewRuntimeEvidenceBuffer(64)
	cp, err := control.NewControlPlane(control.Config{Backend: b, RuntimeEvidenceBuffer: buf, LeaseTTL: ttl})
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
		srv:      srv,
		httpSrv:  httpSrv,
		state:    b.State(),
		runners:  cp.RunnerDirectory(),
		cp:       cp,
		cancel:   cancel,
		evidence: buf,
	}
	t.Cleanup(func() { h.stop() })
	return h
}

// productionServerRunnerHarness is the G1 production-auth variant of
// serverRunnerHarness. It wires the same Redis/Asynq control plane but adds
// the production B3 authz stack: PrincipalAuth (multi-token registry),
// NamespaceAwareAuthorizer (default-deny), SQLAuditSink (durable append-only
// projection backed by MySQL), the T9 AuditReconcileWorker (leader-gated),
// OTel tracer, Prometheus metrics, RequireWorkflowAuth fail-closed, and
// WithManagement (gates /v1/management/dead-letters/*).
//
// The harness intentionally uses ONE replica, so the leader-gated reconcile
// worker always runs (srv.IsLeader() returns true after cp.Start acquires
// leadership).
type productionServerRunnerHarness struct {
	*serverRunnerHarness

	provider     *sqlstore.Provider // SQL store (Store + AuditAppender + AuditReconciler + ReceiptAuditAppender)
	auditSink    *apiserver.SQLAuditSink
	metrics      *metrics.Metrics
	tracer       tracing.Tracer
	tracerProv   *sdktrace.TracerProvider
	spanRecorder *tracetest.SpanRecorder
	reconciler   *control.AuditReconcileWorker
}

// prodLeaderGateAdapter adapts *apiserver.APIServer.IsLeader to the T9
// worker's LeaderGate interface. Mirrors cmd/server/main.go:656. Single-
// replica test always returns true after Start acquires leadership.
type prodLeaderGateAdapter struct {
	isLeader func() bool
}

func (g prodLeaderGateAdapter) IsLeader() bool {
	if g.isLeader == nil {
		return false
	}
	return g.isLeader()
}

// newProductionServerRunnerHarness builds the G1 production-auth harness.
// addr is the Redis address (host:port). dsn is the MySQL DSN. Both must be
// reachable (requireRedis / requireMySQL gate the caller).
//
// mappings is the multi-namespace token registry; each mapping binds one token
// to a (subject, namespace, scopes) triple. The harness wires PrincipalAuth +
// NamespaceAwareAuthorizer + SQLAuditSink so every mutation is admitted under
// audit (fail-closed) and authorized per operation+resource+namespace.
func newProductionServerRunnerHarness(t *testing.T, addr, dsn string, mappings []apiserver.TokenPrincipalMapping) *productionServerRunnerHarness {
	t.Helper()

	// SQL store: shared between cfg.Store (workflow audit) and the SQLAuditSink
	// (authz admission audit + T4 receipt projection). One provider satisfies
	// store.Store, store.AuditAppender, store.ReceiptAuditAppender, and
	// store.AuditReconciler.
	provider, err := mysqlstore.New(dsn)
	if err != nil {
		t.Fatalf("mysqlstore.New: %v", err)
	}
	auditSink := apiserver.NewSQLAuditSink(provider)

	// Metrics + Tracer: real OTel in-memory exporter + Prometheus registry.
	m := metrics.New()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(rec),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	tracer := tracing.NewOTelTracer(tp.Tracer("github.com/xbcio/xflow"))

	// Flush asynq keys so a stale task from a prior crashed run cannot race
	// the consumer this harness starts.
	b, err := distributed.New(addr, nil,
		distributed.WithConcurrency(1),
		distributed.WithConsumer(true),
		distributed.WithAuditObserver(metrics.NewAuditMetrics(m)),
		distributed.WithLeaseObserver(metrics.NewLeaseMetrics(m)),
	)
	if err != nil {
		_ = tp.Shutdown(context.Background())
		t.Fatalf("distributed.New: %v", err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	flushAsynqKeys(context.Background(), t, rdb)
	flushXflowKeys(context.Background(), t, rdb)
	_ = rdb.Close()

	cp, err := control.NewControlPlane(control.Config{Backend: b, Logger: newTestLogger(t), Tracer: tracer})
	if err != nil {
		_ = tp.Shutdown(context.Background())
		t.Fatalf("NewControlPlane: %v", err)
	}
	srv, err := apiserver.New(apiserver.Config{
		Store:               provider,
		Metrics:             m,
		Tracer:              tracer,
		PrincipalAuth:       apiserver.NewBearerPrincipalAuthMulti(mappings),
		Authorizer:          apiserver.NamespaceAwareAuthorizer{},
		AuditSink:           auditSink,
		RequireWorkflowAuth: true,
	}, apiserver.WithControlPlane(cp), apiserver.WithManagement())
	if err != nil {
		_ = tp.Shutdown(context.Background())
		t.Fatalf("apiserver.New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	if err := srv.Start(ctx); err != nil {
		cancel()
		_ = tp.Shutdown(context.Background())
		t.Fatalf("apiserver.Start: %v", err)
	}

	httpSrv := httptest.NewServer(srv.Handler())
	base := &serverRunnerHarness{
		srv:     srv,
		httpSrv: httpSrv,
		state:   b.State(),
		runners: cp.RunnerDirectory(),
		cp:      cp,
		cancel:  cancel,
	}
	t.Cleanup(func() {
		base.stop()
		_ = tp.Shutdown(context.Background())
	})

	// T9 reconcile worker. Leader-gated via srv.IsLeader (single replica ⇒
	// always leader after Start). The worker shares the same SQL provider
	// (store.AuditReconciler) and consults the engine StateStore (Redis) as
	// its AdmissionAuthority. ReconcileOnce is exposed for synchronous settle
	// in tests; the background loop is NOT started so the test controls when
	// reconcile happens.
	ar, ok := interface{}(provider).(store.AuditReconciler)
	if !ok {
		t.Fatalf("provider does not implement store.AuditReconciler")
	}
	authority := control.NewExecutionAuthority(srv.Backend().State())
	elector := prodLeaderGateAdapter{isLeader: srv.IsLeader}
	recWorker := control.NewAuditReconcileWorker(ar, authority, control.AuditReconcileConfig{
		Elector:  elector,
		Observer: metrics.NewReconcileMetrics(m),
	})

	return &productionServerRunnerHarness{
		serverRunnerHarness: base,
		provider:            provider,
		auditSink:           auditSink,
		metrics:             m,
		tracer:              tracer,
		tracerProv:          tp,
		spanRecorder:        rec,
		reconciler:          recWorker,
	}
}
