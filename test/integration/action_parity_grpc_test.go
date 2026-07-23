//go:build integration

package integration

import (
	"context"
	"net"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend/distributed"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/node/resource"
	"github.com/xbcio/xflow/service/apiserver"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/protocol"
	runnersvc "github.com/xbcio/xflow/service/runner"
	"github.com/xbcio/xflow/types"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// TestGRPCActionErrorParity covers the gRPC action node (xflow.grpc) in the A3
// parity matrix. It exercises the built-in status-code classification and the
// permanent config error when no ResourcePool is available.
//
// Resource pool injection is performed with a test-only wrapper so both the
// local embedded and server-runner topologies see a pool without modifying the
// production runner constructor.
func TestGRPCActionErrorParity(t *testing.T) {
	addr := requireRedis(t)

	cases := []parityCase{
		{
			Name: "grpc_no_pool_permanent",
			Build: func() (types.NodeDef, func(engine.HandlerRegistrar), func() int) {
				return grpcParityBuild(t, "grpc_no_pool_permanent", grpcNodeDef("127.0.0.1:1"), false)
			},
			MaxAttempts:            2,
			WantAttempt:            1,
			WantStatus:             types.ExecutionStatusFailed,
			ErrContains:            "grpc.no_pool",
			WantKind:               string(types.ErrorKindPermanent),
			WantRetryable:          false,
			WantHandlerInvocations: 1,
		},
		{
			Name: "grpc_connection_transient_exhausted",
			Build: func() (types.NodeDef, func(engine.HandlerRegistrar), func() int) {
				return grpcParityBuild(t, "grpc_connection_transient_exhausted", grpcNodeDef("127.0.0.1:1"), true)
			},
			MaxAttempts:            2,
			WantAttempt:            2,
			WantStatus:             types.ExecutionStatusFailed,
			ErrContains:            "grpc.",
			WantKind:               string(types.ErrorKindTransient),
			WantRetryable:          true,
			WantHandlerInvocations: 2,
		},
		{
			Name: "grpc_not_found_permanent",
			Build: func() (types.NodeDef, func(engine.HandlerRegistrar), func() int) {
				host := startGRPCStatusServer(t, codes.NotFound)
				return grpcParityBuild(t, "grpc_not_found_permanent", grpcNodeDef(host), true)
			},
			MaxAttempts:            2,
			WantAttempt:            1,
			WantStatus:             types.ExecutionStatusFailed,
			ErrContains:            "grpc.NotFound",
			WantKind:               string(types.ErrorKindPermanent),
			WantRetryable:          false,
			WantHandlerInvocations: 1,
		},
		{
			Name: "grpc_unavailable_transient_exhausted",
			Build: func() (types.NodeDef, func(engine.HandlerRegistrar), func() int) {
				host := startGRPCStatusServer(t, codes.Unavailable)
				return grpcParityBuild(t, "grpc_unavailable_transient_exhausted", grpcNodeDef(host), true)
			},
			MaxAttempts:            2,
			WantAttempt:            2,
			WantStatus:             types.ExecutionStatusFailed,
			ErrContains:            "grpc.Unavailable",
			WantKind:               string(types.ErrorKindTransient),
			WantRetryable:          true,
			WantHandlerInvocations: 2,
		},
		{
			Name: "grpc_invalid_argument_permanent",
			Build: func() (types.NodeDef, func(engine.HandlerRegistrar), func() int) {
				host := startGRPCStatusServer(t, codes.InvalidArgument)
				return grpcParityBuild(t, "grpc_invalid_argument_permanent", grpcNodeDef(host), true)
			},
			MaxAttempts:            2,
			WantAttempt:            1,
			WantStatus:             types.ExecutionStatusFailed,
			ErrContains:            "grpc.InvalidArgument",
			WantKind:               string(types.ErrorKindPermanent),
			WantRetryable:          false,
			WantHandlerInvocations: 1,
		},
		{
			Name: "grpc_permission_denied_permanent",
			Build: func() (types.NodeDef, func(engine.HandlerRegistrar), func() int) {
				host := startGRPCStatusServer(t, codes.PermissionDenied)
				return grpcParityBuild(t, "grpc_permission_denied_permanent", grpcNodeDef(host), true)
			},
			MaxAttempts:            2,
			WantAttempt:            1,
			WantStatus:             types.ExecutionStatusFailed,
			ErrContains:            "grpc.PermissionDenied",
			WantKind:               string(types.ErrorKindPermanent),
			WantRetryable:          false,
			WantHandlerInvocations: 1,
		},
		{
			Name: "grpc_unauthenticated_permanent",
			Build: func() (types.NodeDef, func(engine.HandlerRegistrar), func() int) {
				host := startGRPCStatusServer(t, codes.Unauthenticated)
				return grpcParityBuild(t, "grpc_unauthenticated_permanent", grpcNodeDef(host), true)
			},
			MaxAttempts:            2,
			WantAttempt:            1,
			WantStatus:             types.ExecutionStatusFailed,
			ErrContains:            "grpc.Unauthenticated",
			WantKind:               string(types.ErrorKindPermanent),
			WantRetryable:          false,
			WantHandlerInvocations: 1,
		},
		{
			Name: "grpc_already_exists_permanent",
			Build: func() (types.NodeDef, func(engine.HandlerRegistrar), func() int) {
				host := startGRPCStatusServer(t, codes.AlreadyExists)
				return grpcParityBuild(t, "grpc_already_exists_permanent", grpcNodeDef(host), true)
			},
			MaxAttempts:            2,
			WantAttempt:            1,
			WantStatus:             types.ExecutionStatusFailed,
			ErrContains:            "grpc.AlreadyExists",
			WantKind:               string(types.ErrorKindPermanent),
			WantRetryable:          false,
			WantHandlerInvocations: 1,
		},
		{
			Name: "grpc_unimplemented_permanent",
			Build: func() (types.NodeDef, func(engine.HandlerRegistrar), func() int) {
				host := startGRPCStatusServer(t, codes.Unimplemented)
				return grpcParityBuild(t, "grpc_unimplemented_permanent", grpcNodeDef(host), true)
			},
			MaxAttempts:            2,
			WantAttempt:            1,
			WantStatus:             types.ExecutionStatusFailed,
			ErrContains:            "grpc.Unimplemented",
			WantKind:               string(types.ErrorKindPermanent),
			WantRetryable:          false,
			WantHandlerInvocations: 1,
		},
		{
			Name: "grpc_failed_precondition_permanent",
			Build: func() (types.NodeDef, func(engine.HandlerRegistrar), func() int) {
				host := startGRPCStatusServer(t, codes.FailedPrecondition)
				return grpcParityBuild(t, "grpc_failed_precondition_permanent", grpcNodeDef(host), true)
			},
			MaxAttempts:            2,
			WantAttempt:            1,
			WantStatus:             types.ExecutionStatusFailed,
			ErrContains:            "grpc.FailedPrecondition",
			WantKind:               string(types.ErrorKindPermanent),
			WantRetryable:          false,
			WantHandlerInvocations: 1,
		},
		{
			Name: "grpc_out_of_range_permanent",
			Build: func() (types.NodeDef, func(engine.HandlerRegistrar), func() int) {
				host := startGRPCStatusServer(t, codes.OutOfRange)
				return grpcParityBuild(t, "grpc_out_of_range_permanent", grpcNodeDef(host), true)
			},
			MaxAttempts:            2,
			WantAttempt:            1,
			WantStatus:             types.ExecutionStatusFailed,
			ErrContains:            "grpc.OutOfRange",
			WantKind:               string(types.ErrorKindPermanent),
			WantRetryable:          false,
			WantHandlerInvocations: 1,
		},
		{
			Name: "grpc_deadline_exceeded_transient_exhausted",
			Build: func() (types.NodeDef, func(engine.HandlerRegistrar), func() int) {
				host := startGRPCStatusServer(t, codes.DeadlineExceeded)
				return grpcParityBuild(t, "grpc_deadline_exceeded_transient_exhausted", grpcNodeDef(host), true)
			},
			MaxAttempts:            2,
			WantAttempt:            2,
			WantStatus:             types.ExecutionStatusFailed,
			ErrContains:            "grpc.DeadlineExceeded",
			WantKind:               string(types.ErrorKindTransient),
			WantRetryable:          true,
			WantHandlerInvocations: 2,
		},
		{
			Name: "grpc_resource_exhausted_transient_exhausted",
			Build: func() (types.NodeDef, func(engine.HandlerRegistrar), func() int) {
				host := startGRPCStatusServer(t, codes.ResourceExhausted)
				return grpcParityBuild(t, "grpc_resource_exhausted_transient_exhausted", grpcNodeDef(host), true)
			},
			MaxAttempts:            2,
			WantAttempt:            2,
			WantStatus:             types.ExecutionStatusFailed,
			ErrContains:            "grpc.ResourceExhausted",
			WantKind:               string(types.ErrorKindTransient),
			WantRetryable:          true,
			WantHandlerInvocations: 2,
		},
		{
			Name: "grpc_aborted_transient_exhausted",
			Build: func() (types.NodeDef, func(engine.HandlerRegistrar), func() int) {
				host := startGRPCStatusServer(t, codes.Aborted)
				return grpcParityBuild(t, "grpc_aborted_transient_exhausted", grpcNodeDef(host), true)
			},
			MaxAttempts:            2,
			WantAttempt:            2,
			WantStatus:             types.ExecutionStatusFailed,
			ErrContains:            "grpc.Aborted",
			WantKind:               string(types.ErrorKindTransient),
			WantRetryable:          true,
			WantHandlerInvocations: 2,
		},
		{
			Name: "grpc_canceled_transient_exhausted",
			Build: func() (types.NodeDef, func(engine.HandlerRegistrar), func() int) {
				host := startGRPCStatusServer(t, codes.Canceled)
				return grpcParityBuild(t, "grpc_canceled_transient_exhausted", grpcNodeDef(host), true)
			},
			MaxAttempts:            2,
			WantAttempt:            2,
			WantStatus:             types.ExecutionStatusFailed,
			ErrContains:            "grpc.Canceled",
			WantKind:               string(types.ErrorKindTransient),
			WantRetryable:          true,
			WantHandlerInvocations: 2,
		},
		{
			Name: "grpc_unknown_transient_exhausted",
			Build: func() (types.NodeDef, func(engine.HandlerRegistrar), func() int) {
				host := startGRPCStatusServer(t, codes.Unknown)
				return grpcParityBuild(t, "grpc_unknown_transient_exhausted", grpcNodeDef(host), true)
			},
			MaxAttempts:            2,
			WantAttempt:            2,
			WantStatus:             types.ExecutionStatusFailed,
			ErrContains:            "grpc.Unknown",
			WantKind:               string(types.ErrorKindTransient),
			WantRetryable:          true,
			WantHandlerInvocations: 2,
		},
		{
			Name: "grpc_internal_transient_exhausted",
			Build: func() (types.NodeDef, func(engine.HandlerRegistrar), func() int) {
				host := startGRPCStatusServer(t, codes.Internal)
				return grpcParityBuild(t, "grpc_internal_transient_exhausted", grpcNodeDef(host), true)
			},
			MaxAttempts:            2,
			WantAttempt:            2,
			WantStatus:             types.ExecutionStatusFailed,
			ErrContains:            "grpc.Internal",
			WantKind:               string(types.ErrorKindTransient),
			WantRetryable:          true,
			WantHandlerInvocations: 2,
		},
		{
			Name: "grpc_data_loss_transient_exhausted",
			Build: func() (types.NodeDef, func(engine.HandlerRegistrar), func() int) {
				host := startGRPCStatusServer(t, codes.DataLoss)
				return grpcParityBuild(t, "grpc_data_loss_transient_exhausted", grpcNodeDef(host), true)
			},
			MaxAttempts:            2,
			WantAttempt:            2,
			WantStatus:             types.ExecutionStatusFailed,
			ErrContains:            "grpc.DataLoss",
			WantKind:               string(types.ErrorKindTransient),
			WantRetryable:          true,
			WantHandlerInvocations: 2,
		},
	}

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			source, register, inv := tc.Build()
			retry := &types.RetrySettings{
				MaxAttempts:     tc.MaxAttempts,
				InitialInterval: 50,
			}
			def := ParityWorkflow(source, retry)

			// gRPC cases use the real xflow.grpc action handler via a counting
			// wrapper; WantKind/WantRetryable are explicit manifest literals above.

			localOut := RunParityLocal(t, def, register, nil, tc.Name, "local")
			serverOut := runServerRunnerParity(t, addr, def, register, nil, tc.Name, "server-runner")
			clusterOut := RunParityCluster(t, addr, def, register, nil, tc.Name, "cluster-durable")

			invocations := invCount(inv)
			for _, o := range []*ParityOutcome{&localOut, &serverOut, &clusterOut} {
				o.HandlerInvocations = invocations
			}

			assertParityThreeWay(t, tc, localOut, serverOut, clusterOut)
			logParityMatrixRow(t, tc, "local", localOut)
			logParityMatrixRow(t, tc, "server-runner", serverOut)
			logParityMatrixRow(t, tc, "cluster-durable", clusterOut)
		})
	}
}

// grpcNodeDef builds an xflow.grpc source node with a short timeout so closed
// ports and test servers fail fast.
func grpcNodeDef(host string) types.NodeDef {
	return types.NodeDef{
		Name: "start",
		Type: "xflow.grpc",
		Parameters: map[string]any{
			"service": "test.Service",
			"method":  "Call",
			"host":    host,
			"request": map[string]any{},
			"options": map[string]any{"timeout": "500ms"},
		},
	}
}

// startGRPCStatusServer starts an in-process gRPC server that returns the
// given status code for every method. It uses UnknownServiceHandler so the
// client can call any service/method name.
func startGRPCStatusServer(t *testing.T, code codes.Code) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := grpc.NewServer(grpc.UnknownServiceHandler(func(_ interface{}, stream grpc.ServerStream) error {
		// Consume the unary request message before returning the fixture status.
		_ = stream.RecvMsg(&emptypb.Empty{})
		return status.Errorf(code, "fixture %s", code.String())
	}))
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)
	return lis.Addr().String()
}

// grpcParityBuild returns a parityCase.Build tuple for the xflow.grpc built-in
// handler. When needsPool is true, the returned register callback installs a
// grpcPoolWrapper around a fresh counting handler; otherwise it registers the
// counting handler directly. This keeps counting innermost
// (pool(counting(production))) so the counter is never bypassed by the pool
// wrapper. A fresh counting handler is created on each register call (one per
// topology) and the invocations func reads the most-recently-registered counter.
func grpcParityBuild(t *testing.T, name string, nodeDef types.NodeDef, needsPool bool) (types.NodeDef, func(engine.HandlerRegistrar), func() int) {
	inner, ok := registry.Lookup("xflow.grpc")
	if !ok {
		t.Fatal("xflow.grpc handler not found in node registry")
	}
	var pool types.ResourcePool
	if needsPool {
		pool = resource.NewDefaultResourcePool(types.DefaultResourcePoolConfig())
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = pool.Close(ctx)
		})
	}
	var cur *countingHandler
	register := func(reg engine.HandlerRegistrar) {
		wrapped, c := buildInstrumentedHandler(inner, "grpc-"+name)
		cur = c
		if pool != nil {
			reg.RegisterGlobal("xflow.grpc", &grpcPoolWrapper{delegate: wrapped, pool: pool})
		} else {
			reg.RegisterGlobal("xflow.grpc", wrapped)
		}
	}
	invocations := func() int {
		if cur == nil {
			return 0
		}
		return cur.Count()
	}
	return nodeDef, register, invocations
}

// grpcPoolWrapper injects a ResourcePool into the handler context before
// delegating to the built-in gRPC node.
type grpcPoolWrapper struct {
	delegate types.ActionHandler
	pool     types.ResourcePool
}

func (w *grpcPoolWrapper) Descriptor() types.Descriptor { return w.delegate.Descriptor() }

func (w *grpcPoolWrapper) Execute(ctx context.Context, input *types.Input) (*types.Output, error) {
	return w.delegate.Execute(types.WithResourcePool(ctx, w.pool), input)
}

// newServerRunnerHarnessFast is like newServerRunnerHarness but configures the
// control plane with a short poll wait so transient retry fixtures resolve in
// milliseconds instead of seconds.
func newServerRunnerHarnessFast(t *testing.T, addr string, concurrency int) *serverRunnerHarness {
	t.Helper()
	b, err := distributed.New(addr, nil, distributed.WithConcurrency(concurrency), distributed.WithConsumer(true))
	if err != nil {
		t.Fatalf("distributed.New: %v", err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	flushAsynqKeys(context.Background(), t, rdb)
	_ = rdb.Close()

	buf := engine.NewRuntimeEvidenceBuffer(64)
	cp, err := control.NewControlPlane(control.Config{Backend: b, PollWait: 50 * time.Millisecond, RuntimeEvidenceBuffer: buf})
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
	return &serverRunnerHarness{srv: srv, httpSrv: httpSrv, state: b.State(), runners: cp.RunnerDirectory(), cancel: cancel, evidence: buf}
}

// runServerRunnerParity is a variant of RunParityServerRunner that uses a
// unique runner ID per subtest and a low control-plane poll wait so stale
// runner-directory sessions do not collide and retries are processed quickly.
func runServerRunnerParity(t *testing.T, addr string, def *types.WorkflowDef, register func(engine.HandlerRegistrar), rec *evidenceRecorder, fixture, topology string) ParityOutcome {
	t.Helper()
	h := newServerRunnerHarnessFast(t, addr, 1)
	if len(def.Nodes) == 0 {
		t.Fatal("runServerRunnerParity: workflow has no nodes")
	}

	reg := execution.NewRegistry()
	if register != nil {
		register(reg)
	}

	capSet := make(map[string]struct{})
	for _, n := range def.Nodes {
		capSet[n.Type] = struct{}{}
	}
	caps := make([]protocol.Capability, 0, len(capSet))
	for nodeType := range capSet {
		caps = append(caps, protocol.Capability{NodeType: nodeType})
	}

	runnerID := "grpc-parity-" + strings.ReplaceAll(t.Name(), "/", "-")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner := runnersvc.New(
		protocol.NewClient(h.httpSrv.URL, h.httpSrv.Client()),
		reg,
		runnersvc.Config{
			RunnerID:     runnerID,
			Concurrency:  1,
			Capabilities: caps,
			PollWait:     5 * time.Millisecond,
		},
	)
	errCh := make(chan error, 1)
	go func() { errCh <- runner.Run(ctx) }()
	waitForE2ERunner(t, h.runners, runnerID)

	execID := submitWorkflowHTTP(t, h.httpSrv.URL, h.httpSrv.Client(), def, nil)

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer waitCancel()
	result := waitForCompletion(waitCtx, t, h.state, execID, def.Nodes[0].Name)

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("runner error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("runner did not stop in time")
	}

	out := collectParityOutcome(t, h.state, execID, result, def, h.evidence, rec, fixture, topology)
	// Stop this topology's control plane (apiserver + its Asynq consumer) before
	// returning so it does not race a subsequent topology's consumer for the
	// shared Asynq queue. See serverRunnerHarness.stop.
	h.stop()
	return out
}

// TestGRPCActionErrorParityProductionWiring exercises the production
// ResourcePool wiring added in task-42 against the gRPC node. Unlike the
// wrapper-based fixtures (which register grpcPoolWrapper to inject a pool),
// this registers the real xflow.grpc handler directly and installs the pool
// via runnersvc.Config.ResourcePool — the production path. The gRPC node does
// not consume credentials (it dials host directly), so only the pool is set.
func TestGRPCActionErrorParityProductionWiring(t *testing.T) {
	addr := requireRedis(t)

	delegate, ok := registry.Lookup("xflow.grpc")
	if !ok {
		t.Fatal("xflow.grpc handler not found in node registry")
	}

	cases := []struct {
		name        string
		host        string
		wantAttempt int
		wantStatus  types.ExecutionStatus
		errContains string
	}{
		{
			name:        "grpc_production_wiring_not_found_permanent",
			host:        startGRPCStatusServer(t, codes.NotFound),
			wantAttempt: 1,
			wantStatus:  types.ExecutionStatusFailed,
			errContains: "grpc.NotFound",
		},
		{
			name:        "grpc_production_wiring_unavailable_transient_exhausted",
			host:        startGRPCStatusServer(t, codes.Unavailable),
			wantAttempt: 2,
			wantStatus:  types.ExecutionStatusFailed,
			errContains: "grpc.Unavailable",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			source := grpcNodeDef(tc.host)
			retry := &types.RetrySettings{
				MaxAttempts:     2,
				InitialInterval: 50,
			}
			def := ParityWorkflow(source, retry)

			// Production wiring: register the real handler directly, install
			// the pool via runnersvc.Config (no wrapper, no credential resolver).
			register := func(reg engine.HandlerRegistrar) {
				reg.RegisterGlobal("xflow.grpc", delegate)
			}
			pool := resource.NewDefaultResourcePool(types.DefaultResourcePoolConfig())
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = pool.Close(ctx)
			})

			out := runServerRunnerParityWithPool(t, addr, def, register, pool, nil, tc.name, "server-runner")
			if out.Attempt != tc.wantAttempt {
				t.Errorf("server-runner attempt=%d, want %d", out.Attempt, tc.wantAttempt)
			}
			if out.Status != tc.wantStatus {
				t.Errorf("server-runner status=%s, want %s", out.Status, tc.wantStatus)
			}
			if !strings.Contains(out.ErrStr, tc.errContains) {
				t.Errorf("server-runner error=%q, want substring %q", out.ErrStr, tc.errContains)
			}
		})
	}
}

// runServerRunnerParityWithPool mirrors runServerRunnerParity but installs a
// ResourcePool on the runnersvc.Config, exercising the production wiring path.
func runServerRunnerParityWithPool(t *testing.T, addr string, def *types.WorkflowDef, register func(engine.HandlerRegistrar), pool types.ResourcePool, rec *evidenceRecorder, fixture, topology string) ParityOutcome {
	t.Helper()
	h := newServerRunnerHarnessFast(t, addr, 1)
	if len(def.Nodes) == 0 {
		t.Fatal("runServerRunnerParityWithPool: workflow has no nodes")
	}

	reg := execution.NewRegistry()
	if register != nil {
		register(reg)
	}

	capSet := make(map[string]struct{})
	for _, n := range def.Nodes {
		capSet[n.Type] = struct{}{}
	}
	caps := make([]protocol.Capability, 0, len(capSet))
	for nodeType := range capSet {
		caps = append(caps, protocol.Capability{NodeType: nodeType})
	}

	runnerID := "grpc-parity-prod-" + strings.ReplaceAll(t.Name(), "/", "-")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner := runnersvc.New(
		protocol.NewClient(h.httpSrv.URL, h.httpSrv.Client()),
		reg,
		runnersvc.Config{
			RunnerID:     runnerID,
			Concurrency:  1,
			Capabilities: caps,
			PollWait:     5 * time.Millisecond,
			ResourcePool: pool,
		},
	)
	errCh := make(chan error, 1)
	go func() { errCh <- runner.Run(ctx) }()
	waitForE2ERunner(t, h.runners, runnerID)

	execID := submitWorkflowHTTP(t, h.httpSrv.URL, h.httpSrv.Client(), def, nil)

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer waitCancel()
	result := waitForCompletion(waitCtx, t, h.state, execID, def.Nodes[0].Name)

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("runner error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("runner did not stop in time")
	}

	out := collectParityOutcome(t, h.state, execID, result, def, h.evidence, rec, fixture, topology)
	// Stop this topology's control plane (apiserver + its Asynq consumer) before
	// returning so it does not race a subsequent topology's consumer for the
	// shared Asynq queue. See serverRunnerHarness.stop.
	h.stop()
	return out
}
