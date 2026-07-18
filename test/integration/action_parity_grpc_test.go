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
			Build: func() (types.NodeDef, func(engine.HandlerRegistrar)) {
				return grpcNodeDef("localhost:1"), nil
			},
			MaxAttempts: 2,
			WantAttempt: 1,
			WantStatus:  types.ExecutionStatusFailed,
			ErrContains: "grpc.no_pool",
		},
		{
			Name: "grpc_connection_transient_exhausted",
			Build: func() (types.NodeDef, func(engine.HandlerRegistrar)) {
				return grpcNodeDef("localhost:1"), nil
			},
			MaxAttempts: 2,
			WantAttempt: 2,
			WantStatus:  types.ExecutionStatusFailed,
			ErrContains: "grpc.",
		},
		{
			Name: "grpc_not_found_permanent",
			Build: func() (types.NodeDef, func(engine.HandlerRegistrar)) {
				host := startGRPCStatusServer(t, codes.NotFound)
				return grpcNodeDef(host), nil
			},
			MaxAttempts: 2,
			WantAttempt: 1,
			WantStatus:  types.ExecutionStatusFailed,
			ErrContains: "grpc.NotFound",
		},
		{
			Name: "grpc_unavailable_transient_exhausted",
			Build: func() (types.NodeDef, func(engine.HandlerRegistrar)) {
				host := startGRPCStatusServer(t, codes.Unavailable)
				return grpcNodeDef(host), nil
			},
			MaxAttempts: 2,
			WantAttempt: 2,
			WantStatus:  types.ExecutionStatusFailed,
			ErrContains: "grpc.Unavailable",
		},
	}

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			source, register := tc.Build()
			retry := &types.RetrySettings{
				MaxAttempts:     tc.MaxAttempts,
				InitialInterval: 50,
			}
			def := ParityWorkflow(source, retry)

			var localOut, serverOut ParityOutcome
			if needsPool(tc.Name) {
				pool := resource.NewDefaultResourcePool(types.DefaultResourcePoolConfig())
				t.Cleanup(func() {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					_ = pool.Close(ctx)
				})
				localOut = RunParityLocal(t, def, withResourcePool(register, pool))
				serverOut = runServerRunnerParity(t, addr, def, withResourcePool(register, pool))
			} else {
				localOut = RunParityLocal(t, def, register)
				serverOut = runServerRunnerParity(t, addr, def, register)
			}

			assertParity(t, tc, localOut, serverOut)
		})
	}
}

// needsPool reports whether the named fixture requires a ResourcePool to reach
// its expected classification.
func needsPool(name string) bool {
	switch name {
	case "grpc_connection_transient_exhausted",
		"grpc_not_found_permanent",
		"grpc_unavailable_transient_exhausted":
		return true
	}
	return false
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

// withResourcePool returns a register function that installs a pool-injecting
// wrapper around the built-in xflow.grpc handler. This lets both topologies
// exercise connection and status-code fixtures without changing the production
// runner constructor.
func withResourcePool(register func(engine.HandlerRegistrar), pool types.ResourcePool) func(engine.HandlerRegistrar) {
	return func(reg engine.HandlerRegistrar) {
		if register != nil {
			register(reg)
		}
		if pool == nil {
			return
		}
		delegate, ok := registry.Lookup("xflow.grpc")
		if !ok {
			panic("xflow.grpc handler not found in node registry")
		}
		reg.RegisterGlobal("xflow.grpc", &grpcPoolWrapper{delegate: delegate, pool: pool})
	}
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

	cp, err := control.NewControlPlane(control.Config{Backend: b, PollWait: 50 * time.Millisecond})
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

// runServerRunnerParity is a variant of RunParityServerRunner that uses a
// unique runner ID per subtest and a low control-plane poll wait so stale
// runner-directory sessions do not collide and retries are processed quickly.
func runServerRunnerParity(t *testing.T, addr string, def *types.WorkflowDef, register func(engine.HandlerRegistrar)) ParityOutcome {
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

	return collectParityOutcome(t, h.state, execID, result, def)
}
