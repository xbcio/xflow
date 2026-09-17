package xflow

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/service/protocol"
	runnersvc "github.com/xbcio/xflow/service/runner"
	"github.com/xbcio/xflow/types"
)

const serverEngineTestNodeType = "test.server-engine"

var serverEngineTestNode = node.Define(serverEngineTestNodeType, func(_ context.Context, input *types.Input) (*types.Output, error) {
	return &types.Output{Data: map[string]any{"value": input.Data["value"]}}, nil
})

func TestServerEngineReturnsSharedNonOwningFacade(t *testing.T) {
	srv, err := NewServer(ServerConfig{}, WithServerInsecureNoRunnerAuth())
	if err != nil {
		t.Fatal(err)
	}

	facade := srv.Engine()
	if facade == nil {
		t.Fatal("Engine() = nil")
	}
	if facade != srv.Engine() {
		t.Fatal("Engine() did not return the cached facade")
	}
	if got, want := facade.eng, srv.api.Engine(); got != want {
		t.Fatal("Engine() did not retain the Server's core")
	}
	if got, want := facade.registry, srv.api.Backend().Registry(); got != want {
		t.Fatal("Engine() did not retain the Server's handler registry")
	}
	if got, want := facade.workflowRegistry, srv.api.Backend().WorkflowRegistry(); got != want {
		t.Fatal("Engine() did not retain the Server's workflow registry")
	}
	if got, want := facade.eng.State(), srv.api.Backend().State(); got != want {
		t.Fatal("Engine() did not retain the Server's state store")
	}
	waiter, ok := srv.api.Backend().(backend.Waiter)
	if !ok {
		t.Fatal("Server backend does not implement backend.Waiter")
	}
	if facade.waiter != waiter {
		t.Fatal("Engine() did not retain the Server's completion runtime")
	}
	if !facade.nonOwning {
		t.Fatal("Engine() facade unexpectedly owns Server lifecycle")
	}
}

func TestServerEngineInvokeWaitAndStopDoesNotShutdownServer(t *testing.T) {
	srv, err := NewServer(ServerConfig{}, WithServerInsecureNoRunnerAuth())
	if err != nil {
		t.Fatal(err)
	}

	serverCtx, serverCancel := context.WithCancel(context.Background())
	t.Cleanup(serverCancel)
	if err := srv.Start(serverCtx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			t.Errorf("Shutdown() error = %v", err)
		}
	})

	httpSrv := httptest.NewServer(srv.Handler())
	t.Cleanup(httpSrv.Close)

	registry := execution.NewRegistry()
	registry.RegisterGlobal(serverEngineTestNodeType, serverEngineTestNode)
	runnerCtx, runnerCancel := context.WithCancel(context.Background())
	runner := runnersvc.New(protocol.NewClient(httpSrv.URL, httpSrv.Client()), registry, runnersvc.Config{
		RunnerID:    "server-engine-runner",
		Concurrency: 1,
		Capabilities: []protocol.Capability{
			{NodeType: "xflow.start"},
			{NodeType: serverEngineTestNodeType},
		},
		PollWait: 5 * time.Millisecond,
	})
	runnerErr := make(chan error, 1)
	go func() { runnerErr <- runner.Run(runnerCtx) }()
	t.Cleanup(func() {
		runnerCancel()
		select {
		case err := <-runnerErr:
			if err != nil {
				t.Errorf("runner error = %v", err)
			}
		case <-time.After(time.Second):
			t.Error("runner did not stop")
		}
	})

	wf := Workflow("server-engine")
	start := wf.Node("start", node.Start())
	work := wf.Node("work", serverEngineTestNode.New(nil))
	wf.Connect(start, work)
	workflowID, err := srv.AddWorkflow(context.Background(), wf)
	if err != nil {
		t.Fatalf("AddWorkflow() error = %v", err)
	}

	facade := srv.Engine()
	assertServerEngineInvocation(t, facade, workflowID, "before-stop")

	facade.Stop()
	assertServerEngineInvocation(t, facade, workflowID, "after-stop")
}

func assertServerEngineInvocation(t *testing.T, facade *Engine, workflowID types.WorkflowID, value string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	executionID, err := facade.Invoke(ctx, workflowID, Start(), map[string]any{"value": value})
	if err != nil {
		t.Fatalf("Invoke() error = %v", err)
	}
	result, err := facade.Wait(ctx, executionID)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if result.Status != types.ExecutionStatusSuccess {
		t.Fatalf("Wait() status = %q, want %q (error %q)", result.Status, types.ExecutionStatusSuccess, result.Error)
	}
	output, ok := result.Output["work"].(map[string]any)
	if !ok {
		t.Fatalf("Wait() output[work] = %#v, want node output", result.Output["work"])
	}
	if got := output["value"]; got != value {
		t.Fatalf("Wait() output[work][value] = %#v, want %#v", got, value)
	}
}
