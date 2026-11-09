package xflow

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/trigger"
	"github.com/xbcio/xflow/types"
)

// TestInProcessRunnerHostsACronTriggerEndToEnd is the shape the feature exists
// for: one process runs the control plane AND an embedded runner that hosts a
// cron trigger, with no ServerURL and no second credential. The trigger's seed
// admission and the downstream supply of work both travel in process.
//
// The deadline is generous because the control plane's entry-activation
// reconciler ticks every 10s, and an activation is only delivered on a runner
// heartbeat after that; a healthy run reaches the action node within a couple of
// seconds once assigned.
func TestInProcessRunnerHostsACronTriggerEndToEnd(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv, err := NewServer(ServerConfig{}, WithServerInsecureNoRunnerAuth())
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
	})

	fired := make(chan struct{}, 16)
	tick := node.Define("test.inproc-trigger.tick", func(context.Context, *types.Input) (*types.Output, error) {
		select {
		case fired <- struct{}{}:
		default:
		}
		return &types.Output{Data: map[string]any{"ok": true}}, nil
	})
	registry := execution.NewRegistry()
	registry.RegisterGlobal("test.inproc-trigger.tick", tick)

	runnerCtx, runnerCancel := context.WithCancel(context.Background())
	runner, err := NewRunner(RunnerConfig{
		Transport:    RunnerTransportInProc,
		RunnerID:     "inproc-trigger-runner",
		Capabilities: []string{"xflow.trigger.cron", "test.inproc-trigger.tick"},
		Labels:       map[string]string{"placement": "inproc"},
		Concurrency:  2,
		PollWait:     10 * time.Millisecond,
		// Faster than the 5s default so the activation directive arrives on the
		// first heartbeat after the reconciler assigns it.
		HeartbeatInterval: 500 * time.Millisecond,
	}, WithRunnerControlPlane(srv.ControlServer()), WithRunnerNodeRegistry(registry))
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	runnerErr := make(chan error, 1)
	go func() { runnerErr <- runner.Run(runnerCtx) }()
	t.Cleanup(func() {
		runnerCancel()
		select {
		case <-runnerErr:
		case <-time.After(2 * time.Second):
			t.Error("runner did not stop after its context was cancelled")
		}
	})

	selector := RequiredRunnerSelector(map[string]string{"placement": "inproc"})
	wf := Workflow("inproc-trigger-e2e").RunnerSelector(selector)
	start := wf.Node("tick", trigger.Cron().Cron("@every 1s"))
	run := wf.Node("run", tick.New(nil))
	wf.Connect(start, run)
	if _, err := srv.AddWorkflow(ctx, wf); err != nil {
		t.Fatalf("AddWorkflow: %v", err)
	}

	select {
	case <-fired:
	case err := <-runnerErr:
		t.Fatalf("runner exited before the trigger fired: %v", err)
	case <-time.After(40 * time.Second):
		t.Fatal("the cron trigger never seeded an execution that reached the action node within 40s: " +
			"check the in-process seed admission, the activation directive delivery, and runner placement")
	}
}
