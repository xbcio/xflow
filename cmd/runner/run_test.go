package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	runnersvc "github.com/xbcio/xflow/service/runner"
)

func TestRunCommandPropagatesResolvedDurationsToRunnerService(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runner.yaml")
	data := []byte(`
server:
  url: http://file-server:8080
heartbeat:
  interval: 7s
poll:
  wait: 2s
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	t.Setenv("XFLOW_RUNNER_HEARTBEAT_INTERVAL", "9s")
	t.Setenv("XFLOW_RUNNER_POLL_WAIT", "3s")

	restore := stubRunnerServiceFactory(func(cfg runnersvc.Config) error {
		if cfg.PollWait != 4*time.Second {
			t.Fatalf("PollWait = %s, want 4s", cfg.PollWait)
		}
		return nil
	})
	defer restore()

	err := executeRootWithOptions(commandOptions{
		runFunc: func(cfg runnerConfig) error {
			return runRunner(context.Background(), cfg)
		},
		out: &bytes.Buffer{},
		err: &bytes.Buffer{},
	}, "run", "--config", path, "--heartbeat-interval", "11s", "--poll-wait", "4s")
	if err != nil {
		t.Fatal(err)
	}
}

type runnerServiceFunc func(context.Context) error

func (f runnerServiceFunc) Run(ctx context.Context) error {
	return f(ctx)
}

func stubRunnerServiceFactory(check func(runnersvc.Config) error) func() {
	previous := newRunnerService
	newRunnerService = func(_ runnersvc.ProtocolClient, _ engine.HandlerRegistry, cfg runnersvc.Config) runnerService {
		return runnerServiceFunc(func(context.Context) error {
			return check(cfg)
		})
	}
	return func() {
		newRunnerService = previous
	}
}
