// cmd/runner is the xflow task runner (Execution node).
//
// Responsibilities:
//   - Connect to xflow-server or xflow-gateway via Runner Protocol
//   - Receive runner-assigned node tasks from the server-side dispatcher
//   - Resolve handler via HandlerRegistry (global type→handler map)
//   - Execute node handlers (ActionHandler / SuspendingHandler)
//   - Report results back through Runner Protocol
//
// It does NOT accept external API requests and does NOT connect to Redis,
// Asynq, or StateStore directly — those are server-side responsibilities.
// Scale horizontally by running multiple runner instances.
//
// Node handlers must be registered before the runner starts.
// Import handler packages in an init() block or directly in this file.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/xbcio/xflow/execution"
	_ "github.com/xbcio/xflow/nodes/node"
	"github.com/xbcio/xflow/service/protocol"
	runnersvc "github.com/xbcio/xflow/service/runner"
)

type runnerConfig struct {
	serverURL    string
	runnerID     string
	concurrency  int
	capabilities []protocol.Capability
}

func main() {
	cfg, err := parseRunnerConfig(nil)
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := runRunner(ctx, cfg); err != nil {
		log.Fatal(err)
	}
}

func parseRunnerConfig(args []string) (runnerConfig, error) {
	fs := flag.NewFlagSet("xflow-runner", flag.ContinueOnError)
	cfg := runnerConfig{
		serverURL:   "http://localhost:8080",
		runnerID:    fmt.Sprintf("runner-%d", os.Getpid()),
		concurrency: 1,
	}
	capabilities := "xflow.function"
	fs.StringVar(&cfg.serverURL, "server", cfg.serverURL, "xflow-server base URL")
	fs.StringVar(&cfg.runnerID, "id", cfg.runnerID, "Runner ID")
	fs.IntVar(&cfg.concurrency, "concurrency", cfg.concurrency, "Runner concurrency")
	fs.StringVar(&capabilities, "cap", capabilities, "Comma-separated node type capabilities")
	if args == nil {
		args = os.Args[1:]
	}
	if err := fs.Parse(args); err != nil {
		return runnerConfig{}, err
	}
	cfg.capabilities = parseCapabilities(capabilities)
	return cfg, nil
}

func parseCapabilities(raw string) []protocol.Capability {
	parts := strings.Split(raw, ",")
	capabilities := make([]protocol.Capability, 0, len(parts))
	for _, part := range parts {
		nodeType := strings.TrimSpace(part)
		if nodeType == "" {
			continue
		}
		capabilities = append(capabilities, protocol.Capability{NodeType: nodeType})
	}
	return capabilities
}

func runRunner(ctx context.Context, cfg runnerConfig) error {
	client := protocol.NewClient(cfg.serverURL, http.DefaultClient)
	registry := execution.NewRegistry()
	runner := runnersvc.New(client, registry, runnersvc.Config{
		RunnerID:     cfg.runnerID,
		Concurrency:  cfg.concurrency,
		Capabilities: cfg.capabilities,
	})
	return runner.Run(ctx)
}
