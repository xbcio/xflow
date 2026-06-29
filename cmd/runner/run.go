package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	_ "github.com/xbcio/xflow/nodes/node"
	"github.com/xbcio/xflow/service/protocol"
	runnersvc "github.com/xbcio/xflow/service/runner"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

type runnerConfig struct {
	configPath        string
	serverURL         string
	runnerID          string
	concurrency       int
	changed           map[string]bool
	resolutionIssues  map[string]error
	capRaw            string
	capabilities      []protocol.Capability
	heartbeatInterval string
	pollWait          string
}

func newRunCommand(opts commandOptions, cfg *runnerConfig) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run the xflow task runner",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			recordChangedFlags(cmd, cfg)
			resolved, err := resolveRunnerConfig(*cfg)
			if err != nil {
				return err
			}
			return opts.runFunc(resolved)
		},
	}
	bindRunnerFlags(cmd, cfg)
	return cmd
}

func bindRunnerFlags(cmd *cobra.Command, cfg *runnerConfig) {
	cmd.Flags().StringVar(&cfg.serverURL, "server", cfg.serverURL, "xflow-server base URL")
	cmd.Flags().StringVar(&cfg.runnerID, "id", cfg.runnerID, "Runner ID")
	cmd.Flags().IntVar(&cfg.concurrency, "concurrency", cfg.concurrency, "Runner concurrency")
	cmd.Flags().StringVar(&cfg.capRaw, "cap", cfg.capRaw, "Comma-separated node type capabilities")
	cmd.Flags().StringVar(&cfg.heartbeatInterval, "heartbeat-interval", cfg.heartbeatInterval, "Heartbeat interval")
	cmd.Flags().StringVar(&cfg.pollWait, "poll-wait", cfg.pollWait, "Poll wait duration when no task is available")
}

func recordChangedFlags(cmd *cobra.Command, cfg *runnerConfig) {
	if cfg.changed == nil {
		cfg.changed = map[string]bool{}
	}
	cmd.Flags().Visit(func(f *pflag.Flag) {
		cfg.changed[f.Name] = true
	})
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

type runnerService interface {
	Run(context.Context) error
}

var newRunnerService = func(client runnersvc.ProtocolClient, registry engine.HandlerRegistry, cfg runnersvc.Config) runnerService {
	return runnersvc.New(client, registry, cfg)
}

func runRunner(ctx context.Context, cfg runnerConfig) error {
	serviceCfg, err := runnerServiceConfig(cfg)
	if err != nil {
		return err
	}
	client := protocol.NewClient(cfg.serverURL, http.DefaultClient)
	registry := execution.NewRegistry()
	runner := newRunnerService(client, registry, serviceCfg)
	return runner.Run(ctx)
}

func runnerServiceConfig(cfg runnerConfig) (runnersvc.Config, error) {
	heartbeatInterval, err := parsePositiveDuration("heartbeat interval", cfg.heartbeatInterval)
	if err != nil {
		return runnersvc.Config{}, err
	}
	pollWait, err := parsePositiveDuration("poll wait", cfg.pollWait)
	if err != nil {
		return runnersvc.Config{}, err
	}
	return runnersvc.Config{
		RunnerID:          cfg.runnerID,
		Concurrency:       cfg.concurrency,
		Capabilities:      cfg.capabilities,
		HeartbeatInterval: heartbeatInterval,
		PollWait:          pollWait,
	}, nil
}

func runWithSignals(cfg runnerConfig) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if cfg.runnerID == "" {
		cfg.runnerID = fmt.Sprintf("runner-%d", os.Getpid())
	}
	return runRunner(ctx, cfg)
}
